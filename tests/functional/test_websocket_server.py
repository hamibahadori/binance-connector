"""Functional tests: the live-feed WebSocket server, with a real client on a free local port."""

import asyncio
import json
import threading
import time

import pytest
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.sync.client import connect

from binance_connector.services.websocket_server import OVERFLOW_CLOSE_CODE, LiveFeedServer, WebSocketClient

TOKEN = "live-feed-token-123"
RECEIVE_TIMEOUT_SECONDS = 5


class FakeHub:
    """Stands in for LiveFeedHub: records subscribers, answers snapshots."""

    def __init__(self) -> None:
        self.subscribers: list = []
        self.snapshot_calls: list[str | None] = []
        self.report_sequence = 10

    def subscribe(self, subscriber) -> None:
        self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber) -> None:
        self.subscribers.remove(subscriber)

    def snapshot(self, account_id: str | None = None) -> dict:
        self.snapshot_calls.append(account_id)
        return {"type": "snapshot", "sent_at": "now",
                "data": {"report_sequence": self.report_sequence, "positions": [], "account_id": account_id}}

    def broadcast(self, message: dict) -> None:
        for subscriber in list(self.subscribers):
            subscriber.deliver(message)


def wait_for(condition, timeout_seconds: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


def start_server(hub, token=None):
    stop = threading.Event()
    server = LiveFeedServer(hub=hub, host="127.0.0.1", port=0, token=token)
    thread = threading.Thread(target=server.run, args=(stop.is_set,), daemon=True)
    thread.start()
    server.wait_until_ready(timeout_seconds=5)
    return server, stop, thread


@pytest.fixture
def hub() -> FakeHub:
    return FakeHub()


@pytest.fixture
def server(hub):
    server, stop, thread = start_server(hub)
    yield server
    stop.set()
    thread.join(timeout=5)


@pytest.fixture
def protected_server(hub):
    server, stop, thread = start_server(hub, token=TOKEN)
    yield server
    stop.set()
    thread.join(timeout=5)


def url(server, query: str = "") -> str:
    return f"ws://127.0.0.1:{server.port}/ws{query}"


def receive(client) -> dict:
    return json.loads(client.recv(timeout=RECEIVE_TIMEOUT_SECONDS))


# --- connecting ---------------------------------------------------------------------------------


def test_a_client_gets_a_snapshot_as_soon_as_it_connects(server, hub):
    with connect(url(server)) as client:
        message = receive(client)

    assert message["type"] == "snapshot"
    assert hub.snapshot_calls == [None]


def test_a_client_can_follow_one_account(server, hub):
    with connect(url(server, "?account_id=account-2")) as client:
        message = receive(client)
        assert hub.subscribers[0].account_id == "account-2"

    assert message["data"]["account_id"] == "account-2"


def test_an_unknown_path_is_refused_with_404(server):
    with pytest.raises(InvalidStatus) as refusal:
        connect(f"ws://127.0.0.1:{server.port}/other")

    assert refusal.value.response.status_code == 404


def test_a_disconnected_client_is_unsubscribed(server, hub):
    with connect(url(server)) as client:
        receive(client)
        assert len(hub.subscribers) == 1

    wait_for(lambda: hub.subscribers == [])


# --- messages -----------------------------------------------------------------------------------


def test_broadcast_messages_reach_the_client_as_json(server, hub):
    with connect(url(server)) as client:
        receive(client)
        hub.broadcast({"type": "position_tick", "data": {"position_id": "p1"}})

        assert receive(client) == {"type": "position_tick", "data": {"position_id": "p1"}}


def test_updates_the_snapshot_already_reflects_are_skipped(server, hub):
    with connect(url(server)) as client:
        receive(client)  # report_sequence 10
        hub.broadcast({"type": "position_update", "report_sequence": 9, "data": {}})
        hub.broadcast({"type": "position_update", "report_sequence": 10, "data": {}})
        hub.broadcast({"type": "position_update", "report_sequence": 11, "data": {}})

        assert receive(client)["report_sequence"] == 11


def test_a_client_can_ask_for_a_fresh_snapshot(server, hub):
    with connect(url(server)) as client:
        receive(client)
        client.send(json.dumps({"type": "resync"}))

        assert receive(client)["type"] == "snapshot"
    assert len(hub.snapshot_calls) == 2


def test_a_message_that_is_not_json_is_ignored(server, hub):
    with connect(url(server)) as client:
        receive(client)
        client.send("hello?")
        hub.broadcast({"type": "service_status", "data": {}})

        assert receive(client)["type"] == "service_status"


def test_stopping_the_server_closes_its_clients(hub):
    server, stop, thread = start_server(hub)
    with connect(url(server)) as client:
        receive(client)
        stop.set()
        thread.join(timeout=5)

        with pytest.raises(ConnectionClosed):
            client.recv(timeout=RECEIVE_TIMEOUT_SECONDS)
    assert not thread.is_alive()


# --- token --------------------------------------------------------------------------------------


@pytest.mark.parametrize("query, headers", [("", {}), ("?token=wrong-token", {}),
                                            ("", {"Authorization": "Bearer wrong-token"})])
def test_with_a_token_configured_a_client_without_the_right_one_is_refused(protected_server, query, headers):
    with pytest.raises(InvalidStatus) as refusal:
        connect(url(protected_server, query), additional_headers=headers)

    assert refusal.value.response.status_code == 401


@pytest.mark.parametrize("query, headers", [(f"?token={TOKEN}", {}), ("", {"Authorization": f"Bearer {TOKEN}"})])
def test_the_right_token_in_the_query_or_the_header_gets_through(protected_server, query, headers):
    with connect(url(protected_server, query), additional_headers=headers) as client:
        assert receive(client)["type"] == "snapshot"


# --- slow clients -------------------------------------------------------------------------------


class FakeConnection:
    def __init__(self) -> None:
        self.closed_with: tuple[int, str] | None = None

    async def close(self, code: int, reason: str) -> None:
        self.closed_with = (code, reason)


def test_a_client_that_falls_too_far_behind_is_closed_to_resync():
    async def scenario() -> FakeConnection:
        connection = FakeConnection()
        client = WebSocketClient(connection, account_id=None, loop=asyncio.get_running_loop(), queue_size=2)
        for number in range(3):
            client.offer({"type": "position_tick", "number": number})
        await asyncio.sleep(0)  # let the close task run
        client.offer({"type": "position_tick", "number": 4})  # ignored once closing
        assert client.queue.qsize() == 2
        return connection

    connection = asyncio.run(scenario())

    assert connection.closed_with[0] == OVERFLOW_CLOSE_CODE
