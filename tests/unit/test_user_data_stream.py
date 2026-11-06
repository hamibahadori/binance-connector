"""Unit tests for the user-data stream connection loop and the listenKey REST calls."""

import json

import pytest
import responses

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.binance.user_stream import UserDataStream
from binance_connector.config import DEMO_BASE_URL
from binance_connector.errors import BinanceUnavailableError
from tests.conftest import TEST_API_KEY, TEST_CREDENTIALS
from tests.recorded import stream_events

LISTEN_KEY_URL = f"{DEMO_BASE_URL}/fapi/v1/listenKey"
STREAM_URL = "wss://demo-fstream.binance.com/ws"


# --- listenKey REST calls -----------------------------------------------------------------------


@responses.activate
def test_start_user_stream_sends_only_the_api_key_and_returns_the_listen_key(settings):
    responses.post(LISTEN_KEY_URL, json={"listenKey": "abc123"})

    listen_key = BinanceFuturesClient(settings, TEST_CREDENTIALS).start_user_stream()

    sent_request = responses.calls[0].request
    assert listen_key == "abc123"
    assert sent_request.headers["X-MBX-APIKEY"] == TEST_API_KEY
    assert "signature" not in sent_request.url
    assert "timestamp" not in sent_request.url


@responses.activate
def test_keepalive_and_close_use_put_and_delete(settings):
    responses.put(LISTEN_KEY_URL, json={"listenKey": "abc123"})
    responses.delete(LISTEN_KEY_URL, json={})
    client = BinanceFuturesClient(settings, TEST_CREDENTIALS)

    client.keepalive_user_stream()
    client.close_user_stream()

    assert [mocked_call.request.method for mocked_call in responses.calls] == ["PUT", "DELETE"]


def test_the_websocket_connects_with_the_long_timeout_then_reads_with_the_short_one(monkeypatch):
    """Found on Demo: a 1 s timeout used for connecting failed on a slow link."""
    from binance_connector.binance import user_stream

    class FakeSocket:
        def __init__(self) -> None:
            self.read_timeouts = []

        def settimeout(self, seconds):
            self.read_timeouts.append(seconds)

    created = {}

    def fake_create_connection(url, timeout):
        created["timeout"] = timeout
        created["socket"] = FakeSocket()
        return created["socket"]

    monkeypatch.setattr(user_stream.websocket, "create_connection", fake_create_connection)

    user_stream.WebSocketConnection("wss://example.test/ws/key", connect_timeout_seconds=30, receive_timeout_seconds=1)

    assert created["timeout"] == 30
    assert created["socket"].read_timeouts == [1]


# --- connection loop ----------------------------------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class ScriptedConnection:
    """Plays back a script: a str is a message, None is a receive timeout, an Exception is raised."""

    def __init__(self, script, clock: FakeClock, seconds_per_receive: float) -> None:
        self.script = list(script)
        self.clock = clock
        self.seconds_per_receive = seconds_per_receive
        self.closed = False

    def receive(self):
        self.clock.now += self.seconds_per_receive
        if not self.script:
            return None
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def close(self):
        self.closed = True


class FakeListenKeyClient:
    def __init__(self) -> None:
        self.started = 0
        self.keepalives = 0
        self.start_failures: list[Exception] = []

    def start_user_stream(self) -> str:
        if self.start_failures:
            raise self.start_failures.pop(0)
        self.started += 1
        return f"key-{self.started}"

    def keepalive_user_stream(self) -> None:
        self.keepalives += 1


class StreamHarness:
    """Runs a UserDataStream against scripted connections until the scripts are used up."""

    def __init__(self, scripts, seconds_per_receive=1.0, **stream_options):
        self.clock = FakeClock()
        self.client = FakeListenKeyClient()
        self.scripts = list(scripts)
        self.connections: list[ScriptedConnection] = []
        self.connected_urls: list[str] = []
        self.events: list[dict] = []
        self.connected_count = 0
        self.idle_count = 0
        self.sleeps: list[float] = []
        self.seconds_per_receive = seconds_per_receive
        self.stream = UserDataStream(
            client=self.client,
            stream_url=STREAM_URL,
            connect=self._connect,
            clock=self.clock,
            sleep=self.sleeps.append,
            **stream_options,
        )

    def _connect(self, url):
        self.connected_urls.append(url)
        connection = ScriptedConnection(self.scripts.pop(0), self.clock, self.seconds_per_receive)
        self.connections.append(connection)
        return connection

    def _should_stop(self):
        last_connection_drained = self.connections and not self.connections[-1].script
        return not self.scripts and bool(last_connection_drained)

    def run(self, on_event=None, on_connected=None):
        self.stream.run(
            on_event=on_event or self.events.append,
            on_connected=on_connected or self._count_connection,
            on_idle=self._count_idle,
            should_stop=self._should_stop,
        )

    def _count_connection(self):
        self.connected_count += 1

    def _count_idle(self):
        self.idle_count += 1


def as_message(event: dict) -> str:
    return json.dumps(event)


def test_events_are_delivered_in_order_with_the_listen_key_in_the_url():
    recorded = stream_events()[:3]
    harness = StreamHarness([[as_message(event) for event in recorded] + [None]])

    harness.run()

    assert harness.connected_urls == [f"{STREAM_URL}/key-1"]
    assert harness.events == recorded
    assert harness.connected_count == 1
    assert harness.connections[0].closed


def test_on_idle_runs_after_every_receive_including_timeouts():
    harness = StreamHarness([[None, None, as_message({"e": "TRADE_LITE"})]])

    harness.run()

    assert harness.idle_count == 3


def test_the_listen_key_is_kept_alive_every_30_minutes():
    harness = StreamHarness([[None] * 7], seconds_per_receive=600.0, keepalive_interval_seconds=1800)

    harness.run()

    assert harness.client.keepalives == 2


def test_a_dropped_connection_reconnects_with_a_new_key_and_backs_off():
    harness = StreamHarness([[ConnectionError("reset")], [ConnectionError("reset")], [None]])

    harness.run()

    assert harness.connected_urls == [f"{STREAM_URL}/key-1", f"{STREAM_URL}/key-2", f"{STREAM_URL}/key-3"]
    assert harness.sleeps == [1, 2]
    assert harness.connected_count == 3


def test_backoff_is_capped():
    harness = StreamHarness([[ConnectionError("reset")]] * 9 + [[None]], max_backoff_seconds=60)

    harness.run()

    assert harness.sleeps == [1, 2, 4, 8, 16, 32, 60, 60, 60]


def test_a_failure_to_get_a_listen_key_is_retried():
    harness = StreamHarness([[None]])
    harness.client.start_failures.append(BinanceUnavailableError("no connection"))

    harness.run()

    assert harness.sleeps == [1]
    assert harness.connected_urls == [f"{STREAM_URL}/key-1"]


def test_listen_key_expired_reconnects_immediately_with_a_new_key():
    harness = StreamHarness([[as_message({"e": "listenKeyExpired", "E": 1, "listenKey": "key-1"})], [None]])

    harness.run()

    assert harness.connected_urls == [f"{STREAM_URL}/key-1", f"{STREAM_URL}/key-2"]
    assert harness.sleeps == []
    assert harness.events == []


def test_the_connection_is_renewed_before_binances_24_hour_cut_off():
    harness = StreamHarness([[None, None, None], [None]], seconds_per_receive=12 * 3600.0,
                            max_connection_age_seconds=23 * 3600)

    harness.run()

    assert len(harness.connected_urls) == 2
    assert harness.connections[0].closed
    assert harness.sleeps == []


def test_a_failing_event_handler_does_not_stop_the_stream():
    def explode_on_first(event):
        if event["e"] == "FIRST":
            raise RuntimeError("bug in handler")
        harness.events.append(event)

    harness = StreamHarness([[as_message({"e": "FIRST"}), as_message({"e": "SECOND"})]])

    harness.run(on_event=explode_on_first)

    assert harness.events == [{"e": "SECOND"}]
    assert len(harness.connected_urls) == 1


def test_a_failing_on_connected_callback_does_not_stop_the_stream():
    def failing_reconcile():
        raise RuntimeError("reconcile failed")

    harness = StreamHarness([[as_message({"e": "AFTER"})]])

    harness.run(on_connected=failing_reconcile)

    assert harness.events == [{"e": "AFTER"}]


def test_messages_that_are_not_json_are_skipped():
    harness = StreamHarness([["not json", as_message({"e": "OK"})]])

    harness.run()

    assert harness.events == [{"e": "OK"}]
