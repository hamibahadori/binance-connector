"""Functional tests for the container health checks, against real servers on free ports."""

import socket
import threading
from dataclasses import replace

from binance_connector.healthcheck import api_is_healthy, live_feed_is_healthy, main
from binance_connector.server import build_server
from binance_connector.services.websocket_server import LiveFeedServer


class EmptyHub:
    def subscribe(self, subscriber) -> None: ...

    def unsubscribe(self, subscriber) -> None: ...

    def snapshot(self, account_id=None) -> dict:
        return {"type": "snapshot", "data": {"report_sequence": 0, "positions": []}}


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def start_live_feed(token=None):
    stop = threading.Event()
    server = LiveFeedServer(hub=EmptyHub(), host="127.0.0.1", port=0, token=token)
    thread = threading.Thread(target=server.run, args=(stop.is_set,), daemon=True)
    thread.start()
    return server.wait_until_ready(timeout_seconds=5), stop, thread


def test_a_running_api_is_healthy(settings):
    server = build_server(replace(settings, api_port=0))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        assert api_is_healthy(f"http://127.0.0.1:{server.effective_port}/health")
    finally:
        server.close()


def test_nothing_listening_is_unhealthy():
    port = free_port()

    assert not api_is_healthy(f"http://127.0.0.1:{port}/health")
    assert not live_feed_is_healthy(f"ws://127.0.0.1:{port}/ws")


def test_a_running_live_feed_is_healthy_even_when_it_asks_for_a_token():
    for token in (None, "health-token-123"):
        port, stop, thread = start_live_feed(token)
        try:
            assert live_feed_is_healthy(f"ws://127.0.0.1:{port}/ws"), token
        finally:
            stop.set()
            thread.join(timeout=5)


def test_main_reports_through_its_exit_code(monkeypatch):
    port, stop, thread = start_live_feed()
    try:
        monkeypatch.setenv("WEBSOCKET_PORT", str(port))
        assert main(["worker"]) == 0
        monkeypatch.setenv("API_PORT", str(free_port()))
        assert main(["api"]) == 1
    finally:
        stop.set()
        thread.join(timeout=5)
