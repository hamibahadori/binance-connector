"""Functional tests for building the worker process from settings (no network involved)."""

from dataclasses import replace

import pytest

from binance_connector.errors import ConfigurationError
from binance_connector.worker import WorkerProcess, build_worker


def test_the_worker_is_wired_from_settings(settings):
    worker = build_worker(settings)

    assert isinstance(worker, WorkerProcess)
    assert worker.price_feed is not None
    worker.shutdown()


def test_the_worker_refuses_to_start_on_an_unmigrated_database(settings, tmp_path):
    unmigrated = replace(settings, database_url=f"sqlite:///{tmp_path / 'never-migrated.db'}")

    with pytest.raises(ConfigurationError, match="alembic upgrade head"):
        build_worker(unmigrated)


def test_the_running_worker_serves_the_live_feed(settings):
    import json
    import threading

    from websockets.sync.client import connect

    worker = build_worker(replace(settings, websocket_port=0, websocket_token="wiring-token-123"))
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        port = worker.live_feed_server.wait_until_ready(timeout_seconds=5)
        with connect(f"ws://127.0.0.1:{port}/ws?token=wiring-token-123") as client:
            snapshot = json.loads(client.recv(timeout=5))
            status = json.loads(client.recv(timeout=10))
    finally:
        worker.stop()
        thread.join(timeout=10)
        worker.shutdown()

    assert snapshot["type"] == "snapshot"
    assert snapshot["data"]["positions"] == []
    assert status["type"] == "service_status"
    assert status["data"]["clients"] == 1


def test_the_worker_masks_its_tokens_in_logs(settings):
    from binance_connector.security.redaction import REDACTOR

    worker = build_worker(replace(settings, websocket_token="masked-ws-token-1",
                                  django_report_auth_value="Bearer masked-django-token-1"))
    worker.shutdown()

    assert REDACTOR.redact("masked-ws-token-1 masked-django-token-1") == "*** ***"