"""Functional tests: the API served by waitress (the production server), over real HTTP."""

import threading
from dataclasses import replace

import pytest
import requests

from binance_connector.errors import ConfigurationError
from binance_connector.server import build_server


@pytest.fixture
def running_server(settings):
    server = build_server(replace(settings, api_port=0))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    yield server
    server.close()


def test_waitress_serves_the_api_over_http(running_server):
    response = requests.get(f"http://127.0.0.1:{running_server.effective_port}/health", timeout=5)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_waitress_serves_the_request_routes_too(running_server):
    response = requests.get(f"http://127.0.0.1:{running_server.effective_port}/api/v1/requests/nope", timeout=5)

    assert response.status_code == 404
    assert response.json()["error"] == "request_not_found"


def test_the_server_refuses_to_start_on_an_unmigrated_database(settings, tmp_path):
    with pytest.raises(ConfigurationError, match="alembic upgrade head"):
        build_server(replace(settings, database_url=f"sqlite:///{tmp_path / 'never-migrated.db'}"))
