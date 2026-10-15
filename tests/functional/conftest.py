"""Fixtures for functional tests: the real Flask app with Binance mocked at the HTTP layer."""

import pytest
import responses

from binance_connector import create_app


@pytest.fixture
def app(settings):
    """The full application (routes, service, real client) built with test settings."""
    return create_app(settings)


@pytest.fixture
def http_client(app):
    """Flask's test client, for calling the service's own API."""
    return app.test_client()


@pytest.fixture
def binance():
    """Intercepts every outgoing call to Binance.

    An unregistered call fails the test, and so does a registered one that never
    happens, so each test states exactly which Binance calls it expects.
    """
    with responses.RequestsMock(assert_all_requests_are_fired=True) as mocked_binance:
        yield mocked_binance
