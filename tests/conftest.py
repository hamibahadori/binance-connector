"""Fixtures shared by the unit and functional test layers."""

import pytest

from binance_connector.config import DEMO_BASE_URL, Settings

TEST_API_KEY = "test-api-key"
TEST_API_SECRET = "test-api-secret"


@pytest.fixture
def settings() -> Settings:
    """Settings with fake credentials, pointing at the Demo base URL.

    Unit and functional tests intercept every HTTP call with ``responses``,
    so these credentials never reach Binance.
    """
    return Settings(
        api_key=TEST_API_KEY,
        api_secret=TEST_API_SECRET,
        base_url=DEMO_BASE_URL,
        recv_window_ms=5000,
        timeout_seconds=10.0,
    )
