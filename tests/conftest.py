"""Fixtures shared by the unit and functional test layers."""

import pytest

from binance_connector.config import DEMO_BASE_URL, Settings
from binance_connector.persistence.database import create_database_engine, create_session_factory
from binance_connector.persistence.schema import upgrade_to_head

TEST_API_KEY = "test-api-key"
TEST_API_SECRET = "test-api-secret"


@pytest.fixture
def database_url(tmp_path) -> str:
    """A fresh, fully migrated SQLite file per test (a file, not :memory:, so that
    several sessions and processes see the same data, as in production)."""
    url = f"sqlite:///{tmp_path / 'test.db'}"
    engine = create_database_engine(url)
    upgrade_to_head(engine)
    engine.dispose()
    return url


@pytest.fixture
def database_engine(database_url):
    engine = create_database_engine(database_url)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(database_engine):
    return create_session_factory(database_engine)


@pytest.fixture
def settings(database_url) -> Settings:
    """Settings with fake credentials, the Demo base URL, and a fresh migrated database.

    Unit and functional tests intercept every HTTP call with ``responses``,
    so these credentials never reach Binance.
    """
    return Settings(
        api_key=TEST_API_KEY,
        api_secret=TEST_API_SECRET,
        base_url=DEMO_BASE_URL,
        recv_window_ms=5000,
        timeout_seconds=10.0,
        database_url=database_url,
        trigger_price_type="MARK_PRICE",
    )
