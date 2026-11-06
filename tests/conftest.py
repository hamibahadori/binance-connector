"""Fixtures shared by the unit and functional test layers."""

import pytest
from cryptography.fernet import Fernet

from binance_connector.config import DEMO_BASE_URL, Settings
from binance_connector.persistence.database import create_database_engine, create_session_factory
from binance_connector.persistence.schema import upgrade_to_head
from binance_connector.security.credentials import ApiCredentials, CredentialCipher

TEST_API_KEY = "test-api-key"
TEST_API_SECRET = "test-api-secret"
TEST_CREDENTIALS = ApiCredentials(api_key=TEST_API_KEY, api_secret=TEST_API_SECRET)
"""Fake Binance credentials. Unit and functional tests intercept every HTTP call, so these
never reach Binance."""

TEST_ENCRYPTION_KEY = Fernet.generate_key().decode()


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
def cipher() -> CredentialCipher:
    return CredentialCipher(TEST_ENCRYPTION_KEY)


@pytest.fixture
def settings(database_url) -> Settings:
    """Service settings pointing at Demo, with a fresh migrated database."""
    return Settings(
        credentials_encryption_key=TEST_ENCRYPTION_KEY,
        base_url=DEMO_BASE_URL,
        recv_window_ms=5000,
        timeout_seconds=10.0,
        database_url=database_url,
        trigger_price_type="MARK_PRICE",
    )
