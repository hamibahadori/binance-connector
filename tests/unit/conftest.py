"""Fixtures for the execution-side unit tests: accounts, positions, simulated Binance clients."""

import pytest

from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.positions import PositionRepository
from binance_connector.security.credentials import ApiCredentials
from binance_connector.security.redaction import SecretRedactor
from tests.support import ACCOUNT_IDS, FakeClients


@pytest.fixture
def accounts(session_factory, cipher) -> AccountRepository:
    """Two accounts, owned by two different users."""
    repository = AccountRepository(session_factory, cipher=cipher, redactor=SecretRedactor())
    for account_id in ACCOUNT_IDS:
        repository.upsert(account_id, f"user-of-{account_id}", ApiCredentials(f"key-{account_id}", f"secret-{account_id}"))
    return repository


@pytest.fixture
def positions(session_factory, accounts) -> PositionRepository:
    return PositionRepository(session_factory)


@pytest.fixture
def clients() -> FakeClients:
    return FakeClients()
