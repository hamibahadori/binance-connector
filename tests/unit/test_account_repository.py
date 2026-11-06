"""Unit tests for accounts: one per Django account_id, owned by one user, secrets encrypted."""

import pytest
from sqlalchemy import text

from binance_connector.errors import AccountNotFoundError, AccountOwnershipError
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.security.credentials import ApiCredentials
from binance_connector.security.redaction import SecretRedactor

CREDENTIALS = ApiCredentials(api_key="demo-api-key-0001", api_secret="demo-api-secret-0001")


@pytest.fixture
def redactor() -> SecretRedactor:
    return SecretRedactor()


@pytest.fixture
def repository(session_factory, cipher, redactor) -> AccountRepository:
    return AccountRepository(session_factory, cipher=cipher, redactor=redactor)


def test_a_new_account_is_created_on_first_upsert(repository):
    result = repository.upsert("account-1", "user-1", CREDENTIALS)

    assert result.created is True
    assert result.credentials_changed is False
    assert result.account.account_id == "account-1"
    assert result.account.user_id == "user-1"


def test_credentials_are_stored_encrypted_and_never_in_plain_text(repository, database_engine):
    repository.upsert("account-1", "user-1", CREDENTIALS)

    with database_engine.connect() as connection:
        stored_row = connection.execute(text("SELECT * FROM accounts")).mappings().one()
    stored_text = " ".join(str(value) for value in stored_row.values())
    assert CREDENTIALS.api_secret not in stored_text
    assert CREDENTIALS.api_key not in stored_text


def test_credentials_decrypt_back_and_are_registered_for_log_redaction(repository, redactor):
    repository.upsert("account-1", "user-1", CREDENTIALS)

    credentials = repository.credentials("account-1")

    assert credentials == CREDENTIALS
    assert redactor.redact(f"secret {CREDENTIALS.api_secret}") == "secret ***"


def test_the_snapshot_carries_a_fingerprint_not_the_key(repository):
    account = repository.upsert("account-1", "user-1", CREDENTIALS).account

    assert CREDENTIALS.api_key not in repr(account)
    assert len(account.api_key_fingerprint) == 12


def test_the_same_credentials_again_change_nothing(repository):
    repository.upsert("account-1", "user-1", CREDENTIALS)

    result = repository.upsert("account-1", "user-1", CREDENTIALS)

    assert (result.created, result.credentials_changed) == (False, False)
    assert result.account.credentials_updated_at is None


def test_new_credentials_replace_the_stored_ones(repository):
    repository.upsert("account-1", "user-1", CREDENTIALS)
    rotated = ApiCredentials(api_key="demo-api-key-0002", api_secret="demo-api-secret-0002")

    result = repository.upsert("account-1", "user-1", rotated)

    assert result.credentials_changed is True
    assert result.account.credentials_updated_at is not None
    assert repository.credentials("account-1") == rotated


def test_an_account_cannot_be_claimed_by_another_user(repository):
    repository.upsert("account-1", "user-1", CREDENTIALS)

    with pytest.raises(AccountOwnershipError, match="account-1"):
        repository.upsert("account-1", "user-2", CREDENTIALS)

    assert repository.get("account-1").user_id == "user-1"


def test_unknown_accounts_raise_not_found(repository):
    with pytest.raises(AccountNotFoundError):
        repository.get("account-unknown")
    with pytest.raises(AccountNotFoundError):
        repository.credentials("account-unknown")
