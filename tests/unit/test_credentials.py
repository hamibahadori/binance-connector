"""Unit tests for encrypting stored account credentials."""

import pytest
from cryptography.fernet import Fernet

from binance_connector.errors import ConfigurationError, CredentialDecryptionError
from binance_connector.security.credentials import ApiCredentials, CredentialCipher, fingerprint


@pytest.fixture
def cipher() -> CredentialCipher:
    return CredentialCipher(Fernet.generate_key().decode())


def test_encrypted_text_decrypts_back(cipher):
    token = cipher.encrypt("demo-secret-value")

    assert token != "demo-secret-value"
    assert "demo-secret-value" not in token
    assert cipher.decrypt(token) == "demo-secret-value"


def test_encrypting_twice_gives_different_tokens(cipher):
    assert cipher.encrypt("same") != cipher.encrypt("same")


def test_a_token_from_another_key_is_refused(cipher):
    other_cipher = CredentialCipher(Fernet.generate_key().decode())

    with pytest.raises(CredentialDecryptionError):
        cipher.decrypt(other_cipher.encrypt("demo-secret-value"))


@pytest.mark.parametrize("bad_key", ["", "not-a-fernet-key", "a" * 44])
def test_an_invalid_key_is_a_configuration_error(bad_key):
    with pytest.raises(ConfigurationError, match="CREDENTIALS_ENCRYPTION_KEY"):
        CredentialCipher(bad_key)


def test_credentials_never_show_in_repr():
    credentials = ApiCredentials(api_key="visible-key-123", api_secret="hidden-secret-456")

    assert "visible-key-123" not in repr(credentials)
    assert "hidden-secret-456" not in repr(credentials)


def test_fingerprint_is_stable_short_and_does_not_reveal_the_value():
    first = fingerprint("my-api-key-value")

    assert first == fingerprint("my-api-key-value")
    assert first != fingerprint("another-key")
    assert len(first) == 12
    assert "my-api-key" not in first
