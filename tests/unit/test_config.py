"""Unit tests for loading settings from environment variables."""

import pytest

from binance_connector.config import DEMO_BASE_URL, Settings
from binance_connector.errors import ConfigurationError

REQUIRED_ENVIRONMENT = {
    "BINANCE_API_KEY": "key-value",
    "BINANCE_API_SECRET": "secret-value",
}


def test_reads_required_keys_and_applies_defaults():
    settings = Settings.from_environment(REQUIRED_ENVIRONMENT)

    assert settings.api_key == "key-value"
    assert settings.api_secret == "secret-value"
    assert settings.base_url == DEMO_BASE_URL
    assert settings.recv_window_ms == 5000
    assert settings.timeout_seconds == 10.0


def test_reads_optional_overrides():
    environment = {
        **REQUIRED_ENVIRONMENT,
        "BINANCE_BASE_URL": "https://example.test",
        "BINANCE_RECV_WINDOW_MS": "3000",
        "BINANCE_TIMEOUT_SECONDS": "2.5",
    }

    settings = Settings.from_environment(environment)

    assert settings.base_url == "https://example.test"
    assert settings.recv_window_ms == 3000
    assert settings.timeout_seconds == 2.5


def test_trailing_slash_is_removed_from_base_url():
    environment = {**REQUIRED_ENVIRONMENT, "BINANCE_BASE_URL": "https://example.test/"}

    assert Settings.from_environment(environment).base_url == "https://example.test"


@pytest.mark.parametrize("missing_variable", ["BINANCE_API_KEY", "BINANCE_API_SECRET"])
@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_missing_or_blank_credential_is_rejected(missing_variable, missing_value):
    environment = dict(REQUIRED_ENVIRONMENT)
    if missing_value is None:
        del environment[missing_variable]
    else:
        environment[missing_variable] = missing_value

    with pytest.raises(ConfigurationError, match=missing_variable):
        Settings.from_environment(environment)


@pytest.mark.parametrize(
    ("variable", "bad_value"),
    [
        ("BINANCE_RECV_WINDOW_MS", "five-thousand"),
        ("BINANCE_RECV_WINDOW_MS", "0"),
        ("BINANCE_RECV_WINDOW_MS", "60001"),
        ("BINANCE_TIMEOUT_SECONDS", "soon"),
        ("BINANCE_TIMEOUT_SECONDS", "0"),
    ],
)
def test_invalid_numeric_setting_is_rejected(variable, bad_value):
    environment = {**REQUIRED_ENVIRONMENT, variable: bad_value}

    with pytest.raises(ConfigurationError, match=variable):
        Settings.from_environment(environment)


def test_credentials_are_hidden_from_repr():
    settings = Settings.from_environment(REQUIRED_ENVIRONMENT)

    assert "key-value" not in repr(settings)
    assert "secret-value" not in repr(settings)
