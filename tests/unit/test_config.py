"""Unit tests for loading service settings from environment variables.

Binance credentials are no longer service settings: each account's credentials arrive
with its requests and are stored encrypted.
"""

from decimal import Decimal

import pytest
from cryptography.fernet import Fernet

from binance_connector.config import DEMO_BASE_URL, DEMO_MARKET_STREAM_URL, DEMO_STREAM_URL, Settings
from binance_connector.errors import ConfigurationError

ENCRYPTION_KEY = Fernet.generate_key().decode()
REQUIRED_ENVIRONMENT = {"CREDENTIALS_ENCRYPTION_KEY": ENCRYPTION_KEY}


def test_defaults_point_at_demo():
    settings = Settings.from_environment(REQUIRED_ENVIRONMENT)

    assert settings.base_url == DEMO_BASE_URL
    assert settings.stream_url == DEMO_STREAM_URL
    assert settings.market_stream_url == DEMO_MARKET_STREAM_URL
    assert settings.recv_window_ms == 5000
    assert settings.timeout_seconds == 10.0
    assert settings.database_url == "sqlite:///binance_connector.db"
    assert settings.trigger_price_type == "MARK_PRICE"
    assert settings.reconcile_interval_seconds == 60.0
    assert settings.limit_entry_timeout_seconds == 300.0
    assert settings.price_guard_max_deviation_percent is None
    assert settings.execution_workers == 4
    assert settings.inbound_api_token is None


def test_overrides_are_read():
    settings = Settings.from_environment({
        **REQUIRED_ENVIRONMENT,
        "BINANCE_BASE_URL": "https://example.test/",
        "BINANCE_RECV_WINDOW_MS": "20000",
        "BINANCE_TIMEOUT_SECONDS": "30",
        "DATABASE_URL": "sqlite:///other.db",
        "BINANCE_TRIGGER_PRICE_TYPE": "contract_price",
        "BINANCE_STREAM_URL": "wss://fstream.binance.com/private/ws/",
        "BINANCE_MARKET_STREAM_URL": "wss://fstream.binance.com/market/ws",
        "RECONCILE_INTERVAL_SECONDS": "15",
        "LIMIT_ENTRY_TIMEOUT_SECONDS": "120",
        "PRICE_GUARD_MAX_DEVIATION_PERCENT": "1.5",
        "EXECUTION_WORKERS": "8",
        "INBOUND_API_TOKEN": "token-from-env",
    })

    assert settings.base_url == "https://example.test"
    assert settings.recv_window_ms == 20000
    assert settings.timeout_seconds == 30.0
    assert settings.database_url == "sqlite:///other.db"
    assert settings.trigger_price_type == "CONTRACT_PRICE"
    assert settings.stream_url == "wss://fstream.binance.com/private/ws"
    assert settings.market_stream_url == "wss://fstream.binance.com/market/ws"
    assert settings.reconcile_interval_seconds == 15.0
    assert settings.limit_entry_timeout_seconds == 120.0
    assert settings.price_guard_max_deviation_percent == Decimal("1.5")
    assert settings.execution_workers == 8
    assert settings.inbound_api_token == "token-from-env"


@pytest.mark.parametrize("missing_value", [None, "", "  "])
def test_the_encryption_key_is_required(missing_value):
    environment = {} if missing_value is None else {"CREDENTIALS_ENCRYPTION_KEY": missing_value}

    with pytest.raises(ConfigurationError, match="CREDENTIALS_ENCRYPTION_KEY"):
        Settings.from_environment(environment)


def test_an_invalid_encryption_key_is_rejected():
    with pytest.raises(ConfigurationError, match="CREDENTIALS_ENCRYPTION_KEY"):
        Settings.from_environment({"CREDENTIALS_ENCRYPTION_KEY": "not-a-key"})


@pytest.mark.parametrize(
    ("variable", "bad_value"),
    [
        ("BINANCE_RECV_WINDOW_MS", "five-thousand"),
        ("BINANCE_RECV_WINDOW_MS", "0"),
        ("BINANCE_RECV_WINDOW_MS", "60001"),
        ("BINANCE_TIMEOUT_SECONDS", "soon"),
        ("BINANCE_TIMEOUT_SECONDS", "0"),
        ("RECONCILE_INTERVAL_SECONDS", "-5"),
        ("LIMIT_ENTRY_TIMEOUT_SECONDS", "never"),
        ("PRICE_GUARD_MAX_DEVIATION_PERCENT", "0"),
        ("PRICE_GUARD_MAX_DEVIATION_PERCENT", "wide"),
        ("EXECUTION_WORKERS", "0"),
        ("EXECUTION_WORKERS", "2.5"),
        ("BINANCE_TRIGGER_PRICE_TYPE", "LAST_PRICE"),
    ],
)
def test_invalid_values_are_rejected(variable, bad_value):
    with pytest.raises(ConfigurationError, match=variable):
        Settings.from_environment({**REQUIRED_ENVIRONMENT, variable: bad_value})


def test_an_empty_inbound_token_means_no_token():
    settings = Settings.from_environment({**REQUIRED_ENVIRONMENT, "INBOUND_API_TOKEN": "  "})

    assert settings.inbound_api_token is None


def test_secrets_are_hidden_from_repr():
    settings = Settings.from_environment({**REQUIRED_ENVIRONMENT, "INBOUND_API_TOKEN": "token-from-env"})

    assert ENCRYPTION_KEY not in repr(settings)
    assert "token-from-env" not in repr(settings)
