"""Unit tests for error logs and market data (prices and exchange rules)."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.market_data import ExchangeRules, MarketDataRepository
from binance_connector.security.redaction import SecretRedactor


@pytest.fixture
def redactor() -> SecretRedactor:
    redactor = SecretRedactor()
    redactor.register("secret-value-123456")
    return redactor


def test_error_logs_are_stored_with_their_ids_and_redacted(session_factory, redactor):
    error_logs = ErrorLogRepository(session_factory, redactor=redactor)

    error_logs.record(
        component="execution_manager",
        message="Binance said no for secret-value-123456",
        context={"api_secret": "secret-value-123456", "code": -2019},
        account_id="account-1",
        request_id="request-1",
        position_id="p" * 24,
    )

    entry = error_logs.recent()[0]
    assert entry.component == "execution_manager"
    assert "secret-value-123456" not in entry.message
    assert "secret-value-123456" not in entry.context
    assert json.loads(entry.context)["code"] == -2019
    assert (entry.account_id, entry.request_id, entry.position_id) == ("account-1", "request-1", "p" * 24)


def test_recent_error_logs_are_newest_first(session_factory, redactor):
    error_logs = ErrorLogRepository(session_factory, redactor=redactor)
    for index in range(3):
        error_logs.record(component="test", message=f"error {index}")

    assert [entry.message for entry in error_logs.recent(limit=2)] == ["error 2", "error 1"]


def test_the_latest_mark_price_is_kept_per_symbol(session_factory):
    market_data = MarketDataRepository(session_factory)
    market_data.upsert_price("BTCUSDT", Decimal("75869.5"), datetime(2026, 11, 7, 12, 0, 0, tzinfo=UTC))

    market_data.upsert_price("BTCUSDT", Decimal("75875.9"), datetime(2026, 11, 7, 12, 0, 1, tzinfo=UTC))

    price = market_data.price("BTCUSDT")
    assert price.mark_price == Decimal("75875.9")
    assert price.event_time == datetime(2026, 11, 7, 12, 0, 1, tzinfo=UTC)
    assert market_data.price("ETHUSDT") is None


def test_exchange_rules_round_trip_exactly(session_factory):
    market_data = MarketDataRepository(session_factory)
    rules = ExchangeRules(
        symbol="BTCUSDT", tick_size=Decimal("0.10"), step_size=Decimal("0.0001"), min_quantity=Decimal("0.0001"),
        market_step_size=Decimal("0.0001"), market_min_quantity=Decimal("0.0001"),
        market_max_quantity=Decimal("120"), min_notional=Decimal("50"),
    )

    market_data.upsert_rules(rules)

    stored = market_data.rules("BTCUSDT")
    assert stored.rules == rules
    assert stored.fetched_at.tzinfo is UTC
