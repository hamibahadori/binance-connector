"""Unit tests for the public market inputs: exchange rules (cached) and the current mark price."""

from datetime import timedelta
from decimal import Decimal

import pytest
import responses

from binance_connector.binance.public import BinancePublicClient
from binance_connector.config import DEMO_BASE_URL
from binance_connector.errors import UnknownSymbolError
from binance_connector.persistence.market_data import MarketDataRepository
from binance_connector.persistence.writes import utc_now
from binance_connector.services.market_inputs import ExchangeRulesProvider, MarkPriceProvider
from tests.recorded import exchange_info_reply

EXCHANGE_INFO_URL = f"{DEMO_BASE_URL}/fapi/v1/exchangeInfo"
PREMIUM_INDEX_URL = f"{DEMO_BASE_URL}/fapi/v1/premiumIndex"


@pytest.fixture
def market_data(session_factory) -> MarketDataRepository:
    return MarketDataRepository(session_factory)


@pytest.fixture
def public_client(settings) -> BinancePublicClient:
    return BinancePublicClient(settings)


@responses.activate
def test_rules_are_read_from_exchange_info_and_cached(market_data, public_client):
    responses.get(EXCHANGE_INFO_URL, json=exchange_info_reply())
    provider = ExchangeRulesProvider(market_data=market_data, public_client=public_client)

    first = provider.rules("BTCUSDT")
    second = provider.rules("ETHUSDT")

    assert first.tick_size == Decimal("0.10")
    assert first.step_size == Decimal("0.0001")
    assert first.market_max_quantity == Decimal("120")
    assert first.min_notional == Decimal("50")
    assert second.min_notional == Decimal("20")
    assert len(responses.calls) == 1  # one exchangeInfo call cached every symbol


@responses.activate
def test_stale_rules_are_refreshed(market_data, public_client):
    responses.get(EXCHANGE_INFO_URL, json=exchange_info_reply())
    provider = ExchangeRulesProvider(market_data=market_data, public_client=public_client, max_age=timedelta(hours=24))
    provider.rules("BTCUSDT")
    later = ExchangeRulesProvider(market_data=market_data, public_client=public_client, max_age=timedelta(hours=24),
                                  clock=lambda: utc_now() + timedelta(hours=25))

    later.rules("BTCUSDT")

    assert len(responses.calls) == 2


@responses.activate
def test_an_unknown_symbol_raises(market_data, public_client):
    responses.get(EXCHANGE_INFO_URL, json=exchange_info_reply())
    provider = ExchangeRulesProvider(market_data=market_data, public_client=public_client)

    with pytest.raises(UnknownSymbolError, match="DOGEUSDT"):
        provider.rules("DOGEUSDT")


@responses.activate
def test_a_fresh_stored_price_is_used_without_calling_binance(market_data, public_client):
    market_data.upsert_price("BTCUSDT", Decimal("75869.5"), utc_now())
    provider = MarkPriceProvider(market_data=market_data, public_client=public_client)

    assert provider.price("BTCUSDT") == Decimal("75869.5")
    assert len(responses.calls) == 0


@responses.activate
def test_a_stale_or_missing_price_is_fetched_from_binance(market_data, public_client):
    market_data.upsert_price("BTCUSDT", Decimal("70000"), utc_now() - timedelta(minutes=5))
    responses.get(PREMIUM_INDEX_URL, json={"symbol": "BTCUSDT", "markPrice": "75869.50000000", "time": 1793999730000})
    provider = MarkPriceProvider(market_data=market_data, public_client=public_client,
                                 max_age=timedelta(seconds=10))

    assert provider.price("BTCUSDT") == Decimal("75869.50000000")
    assert "symbol=BTCUSDT" in responses.calls[0].request.url
