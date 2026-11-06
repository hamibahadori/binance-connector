"""Exchange rules and current prices for validating and sizing requests.

* ``ExchangeRulesProvider`` caches Binance's ``exchangeInfo`` filters in the database for
  ``max_age`` (24 h by default). One exchangeInfo call refreshes every symbol.
* ``MarkPriceProvider`` prefers the price the worker's feed stored (phase 5) when it is
  fresh (10 s by default), and otherwise asks Binance's public ``premiumIndex``.

Both raise MarketDataUnavailableError when Binance cannot be reached. Intake then answers
503 and stores nothing, so Django can retry with the same request_id.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from binance_connector.binance.public import BinancePublicClient
from binance_connector.errors import BinanceError, MarketDataUnavailableError, UnknownSymbolError
from binance_connector.persistence.market_data import ExchangeRules, MarketDataRepository
from binance_connector.persistence.writes import utc_now

DEFAULT_RULES_MAX_AGE = timedelta(hours=24)
DEFAULT_PRICE_MAX_AGE = timedelta(seconds=10)


class ExchangeRulesProvider:
    def __init__(
        self,
        *,
        market_data: MarketDataRepository,
        public_client: BinancePublicClient,
        max_age: timedelta = DEFAULT_RULES_MAX_AGE,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._market_data = market_data
        self._public_client = public_client
        self._max_age = max_age
        self._clock = clock

    def rules(self, symbol: str) -> ExchangeRules:
        """Raises UnknownSymbolError, or MarketDataUnavailableError when Binance is unreachable."""
        stored = self._market_data.rules(symbol)
        if stored is not None and self._clock() - stored.fetched_at < self._max_age:
            return stored.rules
        self._refresh()
        stored = self._market_data.rules(symbol)
        if stored is None:
            raise UnknownSymbolError(symbol)
        return stored.rules

    def _refresh(self) -> None:
        try:
            exchange_info = self._public_client.get_exchange_info()
        except BinanceError as error:
            raise MarketDataUnavailableError(f"Binance exchange rules are unavailable: {error}") from error
        for symbol_info in exchange_info.get("symbols", []):
            if symbol_info.get("status", "TRADING") == "TRADING":
                self._market_data.upsert_rules(parse_rules(symbol_info))


class MarkPriceProvider:
    def __init__(
        self,
        *,
        market_data: MarketDataRepository,
        public_client: BinancePublicClient,
        max_age: timedelta = DEFAULT_PRICE_MAX_AGE,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._market_data = market_data
        self._public_client = public_client
        self._max_age = max_age
        self._clock = clock

    def price(self, symbol: str) -> Decimal:
        stored = self._market_data.price(symbol)
        # Judged by Binance's time for the price, not by when the row was written.
        if stored is not None and self._clock() - stored.event_time < self._max_age:
            return stored.mark_price
        try:
            mark_price, event_time = self._public_client.get_mark_price(symbol)
        except BinanceError as error:
            raise MarketDataUnavailableError(f"The {symbol} mark price is unavailable: {error}") from error
        self._market_data.upsert_price(symbol, mark_price, event_time)
        return mark_price


def parse_rules(symbol_info: dict[str, Any]) -> ExchangeRules:
    """Pick the filters the service uses out of one exchangeInfo ``symbols`` entry."""
    filters = {rule["filterType"]: rule for rule in symbol_info["filters"]}
    lot_size = filters["LOT_SIZE"]
    market_lot_size = filters.get("MARKET_LOT_SIZE", lot_size)
    return ExchangeRules(
        symbol=symbol_info["symbol"],
        tick_size=Decimal(filters["PRICE_FILTER"]["tickSize"]),
        step_size=Decimal(lot_size["stepSize"]),
        min_quantity=Decimal(lot_size["minQty"]),
        market_step_size=Decimal(market_lot_size["stepSize"]),
        market_min_quantity=Decimal(market_lot_size["minQty"]),
        market_max_quantity=Decimal(market_lot_size["maxQty"]),
        min_notional=Decimal(filters["MIN_NOTIONAL"]["notional"]),
    )
