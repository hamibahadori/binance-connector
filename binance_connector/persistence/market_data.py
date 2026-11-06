"""Market data shared between the processes: latest mark prices and exchange rules.

The worker's price feed writes ``market_prices`` (phase 5) so the API can answer "latest
state" requests and run the price guard without its own market connection. Exchange rules
(tick size, step size, minimums) are cached here after being read from Binance's
``exchangeInfo``.
"""

from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session, sessionmaker

from binance_connector.persistence.models import ExchangeRuleRecord, MarketPriceRecord
from binance_connector.persistence.writes import utc_now


@dataclass(frozen=True)
class MarketPrice:
    symbol: str
    mark_price: Decimal
    event_time: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ExchangeRules:
    """A symbol's trading rules (Binance PRICE_FILTER, LOT_SIZE, MARKET_LOT_SIZE, MIN_NOTIONAL)."""

    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_quantity: Decimal
    market_step_size: Decimal
    market_min_quantity: Decimal
    market_max_quantity: Decimal
    min_notional: Decimal


@dataclass(frozen=True)
class StoredExchangeRules:
    rules: ExchangeRules
    fetched_at: datetime


class MarketDataRepository:
    def __init__(self, session_factory: sessionmaker[Session], clock: Callable[[], datetime] = utc_now) -> None:
        self._session_factory = session_factory
        self._clock = clock

    def upsert_price(self, symbol: str, mark_price: Decimal, event_time: datetime) -> None:
        with self._session_factory() as session:
            record = session.get(MarketPriceRecord, symbol)
            if record is None:
                record = MarketPriceRecord(symbol=symbol)
                session.add(record)
            record.mark_price = mark_price
            record.event_time = event_time
            record.updated_at = self._clock()
            session.commit()

    def price(self, symbol: str) -> MarketPrice | None:
        with self._session_factory() as session:
            record = session.get(MarketPriceRecord, symbol)
            if record is None:
                return None
            return MarketPrice(record.symbol, record.mark_price, record.event_time, record.updated_at)

    def upsert_rules(self, rules: ExchangeRules) -> None:
        with self._session_factory() as session:
            record = session.get(ExchangeRuleRecord, rules.symbol)
            if record is None:
                record = ExchangeRuleRecord(symbol=rules.symbol)
                session.add(record)
            for field in fields(ExchangeRules):
                setattr(record, field.name, getattr(rules, field.name))
            record.fetched_at = self._clock()
            session.commit()

    def rules(self, symbol: str) -> StoredExchangeRules | None:
        with self._session_factory() as session:
            record = session.get(ExchangeRuleRecord, symbol)
            if record is None:
                return None
            rules = ExchangeRules(**{field.name: getattr(record, field.name) for field in fields(ExchangeRules)})
            return StoredExchangeRules(rules=rules, fetched_at=record.fetched_at)
