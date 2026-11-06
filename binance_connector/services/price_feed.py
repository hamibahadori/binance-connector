"""The live mark-price feed (spec §6, §7): one Binance market stream for every symbol in use.

The worker keeps one WebSocket open to Binance's combined market stream for exactly the
symbols that have an active position, e.g.::

    wss://demo-fstream.binance.com/stream?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s

Each ``markPriceUpdate`` (one per symbol per second; format recorded on Demo, 2026-11-07)
is kept in memory, written to ``market_prices`` (so the API's "latest state" answers and
the price guard see it), and passed to listeners (the WebSocket manager, phase 7).

When the set of symbols changes, the feed reconnects with the new set. That uses only the
combined-stream URL, which was verified on Demo; unsubscribing on a live connection was
not. Connections are renewed before Binance's 24-hour limit, and failures are retried
with exponential backoff (1, 2, 4 … 60 s). A listener or a database write that fails is
logged and never stops the feed.
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from binance_connector.binance.user_stream import StreamConnection, WebSocketConnection
from binance_connector.persistence.market_data import MarketDataRepository

logger = logging.getLogger(__name__)

SYMBOL_CHECK_SECONDS = 5.0
IDLE_SLEEP_SECONDS = 1.0
MAX_CONNECTION_AGE_SECONDS = 23 * 60 * 60
MAX_BACKOFF_SECONDS = 60


@dataclass(frozen=True)
class PriceTick:
    symbol: str
    mark_price: Decimal
    event_time: datetime


@dataclass(frozen=True)
class FeedStatus:
    connected: bool
    symbols: frozenset[str]
    last_tick_at: datetime | None
    last_error: str | None


class PriceFeed:
    def __init__(
        self,
        *,
        market_data: MarketDataRepository,
        symbols: Callable[[], set[str]],
        stream_url: str,
        connect: Callable[[str], StreamConnection] = WebSocketConnection,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        symbol_check_seconds: float = SYMBOL_CHECK_SECONDS,
        max_connection_age_seconds: float = MAX_CONNECTION_AGE_SECONDS,
        max_backoff_seconds: float = MAX_BACKOFF_SECONDS,
    ) -> None:
        self._market_data = market_data
        self._symbols = symbols
        self._stream_url = stream_url.rstrip("/")
        self._connect = connect
        self._clock = clock
        self._sleep = sleep
        self._symbol_check_seconds = symbol_check_seconds
        self._max_connection_age_seconds = max_connection_age_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._latest: dict[str, PriceTick] = {}
        self._listeners: list[Callable[[PriceTick], None]] = []
        self._lock = threading.Lock()
        self._status = FeedStatus(connected=False, symbols=frozenset(), last_tick_at=None, last_error=None)

    # --- reading ------------------------------------------------------------------------------

    def latest(self, symbol: str) -> PriceTick | None:
        with self._lock:
            return self._latest.get(symbol)

    def status(self) -> FeedStatus:
        with self._lock:
            return self._status

    def add_listener(self, listener: Callable[[PriceTick], None]) -> None:
        with self._lock:
            self._listeners.append(listener)

    # --- the loop -----------------------------------------------------------------------------

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Stream prices for the active symbols until ``should_stop()``; never gives up."""
        consecutive_failures = 0
        while not should_stop():
            wanted = frozenset(self._symbols())
            if not wanted:
                self._sleep(IDLE_SLEEP_SECONDS)
                continue
            try:
                self._run_connection(wanted, should_stop)
                consecutive_failures = 0
            except Exception as error:  # noqa: BLE001 - any failure means "reconnect"
                consecutive_failures += 1
                delay_seconds = min(2 ** (consecutive_failures - 1), self._max_backoff_seconds)
                self._update_status(connected=False, last_error=f"{type(error).__name__}: {error}")
                logger.warning("Price feed failed (%s: %s); reconnecting in %s s",
                               type(error).__name__, error, delay_seconds)
                self._sleep(delay_seconds)

    def _run_connection(self, wanted: frozenset[str], should_stop: Callable[[], bool]) -> None:
        streams = "/".join(f"{symbol.lower()}@markPrice@1s" for symbol in sorted(wanted))
        connection = self._connect(f"{self._stream_url}?streams={streams}")
        self._update_status(connected=True, symbols=wanted, last_error=None)
        logger.info("Price feed connected for %s", ", ".join(sorted(wanted)))
        try:
            connected_at = last_symbol_check_at = self._clock()
            while not should_stop():
                now = self._clock()
                if now - connected_at >= self._max_connection_age_seconds:
                    return
                if now - last_symbol_check_at >= self._symbol_check_seconds:
                    last_symbol_check_at = now
                    if frozenset(self._symbols()) != wanted:
                        return  # reconnect with the new set
                message = connection.receive()
                if message is not None:
                    self._handle(message)
        finally:
            connection.close()
            self._update_status(connected=False)

    def _handle(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        data: dict[str, Any] = payload.get("data", payload) if isinstance(payload, dict) else {}
        if data.get("e") != "markPriceUpdate":
            return
        tick = PriceTick(symbol=data["s"], mark_price=Decimal(data["p"]),
                         event_time=datetime.fromtimestamp(data["E"] / 1000, tz=UTC))
        with self._lock:
            self._latest[tick.symbol] = tick
            listeners = list(self._listeners)
        self._update_status(last_tick_at=tick.event_time)
        try:
            self._market_data.upsert_price(tick.symbol, tick.mark_price, tick.event_time)
        except Exception:  # noqa: BLE001 - a database hiccup must not stop the feed
            logger.exception("Price feed: storing the %s price failed", tick.symbol)
        for listener in listeners:
            try:
                listener(tick)
            except Exception:  # noqa: BLE001 - a listener bug must not stop the feed
                logger.exception("Price feed: a listener failed")

    def _update_status(self, **changes: Any) -> None:
        with self._lock:
            current = self._status
            self._status = FeedStatus(
                connected=changes.get("connected", current.connected),
                symbols=changes.get("symbols", current.symbols),
                last_tick_at=changes.get("last_tick_at", current.last_tick_at),
                last_error=changes.get("last_error", current.last_error),
            )
