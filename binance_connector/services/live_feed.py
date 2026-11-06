"""The live feed (spec §7): what the WebSocket tells its clients, and to whom.

``LiveFeedHub`` builds every message and hands it to the subscribed clients. It does not
know about sockets: the WebSocket server (``services.websocket_server``) subscribes one
client per connection. Every message has the same envelope::

    {"type": "...", "sent_at": "<ISO 8601 UTC>", "data": {...}}

Message types:

* ``snapshot``: on connect and on ``{"type": "resync"}``. ``data.positions`` is every
  active position (spec §6's fields, with the latest price) and ``data.report_sequence``
  is the last report the snapshot already reflects. A client that missed messages
  (disconnected, or too slow) reconnects and starts again from a snapshot.
* ``position_tick``: on every mark-price tick (about 1 per second per symbol), one per
  active position on that symbol: price, P&L, TP/SL distance and the tick time.
* ``position_update`` / ``request_update``: every report written for Django (see
  ``persistence.reports``), with ``event`` (e.g. ``POSITION_CLOSED``) and
  ``report_sequence``. ``data`` is the same body Django receives.
* ``service_status``: every few seconds, to every client: the price feed, the account
  streams, report delivery counts and the number of connected clients.

A client subscribed with an ``account_id`` gets that account's ticks and updates only;
``snapshot`` is filtered the same way, ``service_status`` is not.

Threads: ``on_price_tick`` runs on the price feed's thread and ``run`` on its own; both
call ``Subscriber.deliver``, which must therefore be thread-safe and must not block.
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol

from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.market_data import MarketDataRepository, MarketPrice
from binance_connector.persistence.positions import Position, PositionRepository
from binance_connector.persistence.reports import ReportRepository
from binance_connector.persistence.writes import utc_now
from binance_connector.presentation import position_view
from binance_connector.services.price_feed import FeedStatus, PriceTick

logger = logging.getLogger(__name__)

POLL_SECONDS = 0.5
"""How often new reports are looked for (and so the delay of an update, at most)."""
STATUS_SECONDS = 5.0
REFRESH_SECONDS = 10.0
"""How often the positions that get ticks are re-read, besides after every position report."""


class Subscriber(Protocol):
    account_id: str | None
    """Only this account's messages, or every account's if None."""

    def deliver(self, message: dict[str, Any]) -> None:
        """Queue a message for sending. Called from any thread; must not block."""


class PriceSource(Protocol):
    def latest(self, symbol: str) -> PriceTick | None: ...

    def status(self) -> FeedStatus: ...


class LiveFeedHub:
    def __init__(
        self,
        *,
        positions: PositionRepository,
        accounts: AccountRepository,
        reports: ReportRepository,
        market_data: MarketDataRepository,
        price_feed: PriceSource,
        stream_status: Callable[[], dict[str, bool]],
        clock: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        poll_seconds: float = POLL_SECONDS,
        status_seconds: float = STATUS_SECONDS,
        refresh_seconds: float = REFRESH_SECONDS,
    ) -> None:
        self._positions = positions
        self._accounts = accounts
        self._reports = reports
        self._market_data = market_data
        self._price_feed = price_feed
        self._stream_status = stream_status
        self._clock = clock
        self._monotonic = monotonic
        self._sleep = sleep
        self._poll_seconds = poll_seconds
        self._status_seconds = status_seconds
        self._refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._subscribers: list[Subscriber] = []
        self._active_by_symbol: dict[str, list[Position]] = {}
        self._user_ids: dict[str, str] = {}
        self._last_sequence = 0

    # --- subscribers --------------------------------------------------------------------------

    def subscribe(self, subscriber: Subscriber) -> None:
        with self._lock:
            self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: Subscriber) -> None:
        with self._lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)

    @property
    def client_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    # --- messages -----------------------------------------------------------------------------

    def snapshot(self, account_id: str | None = None) -> dict[str, Any]:
        """Every active position (of one account, if given), up to date with ``report_sequence``.

        The sequence is read *before* the positions, so every report after it is either
        already reflected or still to come; a client can drop updates up to it.
        """
        report_sequence = self._reports.last_sequence()
        views = [self._view(position, self._price(position.symbol))
                 for position in self._positions.list_active(account_id)]
        return self._message("snapshot", {"report_sequence": report_sequence, "positions": views})

    def on_price_tick(self, tick: PriceTick) -> None:
        """Send a ``position_tick`` for every active position on the tick's symbol."""
        with self._lock:
            affected = list(self._active_by_symbol.get(tick.symbol, ()))
        market_price = MarketPrice(tick.symbol, tick.mark_price, tick.event_time, tick.event_time)
        for position in affected:
            view = self._view(position, market_price)
            data = {key: view[key] for key in ("position_id", "account_id", "user_id", "symbol", "direction",
                                               "state", "prices", "pnl", "take_profit", "stop_loss")}
            data["updated_at"] = tick.event_time.isoformat()
            self._broadcast(self._message("position_tick", data), account_id=position.account_id)

    def start_following_reports(self) -> None:
        """Publish only reports written from now on (clients get the past from a snapshot)."""
        self._last_sequence = self._reports.last_sequence()

    def publish_new_reports(self) -> int:
        """Send every report written since the last call. Returns how many were sent."""
        new_reports = self._reports.after(self._last_sequence)
        if not new_reports:
            return 0
        if any(report.event_type.startswith(("POSITION_", "PROTECTION_")) for report in new_reports):
            self.refresh_positions()
        for report in new_reports:
            message_type = "request_update" if report.event_type.startswith("REQUEST_") else "position_update"
            message = self._message(message_type, json.loads(report.payload))
            message["event"] = report.event_type
            message["report_sequence"] = report.sequence
            self._broadcast(message, account_id=report.account_id)
            self._last_sequence = report.sequence
        return len(new_reports)

    def service_status(self) -> dict[str, Any]:
        feed = self._price_feed.status()
        return self._message("service_status", {
            "price_feed": {
                "connected": feed.connected,
                "symbols": sorted(feed.symbols),
                "last_tick_at": None if feed.last_tick_at is None else feed.last_tick_at.isoformat(),
                "last_error": feed.last_error,
            },
            "account_streams": {account_id: {"connected": connected}
                                for account_id, connected in sorted(self._stream_status().items())},
            "reports": self._reports.counts(),
            "clients": self.client_count,
        })

    def publish_service_status(self) -> None:
        self._broadcast(self.service_status())

    def refresh_positions(self) -> None:
        """Re-read the active positions that get ticks."""
        by_symbol: dict[str, list[Position]] = {}
        for position in self._positions.list_active():
            by_symbol.setdefault(position.symbol, []).append(position)
        with self._lock:
            self._active_by_symbol = by_symbol

    # --- the loop -----------------------------------------------------------------------------

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Publish new reports and the service status until ``should_stop()``."""
        self.start_following_reports()
        self.refresh_positions()
        next_status_at = next_refresh_at = self._monotonic()
        while not should_stop():
            now = self._monotonic()
            try:
                if now >= next_refresh_at:
                    next_refresh_at = now + self._refresh_seconds
                    self.refresh_positions()
                self.publish_new_reports()
                if now >= next_status_at:
                    next_status_at = now + self._status_seconds
                    self.publish_service_status()
            except Exception:  # noqa: BLE001 - the feed must keep going
                logger.exception("Live feed pass failed")
            self._sleep(self._poll_seconds)

    # --- helpers ------------------------------------------------------------------------------

    def _broadcast(self, message: dict[str, Any], *, account_id: str | None = None) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            if account_id is not None and subscriber.account_id not in (None, account_id):
                continue
            try:
                subscriber.deliver(message)
            except Exception:  # noqa: BLE001 - one broken client must not stop the others
                logger.exception("Live feed: delivering to a client failed")

    def _message(self, message_type: str, data: dict[str, Any]) -> dict[str, Any]:
        return {"type": message_type, "sent_at": self._clock().isoformat(), "data": data}

    def _view(self, position: Position, market_price: MarketPrice | None) -> dict[str, Any]:
        return position_view(position, user_id=self._user_id(position.account_id), market_price=market_price)

    def _user_id(self, account_id: str) -> str:
        # An account's owner never changes (intake refuses another owner), so it is cached.
        with self._lock:
            cached = self._user_ids.get(account_id)
        if cached is None:
            cached = self._accounts.get(account_id).user_id
            with self._lock:
                self._user_ids[account_id] = cached
        return cached

    def _price(self, symbol: str) -> MarketPrice | None:
        tick = self._price_feed.latest(symbol)
        if tick is not None:
            return MarketPrice(symbol, tick.mark_price, tick.event_time, tick.event_time)
        return self._market_data.price(symbol)
