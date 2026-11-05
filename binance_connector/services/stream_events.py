"""Apply Binance user-data stream events to trades.

Field names are the ones Binance Demo actually sent (recorded 2026-11-05):

* ``ALGO_UPDATE.o``: ``caid`` client algo id, ``aid`` algo id, ``X`` status
  (NEW → TRIGGERING → TRIGGERED → FINISHED, or CANCELED / EXPIRED / REJECTED),
  ``ai`` the order it created, ``ap`` average fill price, ``aq`` filled quantity.
* ``ORDER_TRADE_UPDATE.o``: ``c`` client order id, ``i`` order id, ``X`` status, ``ap``
  average price, ``z`` cumulative filled quantity, ``T`` trade time (ms). An order created
  by a TP/SL trigger carries the TP/SL's client algo id as its ``c``.
* ``ACCOUNT_UPDATE.a.P[]``: ``s`` symbol, ``pa`` signed position amount.

Rules:

* A TP/SL fill closes the trade. Whichever of its two events (the algo FINISHED or the
  created order FILLED) arrives first does it; the other then finds the trade CLOSED.
* An exit fill completes a close whose outcome was unknown (trade still CLOSING).
* An entry fill resumes a trade whose entry outcome was unknown (NEEDS_ATTENTION), then
  places its protection. Entry fills for ENTRY_SUBMITTED trades are left alone: the API
  is still handling those synchronously.
* Anything doubtful (a leg EXPIRED/CANCELED/REJECTED while the trade is still protected, a
  position change nobody explains) is not judged from a single event, because events can
  arrive in any order across types. The handler only asks the worker to reconcile the
  symbol shortly afterwards, against Binance's REST state.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import InvalidTransitionError
from binance_connector.persistence.repository import TradeRepository
from binance_connector.services.trade_service import TradeService

logger = logging.getLogger(__name__)

PROTECTION_CLOSE_REASONS = {"take_profit": CloseReason.TAKE_PROFIT, "stop_loss": CloseReason.STOP_LOSS}


def binance_time(milliseconds: int) -> datetime:
    """Binance millisecond timestamps as aware UTC datetimes."""
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


class UserDataEventHandler:
    """Turns one stream event into trade changes. Returns the symbols to reconcile soon."""

    def __init__(self, *, repository: TradeRepository, trade_service: TradeService) -> None:
        self._repository = repository
        self._trade_service = trade_service

    def handle(self, event: dict[str, Any]) -> set[str]:
        event_type = event.get("e")
        if event_type == "ALGO_UPDATE":
            self._on_algo_update(event)
        elif event_type == "ORDER_TRADE_UPDATE":
            self._on_order_update(event)
        elif event_type == "ACCOUNT_UPDATE":
            return self._symbols_with_active_trades(event)
        return set()

    def _on_algo_update(self, event: dict[str, Any]) -> None:
        algo_order = event["o"]
        found = self._repository.find_by_client_id(algo_order.get("caid", ""))
        if found is None or found[1] not in PROTECTION_CLOSE_REASONS:
            return
        trade, role = found
        status = algo_order["X"]
        self._repository.update_fields(
            trade.trade_id,
            event_detail={"binance_event": "ALGO_UPDATE", "order": algo_order},
            **{f"{role}_status": status, f"{role}_algo_id": algo_order.get("aid")},
        )
        if status == "FINISHED" and Decimal(algo_order.get("aq") or "0") > 0:
            self._close_by_protection(
                trade.trade_id,
                role,
                exit_order_id=int(algo_order["ai"]),
                exit_price=Decimal(algo_order["ap"]),
                closed_at=binance_time(event["T"]),
            )

    def _on_order_update(self, event: dict[str, Any]) -> None:
        order = event["o"]
        if order.get("X") != "FILLED":
            return
        found = self._repository.find_by_client_id(order.get("c", ""))
        if found is None:
            return
        trade, role = found
        self._repository.record_event(trade.trade_id, "BINANCE_EVENT", {"binance_event": "ORDER_TRADE_UPDATE",
                                                                        "order": order})
        filled_at = binance_time(order["T"])
        if role in PROTECTION_CLOSE_REASONS:
            self._close_by_protection(
                trade.trade_id, role, exit_order_id=order["i"], exit_price=Decimal(order["ap"]), closed_at=filled_at
            )
        elif role == "exit" and trade.state is TradeState.CLOSING:
            self._close_requested(trade.trade_id, trade.requested_close_reason, order, filled_at)
        elif role == "entry" and trade.state is TradeState.NEEDS_ATTENTION and trade.entry_order_id is None:
            self._resume_entry(trade.trade_id, order, filled_at)

    def _close_by_protection(self, trade_id: str, role: str, *, exit_order_id: int, exit_price: Decimal,
                             closed_at: datetime) -> None:
        try:
            self._repository.transition(
                trade_id,
                TradeState.CLOSED,
                close_reason=PROTECTION_CLOSE_REASONS[role],
                exit_order_id=exit_order_id,
                exit_price=exit_price,
                closed_at=closed_at,
            )
        except InvalidTransitionError:
            return  # already closed by the other event of the same fill
        logger.info("Trade %s closed by its %s at %s", trade_id, role, exit_price)
        self._trade_service.cancel_protection(trade_id)

    def _close_requested(self, trade_id: str, reason: CloseReason | None, order: dict[str, Any],
                         filled_at: datetime) -> None:
        try:
            self._repository.transition(
                trade_id,
                TradeState.CLOSED,
                close_reason=reason or CloseReason.MANUAL,
                exit_order_id=order["i"],
                exit_price=Decimal(order["ap"]),
                closed_at=filled_at,
                last_error=None,
            )
        except InvalidTransitionError:
            return
        self._trade_service.cancel_protection(trade_id)

    def _resume_entry(self, trade_id: str, order: dict[str, Any], filled_at: datetime) -> None:
        try:
            self._repository.transition(
                trade_id,
                TradeState.OPEN,
                entry_order_id=order["i"],
                entry_price=Decimal(order["ap"]),
                entry_quantity=Decimal(order["z"]),
                opened_at=filled_at,
                last_error=None,
            )
        except InvalidTransitionError:
            return
        logger.info("Trade %s: entry confirmed by the stream; placing protection", trade_id)
        self._trade_service.protect(trade_id)

    def _symbols_with_active_trades(self, event: dict[str, Any]) -> set[str]:
        positions = event.get("a", {}).get("P", [])
        return {
            position["s"] for position in positions
            if self._repository.find_active_by_symbol(position["s"]) is not None
        }
