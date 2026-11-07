"""Apply one account's Binance user-data stream events to its positions.

Ported from v1 onto execution records. Field names are what Binance Demo actually sent
(recorded 2026-11-05 and 2026-11-07):

* ``ALGO_UPDATE.o``: ``caid`` client algo id, ``aid`` algo id, ``X`` status
  (NEW → TRIGGERING → TRIGGERED → FINISHED, or CANCELED / EXPIRED / REJECTED),
  ``ap`` average fill price, ``aq`` filled quantity.
* ``ORDER_TRADE_UPDATE.o``: ``c`` client order id, ``i`` order id, ``x`` execution type,
  ``X`` status, ``z`` cumulative filled quantity, ``ap`` average price, ``n`` this fill's
  commission, ``T`` trade time (ms). An order created by a TP/SL trigger carries the TP/SL's
  client algo id as its ``c``.
* ``ACCOUNT_UPDATE.a.P[]``: ``s`` symbol, ``pa`` signed position amount.

Rules:

* A TP/SL fill closes the position; whichever of its two events (algo FINISHED, or the order
  it created FILLED) arrives first does it.
* An entry fill opens the position (a resting limit filled, or an entry whose outcome was
  unknown) and places its protection; later partial fills update the filled quantity.
* An exit fill completes a close whose outcome was unknown (position still CLOSING).
* Commissions are not taken from the stream: the position manager reads them over REST when
  an order fills, because an account's stream can start after its first fill (Demo, 2026-11-07).
* Anything doubtful (a leg expiring, a position change nobody explains) is not judged from
  one event, since events of different types can arrive in any order. The handler returns
  the symbols for the worker to reconcile shortly afterwards, from Binance's REST state.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.persistence.positions import Position, PositionRepository
from binance_connector.services.position_manager import PositionManager

logger = logging.getLogger(__name__)

PROTECTION_ROLES = frozenset({ExecutionRole.TAKE_PROFIT, ExecutionRole.STOP_LOSS})


def binance_time(milliseconds: int) -> datetime:
    """Binance millisecond timestamps as aware UTC datetimes."""
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


class UserDataEventHandler:
    """Turns one stream event of ``account_id`` into position changes. Returns symbols to reconcile."""

    def __init__(self, *, account_id: str, positions: PositionRepository, position_manager: PositionManager) -> None:
        self._account_id = account_id
        self._positions = positions
        self._position_manager = position_manager

    def handle(self, event: dict[str, Any]) -> set[str]:
        event_type = event.get("e")
        if event_type == "ALGO_UPDATE":
            self._on_algo_update(event)
        elif event_type == "ORDER_TRADE_UPDATE":
            self._on_order_update(event)
        elif event_type == "ACCOUNT_UPDATE":
            return self._symbols_with_active_positions(event)
        return set()

    def _on_algo_update(self, event: dict[str, Any]) -> None:
        algo_order = event["o"]
        found = self._find(algo_order.get("caid", ""))
        if found is None or found[1] not in PROTECTION_ROLES:
            return
        position, role = found
        status = algo_order["X"]
        current = position.executions.get(role)
        if current is not None and _algo_rank(status) < _algo_rank(current.status):
            return  # a late update (the fill can arrive first): never move the leg backwards
        updates: dict[str, Any] = {"status": status, "binance_id": algo_order.get("aid")}
        filled_quantity = Decimal(algo_order.get("aq") or "0")
        if status == "FINISHED" and filled_quantity > 0:
            updates.update(executed_quantity=filled_quantity, average_price=Decimal(algo_order["ap"]))
        self._positions.update_execution(position.position_id, role,
                                         event_detail={"binance_event": "ALGO_UPDATE"}, **updates)
        if status == "FINISHED" and filled_quantity > 0:
            self._position_manager.close_by_protection(position.position_id, role,
                                                       exit_average_price=Decimal(algo_order["ap"]),
                                                       closed_at=binance_time(event["T"]))

    def _on_order_update(self, event: dict[str, Any]) -> None:
        order = event["o"]
        found = self._find(order.get("c", ""))
        if found is None:
            return
        position, role = found
        filled_quantity = Decimal(order.get("z") or "0")
        average_price = _positive_or_none(order.get("ap"))
        updates: dict[str, Any] = {"executed_quantity": filled_quantity}
        if average_price is not None:
            updates["average_price"] = average_price
        if role not in PROTECTION_ROLES:
            # A TP/SL's status is its algo order's (ALGO_UPDATE), not the order it created.
            updates.update(status=order["X"], binance_id=order.get("i"))
        position = self._positions.update_execution(position.position_id, role,
                                                    event_detail={"binance_event": "ORDER_TRADE_UPDATE"}, **updates)

        filled_at = binance_time(order["T"])
        if role in PROTECTION_ROLES and order["X"] == "FILLED":
            self._position_manager.close_by_protection(position.position_id, role, exit_average_price=average_price,
                                                       closed_at=filled_at)
        elif role is ExecutionRole.EXIT and order["X"] == "FILLED" and position.state is PositionState.CLOSING:
            self._position_manager.cancel_protection(position.position_id)
            self._position_manager.finish_closed(position.position_id, CloseReason.CLOSE_REQUESTED,
                                                 exit_average_price=average_price, closed_at=filled_at)
        elif role is ExecutionRole.ENTRY and filled_quantity > 0:
            self._position_manager.apply_entry_fill(position.position_id, executed_quantity=filled_quantity,
                                                    average_price=average_price, filled_at=filled_at)

    def _find(self, client_id: str) -> tuple[Position, ExecutionRole] | None:
        found = self._positions.find_by_client_id(client_id)
        if found is None or found[0].account_id != self._account_id:
            return None
        return found

    def _symbols_with_active_positions(self, event: dict[str, Any]) -> set[str]:
        symbols = {position_entry["s"] for position_entry in event.get("a", {}).get("P", [])}
        return {symbol for symbol in symbols if self._positions.find_active(self._account_id, symbol) is not None}


def _positive_or_none(raw_value) -> Decimal | None:
    if raw_value is None or Decimal(raw_value) == 0:
        return None
    return Decimal(raw_value)


ALGO_STATUS_ORDER = {"PENDING": 0, "SUBMITTED": 0, "NEW": 1, "TRIGGERING": 2, "TRIGGERED": 3}
"""An algo order moves NEW → TRIGGERING → TRIGGERED → a final status; anything else is final."""


def _algo_rank(status: str) -> int:
    return ALGO_STATUS_ORDER.get(status, 4)
