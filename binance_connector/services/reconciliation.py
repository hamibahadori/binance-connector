"""Reconciliation: bring each account's active positions in line with what Binance holds.

Ported from v1 onto execution records and accounts. Each account's stream scheduler runs it
after every (re)connect, every ``RECONCILE_INTERVAL_SECONDS``, and a few seconds after an
event flags a symbol. It is what makes the service safe across restarts and missed events
(spec §14): whatever the database last said, Binance's records decide.

By position state:

* **OPENING**
  * entry not yet sent: left to the execution queue;
  * entry SUBMITTED: looked up after the grace period;
  * entry resting (a limit order): looked up to catch a fill the stream missed.
* **ERROR with nothing filled yet**: the entry is looked up. "Not found" is only trusted
  after the grace period, because REST can lag.
* **OPEN / ERROR (filled)**: the position and both TP/SL are read from Binance.
  * A TP/SL that FINISHED with a fill → CLOSED as TAKE_PROFIT / STOP_LOSS.
  * Flat, with no TP/SL fill → our exit order if it filled, otherwise CLOSED as EXTERNAL.
  * A TP/SL that never reached Binance → placed now. One placed but never recorded → adopted.
  * A TP/SL CANCELED/EXPIRED/REJECTED while the position is open, or a position resized
    outside the service → ERROR. Nothing is re-placed automatically (a customer policy).
  * All as expected → an ERROR position heals back to OPEN.
* **CLOSING**: the exit order is looked up. Filled → CLOSED. A TP/SL that beat it, or a flat
  position, is handled as above. Never reached Binance (after the grace period) → OPEN again.

One position failing (Binance unreachable, a rejection) is logged and retried on the next
pass; it never stops the others.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.errors import BinanceAPIError, BinanceError, InvalidTransitionError
from binance_connector.persistence.positions import Position, PositionRepository, client_id_for
from binance_connector.persistence.writes import utc_now
from binance_connector.services.position_manager import (
    LIVE_ORDER_STATUSES,
    PROTECTION_LEGS,
    RETRYABLE_STATUSES,
    ClientSource,
    PositionManager,
    net_position,
)
from binance_connector.services.stream_events import binance_time

logger = logging.getLogger(__name__)

DEFAULT_GRACE_SECONDS = 120
"""How long an order may stay SUBMITTED before "Binance has no such order" is believed.
Longer than the worst case of three Binance calls at the configured timeout."""

ORDER_DOES_NOT_EXIST = -2013
LIVE_ALGO_STATUSES = frozenset({"NEW", "TRIGGERING", "TRIGGERED"})
PROTECTION_LABELS = {role: label for role, _order_type, label in PROTECTION_LEGS}


class ReconciliationService:
    def __init__(
        self,
        *,
        positions: PositionRepository,
        clients: ClientSource,
        position_manager: PositionManager,
        clock: Callable[[], datetime] = utc_now,
        grace_seconds: float = DEFAULT_GRACE_SECONDS,
    ) -> None:
        self._positions = positions
        self._clients = clients
        self._position_manager = position_manager
        self._clock = clock
        self._grace_seconds = grace_seconds

    def reconcile_account(self, account_id: str) -> None:
        for position in self._positions.list_active(account_id=account_id):
            self._reconcile_safely(position.position_id)

    def reconcile_symbol(self, account_id: str, symbol: str) -> None:
        position = self._positions.find_active(account_id, symbol)
        if position is not None:
            self._reconcile_safely(position.position_id)

    def reconcile_position(self, position_id: str) -> None:
        """Reconcile one position. Binance errors propagate (``reconcile_account`` logs them)."""
        position = self._positions.get(position_id)
        client = self._clients.for_account(position.account_id)
        entry = position.executions.get(ExecutionRole.ENTRY)
        if position.state is PositionState.OPENING:
            if entry is None or entry.status in RETRYABLE_STATUSES:
                return  # the execution queue has not sent it yet
            if entry.status == "SUBMITTED" and not self._past_grace(entry.submitted_at, position):
                return
            self._resolve_entry(position, client)
        elif position.state is PositionState.ERROR and not position.entry_filled_quantity:
            if entry is None or entry.status in RETRYABLE_STATUSES:
                self._fail(position, "The entry was never sent to Binance.")
            else:
                self._resolve_entry(position, client)
        elif position.state is PositionState.CLOSING:
            self._resolve_closing(position, client)
        else:
            self._resolve_open(position, client)

    # --- entry --------------------------------------------------------------------------------

    def _resolve_entry(self, position: Position, client) -> None:
        entry = position.executions[ExecutionRole.ENTRY]
        order = _find_order(client, position.symbol, entry.client_id)
        if order is None:
            if self._past_grace(entry.submitted_at, position):
                self._fail(position, f"The entry never reached Binance (no order {entry.client_id}).")
            return
        executed = Decimal(order.get("executedQty") or "0")
        average = _positive_or_none(order.get("avgPrice"))
        self._positions.update_execution(position.position_id, ExecutionRole.ENTRY, status=order["status"],
                                         binance_id=order["orderId"], executed_quantity=executed,
                                         average_price=average, event_detail={"source": "reconciliation"})
        if executed > 0:
            logger.info("Position %s: entry found filled on Binance", position.position_id)
            self._position_manager.apply_entry_fill(position.position_id, executed_quantity=executed,
                                                    average_price=average,
                                                    filled_at=binance_time(order["updateTime"]))
        elif order["status"] in LIVE_ORDER_STATUSES:
            if position.state is PositionState.ERROR:
                self._transition(position, PositionState.OPENING, last_error=None)  # a resting limit after all
        else:
            self._fail(position, f"Binance reports the entry as {order['status']}.")

    # --- open position ------------------------------------------------------------------------

    def _resolve_open(self, position: Position, client) -> None:
        held_amount = net_position(client, position.symbol)
        legs = self._fetch_legs(position, client)
        position = self._record_legs(position, legs)

        filled_role = _filled_leg(legs)
        if filled_role is not None:
            self._close_from_leg(position, filled_role, legs[filled_role])
            return
        if held_amount == 0:
            self._close_flat(position, client)
            return

        problems, missing, waiting = self._assess_legs(position, held_amount, legs)
        if problems:
            self._transition(position, PositionState.ERROR, last_error=" ".join(problems))
            return
        if waiting:
            return
        if missing:
            position = self._position_manager.protect(position.position_id)
            if any(position.executions.get(role) is None or position.executions[role].status not in LIVE_ALGO_STATUSES
                   for role in missing):
                return  # protect() recorded what failed
        if position.state is PositionState.ERROR:
            self._transition(position, PositionState.OPEN, last_error=None)

    def _assess_legs(self, position: Position, held_amount: Decimal,
                     legs: dict[ExecutionRole, dict | None]) -> tuple[list[str], list[ExecutionRole], bool]:
        problems: list[str] = []
        missing: list[ExecutionRole] = []
        waiting = False
        if position.entry_filled_quantity and abs(held_amount) != position.entry_filled_quantity:
            problems.append(f"The position on Binance is {held_amount}, but the position holds "
                            f"{position.entry_filled_quantity}: it was changed outside the service.")
        for role, algo_order in legs.items():
            label = PROTECTION_LABELS[role]
            execution = position.executions.get(role)
            if algo_order is not None:
                if algo_order["algoStatus"] not in LIVE_ALGO_STATUSES:
                    problems.append(f"The {label} is {algo_order['algoStatus']} on Binance while the position is open.")
            elif execution is None or execution.status in RETRYABLE_STATUSES:
                missing.append(role)
            elif execution.status == "SUBMITTED":
                if self._past_grace(execution.submitted_at, position):
                    self._positions.update_execution(position.position_id, role, status="REJECTED",
                                                     last_error="never reached Binance")
                    missing.append(role)
                else:
                    waiting = True
            elif not self._past_grace(execution.submitted_at, position):
                # Placed moments ago: Binance's REST can lag behind its own reply (on Demo it
                # answered -2013 for a take-profit placed 0.9 s earlier). Look again next pass.
                waiting = True
            else:
                problems.append(f"The {label} is no longer known to Binance while the position is open.")
        return problems, missing, waiting

    # --- closing ------------------------------------------------------------------------------

    def _resolve_closing(self, position: Position, client) -> None:
        exit_execution = position.executions.get(ExecutionRole.EXIT)
        exit_order = None if exit_execution is None else _find_order(client, position.symbol, exit_execution.client_id)
        if exit_order is not None and exit_order["status"] == "FILLED":
            self._close_requested(position, exit_order)
            return
        if exit_order is not None and exit_order["status"] in LIVE_ORDER_STATUSES:
            return

        held_amount = net_position(client, position.symbol)
        legs = self._fetch_legs(position, client)
        position = self._record_legs(position, legs)
        filled_role = _filled_leg(legs)
        if filled_role is not None:
            self._close_from_leg(position, filled_role, legs[filled_role])
        elif held_amount == 0:
            self._close_flat(position, client)
        elif exit_order is None and self._past_grace(exit_execution.submitted_at if exit_execution else None, position):
            if exit_execution is not None:
                self._positions.update_execution(position.position_id, ExecutionRole.EXIT, status="REJECTED",
                                                 last_error="never reached Binance")
            self._transition(position, PositionState.OPEN, last_error="The close never reached Binance; the "
                                                                      "position is OPEN again.")

    def _close_from_leg(self, position: Position, role: ExecutionRole, algo_order: dict[str, Any]) -> None:
        logger.info("Position %s: found closed by its %s", position.position_id, PROTECTION_LABELS[role])
        self._position_manager.close_by_protection(
            position.position_id, role, exit_average_price=_positive_or_none(algo_order.get("actualPrice")),
            closed_at=binance_time(algo_order.get("triggerTime") or algo_order["updateTime"]),
        )

    def _close_flat(self, position: Position, client) -> None:
        exit_execution = position.executions.get(ExecutionRole.EXIT)
        exit_order = None if exit_execution is None else _find_order(client, position.symbol, exit_execution.client_id)
        if exit_order is not None and exit_order["status"] == "FILLED":
            self._close_requested(position, exit_order)
            return
        logger.warning("Position %s: closed outside the service", position.position_id)
        self._position_manager.finish_closed(
            position.position_id, CloseReason.EXTERNAL, exit_average_price=None, closed_at=self._clock(),
            note="The position closed without any order from this service (for example a liquidation, or a "
                 "trade made by hand on Binance).",
        )
        self._position_manager.cancel_protection(position.position_id)

    def _close_requested(self, position: Position, exit_order: dict[str, Any]) -> None:
        average = _positive_or_none(exit_order.get("avgPrice"))
        self._positions.update_execution(position.position_id, ExecutionRole.EXIT, status="FILLED",
                                         binance_id=exit_order["orderId"],
                                         executed_quantity=Decimal(exit_order.get("executedQty") or "0"),
                                         average_price=average, event_detail={"source": "reconciliation"})
        self._position_manager.cancel_protection(position.position_id)
        self._position_manager.finish_closed(position.position_id, CloseReason.CLOSE_REQUESTED,
                                             exit_average_price=average,
                                             closed_at=binance_time(exit_order["updateTime"]))

    # --- Binance reads ------------------------------------------------------------------------

    def _fetch_legs(self, position: Position, client) -> dict[ExecutionRole, dict[str, Any] | None]:
        """Binance's record of each *requested* TP/SL (None: Binance has no such order)."""
        legs = {}
        for role, _order_type, _label in PROTECTION_LEGS:
            trigger_price = position.take_profit_price if role is ExecutionRole.TAKE_PROFIT else position.stop_loss_price
            if trigger_price is None:
                continue
            try:
                legs[role] = client.get_algo_order(client_algo_id=client_id_for(position.position_id, role))
            except BinanceAPIError as error:
                if error.code != ORDER_DOES_NOT_EXIST:
                    raise
                legs[role] = None
        return legs

    def _record_legs(self, position: Position, legs: dict[ExecutionRole, dict | None]) -> Position:
        for role, algo_order in legs.items():
            if algo_order is None:
                continue
            updates: dict[str, Any] = {}
            execution = position.executions.get(role)
            if execution is None:
                order_type = "TAKE_PROFIT_MARKET" if role is ExecutionRole.TAKE_PROFIT else "STOP_MARKET"
                self._positions.prepare_execution(position.position_id, role, kind="ALGO", order_type=order_type,
                                                  side=algo_order.get("side", ""))
            if execution is None or execution.status != algo_order["algoStatus"]:
                updates["status"] = algo_order["algoStatus"]
            if execution is None or execution.binance_id is None:
                updates["binance_id"] = algo_order["algoId"]
            if algo_order["algoStatus"] == "FINISHED":
                updates["executed_quantity"] = Decimal(algo_order.get("actualQty") or "0")
                updates["average_price"] = _positive_or_none(algo_order.get("actualPrice"))
            if updates:
                position = self._positions.update_execution(position.position_id, role,
                                                            event_detail={"source": "reconciliation"}, **updates)
        return self._positions.get(position.position_id)

    # --- helpers ------------------------------------------------------------------------------

    def _past_grace(self, since: datetime | None, position: Position) -> bool:
        reference_time = since or position.updated_at
        return (self._clock() - reference_time).total_seconds() >= self._grace_seconds

    def _fail(self, position: Position, reason: str) -> None:
        self._transition(position, PositionState.FAILED, last_error=reason)

    def _transition(self, position: Position, target_state: PositionState, **field_updates) -> None:
        try:
            self._positions.transition(position.position_id, target_state,
                                       event_detail={"source": "reconciliation"}, **field_updates)
        except InvalidTransitionError:
            pass  # another thread moved it on first

    def _reconcile_safely(self, position_id: str) -> None:
        try:
            self.reconcile_position(position_id)
        except BinanceError as error:
            logger.warning("Could not reconcile position %s this time: %s", position_id, error)
        except Exception:  # noqa: BLE001 - one position must never stop the others
            logger.exception("Reconciling position %s failed", position_id)


def _find_order(client, symbol: str, client_order_id: str) -> dict[str, Any] | None:
    try:
        return client.get_order(symbol=symbol, client_order_id=client_order_id)
    except BinanceAPIError as error:
        if error.code == ORDER_DOES_NOT_EXIST:
            return None
        raise


def _filled_leg(legs: dict[ExecutionRole, dict | None]) -> ExecutionRole | None:
    for role, algo_order in legs.items():
        if algo_order and algo_order["algoStatus"] == "FINISHED" and Decimal(algo_order.get("actualQty") or "0") > 0:
            return role
    return None


def _positive_or_none(raw_value) -> Decimal | None:
    if raw_value is None or Decimal(raw_value) == 0:
        return None
    return Decimal(raw_value)
