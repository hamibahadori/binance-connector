"""Reconciliation: bring every active trade in line with what Binance actually holds.

The worker runs this after every stream (re)connect, every ``reconcile_interval_seconds``,
and a few seconds after an event flags a symbol. It is what makes the service safe across
restarts and missed events: whatever the database last said, Binance's REST records
decide.

What it resolves, by trade state:

* **RECEIVED / ENTRY_SUBMITTED**: left to the API during a grace period (it is entering
  synchronously). After that, the entry order is looked up by its client id: filled →
  OPEN and protected; unknown to Binance → ENTRY_FAILED.
* **NEEDS_ATTENTION with no entry yet**: the same entry lookup, without the grace period
  for a fill. "Not found" still waits for the grace period, because REST can lag.
* **OPEN / PROTECTED / NEEDS_ATTENTION**: the position and both TP/SL are read from Binance.
  * A TP/SL that FINISHED with a fill → CLOSED as TAKE_PROFIT / STOP_LOSS.
  * Flat with no TP/SL fill → our exit order if it filled, otherwise CLOSED as EXTERNAL
    (liquidation, a trade made by hand, …).
  * A TP/SL never placed (crash) → placed now. One placed but never recorded → adopted.
  * A TP/SL CANCELED/EXPIRED/REJECTED while the position is open, or a position resized
    outside the service → NEEDS_ATTENTION. Nothing is re-placed automatically: what to do
    then is a policy question for the customer (phase 7).
  * Everything as expected → a NEEDS_ATTENTION trade heals back to PROTECTED / OPEN.
* **CLOSING**: our exit order is looked up. Filled → CLOSED. A TP/SL that beat it, or a
  flat position, is handled as above. Never reached Binance (after the grace period) →
  back to PROTECTED / OPEN.

One trade failing (Binance unreachable, a rejection) is logged and retried on the next
pass; it never stops the others.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import BinanceAPIError, BinanceError, InvalidTransitionError
from binance_connector.persistence.repository import Trade, TradeRepository, utc_now
from binance_connector.services.position_service import FuturesClient, PositionService
from binance_connector.services.stream_events import binance_time
from binance_connector.services.trade_service import PROTECTION_LEGS, TradeService

logger = logging.getLogger(__name__)

DEFAULT_ENTRY_GRACE_SECONDS = 120
"""How long the API may take to finish an entry or close before the worker steps in.
Longer than the API's worst case: three Binance calls at the configured timeout."""

ORDER_DOES_NOT_EXIST = -2013
STILL_EXECUTING_ORDER_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED"})
LIVE_ALGO_STATUSES = frozenset({"NEW", "TRIGGERING", "TRIGGERED"})
PROTECTION_CLOSE_REASONS = {"take_profit": CloseReason.TAKE_PROFIT, "stop_loss": CloseReason.STOP_LOSS}
PROTECTION_LABELS = {role: label for role, _order_type, label in PROTECTION_LEGS}


class ReconciliationService:
    """Compares stored trades with Binance and moves each one to its true state."""

    def __init__(
        self,
        *,
        repository: TradeRepository,
        client: FuturesClient,
        position_service: PositionService,
        trade_service: TradeService,
        clock: Callable[[], datetime] = utc_now,
        entry_grace_seconds: float = DEFAULT_ENTRY_GRACE_SECONDS,
    ) -> None:
        self._repository = repository
        self._client = client
        self._position_service = position_service
        self._trade_service = trade_service
        self._clock = clock
        self._entry_grace_seconds = entry_grace_seconds

    def reconcile_all(self) -> None:
        """Reconcile every active trade."""
        for trade in self._repository.list_active():
            self._reconcile_safely(trade.trade_id)

    def reconcile_symbol(self, symbol: str) -> None:
        """Reconcile the active trade on ``symbol``, if there is one."""
        trade = self._repository.find_active_by_symbol(symbol)
        if trade is not None:
            self._reconcile_safely(trade.trade_id)

    def reconcile_trade(self, trade_id: str) -> None:
        """Reconcile one trade. Binance errors propagate (``reconcile_all`` logs them)."""
        trade = self._repository.get(trade_id)
        if trade.state is TradeState.RECEIVED:
            if self._past_grace(trade):
                self._transition(trade, TradeState.ENTRY_FAILED,
                                 last_error="The entry was never sent to Binance (the service stopped first).")
        elif trade.state is TradeState.ENTRY_SUBMITTED:
            if self._past_grace(trade):
                self._resolve_entry(trade)
        elif trade.state is TradeState.NEEDS_ATTENTION and trade.entry_order_id is None:
            self._resolve_entry(trade)
        elif trade.state is TradeState.CLOSING:
            self._resolve_closing(trade)
        else:
            self._resolve_position(trade)

    # --- entry --------------------------------------------------------------------------------

    def _resolve_entry(self, trade: Trade) -> None:
        entry_order = self._find_order(trade.symbol, trade.entry_client_order_id)
        if entry_order is None:
            if self._past_grace(trade):
                self._transition(trade, TradeState.ENTRY_FAILED,
                                 last_error=f"The entry never reached Binance (no order {trade.entry_client_order_id}).")
            return
        status = entry_order["status"]
        if status in STILL_EXECUTING_ORDER_STATUSES:
            return
        executed_quantity = Decimal(entry_order.get("executedQty") or "0")
        if executed_quantity == 0:
            self._transition(trade, TradeState.ENTRY_FAILED, last_error=f"Binance reports the entry as {status}.")
            return
        opened = self._transition(
            trade,
            TradeState.OPEN,
            entry_order_id=entry_order["orderId"],
            entry_price=_positive_or_none(entry_order.get("avgPrice")),
            entry_quantity=executed_quantity,
            opened_at=binance_time(entry_order["updateTime"]),
            last_error=None,
        )
        if opened is not None:
            logger.info("Trade %s: entry found on Binance; placing protection", trade.trade_id)
            self._trade_service.protect(trade.trade_id)

    # --- open position ------------------------------------------------------------------------

    def _resolve_position(self, trade: Trade) -> None:
        position_amount = self._position_service.net_position(trade.symbol)
        legs = self._fetch_legs(trade)
        trade = self._record_leg_statuses(trade, legs)

        filled_role = _filled_leg(legs)
        if filled_role is not None:
            self._close_from_leg(trade, filled_role, legs[filled_role])
            return
        if position_amount == 0:
            self._close_flat(trade)
            return

        problems = self._problems(trade, position_amount, legs)
        if problems:
            self._transition(trade, TradeState.NEEDS_ATTENTION, last_error=" ".join(problems))
            return
        if any(algo_order is None for algo_order in legs.values()):
            self._trade_service.protect(trade.trade_id)  # places only the legs never placed
            return
        healthy_state = TradeState.PROTECTED if legs else TradeState.OPEN
        if trade.state is not healthy_state:
            self._transition(trade, healthy_state, last_error=None)

    def _problems(self, trade: Trade, position_amount: Decimal, legs: dict[str, dict | None]) -> list[str]:
        problems = []
        if trade.entry_quantity is not None and abs(position_amount) != trade.entry_quantity:
            problems.append(
                f"The position on Binance is {position_amount}, but the trade holds {trade.entry_quantity}: "
                "it was changed outside the service."
            )
        for role, algo_order in legs.items():
            label = PROTECTION_LABELS[role]
            if algo_order is None and getattr(trade, f"{role}_status") is not None:
                problems.append(f"The {label} is no longer known to Binance while the position is open.")
            elif algo_order is not None and algo_order["algoStatus"] not in LIVE_ALGO_STATUSES:
                problems.append(f"The {label} is {algo_order['algoStatus']} on Binance while the position is open.")
        return problems

    # --- closing ------------------------------------------------------------------------------

    def _resolve_closing(self, trade: Trade) -> None:
        exit_order = self._find_order(trade.symbol, trade.exit_client_order_id)
        if exit_order is not None and exit_order["status"] == "FILLED":
            self._close_requested(trade, exit_order)
            return
        if exit_order is not None and exit_order["status"] in STILL_EXECUTING_ORDER_STATUSES:
            return

        position_amount = self._position_service.net_position(trade.symbol)
        legs = self._fetch_legs(trade)
        trade = self._record_leg_statuses(trade, legs)
        filled_role = _filled_leg(legs)
        if filled_role is not None:
            self._close_from_leg(trade, filled_role, legs[filled_role])
        elif position_amount == 0:
            self._close_flat(trade)
        elif self._past_grace(trade):
            reverted_state = (
                TradeState.PROTECTED
                if any(algo_order and algo_order["algoStatus"] == "NEW" for algo_order in legs.values())
                else TradeState.OPEN
            )
            self._transition(trade, reverted_state,
                             last_error=f"The close never reached Binance; the trade is {reverted_state} again.")

    # --- closing helpers ----------------------------------------------------------------------

    def _close_from_leg(self, trade: Trade, role: str, algo_order: dict[str, Any]) -> None:
        closed = self._transition(
            trade,
            TradeState.CLOSED,
            close_reason=PROTECTION_CLOSE_REASONS[role],
            exit_order_id=int(algo_order["actualOrderId"]),
            exit_price=Decimal(algo_order["actualPrice"]),
            closed_at=binance_time(algo_order.get("triggerTime") or algo_order["updateTime"]),
            last_error=None,
        )
        if closed is not None:
            logger.info("Trade %s: found closed by its %s", trade.trade_id, PROTECTION_LABELS[role])
            self._trade_service.cancel_protection(trade.trade_id)

    def _close_flat(self, trade: Trade) -> None:
        exit_order = self._find_order(trade.symbol, trade.exit_client_order_id)
        if exit_order is not None and exit_order["status"] == "FILLED":
            self._close_requested(trade, exit_order)
            return
        closed = self._transition(
            trade,
            TradeState.CLOSED,
            close_reason=CloseReason.EXTERNAL,
            closed_at=self._clock(),
            last_error="The position closed without any order from this service "
            "(for example a liquidation, or a trade made by hand on Binance).",
        )
        if closed is not None:
            logger.warning("Trade %s: position closed outside the service", trade.trade_id)
            self._trade_service.cancel_protection(trade.trade_id)

    def _close_requested(self, trade: Trade, exit_order: dict[str, Any]) -> None:
        closed = self._transition(
            trade,
            TradeState.CLOSED,
            close_reason=trade.requested_close_reason or CloseReason.MANUAL,
            exit_order_id=exit_order["orderId"],
            exit_price=_positive_or_none(exit_order.get("avgPrice")),
            closed_at=binance_time(exit_order["updateTime"]),
            last_error=None,
        )
        if closed is not None:
            self._trade_service.cancel_protection(trade.trade_id)

    # --- Binance reads ------------------------------------------------------------------------

    def _fetch_legs(self, trade: Trade) -> dict[str, dict[str, Any] | None]:
        """Binance's record of each *requested* TP/SL (None: Binance has no such order)."""
        legs = {}
        for role, _order_type, _label in PROTECTION_LEGS:
            if getattr(trade, f"{role}_price") is None:
                continue
            try:
                legs[role] = self._client.get_algo_order(client_algo_id=getattr(trade, f"{role}_client_algo_id"))
            except BinanceAPIError as error:
                if error.code != ORDER_DOES_NOT_EXIST:
                    raise
                legs[role] = None
        return legs

    def _record_leg_statuses(self, trade: Trade, legs: dict[str, dict[str, Any] | None]) -> Trade:
        changes = {}
        for role, algo_order in legs.items():
            if algo_order is None:
                continue
            if getattr(trade, f"{role}_status") != algo_order["algoStatus"]:
                changes[f"{role}_status"] = algo_order["algoStatus"]
            if getattr(trade, f"{role}_algo_id") is None:
                changes[f"{role}_algo_id"] = algo_order["algoId"]
        if not changes:
            return trade
        return self._repository.update_fields(trade.trade_id, event_detail={"source": "reconciliation"}, **changes)

    def _find_order(self, symbol: str, client_order_id: str) -> dict[str, Any] | None:
        try:
            return self._client.get_order(symbol=symbol, client_order_id=client_order_id)
        except BinanceAPIError as error:
            if error.code == ORDER_DOES_NOT_EXIST:
                return None
            raise

    # --- small helpers ------------------------------------------------------------------------

    def _past_grace(self, trade: Trade) -> bool:
        return (self._clock() - trade.updated_at).total_seconds() >= self._entry_grace_seconds

    def _transition(self, trade: Trade, target_state: TradeState, **field_updates) -> Trade | None:
        """Transition, or return None if the API/handler already moved the trade on."""
        try:
            return self._repository.transition(
                trade.trade_id, target_state, event_detail={"source": "reconciliation"}, **field_updates
            )
        except InvalidTransitionError:
            return None

    def _reconcile_safely(self, trade_id: str) -> None:
        try:
            self.reconcile_trade(trade_id)
        except BinanceError as error:
            logger.warning("Could not reconcile trade %s this time: %s", trade_id, error)
        except Exception:  # noqa: BLE001 - one trade must never stop the others
            logger.exception("Reconciling trade %s failed", trade_id)


def _filled_leg(legs: dict[str, dict[str, Any] | None]) -> str | None:
    for role, algo_order in legs.items():
        if algo_order and algo_order["algoStatus"] == "FINISHED" and Decimal(algo_order.get("actualQty") or "0") > 0:
            return role
    return None


def _positive_or_none(raw_value: str | None) -> Decimal | None:
    if raw_value is None or Decimal(raw_value) == 0:
        return None
    return Decimal(raw_value)
