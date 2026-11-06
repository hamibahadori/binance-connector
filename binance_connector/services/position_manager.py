"""Executing positions on Binance: entry, TP/SL protection, close, and limit-entry expiry.

Ported from v1's TradeService onto execution records and many accounts. Each position
trades through its own account's client.

Opening (``open``)::

    check the account (no unmanaged position, one-way mode) → leverage → entry
      MARKET, or a marketable LIMIT → filled at once → OPEN → stop-loss, then take-profit
      resting LIMIT → stays OPENING until filled (stream / reconciliation) or expired

Every order is written as SUBMITTED **before** it is sent. If the service dies between
sending and recording the reply, reconciliation knows to ask Binance about it, and a
re-run never sends it twice.

* A protection leg that fails does not stop the other; the position becomes ERROR.
* An unknown outcome (timeout, 5xx) leaves the order SUBMITTED and the position ERROR (or
  CLOSING), for reconciliation to resolve from Binance's records. Nothing is retried blindly.
* Closing (``close``) market-closes reduce-only, then cancels any TP/SL Binance has not
  expired by itself. An unfilled limit entry is cancelled instead; a partial fill is closed.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from binance_connector.domain.direction import PositionDirection
from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState, is_active
from binance_connector.domain.pnl import realized_pnl
from binance_connector.errors import (
    BinanceAPIError,
    BinanceError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
    HedgeModeNotSupportedError,
    InvalidTransitionError,
)
from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.positions import Position, PositionRepository
from binance_connector.persistence.writes import utc_now

logger = logging.getLogger(__name__)

# (role, Binance order type, label); stop-loss first: if only one leg can be placed, the one
# that limits the loss matters more.
PROTECTION_LEGS = (
    (ExecutionRole.STOP_LOSS, "STOP_MARKET", "stop-loss"),
    (ExecutionRole.TAKE_PROFIT, "TAKE_PROFIT_MARKET", "take-profit"),
)
ALREADY_GONE = -2011
"""Binance's reply to cancelling an order or algo order that is no longer open."""
ONE_WAY_POSITION_SIDE = "BOTH"
LIVE_ORDER_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED"})
RETRYABLE_STATUSES = frozenset({"PENDING", "REJECTED"})
"""Execution statuses that mean "never reached Binance": safe to (re)send."""


class ClientSource(Protocol):
    def for_account(self, account_id: str) -> Any: ...


class PositionManager:
    def __init__(
        self,
        *,
        positions: PositionRepository,
        clients: ClientSource,
        trigger_price_type: str,
        limit_entry_timeout_seconds: float,
        error_logs: ErrorLogRepository | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._positions = positions
        self._clients = clients
        self._trigger_price_type = trigger_price_type
        self._limit_entry_timeout_seconds = limit_entry_timeout_seconds
        self._error_logs = error_logs
        self._clock = clock

    # --- open ---------------------------------------------------------------------------------

    def open(self, position_id: str) -> Position:
        """Send the entry of an OPENING position (once), and protect it if it fills."""
        position = self._positions.get(position_id)
        entry = position.executions.get(ExecutionRole.ENTRY)
        if position.state is not PositionState.OPENING or (entry is not None and entry.status not in RETRYABLE_STATUSES):
            return position  # already sent: the stream and reconciliation take it from here
        client = self._clients.for_account(position.account_id)
        direction = PositionDirection(position.direction)

        try:
            held_amount = net_position(client, position.symbol)
        except HedgeModeNotSupportedError as error:
            return self._fail(position, str(error))
        except BinanceError as error:
            return self._fail(position, f"The account's position could not be checked: {error}")
        if held_amount != 0:
            return self._fail(position, f"Account {position.account_id} already holds {held_amount} {position.symbol} "
                                        "on Binance, not managed by this service.")
        if position.leverage is not None:
            try:
                client.change_leverage(symbol=position.symbol, leverage=position.leverage)
            except BinanceError as error:
                return self._fail(position, f"The leverage could not be set: {error}")

        is_limit = position.entry_type == "LIMIT"
        execution = self._positions.prepare_execution(
            position_id, ExecutionRole.ENTRY, kind="ORDER", order_type="LIMIT" if is_limit else "MARKET",
            side=direction.opening_side, price=position.entry_price if is_limit else None, quantity=position.quantity,
        )
        self._positions.update_execution(position_id, ExecutionRole.ENTRY, status="SUBMITTED",
                                         submitted_at=self._clock(), last_error=None)
        try:
            if is_limit:
                reply = client.place_limit_order(symbol=position.symbol, side=direction.opening_side,
                                                 quantity=position.quantity, price=position.entry_price,
                                                 client_order_id=execution.client_id)
            else:
                reply = client.place_market_order(symbol=position.symbol, side=direction.opening_side,
                                                  quantity=position.quantity, client_order_id=execution.client_id)
        except BinanceUnknownOutcomeError as error:
            return self._transition_or_current(
                position_id, PositionState.ERROR,
                last_error=f"The entry outcome is unknown: {error} Reconciliation will look up order "
                           f"{execution.client_id}.")
        except (BinanceAPIError, BinanceUnavailableError) as error:
            self._positions.update_execution(position_id, ExecutionRole.ENTRY, status="REJECTED", last_error=str(error))
            return self._fail(position, f"The entry failed: {error}")
        return self._record_entry(position_id, client, reply, protect=True)

    def protect(self, position_id: str) -> Position:
        """Place every requested TP/SL not yet on Binance. Safe to call again (reconciliation does).

        Legs already placed, or whose outcome is unknown, are left alone; legs that never
        reached Binance (rejected or not yet sent) are (re)sent.
        """
        position = self._positions.get(position_id)
        if position.take_profit_price is None and position.stop_loss_price is None:
            return position
        client = self._clients.for_account(position.account_id)
        direction = PositionDirection(position.direction)
        failures = []
        for role, order_type, label in PROTECTION_LEGS:
            trigger_price = position.take_profit_price if role is ExecutionRole.TAKE_PROFIT else position.stop_loss_price
            existing = position.executions.get(role)
            if trigger_price is None or (existing is not None and existing.status not in RETRYABLE_STATUSES):
                continue
            execution = self._positions.prepare_execution(position_id, role, kind="ALGO", order_type=order_type,
                                                          side=direction.closing_side, price=trigger_price)
            self._positions.update_execution(position_id, role, status="SUBMITTED", submitted_at=self._clock(),
                                             last_error=None)
            try:
                reply = client.place_conditional_close(
                    symbol=position.symbol, side=direction.closing_side, order_type=order_type,
                    trigger_price=trigger_price, client_algo_id=execution.client_id,
                    working_type=self._trigger_price_type,
                )
            except BinanceUnknownOutcomeError as error:
                failures.append(f"The {label} outcome is unknown: {error}")
                continue
            except (BinanceAPIError, BinanceUnavailableError) as error:
                self._positions.update_execution(position_id, role, status="REJECTED", last_error=str(error))
                failures.append(f"The {label} could not be placed: {error}")
                continue
            self._positions.update_execution(position_id, role, status_only_if="SUBMITTED",
                                             status=reply["algoStatus"], binance_id=reply["algoId"])
        if failures:
            self._log_error(position, "; ".join(failures))
            return self._transition_or_current(position_id, PositionState.ERROR, last_error="; ".join(failures))
        return self._positions.get(position_id)

    # --- close --------------------------------------------------------------------------------

    def close(self, position_id: str, close_request_id: str) -> Position:
        """Close the position for a CLOSE request. Does nothing if it is closing or finished."""
        position = self._positions.get(position_id)
        if not is_active(position.state) or position.state is PositionState.CLOSING:
            return position
        if position.close_request_id is None:
            position = self._positions.update_fields(position_id, close_request_id=close_request_id)
        client = self._clients.for_account(position.account_id)

        if position.state is PositionState.OPENING:
            entry = position.executions.get(ExecutionRole.ENTRY)
            if entry is None or entry.status in RETRYABLE_STATUSES:
                return self._fail(position, "Closed by a CLOSE request before the entry was sent.")
            if entry.status not in LIVE_ORDER_STATUSES:
                return position  # entry outcome unknown: reconciliation first
            position = self._cancel_entry(position, client, "cancelled by a CLOSE request", protect=False)
            if position.state is not PositionState.OPEN:
                return position
        return self._market_close(position, client)

    def cancel_protection(self, position_id: str) -> None:
        """Cancel the position's TP/SL if Binance still has them open; record their final status.

        "Already gone" is the normal case (Binance expires closePosition orders once flat) and
        is recorded as EXPIRED. Other failures are logged and left to reconciliation.
        """
        position = self._positions.get(position_id)
        client = self._clients.for_account(position.account_id)
        for role, _order_type, label in PROTECTION_LEGS:
            execution = position.executions.get(role)
            if execution is None or execution.status != "NEW":
                continue
            try:
                client.cancel_algo_order(client_algo_id=execution.client_id)
                final_status = "CANCELED"
            except BinanceAPIError as error:
                if error.code != ALREADY_GONE:
                    logger.warning("Could not cancel the %s of position %s: %s", label, position_id, error)
                    continue
                final_status = "EXPIRED"
            except (BinanceUnavailableError, BinanceUnknownOutcomeError) as error:
                logger.warning("Could not cancel the %s of position %s: %s", label, position_id, error)
                continue
            self._positions.update_execution(position_id, role, status_only_if="NEW", status=final_status)

    def expire_limit_entries(self) -> list[Position]:
        """Cancel LIMIT entries still unfilled after ``limit_entry_timeout_seconds``."""
        expired = []
        now = self._clock()
        for position in self._positions.list_active():
            entry = position.executions.get(ExecutionRole.ENTRY)
            if (position.state is not PositionState.OPENING or position.entry_type != "LIMIT" or entry is None
                    or entry.status not in LIVE_ORDER_STATUSES or entry.submitted_at is None):
                continue
            if (now - entry.submitted_at).total_seconds() < self._limit_entry_timeout_seconds:
                continue
            client = self._clients.for_account(position.account_id)
            reason = f"expired unfilled after {self._limit_entry_timeout_seconds:g} s"
            expired.append(self._cancel_entry(position, client, reason, protect=True))
        return expired

    # --- shared steps -------------------------------------------------------------------------

    def _record_entry(self, position_id: str, client, reply: dict[str, Any], *, protect: bool) -> Position:
        """Record what Binance said about the entry; a fill (even partial) makes the position OPEN."""
        position = self._positions.get(position_id)
        executed = Decimal(reply.get("executedQty") or "0")
        average = average_fill_price(client, position.symbol, reply) if executed > 0 else None
        self._positions.update_execution(position_id, ExecutionRole.ENTRY, status_only_if="SUBMITTED",
                                         status=reply["status"], binance_id=reply["orderId"],
                                         executed_quantity=executed, average_price=average)
        if executed == 0:
            return self._positions.get(position_id)
        opened = self._transition_or_none(position_id, PositionState.OPEN, entry_average_price=average,
                                          entry_filled_quantity=executed, opened_at=self._clock(), last_error=None)
        if opened is not None and protect:
            return self.protect(position_id)
        return self._positions.get(position_id)

    def _cancel_entry(self, position: Position, client, reason: str, *, protect: bool) -> Position:
        """Cancel a resting limit entry. Nothing filled → FAILED; a partial fill → OPEN with what filled."""
        entry = position.executions[ExecutionRole.ENTRY]
        try:
            client.cancel_order(symbol=position.symbol, client_order_id=entry.client_id)
        except BinanceAPIError as error:
            if error.code != ALREADY_GONE:
                self._log_error(position, f"Cancelling the limit entry failed: {error}")
                return self._positions.get(position.position_id)
        except (BinanceUnavailableError, BinanceUnknownOutcomeError) as error:
            self._log_error(position, f"Cancelling the limit entry failed: {error}")
            return self._positions.get(position.position_id)
        try:
            order = client.get_order(symbol=position.symbol, client_order_id=entry.client_id)
        except BinanceError as error:
            self._log_error(position, f"The cancelled limit entry could not be looked up: {error}")
            return self._positions.get(position.position_id)

        executed = Decimal(order.get("executedQty") or "0")
        average = _positive_or_none(order.get("avgPrice"))
        self._positions.update_execution(position.position_id, ExecutionRole.ENTRY, status=order["status"],
                                         executed_quantity=executed, average_price=average)
        if executed == 0:
            return self._fail(position, f"The limit entry was {reason} before it filled.")
        opened = self._transition_or_none(position.position_id, PositionState.OPEN, entry_average_price=average,
                                          entry_filled_quantity=executed,
                                          opened_at=position.opened_at or self._clock())
        if opened is not None and protect:
            return self.protect(position.position_id)
        return self._positions.get(position.position_id)

    def _market_close(self, position: Position, client) -> Position:
        direction = PositionDirection(position.direction)
        try:
            held_amount = net_position(client, position.symbol)
        except BinanceError as error:
            return self._positions.update_fields(position.position_id, last_error=f"The close could not start: {error}")
        if held_amount == 0:
            return self._transition_or_current(
                position.position_id, PositionState.ERROR,
                last_error="The position was already flat when the close was sent; reconciliation will determine "
                           "how it closed.")
        state_before_closing = position.state
        self._positions.transition(position.position_id, PositionState.CLOSING)
        execution = self._positions.prepare_execution(position.position_id, ExecutionRole.EXIT, kind="ORDER",
                                                      order_type="MARKET", side=direction.closing_side,
                                                      quantity=abs(held_amount))
        self._positions.update_execution(position.position_id, ExecutionRole.EXIT, status="SUBMITTED",
                                         submitted_at=self._clock(), quantity=abs(held_amount), last_error=None)
        try:
            reply = client.place_market_order(symbol=position.symbol, side=direction.closing_side,
                                              quantity=abs(held_amount), client_order_id=execution.client_id,
                                              reduce_only=True)
        except BinanceUnknownOutcomeError as error:
            return self._positions.update_fields(position.position_id,
                                                 last_error=f"The close outcome is unknown: {error}")
        except (BinanceAPIError, BinanceUnavailableError) as error:
            self._positions.update_execution(position.position_id, ExecutionRole.EXIT, status="REJECTED",
                                             last_error=str(error))
            return self._transition_or_current(position.position_id, state_before_closing,
                                               last_error=f"The close failed: {error}")

        average = average_fill_price(client, position.symbol, reply)
        self._positions.update_execution(position.position_id, ExecutionRole.EXIT, status_only_if="SUBMITTED",
                                         status=reply["status"], binance_id=reply["orderId"],
                                         executed_quantity=Decimal(reply.get("executedQty") or "0"),
                                         average_price=average)
        self.cancel_protection(position.position_id)
        return self.finish_closed(position.position_id, CloseReason.CLOSE_REQUESTED, exit_average_price=average,
                                  closed_at=self._clock())

    def finish_closed(self, position_id: str, reason: CloseReason, *, exit_average_price: Decimal | None,
                      closed_at: datetime) -> Position:
        """Move to CLOSED with realized P&L and fees. Shared with the stream handler and reconciliation."""
        position = self._positions.get(position_id)
        pnl = None
        if position.entry_average_price is not None and exit_average_price is not None and position.entry_filled_quantity:
            pnl = realized_pnl(direction=position.direction, entry_price=position.entry_average_price,
                               exit_price=exit_average_price, quantity=position.entry_filled_quantity)
        commissions = [execution.commission for execution in position.executions.values()
                       if execution.commission is not None]
        return self._transition_or_current(
            position_id, PositionState.CLOSED, close_reason=reason, exit_average_price=exit_average_price,
            closed_at=closed_at, realized_pnl=pnl, fees=sum(commissions, Decimal(0)) if commissions else None,
            last_error=None,
        )

    # --- helpers ------------------------------------------------------------------------------

    def _fail(self, position: Position, reason: str) -> Position:
        self._log_error(position, reason)
        return self._transition_or_current(position.position_id, PositionState.FAILED, last_error=reason)

    def _transition_or_current(self, position_id: str, target_state: PositionState, **field_updates) -> Position:
        """Transition, or, if another thread already moved the position on, return it as it is now."""
        return self._transition_or_none(position_id, target_state, **field_updates) or self._positions.get(position_id)

    def _transition_or_none(self, position_id: str, target_state: PositionState, **field_updates) -> Position | None:
        try:
            return self._positions.transition(position_id, target_state, **field_updates)
        except InvalidTransitionError:
            return None

    def _log_error(self, position: Position, message: str) -> None:
        logger.warning("Position %s: %s", position.position_id, message)
        if self._error_logs is not None:
            self._error_logs.record(component="position_manager", message=message,
                                    account_id=position.account_id, request_id=position.open_request_id,
                                    position_id=position.position_id)


def net_position(client, symbol: str) -> Decimal:
    """The account's signed one-way position on ``symbol``; 0 when flat.

    Raises:
        HedgeModeNotSupportedError: the account is in hedge mode.
    """
    entries = client.get_position_risk(symbol=symbol)
    if any(entry.get("positionSide") != ONE_WAY_POSITION_SIDE for entry in entries):
        raise HedgeModeNotSupportedError(symbol)
    return sum((Decimal(entry["positionAmt"]) for entry in entries), start=Decimal(0))


def average_fill_price(client, symbol: str, reply: dict[str, Any]) -> Decimal | None:
    """The fill price: from the reply if present, otherwise one order lookup (Binance's order
    replies carry no avgPrice, verified on Demo). A failed lookup gives None, never an error."""
    from_reply = _positive_or_none(reply.get("avgPrice"))
    if from_reply is not None:
        return from_reply
    try:
        order = client.get_order(symbol=symbol, client_order_id=reply["clientOrderId"])
    except BinanceError as error:
        logger.warning("Order %s filled, but its average price could not be read: %s", reply["clientOrderId"], error)
        return None
    return _positive_or_none(order.get("avgPrice"))


def _positive_or_none(raw_value) -> Decimal | None:
    if raw_value is None or Decimal(raw_value) == 0:
        return None
    return Decimal(raw_value)
