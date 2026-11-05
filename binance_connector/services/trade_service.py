"""Strategy trades: open with take-profit/stop-loss protection, and close.

A trade is one position on one symbol, owned by this service from entry to exit. Its
progress is written to the database at every step, *before* the next Binance call, so a
crash at any point leaves enough behind for reconciliation to finish the job.

Opening::

    reserve the symbol (RECEIVED) → ENTRY_SUBMITTED → market entry → OPEN
      → stop-loss, then take-profit (Binance-side, closePosition=true) → PROTECTED

* The stop-loss is placed first: if only one leg can be placed, the one that limits the
  loss matters more.
* A protection leg that fails does not stop the other one from being placed; the trade
  then NEEDS_ATTENTION with the reason in ``last_error``.
* The API process owns a trade while it is entering. The worker only touches entry-stage
  trades after a grace period (see ``ReconciliationService``), so the two never race on it.

Closing is in ``close_trade`` (manual or upstream request).
"""

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
    InvalidTransitionError,
    NoOpenPositionError,
    TradeConflictError,
    TradeNotClosableError,
)
from binance_connector.persistence.repository import NewTrade, Trade, TradeRepository, utc_now
from binance_connector.services.position_service import (
    ClosePositionCommand,
    FuturesClient,
    OpenPositionCommand,
    PositionDirection,
    PositionService,
)

logger = logging.getLogger(__name__)

# (role, Binance order type, label for messages); stop-loss first, see module docstring.
PROTECTION_LEGS = (
    ("stop_loss", "STOP_MARKET", "stop-loss"),
    ("take_profit", "TAKE_PROFIT_MARKET", "take-profit"),
)

ALGO_ALREADY_GONE_CODE = -2011
"""Binance's reply when cancelling an algo order that is no longer open (it expires by
itself once the position is flat; verified on Demo)."""

CLOSABLE_STATES = frozenset({TradeState.OPEN, TradeState.PROTECTED, TradeState.NEEDS_ATTENTION})


@dataclass(frozen=True)
class OpenTradeCommand:
    """A validated Strategy request to open a trade."""

    symbol: str
    direction: PositionDirection
    quantity: Decimal
    leverage: int | None = None
    take_profit_price: Decimal | None = None
    stop_loss_price: Decimal | None = None
    strategy_request_id: str | None = None


@dataclass(frozen=True)
class OpenTradeOutcome:
    """The trade, and whether it already existed for this strategy_request_id."""

    trade: Trade
    replayed: bool


def new_trade_id() -> str:
    """24 hex characters: unique, and short enough for ``lx-<id>-tp`` to fit Binance's 36."""
    return uuid.uuid4().hex[:24]


class TradeService:
    """Opens, protects and closes trades. Used by the API, and by reconciliation to resume.

    Args:
        repository: where trades are stored.
        client: Binance client (algo-order calls).
        position_service: market entry/exit with leverage and average-price lookup.
        trigger_price_type: ``MARK_PRICE`` or ``CONTRACT_PRICE`` for TP/SL triggers.
        generate_trade_id: id factory (injectable for tests).
        clock: current UTC time (injectable for tests).
    """

    def __init__(
        self,
        *,
        repository: TradeRepository,
        client: FuturesClient,
        position_service: PositionService,
        trigger_price_type: str,
        generate_trade_id: Callable[[], str] = new_trade_id,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._repository = repository
        self._client = client
        self._position_service = position_service
        self._trigger_price_type = trigger_price_type
        self._generate_trade_id = generate_trade_id
        self._clock = clock

    # --- open -----------------------------------------------------------------------------------

    def open_trade(self, command: OpenTradeCommand) -> OpenTradeOutcome:
        """Open a trade, or return the existing one for a repeated strategy_request_id.

        Binance failures do not raise: they end in ENTRY_FAILED or NEEDS_ATTENTION, and the
        returned trade says which.

        Raises:
            TradeConflictError: the symbol already has an active trade, or holds a position
                on Binance that this service does not manage.
            HedgeModeNotSupportedError: the account is in hedge mode.
        """
        replayed_trade = self._existing_for(command.strategy_request_id)
        if replayed_trade is not None:
            return OpenTradeOutcome(replayed_trade, replayed=True)

        active_trade = self._repository.find_active_by_symbol(command.symbol)
        if active_trade is not None:
            raise TradeConflictError(f"{command.symbol} already has an active trade ({active_trade.trade_id}).")
        if self._position_service.net_position(command.symbol) != 0:
            raise TradeConflictError(
                f"{command.symbol} already has a position on Binance that is not managed by this service."
            )

        try:
            trade = self._repository.create(self._new_trade(command))
        except TradeConflictError:
            # Lost a race with an identical request: answer with the winner's trade.
            replayed_trade = self._existing_for(command.strategy_request_id)
            if replayed_trade is None:
                raise
            return OpenTradeOutcome(replayed_trade, replayed=True)

        trade = self._enter(trade)
        if trade.state is TradeState.OPEN:
            trade = self.protect(trade.trade_id)
        return OpenTradeOutcome(trade, replayed=False)

    def protect(self, trade_id: str) -> Trade:
        """Place every requested take-profit/stop-loss that is not on Binance yet.

        Safe to call again (reconciliation does, after a crash): legs whose status is
        already known are skipped. Ends PROTECTED when every requested leg is placed,
        NEEDS_ATTENTION when any could not be, and leaves a trade with no legs OPEN.
        """
        trade = self._repository.get(trade_id)
        if not (trade.wants_take_profit or trade.wants_stop_loss):
            return trade

        direction = PositionDirection(trade.direction)
        failures = []
        for role, order_type, label in PROTECTION_LEGS:
            trigger_price = getattr(trade, f"{role}_price")
            if trigger_price is None or getattr(trade, f"{role}_status") is not None:
                continue
            try:
                reply = self._client.place_conditional_close(
                    symbol=trade.symbol,
                    side=direction.closing_side,
                    order_type=order_type,
                    trigger_price=trigger_price,
                    client_algo_id=getattr(trade, f"{role}_client_algo_id"),
                    working_type=self._trigger_price_type,
                )
            except BinanceUnknownOutcomeError as error:
                failures.append(f"The {label} outcome is unknown: {error}")
                continue
            except (BinanceAPIError, BinanceUnavailableError) as error:
                failures.append(f"The {label} could not be placed: {error}")
                continue
            self._repository.update_fields(
                trade_id,
                only_if_unset=(f"{role}_algo_id", f"{role}_status"),
                **{f"{role}_algo_id": reply["algoId"], f"{role}_status": reply["algoStatus"]},
            )

        if failures:
            logger.warning("Trade %s is not fully protected: %s", trade_id, "; ".join(failures))
            return self._transition_or_current(trade_id, TradeState.NEEDS_ATTENTION, last_error="; ".join(failures))
        return self._transition_or_current(trade_id, TradeState.PROTECTED, last_error=None)

    def _enter(self, trade: Trade) -> Trade:
        trade = self._repository.transition(trade.trade_id, TradeState.ENTRY_SUBMITTED)
        command = OpenPositionCommand(
            symbol=trade.symbol,
            direction=PositionDirection(trade.direction),
            quantity=trade.quantity,
            leverage=trade.leverage,
            client_order_id=trade.entry_client_order_id,
        )
        try:
            result = self._position_service.open_position(command)
        except BinanceUnknownOutcomeError as error:
            return self._transition_or_current(
                trade.trade_id,
                TradeState.NEEDS_ATTENTION,
                last_error=f"The entry outcome is unknown: {error} Reconciliation will look up "
                f"order {trade.entry_client_order_id}.",
            )
        except (BinanceAPIError, BinanceUnavailableError) as error:
            return self._transition_or_current(
                trade.trade_id, TradeState.ENTRY_FAILED, last_error=f"The entry failed: {error}"
            )
        return self._transition_or_current(
            trade.trade_id,
            TradeState.OPEN,
            entry_order_id=result.order_id,
            entry_price=result.average_price,
            entry_quantity=result.executed_quantity,
            opened_at=self._clock(),
        )

    # --- close ----------------------------------------------------------------------------------

    def close_trade(self, trade_id: str, reason: CloseReason) -> Trade:
        """Close a trade on request (``reason`` MANUAL or UPSTREAM).

        Order of operations: CLOSING is recorded first, then a reduce-only market order
        closes whatever is open, then the trade's TP/SL are cancelled. Binance normally
        expires them by itself once the position is flat; cancelling covers the case where
        it does not, and "already gone" (``-2011``) counts as success. Protection stays in
        place until the position is actually closed.

        Raises:
            TradeNotFoundError: no such trade.
            TradeNotClosableError: the trade is entering, already closing, or finished.
        """
        trade = self._repository.get(trade_id)
        if trade.state not in CLOSABLE_STATES:
            raise TradeNotClosableError(trade_id, trade.state)
        state_before_closing = trade.state
        try:
            trade = self._repository.transition(trade_id, TradeState.CLOSING, requested_close_reason=reason)
        except InvalidTransitionError as error:
            raise TradeNotClosableError(trade_id, self._repository.get(trade_id).state) from error

        try:
            result = self._position_service.close_position(
                ClosePositionCommand(symbol=trade.symbol, client_order_id=trade.exit_client_order_id)
            )
        except NoOpenPositionError:
            # Flat already: a TP/SL (or something outside the service) closed it first.
            # Let reconciliation work out which, with Binance's records in hand.
            return self._transition_or_current(
                trade_id,
                TradeState.NEEDS_ATTENTION,
                last_error="The position was already flat when the close was sent; reconciliation will "
                "determine how it closed.",
            )
        except BinanceUnknownOutcomeError as error:
            # Stays CLOSING; reconciliation looks up the exit order.
            return self._repository.update_fields(
                trade_id, last_error=f"The close outcome is unknown: {error}"
            )
        except (BinanceAPIError, BinanceUnavailableError) as error:
            # Nothing happened; the trade is as it was.
            return self._transition_or_current(
                trade_id, state_before_closing, last_error=f"The close failed: {error}"
            )

        self.cancel_protection(trade_id)
        return self._transition_or_current(
            trade_id,
            TradeState.CLOSED,
            close_reason=reason,
            exit_order_id=result.order_id,
            exit_price=result.average_price,
            closed_at=self._clock(),
        )

    def cancel_protection(self, trade_id: str) -> None:
        """Cancel the trade's TP/SL if Binance still has them open; record their final status.

        "Already gone" is the normal case (Binance expires closePosition orders once flat)
        and is not an error. Other failures are logged and left to reconciliation.
        """
        trade = self._repository.get(trade_id)
        for role, _order_type, label in PROTECTION_LEGS:
            if getattr(trade, f"{role}_status") != "NEW":
                continue
            try:
                self._client.cancel_algo_order(client_algo_id=getattr(trade, f"{role}_client_algo_id"))
                final_status = "CANCELED"
            except BinanceAPIError as error:
                if error.code != ALGO_ALREADY_GONE_CODE:
                    logger.warning("Could not cancel the %s of trade %s: %s", label, trade_id, error)
                    continue
                final_status = "EXPIRED"
            except (BinanceUnavailableError, BinanceUnknownOutcomeError) as error:
                logger.warning("Could not cancel the %s of trade %s: %s", label, trade_id, error)
                continue
            self._repository.update_fields(trade_id, **{f"{role}_status": final_status})

    # --- helpers --------------------------------------------------------------------------------

    def _existing_for(self, strategy_request_id: str | None) -> Trade | None:
        if not strategy_request_id:
            return None
        return self._repository.find_by_strategy_request_id(strategy_request_id)

    def _new_trade(self, command: OpenTradeCommand) -> NewTrade:
        return NewTrade(
            trade_id=self._generate_trade_id(),
            symbol=command.symbol,
            direction=command.direction.value,
            quantity=command.quantity,
            leverage=command.leverage,
            take_profit_price=command.take_profit_price,
            stop_loss_price=command.stop_loss_price,
            strategy_request_id=command.strategy_request_id,
        )

    def _transition_or_current(self, trade_id: str, target_state: TradeState, **field_updates) -> Trade:
        """Transition, or, if another process already moved the trade on, return it as it is now."""
        try:
            return self._repository.transition(trade_id, target_state, **field_updates)
        except InvalidTransitionError:
            current = self._repository.get(trade_id)
            logger.info("Trade %s is already %s; not moving it to %s", trade_id, current.state, target_state)
            return current
