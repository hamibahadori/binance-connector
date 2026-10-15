"""Open and close USD-M futures positions.

Rules this module enforces:

* **One-way position mode only.** A symbol has a single net position: positive
  ``positionAmt`` means long, negative means short.
* **MARKET orders only.** A position opens or closes immediately, so the service
  never has to track a resting order.
* **Closing is reduce-only.** The close order is on the opposite side and flagged
  ``reduceOnly``, so it can shrink the position but can never flip it.
* **Every order has a client order id.** It comes from the caller or is
  generated here, and it is reported back even when the outcome is unknown, so
  the caller can reconcile instead of blindly retrying.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from binance_connector.errors import (
    BinanceUnknownOutcomeError,
    CloseQuantityExceedsPositionError,
    HedgeModeNotSupportedError,
    NoOpenPositionError,
)

ONE_WAY_POSITION_SIDE = "BOTH"
"""Binance's ``positionSide`` for a one-way-mode position."""


class PositionDirection(StrEnum):
    """Which way a position is exposed to the price."""

    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def opening_side(self) -> str:
        """The order side that opens (or grows) a position in this direction."""
        return "BUY" if self is PositionDirection.LONG else "SELL"

    @property
    def closing_side(self) -> str:
        """The order side that reduces a position in this direction."""
        return "SELL" if self is PositionDirection.LONG else "BUY"


@dataclass(frozen=True)
class OpenPositionCommand:
    """A request to open (or add to) a position with a MARKET order.

    ``leverage``, when given, is applied to the symbol before the order is placed.
    ``client_order_id``, when omitted, is generated.
    """

    symbol: str
    direction: PositionDirection
    quantity: Decimal
    leverage: int | None = None
    client_order_id: str | None = None


@dataclass(frozen=True)
class ClosePositionCommand:
    """A request to close a position, fully (``quantity=None``) or partly."""

    symbol: str
    quantity: Decimal | None = None
    client_order_id: str | None = None


@dataclass(frozen=True)
class OrderResult:
    """The outcome of an open or close, built from Binance's order response.

    ``direction`` is the direction of the position that was opened or closed.
    ``side`` is the order side actually sent to Binance.
    """

    symbol: str
    direction: PositionDirection
    side: str
    order_id: int
    client_order_id: str
    status: str
    requested_quantity: Decimal
    executed_quantity: Decimal
    average_price: Decimal
    leverage: int | None = None


class FuturesClient(Protocol):
    """The subset of the Binance client this service depends on."""

    def change_leverage(self, *, symbol: str, leverage: int) -> dict[str, Any]: ...

    def place_market_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal,
        client_order_id: str,
        reduce_only: bool = False,
    ) -> dict[str, Any]: ...

    def get_position_risk(self, *, symbol: str) -> list[dict[str, Any]]: ...


def new_client_order_id() -> str:
    """A unique id within Binance's limit of 36 characters from ``[.A-Z:/a-z0-9_-]``.

    The ``lx-`` prefix marks orders placed by Live EX in the Binance order history.
    """
    return f"lx-{uuid.uuid4().hex}"


class PositionService:
    """Opens and closes positions through a ``FuturesClient``.

    Args:
        client: anything implementing ``FuturesClient``, normally ``BinanceFuturesClient``.
        generate_client_order_id: id factory used when the caller does not supply one.
    """

    def __init__(
        self,
        client: FuturesClient,
        generate_client_order_id: Callable[[], str] = new_client_order_id,
    ) -> None:
        self._client = client
        self.generate_client_order_id = generate_client_order_id

    def open_position(self, command: OpenPositionCommand) -> OrderResult:
        """Optionally set leverage, then place a MARKET order in the requested direction.

        Raises:
            BinanceAPIError: Binance rejected the leverage change or the order.
            BinanceUnavailableError: Binance could not be reached.
            BinanceUnknownOutcomeError: the order may have executed; carries the client order id.
        """
        client_order_id = command.client_order_id or self.generate_client_order_id()
        if command.leverage is not None:
            self._client.change_leverage(symbol=command.symbol, leverage=command.leverage)

        order_response = self._place_order(
            symbol=command.symbol,
            side=command.direction.opening_side,
            quantity=command.quantity,
            client_order_id=client_order_id,
            reduce_only=False,
        )
        return _order_result(order_response, command.direction, command.quantity, command.leverage)

    def close_position(self, command: ClosePositionCommand) -> OrderResult:
        """Read the open position, then reduce it with an opposite-side MARKET order.

        Raises:
            NoOpenPositionError: the symbol has no open position.
            HedgeModeNotSupportedError: the account is in hedge mode.
            CloseQuantityExceedsPositionError: a partial close asked for more than is open.
            BinanceAPIError / BinanceUnavailableError / BinanceUnknownOutcomeError: as for open.
        """
        position_amount = self._read_net_position(command.symbol)
        direction = PositionDirection.LONG if position_amount > 0 else PositionDirection.SHORT
        open_quantity = abs(position_amount)

        close_quantity = command.quantity if command.quantity is not None else open_quantity
        if close_quantity > open_quantity:
            raise CloseQuantityExceedsPositionError(command.symbol, close_quantity, open_quantity)

        order_response = self._place_order(
            symbol=command.symbol,
            side=direction.closing_side,
            quantity=close_quantity,
            client_order_id=command.client_order_id or self.generate_client_order_id(),
            reduce_only=True,
        )
        return _order_result(order_response, direction, close_quantity, leverage=None)

    def _read_net_position(self, symbol: str) -> Decimal:
        """Return the signed one-way position amount for ``symbol``; never zero.

        Raises:
            HedgeModeNotSupportedError: any entry is a hedge-mode (LONG/SHORT) entry.
            NoOpenPositionError: there is no non-zero one-way position.
        """
        position_entries = self._client.get_position_risk(symbol=symbol)
        if any(entry.get("positionSide") != ONE_WAY_POSITION_SIDE for entry in position_entries):
            raise HedgeModeNotSupportedError(symbol)

        position_amount = sum(
            (Decimal(entry["positionAmt"]) for entry in position_entries), start=Decimal(0)
        )
        if position_amount == 0:
            raise NoOpenPositionError(symbol)
        return position_amount

    def _place_order(self, *, client_order_id: str, **order_arguments: Any) -> dict[str, Any]:
        """Place the order, attaching our client order id to an unknown-outcome error."""
        try:
            return self._client.place_market_order(client_order_id=client_order_id, **order_arguments)
        except BinanceUnknownOutcomeError as error:
            error.client_order_id = client_order_id
            raise


def _order_result(
    order_response: dict[str, Any],
    direction: PositionDirection,
    requested_quantity: Decimal,
    leverage: int | None,
) -> OrderResult:
    return OrderResult(
        symbol=order_response["symbol"],
        direction=direction,
        side=order_response["side"],
        order_id=order_response["orderId"],
        client_order_id=order_response["clientOrderId"],
        status=order_response["status"],
        requested_quantity=requested_quantity,
        executed_quantity=Decimal(order_response["executedQty"]),
        average_price=Decimal(order_response["avgPrice"]),
        leverage=leverage,
    )
