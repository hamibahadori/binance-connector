"""Request bodies for the Strategy trades API.

Open: symbol, direction, quantity (base asset), optional leverage, optional take-profit and
stop-loss trigger prices, optional ``strategy_request_id`` for idempotent retries.

The customer's written spec for the Strategy payload has not arrived yet; field names and
units (e.g. size in coins vs. USDT) may change when it does.
"""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from binance_connector.api.schemas import (
    Leverage,
    PositiveQuantity,
    Symbol,
    _RequestBody,
    _upper_case_text,
)
from binance_connector.domain.trade_state import CloseReason
from binance_connector.services.position_service import PositionDirection
from binance_connector.services.trade_service import OpenTradeCommand

TriggerPrice = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
StrategyRequestId = Annotated[str, Field(pattern=r"^[A-Za-z0-9._:-]{1,64}$")]


class OpenTradeRequest(_RequestBody):
    """Body of ``POST /api/v1/strategy/trades``."""

    symbol: Symbol
    direction: PositionDirection
    quantity: PositiveQuantity
    leverage: Leverage | None = None
    take_profit_price: TriggerPrice | None = None
    stop_loss_price: TriggerPrice | None = None
    strategy_request_id: StrategyRequestId | None = None

    _normalise_text = field_validator("symbol", "direction", mode="before")(_upper_case_text)

    @model_validator(mode="after")
    def _take_profit_beyond_stop_loss(self) -> "OpenTradeRequest":
        """A long profits upwards (TP above SL); a short profits downwards (TP below SL).

        Whether each price is on the right side of the *entry* price can only be known
        after the fill; Binance rejects one that is already crossed (``-2021``).
        """
        if self.take_profit_price is None or self.stop_loss_price is None:
            return self
        profitable_side_ok = (
            self.take_profit_price > self.stop_loss_price
            if self.direction is PositionDirection.LONG
            else self.take_profit_price < self.stop_loss_price
        )
        if not profitable_side_ok:
            relation = "above" if self.direction is PositionDirection.LONG else "below"
            raise ValueError(f"take_profit_price must be {relation} stop_loss_price for a {self.direction.value}")
        return self

    def to_command(self) -> OpenTradeCommand:
        return OpenTradeCommand(
            symbol=self.symbol,
            direction=self.direction,
            quantity=self.quantity,
            leverage=self.leverage,
            take_profit_price=self.take_profit_price,
            stop_loss_price=self.stop_loss_price,
            strategy_request_id=self.strategy_request_id,
        )


class CloseTradeRequest(_RequestBody):
    """Body of ``POST /api/v1/strategy/trades/<trade_id>/close``.

    ``MANUAL`` (default) for a person's request, ``UPSTREAM`` for an exit command from the
    upstream system. TAKE_PROFIT / STOP_LOSS / EXTERNAL are set by the service only.
    """

    reason: Literal[CloseReason.MANUAL, CloseReason.UPSTREAM] = CloseReason.MANUAL

    _normalise_text = field_validator("reason", mode="before")(_upper_case_text)
