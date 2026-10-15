"""Request bodies for the positions API, validated with pydantic.

Validation stops obviously bad input before any Binance call. Rules that
depend on the market, such as a symbol's step size or minimum notional, are
left to Binance, which knows them precisely.
"""

from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from binance_connector.services.position_service import (
    ClosePositionCommand,
    OpenPositionCommand,
    PositionDirection,
)

SYMBOL_PATTERN = r"^[A-Z0-9]{2,20}$"
"""Binance futures symbols are upper-case letters and digits, e.g. BTCUSDT or 1000PEPEUSDT."""

CLIENT_ORDER_ID_PATTERN = r"^[.A-Z:/a-z0-9_-]{1,36}$"
"""Binance's documented rule for ``newClientOrderId``."""

MAX_LEVERAGE = 125
"""Binance's highest leverage on any symbol. Lower per-symbol limits are enforced by Binance."""

Symbol = Annotated[str, Field(pattern=SYMBOL_PATTERN)]
PositiveQuantity = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
Leverage = Annotated[StrictInt, Field(ge=1, le=MAX_LEVERAGE)]
ClientOrderId = Annotated[str, Field(pattern=CLIENT_ORDER_ID_PATTERN)]


def _upper_case_text(value: Any) -> Any:
    """Trim and upper-case strings; leave anything else for normal validation to reject."""
    return value.strip().upper() if isinstance(value, str) else value


class _RequestBody(BaseModel):
    """Shared settings: unknown fields are errors, so a typo is never silently ignored."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class OpenPositionRequest(_RequestBody):
    """Body of ``POST /api/v1/positions/open``."""

    symbol: Symbol
    direction: PositionDirection
    quantity: PositiveQuantity
    leverage: Leverage | None = None
    client_order_id: ClientOrderId | None = None

    _normalise_text = field_validator("symbol", "direction", mode="before")(_upper_case_text)

    def to_command(self) -> OpenPositionCommand:
        return OpenPositionCommand(
            symbol=self.symbol,
            direction=self.direction,
            quantity=self.quantity,
            leverage=self.leverage,
            client_order_id=self.client_order_id,
        )


class ClosePositionRequest(_RequestBody):
    """Body of ``POST /api/v1/positions/close``. Omit ``quantity`` to close everything."""

    symbol: Symbol
    quantity: PositiveQuantity | None = None
    client_order_id: ClientOrderId | None = None

    _normalise_text = field_validator("symbol", mode="before")(_upper_case_text)

    def to_command(self) -> ClosePositionCommand:
        return ClosePositionCommand(
            symbol=self.symbol,
            quantity=self.quantity,
            client_order_id=self.client_order_id,
        )
