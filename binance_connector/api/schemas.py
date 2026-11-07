"""Shared building blocks for request validation (pydantic).

These field types stop malformed input before anything is stored. Rules that depend on
the market (step size, tick size, minimum notional) are checked afterwards by the intake
service against Binance's rules. The request models are in ``request_schemas``.
"""

from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt

SYMBOL_PATTERN = r"^[A-Z0-9]{2,20}$"
"""Binance futures symbols are upper-case letters and digits, e.g. BTCUSDT or 1000PEPEUSDT."""

MAX_LEVERAGE = 125
"""Binance's highest leverage on any symbol. Lower per-symbol limits are enforced by Binance."""

Symbol = Annotated[str, Field(pattern=SYMBOL_PATTERN)]
PositiveQuantity = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
Leverage = Annotated[StrictInt, Field(ge=1, le=MAX_LEVERAGE)]


def _upper_case_text(value: Any) -> Any:
    """Trim and upper-case strings; leave anything else for normal validation to reject."""
    return value.strip().upper() if isinstance(value, str) else value


class _RequestBody(BaseModel):
    """Shared settings: unknown fields are errors, so a typo is never silently ignored."""

    model_config = ConfigDict(extra="forbid", frozen=True)
