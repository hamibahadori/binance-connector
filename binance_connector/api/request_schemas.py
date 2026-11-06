"""Validation of Django's requests (spec §3), in two layers.

1. ``RequestEnvelope``: who is asking, and for what (request_id, user_id, account_id,
   api_key, secret_key, action, timestamp). If this fails the request cannot even be
   identified: intake answers 400 and stores nothing.
2. ``OpenPayload`` / ``ClosePayload``: the action's own fields. Failures here are business
   rejections: the request is stored as REJECTED with the reasons, so Django can read it back
   and a retry with the same request_id returns the same answer.

Rules that depend on the market (step size, minimum notional, tick size, where TP/SL sit
relative to the price) are checked afterwards by the intake service, using Binance's rules.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from binance_connector.api.schemas import Leverage, PositiveQuantity, Symbol, _RequestBody, _upper_case_text
from binance_connector.domain.lifecycle import RequestAction
from binance_connector.domain.sizing import EntryType, QuantityType
from binance_connector.services.position_service import PositionDirection

Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9._:-]{1,64}$")]
Credential = Annotated[str, Field(min_length=8, max_length=256)]
PositivePrice = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]

SECRET_FIELDS = frozenset({"api_key", "secret_key"})
ENVELOPE_FIELDS = frozenset({"request_id", "user_id", "account_id", "api_key", "secret_key", "action", "timestamp"})


class RequestEnvelope(BaseModel):
    """The identity part of every request. Other fields are left for the action's payload."""

    model_config = ConfigDict(extra="allow", frozen=True)

    request_id: Identifier
    user_id: Identifier
    account_id: Identifier
    api_key: Credential = Field(repr=False)
    secret_key: Credential = Field(repr=False)
    action: RequestAction
    timestamp: datetime
    """ISO 8601 (``2026-11-07T12:00:00Z``) or Unix epoch milliseconds."""

    _normalise_action = field_validator("action", mode="before")(_upper_case_text)

    @field_validator("timestamp")
    @classmethod
    def _assume_utc(cls, value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=UTC)


class OpenPayload(_RequestBody):
    """The fields of an OPEN request (everything except the envelope)."""

    symbol: Symbol
    direction: PositionDirection
    quantity: PositiveQuantity
    quantity_type: QuantityType = QuantityType.BASE
    leverage: Leverage | None = None
    entry_type: EntryType = EntryType.MARKET
    entry_price: PositivePrice | None = None
    """LIMIT: the limit price. MARKET: Django's expected price (slippage is reported against
    it, and the price guard compares it with the mark price)."""
    take_profit_price: PositivePrice | None = None
    stop_loss_price: PositivePrice | None = None

    _normalise_text = field_validator("symbol", "direction", "quantity_type", "entry_type", mode="before")(
        _upper_case_text
    )

    @model_validator(mode="after")
    def _limit_needs_a_price(self) -> "OpenPayload":
        if self.entry_type is EntryType.LIMIT and self.entry_price is None:
            raise ValueError("a LIMIT entry needs an entry_price")
        return self


class ClosePayload(_RequestBody):
    """The fields of a CLOSE request: which position, by id or by symbol."""

    position_id: Identifier | None = None
    symbol: Symbol | None = None

    _normalise_text = field_validator("symbol", mode="before")(_upper_case_text)

    @model_validator(mode="after")
    def _needs_a_target(self) -> "ClosePayload":
        if self.position_id is None and self.symbol is None:
            raise ValueError("a CLOSE needs a position_id or symbol")
        return self


def payload_fields(body: dict[str, Any]) -> dict[str, Any]:
    """The action's own fields: the body without the envelope."""
    return {key: value for key, value in body.items() if key not in ENVELOPE_FIELDS}


def without_secrets(body: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in body.items() if key not in SECRET_FIELDS}
