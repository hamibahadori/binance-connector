"""Unit tests for the shared field rules (symbol, direction, quantity, leverage).

They are exercised through OpenTradeRequest, the model that uses all of them.
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from binance_connector.api.trade_schemas import OpenTradeRequest
from binance_connector.services.position_service import PositionDirection

VALID_OPEN_BODY = {"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002"}


def test_symbol_and_direction_are_case_insensitive_and_trimmed():
    request_model = OpenTradeRequest.model_validate({"symbol": " btcusdt ", "direction": "short", "quantity": "1"})

    assert request_model.symbol == "BTCUSDT"
    assert request_model.direction is PositionDirection.SHORT


def test_json_number_quantity_keeps_its_decimal_value():
    request_model = OpenTradeRequest.model_validate({**VALID_OPEN_BODY, "quantity": 0.002})

    assert request_model.quantity == Decimal("0.002")


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("symbol", ""),
        ("symbol", "BTC-USDT"),
        ("symbol", "B"),
        ("direction", "UP"),
        ("quantity", "0"),
        ("quantity", "-1"),
        ("quantity", "abc"),
        ("quantity", "NaN"),
        ("quantity", "Infinity"),
        ("leverage", 0),
        ("leverage", 126),
        ("leverage", 2.5),
        ("leverage", "10"),
        ("leverage", True),
    ],
)
def test_invalid_field_is_rejected(field_name, bad_value):
    with pytest.raises(ValidationError) as raised:
        OpenTradeRequest.model_validate({**VALID_OPEN_BODY, field_name: bad_value})

    assert raised.value.errors()[0]["loc"][0] == field_name


@pytest.mark.parametrize("required_field", ["symbol", "direction", "quantity"])
def test_missing_required_field_is_rejected(required_field):
    body = {key: value for key, value in VALID_OPEN_BODY.items() if key != required_field}

    with pytest.raises(ValidationError) as raised:
        OpenTradeRequest.model_validate(body)

    assert raised.value.errors()[0]["loc"] == (required_field,)
