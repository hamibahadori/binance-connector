"""Unit tests for request-body validation."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from binance_connector.api.schemas import ClosePositionRequest, OpenPositionRequest
from binance_connector.services.position_service import (
    ClosePositionCommand,
    OpenPositionCommand,
    PositionDirection,
)

VALID_OPEN_BODY = {"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002"}


def test_valid_open_body_becomes_a_command():
    request_model = OpenPositionRequest.model_validate(
        {**VALID_OPEN_BODY, "leverage": 10, "client_order_id": "my-order-1"}
    )

    assert request_model.to_command() == OpenPositionCommand(
        symbol="BTCUSDT",
        direction=PositionDirection.LONG,
        quantity=Decimal("0.002"),
        leverage=10,
        client_order_id="my-order-1",
    )


def test_symbol_and_direction_are_case_insensitive_and_trimmed():
    request_model = OpenPositionRequest.model_validate(
        {"symbol": " btcusdt ", "direction": "short", "quantity": "1"}
    )

    assert request_model.symbol == "BTCUSDT"
    assert request_model.direction is PositionDirection.SHORT


def test_json_number_quantity_keeps_its_decimal_value():
    request_model = OpenPositionRequest.model_validate({**VALID_OPEN_BODY, "quantity": 0.002})

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
        ("client_order_id", ""),
        ("client_order_id", "x" * 37),
        ("client_order_id", "has space"),
    ],
)
def test_invalid_open_field_is_rejected(field_name, bad_value):
    with pytest.raises(ValidationError) as raised:
        OpenPositionRequest.model_validate({**VALID_OPEN_BODY, field_name: bad_value})

    assert raised.value.errors()[0]["loc"][0] == field_name


@pytest.mark.parametrize("required_field", ["symbol", "direction", "quantity"])
def test_missing_required_open_field_is_rejected(required_field):
    body = {key: value for key, value in VALID_OPEN_BODY.items() if key != required_field}

    with pytest.raises(ValidationError) as raised:
        OpenPositionRequest.model_validate(body)

    assert raised.value.errors()[0]["loc"] == (required_field,)


def test_unknown_field_is_rejected():
    with pytest.raises(ValidationError) as raised:
        OpenPositionRequest.model_validate({**VALID_OPEN_BODY, "price": "65000"})

    assert raised.value.errors()[0]["loc"] == ("price",)


def test_close_body_needs_only_a_symbol():
    request_model = ClosePositionRequest.model_validate({"symbol": "ethusdt"})

    assert request_model.to_command() == ClosePositionCommand(symbol="ETHUSDT")


def test_close_body_accepts_quantity_and_client_order_id():
    request_model = ClosePositionRequest.model_validate(
        {"symbol": "BTCUSDT", "quantity": "0.001", "client_order_id": "close-1"}
    )

    assert request_model.to_command() == ClosePositionCommand(
        symbol="BTCUSDT", quantity=Decimal("0.001"), client_order_id="close-1"
    )


@pytest.mark.parametrize("bad_quantity", ["0", "-0.001", "abc"])
def test_close_body_rejects_a_non_positive_quantity(bad_quantity):
    with pytest.raises(ValidationError):
        ClosePositionRequest.model_validate({"symbol": "BTCUSDT", "quantity": bad_quantity})
