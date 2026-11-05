"""Unit tests for validating Strategy trade request bodies."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from binance_connector.api.trade_schemas import CloseTradeRequest, OpenTradeRequest
from binance_connector.domain.trade_state import CloseReason
from binance_connector.services.position_service import PositionDirection
from binance_connector.services.trade_service import OpenTradeCommand

VALID_LONG = {
    "symbol": "BTCUSDT",
    "direction": "LONG",
    "quantity": "0.002",
    "leverage": 5,
    "take_profit_price": "82000.0",
    "stop_loss_price": "74000.0",
    "strategy_request_id": "strategy-7:signal-42",
}


def test_a_full_long_request_becomes_a_command():
    command = OpenTradeRequest.model_validate(VALID_LONG).to_command()

    assert command == OpenTradeCommand(
        symbol="BTCUSDT",
        direction=PositionDirection.LONG,
        quantity=Decimal("0.002"),
        leverage=5,
        take_profit_price=Decimal("82000.0"),
        stop_loss_price=Decimal("74000.0"),
        strategy_request_id="strategy-7:signal-42",
    )


def test_tp_sl_leverage_and_request_id_are_optional():
    command = OpenTradeRequest.model_validate({"symbol": "ethusdt", "direction": "short", "quantity": 1}).to_command()

    assert command.symbol == "ETHUSDT"
    assert command.direction is PositionDirection.SHORT
    assert (command.take_profit_price, command.stop_loss_price, command.leverage) == (None, None, None)


@pytest.mark.parametrize(
    ("direction", "take_profit_price", "stop_loss_price"),
    [("LONG", "74000", "82000"), ("SHORT", "82000", "74000"), ("LONG", "80000", "80000")],
)
def test_take_profit_must_be_on_the_profitable_side_of_the_stop_loss(direction, take_profit_price, stop_loss_price):
    body = {**VALID_LONG, "direction": direction, "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price}

    with pytest.raises(ValidationError, match="take_profit_price"):
        OpenTradeRequest.model_validate(body)


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("take_profit_price", "0"),
        ("stop_loss_price", "-1"),
        ("take_profit_price", "abc"),
        ("strategy_request_id", ""),
        ("strategy_request_id", "has space"),
        ("strategy_request_id", "x" * 65),
        ("leverage", 0),
    ],
)
def test_invalid_fields_are_rejected(field_name, bad_value):
    with pytest.raises(ValidationError) as raised:
        OpenTradeRequest.model_validate({**VALID_LONG, field_name: bad_value})

    assert raised.value.errors()[0]["loc"][0] == field_name


def test_unknown_fields_are_rejected():
    with pytest.raises(ValidationError):
        OpenTradeRequest.model_validate({**VALID_LONG, "takeprofit": "1"})


@pytest.mark.parametrize(("body", "expected_reason"), [({}, CloseReason.MANUAL),
                                                        ({"reason": "upstream"}, CloseReason.UPSTREAM)])
def test_close_reason_defaults_to_manual(body, expected_reason):
    assert CloseTradeRequest.model_validate(body).reason is expected_reason


@pytest.mark.parametrize("bad_reason", ["TAKE_PROFIT", "EXTERNAL", "whatever"])
def test_callers_can_only_give_manual_or_upstream_as_close_reason(bad_reason):
    with pytest.raises(ValidationError):
        CloseTradeRequest.model_validate({"reason": bad_reason})
