"""Unit tests for the open/close business rules.

The service is driven through a fake client that records every call, so these
tests check which Binance calls are made, in which order, with which values,
with no HTTP involved.
"""

from decimal import Decimal
from typing import Any

import pytest

from binance_connector.errors import (
    BinanceUnknownOutcomeError,
    CloseQuantityExceedsPositionError,
    HedgeModeNotSupportedError,
    NoOpenPositionError,
)
from binance_connector.services.position_service import (
    ClosePositionCommand,
    OpenPositionCommand,
    PositionDirection,
    PositionService,
)

GENERATED_CLIENT_ORDER_ID = "generated-id"


class FakeFuturesClient:
    """Stands in for BinanceFuturesClient and records what the service asks of it."""

    def __init__(
        self,
        position_entries: list[dict[str, Any]] | None = None,
        order_error: Exception | None = None,
    ) -> None:
        self.position_entries = position_entries or []
        self.order_error = order_error
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def change_leverage(self, *, symbol: str, leverage: int) -> dict[str, Any]:
        self.calls.append(("change_leverage", {"symbol": symbol, "leverage": leverage}))
        return {"symbol": symbol, "leverage": leverage}

    def place_market_order(self, **order_arguments: Any) -> dict[str, Any]:
        self.calls.append(("place_market_order", order_arguments))
        if self.order_error is not None:
            raise self.order_error
        quantity_text = format(order_arguments["quantity"], "f")
        return {
            "orderId": 4000000001,
            "symbol": order_arguments["symbol"],
            "status": "FILLED",
            "clientOrderId": order_arguments["client_order_id"],
            "side": order_arguments["side"],
            "origQty": quantity_text,
            "executedQty": quantity_text,
            "avgPrice": "65000.10",
        }

    def get_position_risk(self, *, symbol: str) -> list[dict[str, Any]]:
        self.calls.append(("get_position_risk", {"symbol": symbol}))
        return self.position_entries

    def call_names(self) -> list[str]:
        return [call_name for call_name, _ in self.calls]

    def order_arguments(self) -> dict[str, Any]:
        order_calls = [arguments for call_name, arguments in self.calls if call_name == "place_market_order"]
        assert len(order_calls) == 1, f"expected exactly one order, got {len(order_calls)}"
        return order_calls[0]


def one_way_position(position_amount: str, symbol: str = "BTCUSDT") -> dict[str, Any]:
    """A positionRisk entry as Binance returns it in one-way mode."""
    return {"symbol": symbol, "positionSide": "BOTH", "positionAmt": position_amount}


def make_service(fake_client: FakeFuturesClient) -> PositionService:
    return PositionService(fake_client, generate_client_order_id=lambda: GENERATED_CLIENT_ORDER_ID)


# --- open -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("direction", "expected_side"),
    [(PositionDirection.LONG, "BUY"), (PositionDirection.SHORT, "SELL")],
)
def test_open_places_a_market_order_on_the_matching_side(direction, expected_side):
    fake_client = FakeFuturesClient()

    result = make_service(fake_client).open_position(
        OpenPositionCommand(symbol="BTCUSDT", direction=direction, quantity=Decimal("0.002"))
    )

    assert fake_client.order_arguments() == {
        "symbol": "BTCUSDT",
        "side": expected_side,
        "quantity": Decimal("0.002"),
        "client_order_id": GENERATED_CLIENT_ORDER_ID,
        "reduce_only": False,
    }
    assert result.direction is direction
    assert result.side == expected_side


def test_open_sets_leverage_before_placing_the_order():
    fake_client = FakeFuturesClient()

    result = make_service(fake_client).open_position(
        OpenPositionCommand(
            symbol="BTCUSDT", direction=PositionDirection.LONG, quantity=Decimal("0.002"), leverage=10
        )
    )

    assert fake_client.call_names() == ["change_leverage", "place_market_order"]
    assert fake_client.calls[0][1] == {"symbol": "BTCUSDT", "leverage": 10}
    assert result.leverage == 10


def test_open_without_leverage_leaves_leverage_untouched():
    fake_client = FakeFuturesClient()

    result = make_service(fake_client).open_position(
        OpenPositionCommand(symbol="BTCUSDT", direction=PositionDirection.LONG, quantity=Decimal("0.002"))
    )

    assert fake_client.call_names() == ["place_market_order"]
    assert result.leverage is None


def test_open_uses_the_callers_client_order_id_when_given():
    fake_client = FakeFuturesClient()

    result = make_service(fake_client).open_position(
        OpenPositionCommand(
            symbol="BTCUSDT",
            direction=PositionDirection.LONG,
            quantity=Decimal("0.002"),
            client_order_id="caller-chosen-id",
        )
    )

    assert fake_client.order_arguments()["client_order_id"] == "caller-chosen-id"
    assert result.client_order_id == "caller-chosen-id"


def test_open_maps_the_binance_order_response_into_the_result():
    fake_client = FakeFuturesClient()

    result = make_service(fake_client).open_position(
        OpenPositionCommand(symbol="BTCUSDT", direction=PositionDirection.LONG, quantity=Decimal("0.002"))
    )

    assert result.symbol == "BTCUSDT"
    assert result.order_id == 4000000001
    assert result.client_order_id == GENERATED_CLIENT_ORDER_ID
    assert result.status == "FILLED"
    assert result.requested_quantity == Decimal("0.002")
    assert result.executed_quantity == Decimal("0.002")
    assert result.average_price == Decimal("65000.10")


def test_open_with_unknown_outcome_reports_the_client_order_id():
    fake_client = FakeFuturesClient(order_error=BinanceUnknownOutcomeError("timed out"))

    with pytest.raises(BinanceUnknownOutcomeError) as raised:
        make_service(fake_client).open_position(
            OpenPositionCommand(symbol="BTCUSDT", direction=PositionDirection.LONG, quantity=Decimal("0.002"))
        )

    assert raised.value.client_order_id == GENERATED_CLIENT_ORDER_ID


# --- close ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("position_amount", "expected_side", "expected_quantity", "expected_direction"),
    [
        ("0.005", "SELL", Decimal("0.005"), PositionDirection.LONG),
        ("-0.004", "BUY", Decimal("0.004"), PositionDirection.SHORT),
    ],
)
def test_close_without_quantity_closes_the_whole_position(
    position_amount, expected_side, expected_quantity, expected_direction
):
    fake_client = FakeFuturesClient(position_entries=[one_way_position(position_amount)])

    result = make_service(fake_client).close_position(ClosePositionCommand(symbol="BTCUSDT"))

    assert fake_client.call_names() == ["get_position_risk", "place_market_order"]
    assert fake_client.order_arguments() == {
        "symbol": "BTCUSDT",
        "side": expected_side,
        "quantity": expected_quantity,
        "client_order_id": GENERATED_CLIENT_ORDER_ID,
        "reduce_only": True,
    }
    assert result.direction is expected_direction
    assert result.requested_quantity == expected_quantity


@pytest.mark.parametrize("close_quantity", [Decimal("0.001"), Decimal("0.005")])
def test_close_with_quantity_closes_that_much(close_quantity):
    fake_client = FakeFuturesClient(position_entries=[one_way_position("0.005")])

    make_service(fake_client).close_position(
        ClosePositionCommand(symbol="BTCUSDT", quantity=close_quantity)
    )

    assert fake_client.order_arguments()["quantity"] == close_quantity
    assert fake_client.order_arguments()["reduce_only"] is True


def test_close_quantity_larger_than_the_position_is_refused_before_ordering():
    fake_client = FakeFuturesClient(position_entries=[one_way_position("-0.004")])

    with pytest.raises(CloseQuantityExceedsPositionError) as raised:
        make_service(fake_client).close_position(
            ClosePositionCommand(symbol="BTCUSDT", quantity=Decimal("0.005"))
        )

    assert raised.value.open_quantity == Decimal("0.004")
    assert fake_client.call_names() == ["get_position_risk"]


@pytest.mark.parametrize(
    "position_entries",
    [[], [one_way_position("0")], [one_way_position("0.000")]],
    ids=["no-entries", "zero", "zero-with-decimals"],
)
def test_close_with_no_open_position_is_refused(position_entries):
    fake_client = FakeFuturesClient(position_entries=position_entries)

    with pytest.raises(NoOpenPositionError):
        make_service(fake_client).close_position(ClosePositionCommand(symbol="BTCUSDT"))

    assert fake_client.call_names() == ["get_position_risk"]


def test_close_refuses_an_account_in_hedge_mode():
    hedge_mode_entries = [
        {"symbol": "BTCUSDT", "positionSide": "LONG", "positionAmt": "0.005"},
        {"symbol": "BTCUSDT", "positionSide": "SHORT", "positionAmt": "0"},
    ]
    fake_client = FakeFuturesClient(position_entries=hedge_mode_entries)

    with pytest.raises(HedgeModeNotSupportedError):
        make_service(fake_client).close_position(ClosePositionCommand(symbol="BTCUSDT"))

    assert fake_client.call_names() == ["get_position_risk"]


def test_close_uses_the_callers_client_order_id_when_given():
    fake_client = FakeFuturesClient(position_entries=[one_way_position("0.005")])

    result = make_service(fake_client).close_position(
        ClosePositionCommand(symbol="BTCUSDT", client_order_id="caller-close-id")
    )

    assert result.client_order_id == "caller-close-id"


def test_close_with_unknown_outcome_reports_the_client_order_id():
    fake_client = FakeFuturesClient(
        position_entries=[one_way_position("0.005")],
        order_error=BinanceUnknownOutcomeError("timed out"),
    )

    with pytest.raises(BinanceUnknownOutcomeError) as raised:
        make_service(fake_client).close_position(ClosePositionCommand(symbol="BTCUSDT"))

    assert raised.value.client_order_id == GENERATED_CLIENT_ORDER_ID


def test_generated_client_order_ids_are_unique_and_fit_binance_rules():
    service = PositionService(FakeFuturesClient())

    generated_ids = {service.generate_client_order_id() for _ in range(100)}

    assert len(generated_ids) == 100
    assert all(len(generated_id) <= 36 for generated_id in generated_ids)
