"""Unit tests for the position view's live figures: current price, P&L, fees, TP/SL distance."""

from datetime import UTC, datetime
from decimal import Decimal

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.persistence.market_data import MarketPrice
from binance_connector.presentation import position_view
from tests.support import POSITION_ID, store_position

PRICE_TIME = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)


def market(price: str) -> MarketPrice:
    return MarketPrice("BTCUSDT", Decimal(price), PRICE_TIME, PRICE_TIME)


def open_position(positions, entry_commission="0.0624"):
    store_position(positions)
    positions.prepare_execution(POSITION_ID, ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET", side="BUY")
    positions.update_execution(POSITION_ID, ExecutionRole.ENTRY, status="FILLED", commission=Decimal(entry_commission))
    return positions.transition(POSITION_ID, PositionState.OPEN, entry_average_price=Decimal("78000"),
                                entry_filled_quantity=Decimal("0.002"))


def test_an_open_position_shows_the_current_price_and_unrealized_pnl_net_of_fees(positions):
    view = position_view(open_position(positions), user_id="user-1", market_price=market("79000"))

    assert view["prices"]["current"] == "79000"
    assert view["prices"]["current_at"] == "2026-11-07T12:00:00+00:00"
    assert view["pnl"] == {"kind": "UNREALIZED", "gross": "2.000", "fees": "0.0624", "net": "1.9376"}


def test_distances_to_take_profit_and_stop_loss_follow_the_current_price(positions):
    view = position_view(open_position(positions), user_id="user-1", market_price=market("80000"))

    assert view["take_profit"]["distance"] == {"absolute": "2000.0", "percent": "2.5000"}
    assert view["stop_loss"]["distance"] == {"absolute": "6000.0", "percent": "7.5000"}


def test_without_a_price_there_is_no_unrealized_pnl_or_distance(positions):
    view = position_view(open_position(positions), user_id="user-1", market_price=None)

    assert view["pnl"]["kind"] is None
    assert view["take_profit"]["distance"] is None


def test_a_closed_position_shows_its_realized_pnl_not_the_current_price_move(positions):
    open_position(positions)
    closed = positions.transition(POSITION_ID, PositionState.CLOSED, close_reason=CloseReason.TAKE_PROFIT,
                                  exit_average_price=Decimal("82000"), realized_pnl=Decimal("8.000"),
                                  fees=Decimal("0.128"))

    view = position_view(closed, user_id="user-1", market_price=market("70000"))

    assert view["pnl"] == {"kind": "REALIZED", "gross": "8.000", "fees": "0.128", "net": "7.872"}
    assert view["close_reason"] == "TAKE_PROFIT"
