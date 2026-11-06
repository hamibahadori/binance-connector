"""Unit tests for order sizing (quantity types, rounding, Binance minimums) and P&L."""

from decimal import Decimal

import pytest

from binance_connector.domain.pnl import price_distance, realized_pnl, unrealized_pnl
from binance_connector.domain.sizing import EntryType, QuantityType, SizingError, size_order
from binance_connector.persistence.market_data import ExchangeRules

# BTCUSDT on Demo, recorded 2026-11-07 (tests/fixtures/demo_capture_2026_11_07.json).
BTCUSDT_RULES = ExchangeRules(
    symbol="BTCUSDT", tick_size=Decimal("0.10"), step_size=Decimal("0.0001"), min_quantity=Decimal("0.0001"),
    market_step_size=Decimal("0.0001"), market_min_quantity=Decimal("0.0001"),
    market_max_quantity=Decimal("120"), min_notional=Decimal("50"),
)


def size(amount, quantity_type=QuantityType.BASE, price="75000", leverage=None, entry_type=EntryType.MARKET):
    return size_order(amount=Decimal(amount), quantity_type=quantity_type, reference_price=Decimal(price),
                      leverage=leverage, rules=BTCUSDT_RULES, entry_type=entry_type)


# --- quantity types -----------------------------------------------------------------------------


def test_a_base_quantity_is_used_as_is():
    assert size("0.002") == Decimal("0.002")


def test_a_notional_is_converted_with_the_reference_price_and_rounded_down():
    # 150 / 75869.5 = 0.001977...; rounded down to the 0.0001 step.
    assert size("150", QuantityType.NOTIONAL, price="75869.5") == Decimal("0.0019")


def test_a_margin_is_multiplied_by_the_leverage_first():
    # 30 USDT margin x 5 = 150 USDT notional at 75000 = 0.002 BTC.
    assert size("30", QuantityType.MARGIN, leverage=5) == Decimal("0.002")


def test_a_margin_needs_a_leverage():
    with pytest.raises(SizingError, match="leverage"):
        size("30", QuantityType.MARGIN)


def test_a_base_quantity_off_the_step_is_refused_not_rounded():
    with pytest.raises(SizingError, match="multiple of 0.0001"):
        size("0.00025")


# --- Binance minimums and maximums --------------------------------------------------------------


def test_an_order_below_the_minimum_notional_is_refused():
    with pytest.raises(SizingError, match="50"):
        size("0.0005")  # 37.5 USDT


def test_a_notional_that_rounds_to_nothing_is_refused():
    with pytest.raises(SizingError, match="minimum quantity"):
        size("0.5", QuantityType.NOTIONAL)


def test_a_market_order_above_the_market_maximum_is_refused():
    with pytest.raises(SizingError, match="120"):
        size("121")


def test_a_limit_order_uses_the_limit_step_and_has_no_market_maximum():
    assert size("121", entry_type=EntryType.LIMIT) == Decimal("121")


# --- P&L ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("direction", "current_price", "expected_pnl"),
    [("LONG", "76000", "2.000"), ("LONG", "74000", "-2.000"), ("SHORT", "74000", "2.000"), ("SHORT", "76000", "-2.000")],
)
def test_unrealized_pnl_follows_the_direction(direction, current_price, expected_pnl):
    pnl = unrealized_pnl(direction=direction, entry_price=Decimal("75000"), quantity=Decimal("0.002"),
                         current_price=Decimal(current_price))

    assert pnl == Decimal(expected_pnl)


def test_realized_pnl_uses_the_exit_price():
    assert realized_pnl(direction="SHORT", entry_price=Decimal("75000"), exit_price=Decimal("74500"),
                        quantity=Decimal("0.004")) == Decimal("2.0000")


def test_price_distance_is_absolute_and_in_percent_of_the_current_price():
    distance = price_distance(current_price=Decimal("80000"), target_price=Decimal("82000"))

    assert distance.absolute == Decimal("2000")
    assert distance.percent == Decimal("2.5")


def test_price_distance_percent_is_rounded_to_four_decimals():
    """Found on Demo: 3788.8 / 75776.3 printed with 25 decimals."""
    distance = price_distance(current_price=Decimal("75776.3"), target_price=Decimal("79565.1"))

    assert str(distance.percent) == "5.0000"


def test_price_distance_is_none_without_a_target():
    assert price_distance(current_price=Decimal("80000"), target_price=None) is None
