"""Turn a requested amount into a Binance order quantity (decision D-50).

Django may express the size of an OPEN three ways:

* ``BASE``     coins, e.g. 0.002 BTC. Must already be a multiple of the step size: an
               explicit coin amount is not silently changed.
* ``NOTIONAL`` USDT value; coins = notional / price.
* ``MARGIN``   USDT margin; coins = margin × leverage / price (leverage required).

The price is the limit price for a LIMIT entry and the current mark price for a MARKET one.
Converted amounts are **rounded down** to the step size: never more than asked for. The
result must then satisfy Binance's per-symbol rules (recorded on Demo, 2026-11-07):

* MARKET orders use MARKET_LOT_SIZE (step, minimum, maximum); LIMIT orders use LOT_SIZE.
* quantity × price must reach MIN_NOTIONAL (50 USDT on BTCUSDT).
"""

from decimal import ROUND_DOWN, Decimal
from enum import StrEnum

from binance_connector.persistence.market_data import ExchangeRules


class QuantityType(StrEnum):
    BASE = "BASE"
    NOTIONAL = "NOTIONAL"
    MARGIN = "MARGIN"


class EntryType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class SizingError(ValueError):
    """The requested amount cannot become a valid order; the message is the reason."""


def plain(value: Decimal) -> str:
    """A Decimal without trailing zeros or scientific notation: 120, 0.0001, 37.5."""
    return format(value.normalize(), "f")


def is_multiple(value: Decimal, step: Decimal) -> bool:
    return value % step == 0


def round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def size_order(
    *,
    amount: Decimal,
    quantity_type: QuantityType,
    reference_price: Decimal,
    leverage: int | None,
    rules: ExchangeRules,
    entry_type: EntryType,
) -> Decimal:
    """The order quantity in coins, or SizingError explaining why there is none."""
    is_market = entry_type is EntryType.MARKET
    step = rules.market_step_size if is_market else rules.step_size
    minimum = rules.market_min_quantity if is_market else rules.min_quantity

    if quantity_type is QuantityType.BASE:
        if not is_multiple(amount, step):
            raise SizingError(f"quantity {amount} must be a multiple of {plain(step)} for {rules.symbol}")
        quantity = amount
    elif quantity_type is QuantityType.NOTIONAL:
        quantity = round_down(amount / reference_price, step)
    else:
        if leverage is None:
            raise SizingError("a MARGIN quantity needs a leverage")
        quantity = round_down(amount * leverage / reference_price, step)

    if quantity < minimum:
        raise SizingError(f"the quantity {plain(quantity)} is below the minimum quantity {plain(minimum)} "
                          f"for {rules.symbol}")
    if is_market and quantity > rules.market_max_quantity:
        raise SizingError(f"the quantity {plain(quantity)} is above the market maximum "
                          f"{plain(rules.market_max_quantity)} for {rules.symbol}")
    notional = quantity * reference_price
    if notional < rules.min_notional:
        raise SizingError(f"the order value {plain(notional)} USDT is below the minimum notional "
                          f"{plain(rules.min_notional)} USDT for {rules.symbol}")
    return quantity
