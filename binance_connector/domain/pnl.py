"""Profit and loss, and distances to take-profit / stop-loss (spec §6, §7).

P&L here is the price P&L, before fees: (exit or current − entry) × quantity, with the sign
flipped for a short. Fees come from Binance's per-fill commission and are reported
separately, together with the net figure (decision D-57/F).
"""

from dataclasses import dataclass
from decimal import Decimal


def _sign(direction: str) -> int:
    return 1 if direction == "LONG" else -1


def unrealized_pnl(*, direction: str, entry_price: Decimal, quantity: Decimal, current_price: Decimal) -> Decimal:
    return (current_price - entry_price) * quantity * _sign(direction)


def realized_pnl(*, direction: str, entry_price: Decimal, exit_price: Decimal, quantity: Decimal) -> Decimal:
    return (exit_price - entry_price) * quantity * _sign(direction)


@dataclass(frozen=True)
class PriceDistance:
    """How far the current price is from a target: absolute, and as % of the current price."""

    absolute: Decimal
    percent: Decimal


def price_distance(*, current_price: Decimal, target_price: Decimal | None) -> PriceDistance | None:
    if target_price is None:
        return None
    absolute = abs(target_price - current_price)
    return PriceDistance(absolute=absolute, percent=(absolute / current_price * 100).normalize())
