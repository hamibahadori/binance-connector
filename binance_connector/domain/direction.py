"""Which way a position is exposed to the price, and the order sides that follow from it."""

from enum import StrEnum


class PositionDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def opening_side(self) -> str:
        """The order side that opens (or grows) a position in this direction."""
        return "BUY" if self is PositionDirection.LONG else "SELL"

    @property
    def closing_side(self) -> str:
        """The order side that reduces a position in this direction (also its TP/SL side)."""
        return "SELL" if self is PositionDirection.LONG else "BUY"
