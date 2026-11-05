"""The lifecycle of a Strategy trade, as an explicit state machine.

::

    RECEIVED → ENTRY_SUBMITTED → OPEN → PROTECTED → CLOSING → CLOSED
        │             │            │        │           │
        └→ ENTRY_FAILED ←┘         └────────┴───────────┴→ NEEDS_ATTENTION

* ``RECEIVED``        stored, nothing sent to Binance yet.
* ``ENTRY_SUBMITTED`` the entry order is being (or has been) sent; its outcome is not known yet.
* ``OPEN``            the entry filled; no take-profit/stop-loss is in place (none was asked
                      for, or placing it is still in progress).
* ``PROTECTED``       the entry filled and every requested take-profit/stop-loss is on Binance.
* ``CLOSING``         a manual or upstream close is being executed.
* ``CLOSED``          the position is gone; ``close_reason`` says why.
* ``ENTRY_FAILED``    the entry never executed.
* ``NEEDS_ATTENTION`` something did not go to plan (an unknown outcome, protection that could
                      not be placed, a position changed outside the service). Reconciliation
                      moves the trade on once Binance's real state is known.

Every check of a transition happens against the freshly loaded state (see
``TradeRepository.transition``), so the API process and the worker process can never
both "win" a conflicting change.
"""

from enum import StrEnum

from binance_connector.errors import InvalidTransitionError


class TradeState(StrEnum):
    RECEIVED = "RECEIVED"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    OPEN = "OPEN"
    PROTECTED = "PROTECTED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    ENTRY_FAILED = "ENTRY_FAILED"
    NEEDS_ATTENTION = "NEEDS_ATTENTION"


class CloseReason(StrEnum):
    """Why a trade ended up CLOSED."""

    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    MANUAL = "MANUAL"
    UPSTREAM = "UPSTREAM"
    EXTERNAL = "EXTERNAL"
    """The position disappeared without one of our orders explaining it (e.g. liquidation)."""


TERMINAL_STATES = frozenset({TradeState.CLOSED, TradeState.ENTRY_FAILED})
ACTIVE_STATES = frozenset(set(TradeState) - TERMINAL_STATES)

ALLOWED_TRANSITIONS: dict[TradeState, frozenset[TradeState]] = {
    TradeState.RECEIVED: frozenset({TradeState.ENTRY_SUBMITTED, TradeState.ENTRY_FAILED}),
    TradeState.ENTRY_SUBMITTED: frozenset(
        {TradeState.OPEN, TradeState.ENTRY_FAILED, TradeState.NEEDS_ATTENTION}
    ),
    TradeState.OPEN: frozenset(
        {TradeState.PROTECTED, TradeState.CLOSING, TradeState.CLOSED, TradeState.NEEDS_ATTENTION}
    ),
    TradeState.PROTECTED: frozenset({TradeState.CLOSING, TradeState.CLOSED, TradeState.NEEDS_ATTENTION}),
    # A close that Binance rejected leaves the position as it was.
    TradeState.CLOSING: frozenset(
        {TradeState.CLOSED, TradeState.OPEN, TradeState.PROTECTED, TradeState.NEEDS_ATTENTION}
    ),
    TradeState.NEEDS_ATTENTION: frozenset(
        {
            TradeState.OPEN,
            TradeState.PROTECTED,
            TradeState.CLOSING,
            TradeState.CLOSED,
            TradeState.ENTRY_FAILED,
        }
    ),
    TradeState.CLOSED: frozenset(),
    TradeState.ENTRY_FAILED: frozenset(),
}


def is_active(state: TradeState) -> bool:
    """True while the trade may still hold, or come to hold, a position."""
    return state in ACTIVE_STATES


def check_transition(current_state: TradeState, target_state: TradeState) -> None:
    """Raise InvalidTransitionError unless ``current_state → target_state`` is allowed."""
    if target_state not in ALLOWED_TRANSITIONS[current_state]:
        raise InvalidTransitionError(current_state, target_state)
