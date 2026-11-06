"""Request and position lifecycles (spec §5), as explicit state machines.

Requests (what Django sent)::

    RECEIVED → VALIDATING → ACCEPTED | REJECTED

An ACCEPTED request is queued for the worker (``ProcessingState`` PENDING → IN_PROGRESS → DONE).

Positions (what is held on Binance)::

    OPENING → OPEN → CLOSING → CLOSED
       │        │        │
       ├→ FAILED ←───────┤          FAILED: the entry never executed (terminal)
       └→ ERROR ←────────┘          ERROR: a live position needs attention (recoverable)

* OPENING   the entry is being placed, or a limit entry waits for its fill.
* OPEN      the position is held; take-profit/stop-loss status is reported separately.
* CLOSING   a close is being executed (or an unfilled limit entry is being cancelled).
* CLOSED    the position is gone; ``close_reason`` says why.
* FAILED    nothing was ever held: Binance rejected the entry, or a limit entry expired
            or was cancelled unfilled. An addition to the spec's names (decision D-57/D).
* ERROR     something did not go to plan with a position that may hold something.
            Reconciliation moves it on once Binance's real state is known.

These names are the spec's, with PROTECTED folded into OPEN and FAILED added.
"""

from enum import StrEnum

from binance_connector.errors import InvalidTransitionError


class RequestAction(StrEnum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class RequestStatus(StrEnum):
    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


class ProcessingState(StrEnum):
    """Where an ACCEPTED request is in the worker's queue."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"


class PositionState(StrEnum):
    OPENING = "OPENING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"
    ERROR = "ERROR"


class CloseReason(StrEnum):
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    CLOSE_REQUESTED = "CLOSE_REQUESTED"
    """Django sent a CLOSE request."""
    EXTERNAL = "EXTERNAL"
    """The position disappeared without one of our orders explaining it (e.g. liquidation)."""


class ExecutionRole(StrEnum):
    """What a Binance order does for its position. The value is the client-id suffix."""

    ENTRY = "e"
    EXIT = "x"
    TAKE_PROFIT = "tp"
    STOP_LOSS = "sl"


REQUEST_TRANSITIONS: dict[RequestStatus, frozenset[RequestStatus]] = {
    RequestStatus.RECEIVED: frozenset({RequestStatus.VALIDATING, RequestStatus.REJECTED}),
    RequestStatus.VALIDATING: frozenset({RequestStatus.ACCEPTED, RequestStatus.REJECTED}),
    RequestStatus.ACCEPTED: frozenset(),
    RequestStatus.REJECTED: frozenset(),
}

POSITION_TRANSITIONS: dict[PositionState, frozenset[PositionState]] = {
    PositionState.OPENING: frozenset(
        {PositionState.OPEN, PositionState.FAILED, PositionState.ERROR, PositionState.CLOSING}
    ),
    PositionState.OPEN: frozenset({PositionState.CLOSING, PositionState.CLOSED, PositionState.ERROR}),
    # A rejected close leaves the position open; cancelling an unfilled limit entry ends FAILED.
    PositionState.CLOSING: frozenset(
        {PositionState.CLOSED, PositionState.OPEN, PositionState.FAILED, PositionState.ERROR}
    ),
    PositionState.ERROR: frozenset(
        {PositionState.OPEN, PositionState.CLOSING, PositionState.CLOSED, PositionState.FAILED}
    ),
    PositionState.CLOSED: frozenset(),
    PositionState.FAILED: frozenset(),
}

TERMINAL_POSITION_STATES = frozenset({PositionState.CLOSED, PositionState.FAILED})
ACTIVE_POSITION_STATES = frozenset(set(PositionState) - TERMINAL_POSITION_STATES)


def is_active(state: PositionState) -> bool:
    """True while the position may hold, or come to hold, something on Binance."""
    return state in ACTIVE_POSITION_STATES


def check_request_transition(current_status: RequestStatus, target_status: RequestStatus) -> None:
    if target_status not in REQUEST_TRANSITIONS[current_status]:
        raise InvalidTransitionError(current_status, target_status)


def check_position_transition(current_state: PositionState, target_state: PositionState) -> None:
    if target_state not in POSITION_TRANSITIONS[current_state]:
        raise InvalidTransitionError(current_state, target_state)
