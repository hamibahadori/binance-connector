"""Unit tests for the request and position state machines (spec §5 names, plus FAILED)."""

import pytest

from binance_connector.domain.lifecycle import (
    ACTIVE_POSITION_STATES,
    TERMINAL_POSITION_STATES,
    PositionState,
    RequestStatus,
    check_position_transition,
    check_request_transition,
)
from binance_connector.errors import InvalidTransitionError

# --- requests -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("current_status", "next_status"),
    [
        (RequestStatus.RECEIVED, RequestStatus.VALIDATING),
        (RequestStatus.VALIDATING, RequestStatus.ACCEPTED),
        (RequestStatus.VALIDATING, RequestStatus.REJECTED),
        (RequestStatus.RECEIVED, RequestStatus.REJECTED),
    ],
)
def test_allowed_request_transitions(current_status, next_status):
    check_request_transition(current_status, next_status)


@pytest.mark.parametrize(
    ("current_status", "next_status"),
    [
        (RequestStatus.ACCEPTED, RequestStatus.REJECTED),
        (RequestStatus.REJECTED, RequestStatus.ACCEPTED),
        (RequestStatus.RECEIVED, RequestStatus.ACCEPTED),
    ],
)
def test_impossible_request_transitions(current_status, next_status):
    with pytest.raises(InvalidTransitionError):
        check_request_transition(current_status, next_status)


# --- positions ----------------------------------------------------------------------------------

HAPPY_PATH = [PositionState.OPENING, PositionState.OPEN, PositionState.CLOSING, PositionState.CLOSED]


def test_the_happy_path_is_allowed():
    for current_state, next_state in zip(HAPPY_PATH, HAPPY_PATH[1:]):
        check_position_transition(current_state, next_state)


@pytest.mark.parametrize(
    ("current_state", "next_state"),
    [
        (PositionState.OPENING, PositionState.FAILED),
        (PositionState.OPENING, PositionState.ERROR),
        (PositionState.OPENING, PositionState.CLOSING),
        (PositionState.OPEN, PositionState.CLOSED),
        (PositionState.OPEN, PositionState.ERROR),
        (PositionState.CLOSING, PositionState.OPEN),
        (PositionState.CLOSING, PositionState.FAILED),
        (PositionState.CLOSING, PositionState.ERROR),
        (PositionState.ERROR, PositionState.OPENING),  # an "unknown" entry was a resting limit after all
        (PositionState.ERROR, PositionState.OPEN),
        (PositionState.ERROR, PositionState.CLOSING),
        (PositionState.ERROR, PositionState.CLOSED),
        (PositionState.ERROR, PositionState.FAILED),
    ],
)
def test_shortcut_and_recovery_transitions(current_state, next_state):
    check_position_transition(current_state, next_state)


@pytest.mark.parametrize(
    ("current_state", "next_state"),
    [
        (PositionState.OPEN, PositionState.OPENING),
        (PositionState.OPEN, PositionState.FAILED),
        (PositionState.OPEN, PositionState.OPEN),
        (PositionState.OPENING, PositionState.CLOSED),
    ],
)
def test_impossible_position_transitions(current_state, next_state):
    with pytest.raises(InvalidTransitionError):
        check_position_transition(current_state, next_state)


def test_closed_and_failed_are_terminal():
    for terminal_state in TERMINAL_POSITION_STATES:
        for any_state in PositionState:
            with pytest.raises(InvalidTransitionError):
                check_position_transition(terminal_state, any_state)


def test_every_position_state_is_active_or_terminal():
    assert ACTIVE_POSITION_STATES | TERMINAL_POSITION_STATES == set(PositionState)
    assert not ACTIVE_POSITION_STATES & TERMINAL_POSITION_STATES
