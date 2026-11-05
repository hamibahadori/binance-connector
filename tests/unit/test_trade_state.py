"""Unit tests for the trade lifecycle state machine."""

import pytest

from binance_connector.domain.trade_state import (
    ACTIVE_STATES,
    TERMINAL_STATES,
    TradeState,
    check_transition,
    is_active,
)
from binance_connector.errors import InvalidTransitionError

HAPPY_PATH = [
    TradeState.RECEIVED,
    TradeState.ENTRY_SUBMITTED,
    TradeState.OPEN,
    TradeState.PROTECTED,
    TradeState.CLOSING,
    TradeState.CLOSED,
]


def test_the_happy_path_is_allowed_step_by_step():
    for current_state, next_state in zip(HAPPY_PATH, HAPPY_PATH[1:]):
        check_transition(current_state, next_state)


@pytest.mark.parametrize(
    ("current_state", "next_state"),
    [
        (TradeState.RECEIVED, TradeState.ENTRY_FAILED),
        (TradeState.ENTRY_SUBMITTED, TradeState.ENTRY_FAILED),
        (TradeState.ENTRY_SUBMITTED, TradeState.NEEDS_ATTENTION),
        (TradeState.OPEN, TradeState.CLOSED),
        (TradeState.OPEN, TradeState.CLOSING),
        (TradeState.PROTECTED, TradeState.CLOSED),
        (TradeState.PROTECTED, TradeState.NEEDS_ATTENTION),
        (TradeState.CLOSING, TradeState.OPEN),
        (TradeState.CLOSING, TradeState.PROTECTED),
        (TradeState.NEEDS_ATTENTION, TradeState.OPEN),
        (TradeState.NEEDS_ATTENTION, TradeState.PROTECTED),
        (TradeState.NEEDS_ATTENTION, TradeState.CLOSING),
        (TradeState.NEEDS_ATTENTION, TradeState.CLOSED),
        (TradeState.NEEDS_ATTENTION, TradeState.ENTRY_FAILED),
    ],
)
def test_recovery_and_shortcut_transitions_are_allowed(current_state, next_state):
    check_transition(current_state, next_state)


@pytest.mark.parametrize(
    ("current_state", "next_state"),
    [
        (TradeState.CLOSED, TradeState.OPEN),
        (TradeState.ENTRY_FAILED, TradeState.OPEN),
        (TradeState.RECEIVED, TradeState.OPEN),
        (TradeState.OPEN, TradeState.ENTRY_SUBMITTED),
        (TradeState.PROTECTED, TradeState.OPEN),
        (TradeState.OPEN, TradeState.OPEN),
    ],
)
def test_impossible_transitions_are_refused(current_state, next_state):
    with pytest.raises(InvalidTransitionError) as raised:
        check_transition(current_state, next_state)

    assert raised.value.current_state is current_state
    assert raised.value.target_state is next_state


def test_terminal_states_allow_no_way_out():
    for terminal_state in TERMINAL_STATES:
        for any_state in TradeState:
            with pytest.raises(InvalidTransitionError):
                check_transition(terminal_state, any_state)


def test_every_state_is_either_active_or_terminal():
    assert ACTIVE_STATES | TERMINAL_STATES == set(TradeState)
    assert not ACTIVE_STATES & TERMINAL_STATES
    assert is_active(TradeState.PROTECTED)
    assert not is_active(TradeState.CLOSED)
