"""Unit tests for positions, their Binance execution records, and their history."""

import json
from datetime import UTC
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.errors import InvalidTransitionError, PositionConflictError, PositionNotFoundError
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.positions import NewPosition, PositionRepository
from binance_connector.security.credentials import ApiCredentials
from binance_connector.security.redaction import SecretRedactor

POSITION_ID = "p00000000000000000000001"


@pytest.fixture
def repository(session_factory, cipher) -> PositionRepository:
    accounts = AccountRepository(session_factory, cipher=cipher, redactor=SecretRedactor())
    for account_id, user_id in (("account-1", "user-1"), ("account-2", "user-2")):
        accounts.upsert(account_id, user_id, ApiCredentials(f"key-{account_id}-x", f"secret-{account_id}-x"))
    return PositionRepository(session_factory)


def new_position(position_id=POSITION_ID, account_id="account-1", symbol="BTCUSDT", **overrides) -> NewPosition:
    fields = {
        "position_id": position_id,
        "account_id": account_id,
        "symbol": symbol,
        "direction": "LONG",
        "quantity": Decimal("0.0020"),
        "requested_quantity": Decimal("150"),
        "quantity_type": "NOTIONAL",
        "leverage": 5,
        "entry_type": "MARKET",
        "entry_price": Decimal("75000.0"),
        "take_profit_price": Decimal("82000.0"),
        "stop_loss_price": Decimal("74000.0"),
        "open_request_id": "request-1",
    }
    fields.update(overrides)
    return NewPosition(**fields)


def test_a_new_position_starts_opening(repository):
    position = repository.create(new_position())

    assert position.state is PositionState.OPENING
    assert position.account_id == "account-1"
    assert str(position.quantity) == "0.0020"
    assert position.created_at.tzinfo is UTC
    assert position.executions == {}


def test_one_active_position_per_symbol_per_account(repository):
    repository.create(new_position())

    with pytest.raises(PositionConflictError, match="BTCUSDT"):
        repository.create(new_position("p00000000000000000000002"))


def test_different_accounts_can_hold_the_same_symbol_at_once(repository):
    repository.create(new_position())

    other = repository.create(new_position("p00000000000000000000002", account_id="account-2"))

    assert other.state is PositionState.OPENING
    assert len(repository.list_active()) == 2
    assert [position.position_id for position in repository.list_active(account_id="account-2")] == [other.position_id]


def test_a_terminal_position_frees_its_symbol_for_the_account(repository):
    repository.create(new_position())
    repository.transition(POSITION_ID, PositionState.FAILED, last_error="rejected")

    assert repository.create(new_position("p00000000000000000000002")).state is PositionState.OPENING


def test_find_active_is_scoped_to_the_account(repository):
    repository.create(new_position())

    assert repository.find_active("account-1", "BTCUSDT").position_id == POSITION_ID
    assert repository.find_active("account-2", "BTCUSDT") is None


def test_transitions_set_fields_and_are_recorded(repository):
    repository.create(new_position())

    position = repository.transition(POSITION_ID, PositionState.OPEN, event_detail={"note": "filled"},
                                     entry_average_price=Decimal("75010.5"))

    assert position.entry_average_price == Decimal("75010.5")
    events = repository.events(POSITION_ID)
    assert [(event.from_state, event.to_state) for event in events] == [
        (None, PositionState.OPENING),
        (PositionState.OPENING, PositionState.OPEN),
    ]
    assert json.loads(events[1].detail) == {"note": "filled"}


def test_invalid_transitions_change_nothing(repository):
    repository.create(new_position())

    with pytest.raises(InvalidTransitionError):
        repository.transition(POSITION_ID, PositionState.CLOSED)

    assert repository.get(POSITION_ID).state is PositionState.OPENING


def test_closing_records_the_reason(repository):
    repository.create(new_position())
    repository.transition(POSITION_ID, PositionState.OPEN)

    position = repository.transition(POSITION_ID, PositionState.CLOSED, close_reason=CloseReason.TAKE_PROFIT)

    assert position.close_reason is CloseReason.TAKE_PROFIT
    assert repository.find_active("account-1", "BTCUSDT") is None


# --- execution records --------------------------------------------------------------------------


def test_an_execution_is_prepared_with_a_derived_client_id(repository):
    repository.create(new_position())

    execution = repository.prepare_execution(POSITION_ID, ExecutionRole.STOP_LOSS, kind="ALGO",
                                             order_type="STOP_MARKET", side="SELL", price=Decimal("74000.0"))

    assert execution.client_id == f"lx-{POSITION_ID}-sl"
    assert execution.status == "PENDING"
    assert repository.get(POSITION_ID).executions[ExecutionRole.STOP_LOSS].price == Decimal("74000.0")


def test_preparing_an_execution_twice_returns_the_existing_one(repository):
    repository.create(new_position())
    first = repository.prepare_execution(POSITION_ID, ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET",
                                         side="BUY", quantity=Decimal("0.002"))
    repository.update_execution(POSITION_ID, ExecutionRole.ENTRY, status="FILLED")

    second = repository.prepare_execution(POSITION_ID, ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET",
                                          side="BUY", quantity=Decimal("0.002"))

    assert second.client_id == first.client_id
    assert second.status == "FILLED"


def test_execution_updates_respect_only_if_unset(repository):
    repository.create(new_position())
    repository.prepare_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, kind="ALGO",
                                 order_type="TAKE_PROFIT_MARKET", side="SELL", price=Decimal("82000"))
    repository.update_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, status="FINISHED")

    position = repository.update_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, only_if_unset=("binance_id",),
                                           binance_id=77, status="NEW", status_only_if="PENDING")

    execution = position.executions[ExecutionRole.TAKE_PROFIT]
    assert execution.binance_id == 77
    assert execution.status == "FINISHED"


@pytest.mark.parametrize("role", list(ExecutionRole))
def test_a_position_is_found_by_any_execution_client_id(repository, role):
    repository.create(new_position())
    execution = repository.prepare_execution(POSITION_ID, role, kind="ORDER", order_type="MARKET", side="BUY")

    position, found_role = repository.find_by_client_id(execution.client_id)

    assert position.position_id == POSITION_ID
    assert found_role is role


def test_unrelated_client_ids_find_nothing(repository):
    repository.create(new_position())

    assert repository.find_by_client_id("autoclose-123") is None


def test_unknown_positions_raise_not_found(repository):
    with pytest.raises(PositionNotFoundError):
        repository.get("unknown")
