"""Unit tests for storing trades and their state changes in SQLite."""

import json
from datetime import UTC
from decimal import Decimal

import pytest

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import InvalidTransitionError, TradeConflictError, TradeNotFoundError
from binance_connector.persistence.repository import NewTrade, TradeRepository


def new_trade(trade_id: str = "trade000000000000000001", symbol: str = "BTCUSDT", **overrides) -> NewTrade:
    fields = {
        "trade_id": trade_id,
        "symbol": symbol,
        "direction": "LONG",
        "quantity": Decimal("0.0020"),
        "leverage": 5,
        "take_profit_price": Decimal("90000.0"),
        "stop_loss_price": Decimal("75000.0"),
        "strategy_request_id": None,
    }
    fields.update(overrides)
    return NewTrade(**fields)


@pytest.fixture
def repository(session_factory) -> TradeRepository:
    return TradeRepository(session_factory)


def test_a_created_trade_starts_received_with_derived_client_ids(repository):
    trade = repository.create(new_trade())

    assert trade.state is TradeState.RECEIVED
    assert trade.entry_client_order_id == "lx-trade000000000000000001-e"
    assert trade.exit_client_order_id == "lx-trade000000000000000001-x"
    assert trade.take_profit_client_algo_id == "lx-trade000000000000000001-tp"
    assert trade.stop_loss_client_algo_id == "lx-trade000000000000000001-sl"
    assert all(
        len(client_id) <= 36
        for client_id in (trade.entry_client_order_id, trade.take_profit_client_algo_id)
    )


def test_decimals_and_times_round_trip_exactly(repository):
    created = repository.create(new_trade())

    loaded = repository.get(created.trade_id)

    assert loaded.quantity == Decimal("0.0020")
    assert str(loaded.quantity) == "0.0020"
    assert loaded.take_profit_price == Decimal("90000.0")
    assert loaded.created_at.tzinfo is UTC


def test_unknown_trade_raises_not_found(repository):
    with pytest.raises(TradeNotFoundError):
        repository.get("does-not-exist")


def test_only_one_active_trade_per_symbol(repository):
    repository.create(new_trade("trade-a"))

    with pytest.raises(TradeConflictError, match="BTCUSDT"):
        repository.create(new_trade("trade-b"))


def test_a_symbol_is_free_again_once_its_trade_is_terminal(repository):
    repository.create(new_trade("trade-a"))
    repository.transition("trade-a", TradeState.ENTRY_FAILED, last_error="rejected")

    second_trade = repository.create(new_trade("trade-b"))

    assert second_trade.state is TradeState.RECEIVED


def test_strategy_request_id_is_unique_and_searchable(repository):
    repository.create(new_trade("trade-a", strategy_request_id="strategy-42"))
    repository.transition("trade-a", TradeState.ENTRY_FAILED)

    assert repository.find_by_strategy_request_id("strategy-42").trade_id == "trade-a"
    assert repository.find_by_strategy_request_id("strategy-unknown") is None
    with pytest.raises(TradeConflictError, match="strategy-42"):
        repository.create(new_trade("trade-b", strategy_request_id="strategy-42"))


def test_transition_updates_fields_and_records_an_event(repository):
    repository.create(new_trade())

    trade = repository.transition(
        "trade000000000000000001",
        TradeState.ENTRY_SUBMITTED,
        event_detail={"note": "sending entry"},
    )

    assert trade.state is TradeState.ENTRY_SUBMITTED
    events = repository.events(trade.trade_id)
    assert [(event.from_state, event.to_state) for event in events] == [
        (None, TradeState.RECEIVED),
        (TradeState.RECEIVED, TradeState.ENTRY_SUBMITTED),
    ]
    assert json.loads(events[1].detail) == {"note": "sending entry"}


def test_closing_a_trade_records_its_reason_and_frees_the_symbol(repository):
    repository.create(new_trade())
    for next_state in (TradeState.ENTRY_SUBMITTED, TradeState.OPEN, TradeState.PROTECTED):
        repository.transition("trade000000000000000001", next_state)

    closed = repository.transition(
        "trade000000000000000001",
        TradeState.CLOSED,
        close_reason=CloseReason.TAKE_PROFIT,
        exit_price=Decimal("90001.5"),
    )

    assert closed.close_reason is CloseReason.TAKE_PROFIT
    assert closed.exit_price == Decimal("90001.5")
    assert repository.find_active_by_symbol("BTCUSDT") is None
    assert repository.list_active() == []


def test_invalid_transition_changes_nothing(repository):
    repository.create(new_trade())

    with pytest.raises(InvalidTransitionError):
        repository.transition("trade000000000000000001", TradeState.CLOSED)

    assert repository.get("trade000000000000000001").state is TradeState.RECEIVED
    assert len(repository.events("trade000000000000000001")) == 1


def test_a_second_writer_sees_the_first_writers_state(session_factory):
    """Two repositories (like the API and the worker) share one database file."""
    api_repository = TradeRepository(session_factory)
    worker_repository = TradeRepository(session_factory)
    api_repository.create(new_trade())
    for next_state in (TradeState.ENTRY_SUBMITTED, TradeState.OPEN):
        api_repository.transition("trade000000000000000001", next_state)

    worker_repository.transition("trade000000000000000001", TradeState.CLOSED,
                                 close_reason=CloseReason.STOP_LOSS)

    with pytest.raises(InvalidTransitionError):
        api_repository.transition("trade000000000000000001", TradeState.PROTECTED)


def test_update_fields_without_a_state_change(repository):
    repository.create(new_trade())

    trade = repository.update_fields("trade000000000000000001", take_profit_status="NEW", take_profit_algo_id=77)

    assert trade.take_profit_status == "NEW"
    assert trade.take_profit_algo_id == 77
    assert trade.state is TradeState.RECEIVED


def test_unknown_field_names_are_refused(repository):
    repository.create(new_trade())

    with pytest.raises(AttributeError, match="take_profit_staus"):
        repository.update_fields("trade000000000000000001", take_profit_staus="NEW")


@pytest.mark.parametrize(
    ("client_id", "expected_role"),
    [
        ("lx-trade000000000000000001-e", "entry"),
        ("lx-trade000000000000000001-x", "exit"),
        ("lx-trade000000000000000001-tp", "take_profit"),
        ("lx-trade000000000000000001-sl", "stop_loss"),
    ],
)
def test_a_trade_is_found_by_any_of_its_client_ids(repository, client_id, expected_role):
    repository.create(new_trade())

    trade, role = repository.find_by_client_id(client_id)

    assert trade.trade_id == "trade000000000000000001"
    assert role == expected_role


def test_unrelated_client_ids_find_nothing(repository):
    repository.create(new_trade())

    assert repository.find_by_client_id("autoclose-123") is None
