"""Unit tests for executing a CLOSE (market close or cancelling an unfilled limit) and limit expiry."""

from datetime import timedelta
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.errors import BinanceAPIError, BinanceUnknownOutcomeError
from binance_connector.persistence.writes import utc_now
from binance_connector.services.position_manager import PositionManager
from tests.unit.test_position_manager_open import POSITION_ID, binance, store

CLOSE_REQUEST = "request-close-1"


@pytest.fixture
def manager(positions, clients) -> PositionManager:
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300)


@pytest.fixture
def open_position(manager, positions):
    store(positions)
    return manager.open(POSITION_ID)


def test_closing_sells_reduce_only_and_records_the_result(manager, clients, open_position):
    binance(clients).fill_price = Decimal("79000.0")

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.CLOSED
    assert position.close_reason is CloseReason.CLOSE_REQUESTED
    assert position.close_request_id == CLOSE_REQUEST
    assert position.exit_average_price == Decimal("79000.0")
    assert position.realized_pnl == Decimal("2.0000")  # (79000 - 78000) x 0.002
    assert position.closed_at is not None
    exit_order = binance(clients).orders[f"lx-{POSITION_ID}-x"]
    assert (exit_order["side"], exit_order["reduceOnly"]) == ("SELL", True)
    assert position.executions[ExecutionRole.EXIT].status == "FILLED"


def test_protection_binance_expired_by_itself_is_recorded_as_expired(manager, open_position):
    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "EXPIRED"
    assert position.executions[ExecutionRole.STOP_LOSS].status == "EXPIRED"


def test_protection_left_open_by_binance_is_cancelled(manager, clients, open_position):
    fake = binance(clients)
    fake._set_position = lambda symbol, amount: fake.positions.__setitem__(symbol, amount)  # no auto-expiry

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "CANCELED"
    assert fake.call_names().count("cancel_algo_order") == 2


def test_a_rejected_close_leaves_the_position_open(manager, clients, open_position):
    binance(clients).fail_next("place_market_order", BinanceAPIError(400, -1021, "Timestamp outside recvWindow."))

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.OPEN
    assert "-1021" in position.last_error


def test_an_unknown_close_outcome_stays_closing(manager, clients, open_position):
    binance(clients).fail_next("place_market_order", BinanceUnknownOutcomeError("read timeout"))

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.CLOSING
    assert position.executions[ExecutionRole.EXIT].status == "SUBMITTED"


def test_a_position_already_flat_is_an_error_for_reconciliation(manager, clients, open_position):
    binance(clients).trigger(f"lx-{POSITION_ID}-tp", fill_price="82010.0")

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.ERROR
    assert "already flat" in position.last_error


def test_closing_twice_does_nothing_more(manager, clients, open_position):
    manager.close(POSITION_ID, CLOSE_REQUEST)
    calls_after_first = len(binance(clients).calls)

    position = manager.close(POSITION_ID, "request-close-2")

    assert position.state is PositionState.CLOSED
    assert len(binance(clients).calls) == calls_after_first


# --- limit entries ------------------------------------------------------------------------------


@pytest.fixture
def resting_limit(manager, positions):
    store(positions, entry_type="LIMIT", entry_price=Decimal("70000.0"))
    return manager.open(POSITION_ID)


def test_closing_an_unfilled_limit_entry_cancels_it_and_fails_the_position(manager, clients, resting_limit):
    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.FAILED
    assert "cancelled" in position.last_error
    assert position.executions[ExecutionRole.ENTRY].status == "CANCELED"
    assert "place_market_order" not in binance(clients).call_names()


def test_closing_a_partly_filled_limit_entry_cancels_the_rest_and_closes_the_fill(manager, clients, resting_limit):
    binance(clients).fill_resting(f"lx-{POSITION_ID}-e", quantity="0.001")

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.CLOSED
    assert binance(clients).positions["BTCUSDT"] == 0
    assert dict(binance(clients).calls)["place_market_order"]["quantity"] == Decimal("0.001")


def manager_at(positions, clients, seconds_later):
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300,
                           clock=lambda: utc_now() + timedelta(seconds=seconds_later))


def test_a_limit_entry_unfilled_after_the_timeout_is_cancelled_and_fails(positions, clients, resting_limit):
    later_manager = manager_at(positions, clients, 301)

    expired = later_manager.expire_limit_entries()

    assert [position.position_id for position in expired] == [POSITION_ID]
    position = positions.get(POSITION_ID)
    assert position.state is PositionState.FAILED
    assert "expired" in position.last_error


def test_a_limit_entry_within_its_timeout_is_left_waiting(positions, clients, resting_limit):
    assert manager_at(positions, clients, 100).expire_limit_entries() == []
    assert positions.get(POSITION_ID).state is PositionState.OPENING


def test_a_partly_filled_limit_entry_at_expiry_keeps_the_fill_and_is_protected(positions, clients, resting_limit):
    binance(clients).fill_resting(f"lx-{POSITION_ID}-e", quantity="0.001")

    manager_at(positions, clients, 301).expire_limit_entries()

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.OPEN
    assert position.entry_filled_quantity == Decimal("0.001")
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"
