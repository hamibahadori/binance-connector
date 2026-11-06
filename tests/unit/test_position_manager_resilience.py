"""Unit tests for transient Binance failures (spec §14.12): retried when provably safe, never otherwise.

Two failures mean the request was never executed, so resending it cannot duplicate an order:

* ``BinanceUnavailableError``: the connection was never established (connect timeout);
* ``-1021``: Binance refused the request unprocessed, its timestamp being outside recvWindow
  (seen intermittently on slow links, D-31).

Everything else, including an unknown outcome, is left to the existing handling.
"""

import json

import pytest

from binance_connector.domain.lifecycle import ExecutionRole, PositionState
from binance_connector.errors import BinanceAPIError, BinanceUnavailableError, BinanceUnknownOutcomeError
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.reports import ReportOutbox, ReportRepository
from binance_connector.services.position_manager import PositionManager
from tests.support import POSITION_ID, store_position

OUTSIDE_RECV_WINDOW = BinanceAPIError(400, -1021, "Timestamp for this request is outside of the recvWindow.")
INVALID_API_KEY = BinanceAPIError(401, -2015, "Invalid API-key, IP, or permissions for action.")
INSUFFICIENT_MARGIN = BinanceAPIError(400, -2019, "Margin is insufficient.")


@pytest.fixture
def positions(session_factory, accounts) -> PositionRepository:
    return PositionRepository(session_factory, outbox=ReportOutbox())


@pytest.fixture
def reports(session_factory) -> ReportRepository:
    return ReportRepository(session_factory)


@pytest.fixture
def waits() -> list[float]:
    return []


@pytest.fixture
def manager(positions, clients, waits) -> PositionManager:
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300, sleep=waits.append)


def binance(clients):
    return clients.for_account("account-1")


def unavailable() -> BinanceUnavailableError:
    return BinanceUnavailableError("Could not connect to Binance within 10 s.")


# --- open ---------------------------------------------------------------------------------------


def test_an_entry_refused_for_its_timestamp_is_sent_again(manager, positions, clients, waits):
    store_position(positions, leverage=None)
    binance(clients).fail_next("place_market_order", OUTSIDE_RECV_WINDOW)

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPEN
    assert binance(clients).call_names().count("place_market_order") == 2
    assert waits == [1]


def test_a_position_check_that_could_not_connect_is_retried(manager, positions, clients, waits):
    store_position(positions)
    binance(clients).fail_next("get_position_risk", unavailable())
    binance(clients).fail_next("get_position_risk", unavailable())

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPEN
    assert waits == [1, 2]


def test_a_leverage_change_that_could_not_connect_is_retried(manager, positions, clients):
    store_position(positions, leverage=5)
    binance(clients).fail_next("change_leverage", unavailable())

    assert manager.open(POSITION_ID).state is PositionState.OPEN


def test_after_three_attempts_the_entry_gives_up(manager, positions, clients, waits):
    store_position(positions, leverage=None)
    for _attempt in range(3):
        binance(clients).fail_next("place_market_order", unavailable())

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.FAILED
    assert "Could not connect" in position.last_error
    assert waits == [1, 2]


def test_a_real_rejection_is_not_retried(manager, positions, clients, waits):
    store_position(positions, leverage=None)
    binance(clients).fail_next("place_market_order", INSUFFICIENT_MARGIN)

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.FAILED
    assert binance(clients).call_names().count("place_market_order") == 1
    assert waits == []


def test_an_unknown_outcome_is_never_resent(manager, positions, clients, waits):
    store_position(positions, leverage=None)
    binance(clients).fail_next("place_market_order", BinanceUnknownOutcomeError("read timed out"))

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.ERROR
    assert binance(clients).call_names().count("place_market_order") == 1
    assert waits == []


def test_invalid_credentials_fail_the_position_with_binances_reason(manager, positions, clients, reports, waits):
    store_position(positions)
    binance(clients).fail_next("get_position_risk", INVALID_API_KEY)

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.FAILED
    assert "-2015" in position.last_error and "Invalid API-key" in position.last_error
    assert waits == []
    assert reports.all()[-1].event_type == "POSITION_FAILED"


def test_a_protection_leg_refused_for_its_timestamp_is_sent_again(manager, positions, clients):
    store_position(positions)
    binance(clients).fail_next("place_conditional_close", OUTSIDE_RECV_WINDOW)

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPEN
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "NEW"


# --- close --------------------------------------------------------------------------------------


@pytest.fixture
def open_position(manager, positions):
    store_position(positions)
    return manager.open(POSITION_ID)


def test_a_close_that_could_not_connect_is_retried(manager, clients, open_position):
    binance(clients).fail_next("place_market_order", unavailable())

    assert manager.close(POSITION_ID, "close-1").state is PositionState.CLOSED


def test_a_close_that_cannot_start_is_reported_to_django(manager, positions, clients, reports, open_position):
    for _attempt in range(3):
        binance(clients).fail_next("get_position_risk", unavailable())

    position = manager.close(POSITION_ID, "close-1")

    assert position.state is PositionState.OPEN
    assert "The close could not start" in position.last_error
    last_report = reports.all()[-1]
    assert last_report.event_type == "CLOSE_FAILED"
    assert json.loads(last_report.payload)["error"] == position.last_error
