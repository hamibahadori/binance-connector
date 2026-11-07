"""Unit tests for executing a CLOSE (market close or cancelling an unfilled limit) and limit expiry."""

from datetime import timedelta
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.errors import BinanceAPIError, BinanceUnknownOutcomeError
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.reports import ReportOutbox, ReportRepository
from binance_connector.persistence.writes import utc_now
from binance_connector.services.position_manager import PositionManager
from tests.unit.test_position_manager_open import POSITION_ID, binance, store

CLOSE_REQUEST = "request-close-1"


@pytest.fixture
def positions(session_factory, accounts) -> PositionRepository:
    """Positions that also write their reports, so the report for Django can be checked."""
    return PositionRepository(session_factory, outbox=ReportOutbox())


@pytest.fixture
def manager(positions, clients) -> PositionManager:
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300,
                           sleep=lambda _seconds: None)


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


def test_fees_are_read_from_binances_trades_at_close(manager, clients, open_position):
    binance(clients).fill_price = Decimal("79000.0")

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    # 0.04% taker commission on each fill, as Binance Demo charged (0.0019 x 75776.3 x 0.0004 = 0.05759).
    entry_commission = Decimal("0.002") * Decimal("78000.0") * Decimal("0.0004")
    exit_commission = Decimal("0.002") * Decimal("79000.0") * Decimal("0.0004")
    assert position.executions[ExecutionRole.ENTRY].commission == entry_commission
    assert position.executions[ExecutionRole.EXIT].commission == exit_commission
    assert position.fees == entry_commission + exit_commission
    assert "get_order_trades" in binance(clients).call_names()


def test_an_order_filled_in_several_trades_has_its_commissions_summed(manager, clients, open_position):
    fake = binance(clients)
    entry_order_id = fake.orders[f"lx-{POSITION_ID}-e"]["orderId"]
    fake.trades[entry_order_id] = [
        {"orderId": entry_order_id, "qty": "0.0013", "commission": "0.04", "commissionAsset": "USDT"},
        {"orderId": entry_order_id, "qty": "0.0007", "commission": "0.02", "commissionAsset": "USDT"},
    ]

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.executions[ExecutionRole.ENTRY].commission == Decimal("0.06")


def test_fees_stay_unknown_rather_than_wrong_if_trades_cannot_be_read(manager, clients, open_position):
    from binance_connector.errors import BinanceUnavailableError

    for _ in range(4):
        binance(clients).fail_next("get_order_trades", BinanceUnavailableError("no connection"))

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.CLOSED
    assert position.executions[ExecutionRole.EXIT].commission is None


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
    binance(clients).fail_next("place_market_order", BinanceAPIError(400, -2022, "ReduceOnly Order is rejected."))

    position = manager.close(POSITION_ID, CLOSE_REQUEST)

    assert position.state is PositionState.OPEN
    assert "-2022" in position.last_error


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


# --- closed by its take-profit or stop-loss -----------------------------------------------------


@pytest.mark.parametrize("status_when_filled", ["NEW", "TRIGGERING", "TRIGGERED"])
def test_the_leg_that_closed_the_position_is_recorded_as_finished(manager, positions, open_position,
                                                                  status_when_filled):
    """Found on Demo: the fill closes the position before Binance's FINISHED event for the
    algo order, which then never arrives (the account's stream stops once nothing is open)."""
    positions.update_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, status=status_when_filled)

    closed = manager.close_by_protection(POSITION_ID, ExecutionRole.TAKE_PROFIT,
                                         exit_average_price=Decimal("82000"), closed_at=utc_now())

    assert (closed.state, closed.close_reason) == (PositionState.CLOSED, CloseReason.TAKE_PROFIT)
    assert closed.executions[ExecutionRole.TAKE_PROFIT].status == "FINISHED"


def test_the_final_report_shows_the_leg_finished(manager, session_factory, open_position):
    import json

    manager.close_by_protection(POSITION_ID, ExecutionRole.STOP_LOSS, exit_average_price=Decimal("74000"),
                                closed_at=utc_now())

    closed_report = [report for report in ReportRepository(session_factory).all()
                     if report.event_type == "POSITION_CLOSED"][-1]
    body = json.loads(closed_report.payload)
    assert body["stop_loss"]["status"] == "FINISHED"
    # the other leg is removed before the close is recorded (found on Demo: it read NEW)
    assert body["take_profit"]["status"] in ("CANCELED", "EXPIRED")