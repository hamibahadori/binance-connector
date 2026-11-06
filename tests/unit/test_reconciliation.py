"""Unit tests for reconciliation: bringing stored positions in line with Binance's real state.

Each test sets up what a crash, a missed stream event or an outside change would leave
behind, then checks that one reconciliation pass resolves it.
"""

from datetime import timedelta
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.errors import BinanceAPIError, BinanceUnavailableError
from binance_connector.persistence.writes import utc_now
from binance_connector.services.position_manager import PositionManager
from binance_connector.services.reconciliation import ReconciliationService
from tests.support import POSITION_ID, store_position

GRACE_SECONDS = 120
ENTRY_ID, EXIT_ID = f"lx-{POSITION_ID}-e", f"lx-{POSITION_ID}-x"
TAKE_PROFIT_ID, STOP_LOSS_ID = f"lx-{POSITION_ID}-tp", f"lx-{POSITION_ID}-sl"


class ShiftedClock:
    def __init__(self) -> None:
        self.offset_seconds = 0

    def __call__(self):
        return utc_now() + timedelta(seconds=self.offset_seconds)


@pytest.fixture
def clock() -> ShiftedClock:
    return ShiftedClock()


@pytest.fixture
def manager(positions, clients) -> PositionManager:
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300)


@pytest.fixture
def reconciler(positions, clients, manager, clock) -> ReconciliationService:
    return ReconciliationService(positions=positions, clients=clients, position_manager=manager, clock=clock,
                                 grace_seconds=GRACE_SECONDS)


@pytest.fixture
def binance(clients):
    return clients.for_account("account-1")


def open_on_binance(binance, quantity="0.002"):
    binance.place_market_order(symbol="BTCUSDT", side="BUY", quantity=Decimal(quantity), client_order_id=ENTRY_ID)


def protect_on_binance(binance):
    for client_algo_id, order_type, trigger in ((STOP_LOSS_ID, "STOP_MARKET", "74000.0"),
                                                 (TAKE_PROFIT_ID, "TAKE_PROFIT_MARKET", "82000.0")):
        binance.place_conditional_close(symbol="BTCUSDT", side="SELL", order_type=order_type,
                                        trigger_price=Decimal(trigger), client_algo_id=client_algo_id,
                                        working_type="MARK_PRICE")


def submitted_entry(positions, status="SUBMITTED", order_type="MARKET"):
    store_position(positions, entry_type="LIMIT" if order_type == "LIMIT" else "MARKET",
                   entry_price=Decimal("70000.0") if order_type == "LIMIT" else None)
    positions.prepare_execution(POSITION_ID, ExecutionRole.ENTRY, kind="ORDER", order_type=order_type, side="BUY",
                                quantity=Decimal("0.002"))
    positions.update_execution(POSITION_ID, ExecutionRole.ENTRY, status=status, submitted_at=utc_now())


def open_and_protected(positions, binance, manager):
    store_position(positions)
    manager.open(POSITION_ID)
    binance.calls.clear()
    return positions.get(POSITION_ID)


# --- entries ------------------------------------------------------------------------------------


def test_an_opening_position_whose_entry_was_never_sent_is_left_to_the_execution_queue(reconciler, positions,
                                                                                      binance, clock):
    store_position(positions)
    clock.offset_seconds = GRACE_SECONDS + 1

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).state is PositionState.OPENING
    assert binance.calls == []


def test_a_submitted_entry_that_filled_while_nobody_watched_is_opened_and_protected(reconciler, positions,
                                                                                    binance, clock):
    submitted_entry(positions)
    open_on_binance(binance)
    clock.offset_seconds = GRACE_SECONDS + 1

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.OPEN
    assert position.entry_average_price == Decimal("78000.0")
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"


def test_a_submitted_entry_inside_the_grace_period_is_left_alone(reconciler, positions, binance):
    submitted_entry(positions)

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).state is PositionState.OPENING
    assert binance.calls == []


def test_an_entry_that_never_reached_binance_fails_after_the_grace_period(reconciler, positions, clock):
    submitted_entry(positions)
    positions.transition(POSITION_ID, PositionState.ERROR, last_error="unknown")
    clock.offset_seconds = GRACE_SECONDS + 1

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.FAILED
    assert "never reached Binance" in position.last_error


def test_an_unknown_entry_that_did_fill_is_resumed_without_waiting(reconciler, positions, binance):
    submitted_entry(positions)
    positions.transition(POSITION_ID, PositionState.ERROR, last_error="unknown")
    open_on_binance(binance)

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).state is PositionState.OPEN


def test_a_resting_limit_fill_missed_by_the_stream_is_found(reconciler, positions, binance, manager):
    store_position(positions, entry_type="LIMIT", entry_price=Decimal("70000.0"))
    manager.open(POSITION_ID)
    binance.fill_resting(ENTRY_ID)

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.OPEN
    assert position.entry_average_price == Decimal("70000.0")
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "NEW"


def test_a_resting_limit_still_waiting_is_left_alone(reconciler, positions, binance, manager):
    store_position(positions, entry_type="LIMIT", entry_price=Decimal("70000.0"))
    manager.open(POSITION_ID)

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).state is PositionState.OPENING


# --- open positions -----------------------------------------------------------------------------


def test_a_healthy_position_is_left_as_it_is(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).state is PositionState.OPEN
    assert "place_conditional_close" not in binance.call_names()


def test_a_take_profit_fill_missed_by_the_stream_is_found(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)
    binance.trigger(TAKE_PROFIT_ID, fill_price="82010.5")

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.CLOSED
    assert position.close_reason is CloseReason.TAKE_PROFIT
    assert position.exit_average_price == Decimal("82010.5")
    assert position.realized_pnl == Decimal("8.0210")  # (82010.5 - 78000) x 0.002
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "FINISHED"
    assert position.executions[ExecutionRole.STOP_LOSS].status == "EXPIRED"


def test_a_position_gone_without_our_orders_is_closed_as_external(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)
    binance.flatten_externally("BTCUSDT")

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.CLOSED
    assert position.close_reason is CloseReason.EXTERNAL


def test_protection_that_failed_to_place_is_placed_now(reconciler, positions, binance, manager):
    store_position(positions)
    binance.fail_next("place_conditional_close", BinanceAPIError(400, -2021, "Order would immediately trigger."))
    manager.open(POSITION_ID)
    assert positions.get(POSITION_ID).state is PositionState.ERROR

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.OPEN
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"
    assert position.last_error is None


def test_protection_placed_but_never_recorded_is_adopted_not_duplicated(reconciler, positions, binance, manager):
    store_position(positions, take_profit_price=None, stop_loss_price=None)
    manager.open(POSITION_ID)
    positions.update_fields(POSITION_ID, take_profit_price=Decimal("82000.0"), stop_loss_price=Decimal("74000.0"))
    for role, order_type in ((ExecutionRole.STOP_LOSS, "STOP_MARKET"), (ExecutionRole.TAKE_PROFIT, "TAKE_PROFIT_MARKET")):
        positions.prepare_execution(POSITION_ID, role, kind="ALGO", order_type=order_type, side="SELL")
        positions.update_execution(POSITION_ID, role, status="SUBMITTED")  # crash before the reply was stored
    protect_on_binance(binance)
    binance.calls.clear()

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"
    assert position.executions[ExecutionRole.STOP_LOSS].binance_id is not None
    assert "place_conditional_close" not in binance.call_names()


def test_protection_lost_while_the_position_is_open_is_an_error(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)
    binance.algo_orders[STOP_LOSS_ID]["algoStatus"] = "CANCELED"  # e.g. cancelled by hand on Binance

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.ERROR
    assert "stop-loss" in position.last_error and "CANCELED" in position.last_error


def test_a_position_resized_outside_the_service_is_an_error(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)
    binance.positions["BTCUSDT"] = Decimal("0.005")

    reconciler.reconcile_account("account-1")

    assert "0.005" in positions.get(POSITION_ID).last_error


def test_an_error_heals_once_binance_looks_right_again(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)
    positions.transition(POSITION_ID, PositionState.ERROR, last_error="temporary")

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.OPEN
    assert position.last_error is None


# --- closing ------------------------------------------------------------------------------------


def closing(positions, binance, manager):
    open_and_protected(positions, binance, manager)
    positions.transition(POSITION_ID, PositionState.CLOSING)
    positions.prepare_execution(POSITION_ID, ExecutionRole.EXIT, kind="ORDER", order_type="MARKET", side="SELL")
    positions.update_execution(POSITION_ID, ExecutionRole.EXIT, status="SUBMITTED", submitted_at=utc_now())


def test_a_close_whose_outcome_was_unknown_is_completed_from_binances_record(reconciler, positions, binance, manager):
    closing(positions, binance, manager)
    binance.fill_price = Decimal("79000.0")
    binance.place_market_order(symbol="BTCUSDT", side="SELL", quantity=Decimal("0.002"), client_order_id=EXIT_ID,
                               reduce_only=True)

    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.CLOSED
    assert position.close_reason is CloseReason.CLOSE_REQUESTED
    assert position.exit_average_price == Decimal("79000.0")


def test_a_close_that_never_reached_binance_reverts_after_the_grace_period(reconciler, positions, binance, manager,
                                                                          clock):
    closing(positions, binance, manager)

    reconciler.reconcile_account("account-1")
    assert positions.get(POSITION_ID).state is PositionState.CLOSING

    clock.offset_seconds = GRACE_SECONDS + 1
    reconciler.reconcile_account("account-1")

    position = positions.get(POSITION_ID)
    assert position.state is PositionState.OPEN
    assert "never reached Binance" in position.last_error


def test_a_take_profit_that_beat_a_requested_close_wins(reconciler, positions, binance, manager):
    closing(positions, binance, manager)
    binance.trigger(TAKE_PROFIT_ID, fill_price="82010.5")

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).close_reason is CloseReason.TAKE_PROFIT


# --- scope and robustness -----------------------------------------------------------------------


def test_only_the_given_accounts_positions_are_reconciled(reconciler, positions, binance, clients, manager):
    open_and_protected(positions, binance, manager)
    store_position(positions, position_id="p00000000000000000000002", account_id="account-2")

    reconciler.reconcile_account("account-1")

    assert "account-2" not in clients.by_account or clients.by_account["account-2"].calls == []


def test_reconcile_symbol_only_touches_that_symbol(reconciler, positions, binance, manager):
    open_and_protected(positions, binance, manager)

    reconciler.reconcile_symbol("account-1", "ETHUSDT")

    assert binance.calls == []


def test_one_position_failing_to_reconcile_does_not_stop_the_others(reconciler, positions, binance, manager, clock):
    submitted_entry(positions)  # position 1: its lookup will fail
    store_position(positions, position_id="p00000000000000000000002", symbol="ETHUSDT")
    manager.open("p00000000000000000000002")
    binance.trigger("lx-p00000000000000000000002-tp", fill_price="90000")
    clock.offset_seconds = GRACE_SECONDS + 1
    binance.fail_next("get_order", BinanceUnavailableError("no connection"))

    reconciler.reconcile_account("account-1")

    assert positions.get("p00000000000000000000002").state is PositionState.CLOSED


def test_a_binance_rejection_while_reconciling_leaves_the_position_for_next_time(reconciler, positions, binance,
                                                                                manager):
    open_and_protected(positions, binance, manager)
    binance.fail_next("get_position_risk", BinanceAPIError(400, -1021, "Timestamp outside recvWindow."))

    reconciler.reconcile_account("account-1")

    assert positions.get(POSITION_ID).state is PositionState.OPEN
