"""Unit tests for executing an OPEN: entry (market or limit), then TP/SL protection."""

from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import ExecutionRole, PositionState
from binance_connector.errors import BinanceAPIError, BinanceUnavailableError, BinanceUnknownOutcomeError
from binance_connector.services.position_manager import PositionManager
from tests.fakes import FakeBinance
from tests.support import POSITION_ID, store_position


@pytest.fixture
def manager(positions, clients) -> PositionManager:
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300,
                           sleep=lambda _seconds: None)


def store(positions, position_id=POSITION_ID, account_id="account-1", **overrides):
    return store_position(positions, position_id, account_id, **overrides)


def binance(clients, account_id="account-1") -> FakeBinance:
    return clients.for_account(account_id)


# --- market entries -----------------------------------------------------------------------------


def test_a_market_long_with_tp_and_sl_ends_open_and_protected(manager, positions, clients):
    store(positions)

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPEN
    assert binance(clients).call_names() == [
        "get_position_risk", "change_leverage", "place_market_order", "get_order", "get_order_trades",
        "place_conditional_close", "place_conditional_close",
    ]
    entry = position.executions[ExecutionRole.ENTRY]
    assert (entry.status, entry.executed_quantity, entry.average_price) == ("FILLED", Decimal("0.002"),
                                                                            Decimal("78000.0"))
    assert entry.client_id == f"lx-{POSITION_ID}-e"
    assert position.entry_average_price == Decimal("78000.0")
    assert position.entry_filled_quantity == Decimal("0.002")
    assert position.opened_at is not None
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "NEW"
    assert position.executions[ExecutionRole.STOP_LOSS].binance_id is not None


def test_an_open_position_reports_its_entry_commission_as_fees_so_far(manager, positions):
    """Found on Demo: fees showed null until the close although the entry fee was known."""
    from binance_connector.presentation import position_view

    store(positions)

    position = manager.open(POSITION_ID)

    entry_commission = Decimal("0.002") * Decimal("78000.0") * Decimal("0.0004")
    assert position.commissions_so_far == entry_commission
    assert position_view(position, user_id="user-1")["pnl"]["fees"] == format(entry_commission, "f")


def test_the_stop_loss_goes_first_and_protection_closes_the_whole_position(manager, positions, clients):
    store(positions)

    manager.open(POSITION_ID)

    protection_calls = [arguments for name, arguments in binance(clients).calls if name == "place_conditional_close"]
    assert [call["order_type"] for call in protection_calls] == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]
    assert {call["side"] for call in protection_calls} == {"SELL"}
    assert {call["working_type"] for call in protection_calls} == {"MARK_PRICE"}


def test_a_short_sells_to_enter_and_buys_to_protect(manager, positions, clients):
    store(positions, direction="SHORT", take_profit_price=Decimal("74000.0"), stop_loss_price=Decimal("82000.0"))

    manager.open(POSITION_ID)

    calls = dict(binance(clients).calls)
    assert calls["place_market_order"]["side"] == "SELL"
    assert calls["place_conditional_close"]["side"] == "BUY"


def test_without_leverage_or_protection_only_the_entry_is_sent(manager, positions, clients):
    store(positions, leverage=None, take_profit_price=None, stop_loss_price=None)

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPEN
    assert binance(clients).call_names() == ["get_position_risk", "place_market_order", "get_order",
                                            "get_order_trades"]


def test_the_entry_is_recorded_as_submitted_before_it_is_sent(manager, positions, clients):
    store(positions)
    seen_statuses = []
    fake = binance(clients)
    original_place = fake.place_market_order

    def place_and_check(**arguments):
        seen_statuses.append(positions.get(POSITION_ID).executions[ExecutionRole.ENTRY].status)
        return original_place(**arguments)

    fake.place_market_order = place_and_check
    manager.open(POSITION_ID)

    assert seen_statuses == ["SUBMITTED"]


def test_each_account_trades_with_its_own_client(manager, positions, clients):
    store(positions)
    store(positions, position_id="p00000000000000000000002", account_id="account-2")

    manager.open(POSITION_ID)
    manager.open("p00000000000000000000002")

    assert binance(clients, "account-1").positions["BTCUSDT"] == Decimal("0.002")
    assert binance(clients, "account-2").positions["BTCUSDT"] == Decimal("0.002")
    assert binance(clients, "account-2").orders.keys() == {"lx-p00000000000000000000002-e"}


# --- failures before or during the entry ----------------------------------------------------------


def test_an_unmanaged_position_on_the_account_fails_the_open_before_any_order(manager, positions, clients):
    store(positions)
    binance(clients).positions["BTCUSDT"] = Decimal("0.010")

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.FAILED
    assert "not managed by this service" in position.last_error
    assert "place_market_order" not in binance(clients).call_names()


def test_a_hedge_mode_account_fails_the_open(manager, positions, clients):
    store(positions)
    binance(clients).hedge_mode = True

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.FAILED
    assert "hedge" in position.last_error.lower()


@pytest.mark.parametrize("failing_call", ["change_leverage", "place_market_order"])
def test_a_rejection_fails_the_position(manager, positions, clients, failing_call):
    store(positions)
    binance(clients).fail_next(failing_call, BinanceAPIError(400, -2019, "Margin is insufficient."))

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.FAILED
    assert "-2019" in position.last_error
    assert "place_conditional_close" not in binance(clients).call_names()


def test_an_unreachable_binance_fails_the_position(manager, positions, clients):
    store(positions)
    for _attempt in range(3):  # transient failures are retried; a lasting outage is not
        binance(clients).fail_next("place_market_order", BinanceUnavailableError("no connection"))

    assert manager.open(POSITION_ID).state is PositionState.FAILED


def test_an_unknown_entry_outcome_is_an_error_left_for_reconciliation(manager, positions, clients):
    store(positions)
    binance(clients).fail_next("place_market_order", BinanceUnknownOutcomeError("read timeout"))

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.ERROR
    assert position.executions[ExecutionRole.ENTRY].status == "SUBMITTED"
    assert "unknown" in position.last_error.lower()


def test_a_failed_stop_loss_still_places_the_take_profit_and_is_an_error(manager, positions, clients):
    store(positions)
    binance(clients).fail_next("place_conditional_close", BinanceAPIError(400, -2021, "Order would immediately trigger."))

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.ERROR
    assert "stop-loss" in position.last_error and "-2021" in position.last_error
    assert position.executions[ExecutionRole.STOP_LOSS].status == "REJECTED"
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "NEW"


# --- limit entries ------------------------------------------------------------------------------


def test_a_resting_limit_entry_waits_in_opening_without_protection(manager, positions, clients):
    store(positions, entry_type="LIMIT", entry_price=Decimal("70000.0"))

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPENING
    entry = position.executions[ExecutionRole.ENTRY]
    assert (entry.order_type, entry.status, entry.price) == ("LIMIT", "NEW", Decimal("70000.0"))
    assert entry.binance_id is not None
    assert "place_conditional_close" not in binance(clients).call_names()


def test_a_marketable_limit_entry_fills_at_once_and_is_protected(manager, positions, clients):
    store(positions, entry_type="LIMIT", entry_price=Decimal("78100.0"))

    position = manager.open(POSITION_ID)

    assert position.state is PositionState.OPEN
    assert position.executions[ExecutionRole.ENTRY].status == "FILLED"
    assert binance(clients).call_names().count("place_conditional_close") == 2


# --- idempotency --------------------------------------------------------------------------------


def test_opening_again_does_nothing_once_the_entry_was_sent(manager, positions, clients):
    store(positions)
    manager.open(POSITION_ID)
    calls_after_first = len(binance(clients).calls)

    manager.open(POSITION_ID)

    assert len(binance(clients).calls) == calls_after_first


def test_a_retried_open_never_resends_an_entry_whose_outcome_is_unknown(manager, positions, clients):
    store(positions)
    positions.prepare_execution(POSITION_ID, ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET", side="BUY",
                                quantity=Decimal("0.002"))
    positions.update_execution(POSITION_ID, ExecutionRole.ENTRY, status="SUBMITTED")

    manager.open(POSITION_ID)

    assert "place_market_order" not in binance(clients).call_names()
