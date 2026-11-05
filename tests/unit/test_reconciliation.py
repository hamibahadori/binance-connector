"""Unit tests for reconciliation: bringing stored trades in line with Binance's real state.

Each test sets up what a crash, a missed stream event or an outside change would leave
behind, then checks that one reconciliation pass resolves it.
"""

from datetime import timedelta
from decimal import Decimal

import pytest

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import BinanceAPIError, BinanceUnavailableError
from binance_connector.persistence.repository import NewTrade, TradeRepository, utc_now
from binance_connector.services.position_service import PositionService
from binance_connector.services.reconciliation import ReconciliationService
from binance_connector.services.trade_service import TradeService
from tests.fakes import FakeBinance

TRADE_ID = "t00000000000000000000001"
ENTRY_ID, EXIT_ID = f"lx-{TRADE_ID}-e", f"lx-{TRADE_ID}-x"
TAKE_PROFIT_ID, STOP_LOSS_ID = f"lx-{TRADE_ID}-tp", f"lx-{TRADE_ID}-sl"
GRACE_SECONDS = 120


class ShiftedClock:
    def __init__(self) -> None:
        self.offset_seconds = 0

    def __call__(self):
        return utc_now() + timedelta(seconds=self.offset_seconds)


@pytest.fixture
def binance() -> FakeBinance:
    return FakeBinance(fill_price="78000.0")


@pytest.fixture
def repository(session_factory) -> TradeRepository:
    return TradeRepository(session_factory)


@pytest.fixture
def clock() -> ShiftedClock:
    return ShiftedClock()


@pytest.fixture
def trade_service(binance, repository) -> TradeService:
    return TradeService(repository=repository, client=binance, position_service=PositionService(binance),
                        trigger_price_type="MARK_PRICE", generate_trade_id=lambda: TRADE_ID)


@pytest.fixture
def reconciler(binance, repository, trade_service, clock) -> ReconciliationService:
    return ReconciliationService(repository=repository, client=binance, position_service=PositionService(binance),
                                 trade_service=trade_service, clock=clock, entry_grace_seconds=GRACE_SECONDS)


def store(repository, states, symbol="BTCUSDT", trade_id=TRADE_ID, with_protection=True, **fields):
    repository.create(NewTrade(
        trade_id=trade_id, symbol=symbol, direction="LONG", quantity=Decimal("0.002"),
        take_profit_price=Decimal("82000.0") if with_protection else None,
        stop_loss_price=Decimal("74000.0") if with_protection else None,
    ))
    for state in states:
        repository.transition(trade_id, state)
    if fields:
        repository.update_fields(trade_id, **fields)
    return repository.get(trade_id)


def open_on_binance(binance, quantity="0.002"):
    binance.place_market_order(symbol="BTCUSDT", side="BUY", quantity=Decimal(quantity), client_order_id=ENTRY_ID)


def protect_on_binance(binance):
    for client_algo_id, order_type, trigger in ((STOP_LOSS_ID, "STOP_MARKET", "74000.0"),
                                                 (TAKE_PROFIT_ID, "TAKE_PROFIT_MARKET", "82000.0")):
        binance.place_conditional_close(symbol="BTCUSDT", side="SELL", order_type=order_type,
                                        trigger_price=Decimal(trigger), client_algo_id=client_algo_id,
                                        working_type="MARK_PRICE")


def store_protected(repository, binance):
    open_on_binance(binance)
    protect_on_binance(binance)
    return store(repository, [TradeState.ENTRY_SUBMITTED, TradeState.OPEN, TradeState.PROTECTED],
                 entry_quantity=Decimal("0.002"), take_profit_status="NEW", stop_loss_status="NEW",
                 take_profit_algo_id=binance.algo_orders[TAKE_PROFIT_ID]["algoId"],
                 stop_loss_algo_id=binance.algo_orders[STOP_LOSS_ID]["algoId"])


ENTERING = [TradeState.ENTRY_SUBMITTED]
ENTRY_UNKNOWN = [TradeState.ENTRY_SUBMITTED, TradeState.NEEDS_ATTENTION]


# --- entry stage --------------------------------------------------------------------------------


def test_a_trade_the_api_is_still_entering_is_left_alone(reconciler, repository, binance):
    store(repository, ENTERING)

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.ENTRY_SUBMITTED
    assert binance.calls == []


def test_an_entry_that_filled_while_nobody_was_watching_is_resumed_and_protected(reconciler, repository,
                                                                                    binance, clock):
    store(repository, ENTERING)
    open_on_binance(binance)
    binance.calls.clear()
    clock.offset_seconds = GRACE_SECONDS + 1

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.PROTECTED
    assert trade.entry_price == Decimal("78000.0")
    assert trade.entry_quantity == Decimal("0.002")
    assert (trade.take_profit_status, trade.stop_loss_status) == ("NEW", "NEW")


def test_an_unknown_entry_that_never_reached_binance_fails_after_the_grace_period(reconciler, repository, clock):
    store(repository, ENTRY_UNKNOWN)

    reconciler.reconcile_all()
    assert repository.get(TRADE_ID).state is TradeState.NEEDS_ATTENTION

    clock.offset_seconds = GRACE_SECONDS + 1
    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.ENTRY_FAILED
    assert "never reached Binance" in trade.last_error
    assert repository.find_active_by_symbol("BTCUSDT") is None


def test_a_trade_stuck_before_its_entry_was_sent_fails_after_the_grace_period(reconciler, repository, clock):
    store(repository, [])
    clock.offset_seconds = GRACE_SECONDS + 1

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.ENTRY_FAILED


def test_an_unknown_entry_that_did_fill_is_resumed_without_waiting(reconciler, repository, binance):
    store(repository, ENTRY_UNKNOWN)
    open_on_binance(binance)

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.PROTECTED


# --- open / protected ---------------------------------------------------------------------------


def test_a_healthy_protected_trade_is_left_as_it_is(reconciler, repository, binance):
    store_protected(repository, binance)
    binance.calls.clear()

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.PROTECTED
    assert "place_conditional_close" not in binance.call_names()
    assert "place_market_order" not in binance.call_names()


def test_a_take_profit_fill_missed_by_the_stream_is_found(reconciler, repository, binance):
    store_protected(repository, binance)
    binance.trigger(TAKE_PROFIT_ID, fill_price="82010.5")

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.TAKE_PROFIT
    assert trade.exit_price == Decimal("82010.5")
    assert trade.exit_order_id == int(binance.algo_orders[TAKE_PROFIT_ID]["actualOrderId"])
    assert trade.take_profit_status == "FINISHED"
    assert trade.stop_loss_status == "EXPIRED"


def test_a_position_that_vanished_without_our_orders_is_closed_as_external(reconciler, repository, binance):
    store_protected(repository, binance)
    binance.flatten_externally("BTCUSDT")

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.EXTERNAL
    assert (trade.take_profit_status, trade.stop_loss_status) == ("EXPIRED", "EXPIRED")


def test_protection_never_placed_because_of_a_crash_is_placed(reconciler, repository, binance):
    open_on_binance(binance)
    store(repository, [TradeState.ENTRY_SUBMITTED, TradeState.OPEN], entry_quantity=Decimal("0.002"))

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.PROTECTED
    assert binance.call_names().count("place_conditional_close") == 2


def test_protection_placed_but_never_recorded_is_adopted_not_duplicated(reconciler, repository, binance):
    open_on_binance(binance)
    protect_on_binance(binance)
    store(repository, [TradeState.ENTRY_SUBMITTED, TradeState.OPEN], entry_quantity=Decimal("0.002"))
    binance.calls.clear()

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.PROTECTED
    assert trade.stop_loss_algo_id == binance.algo_orders[STOP_LOSS_ID]["algoId"]
    assert "place_conditional_close" not in binance.call_names()


def test_protection_lost_while_the_position_is_open_needs_attention(reconciler, repository, binance):
    store_protected(repository, binance)
    binance.algo_orders[STOP_LOSS_ID]["algoStatus"] = "CANCELED"  # e.g. cancelled by hand in the Binance UI

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.NEEDS_ATTENTION
    assert "stop-loss" in trade.last_error and "CANCELED" in trade.last_error
    assert binance.call_names().count("place_conditional_close") == 2  # only the original two


def test_a_position_resized_outside_the_service_needs_attention(reconciler, repository, binance):
    store_protected(repository, binance)
    binance.positions["BTCUSDT"] = Decimal("0.005")

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.NEEDS_ATTENTION
    assert "0.005" in trade.last_error


def test_a_trade_needing_attention_heals_once_binance_looks_right_again(reconciler, repository, binance):
    store_protected(repository, binance)
    repository.transition(TRADE_ID, TradeState.NEEDS_ATTENTION, last_error="temporary")

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.PROTECTED
    assert trade.last_error is None


def test_a_failed_stop_loss_is_tried_again(reconciler, repository, binance):
    open_on_binance(binance)
    store(repository, [TradeState.ENTRY_SUBMITTED, TradeState.OPEN, TradeState.NEEDS_ATTENTION],
          entry_quantity=Decimal("0.002"), last_error="The stop-loss could not be placed")

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.PROTECTED


def test_a_trade_without_protection_that_needs_attention_heals_to_open(reconciler, repository, binance):
    open_on_binance(binance)
    store(repository, [TradeState.ENTRY_SUBMITTED, TradeState.OPEN, TradeState.NEEDS_ATTENTION],
          with_protection=False, entry_quantity=Decimal("0.002"))

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.OPEN


# --- closing ------------------------------------------------------------------------------------


def test_a_close_whose_outcome_was_unknown_is_completed_from_binances_record(reconciler, repository, binance):
    store_protected(repository, binance)
    repository.transition(TRADE_ID, TradeState.CLOSING, requested_close_reason=CloseReason.UPSTREAM)
    binance.fill_price = Decimal("79000.0")
    binance.place_market_order(symbol="BTCUSDT", side="SELL", quantity=Decimal("0.002"),
                               client_order_id=EXIT_ID, reduce_only=True)

    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.UPSTREAM
    assert trade.exit_price == Decimal("79000.0")


def test_a_close_that_never_reached_binance_reverts_after_the_grace_period(reconciler, repository, binance,
                                                                            clock):
    store_protected(repository, binance)
    repository.transition(TRADE_ID, TradeState.CLOSING, requested_close_reason=CloseReason.MANUAL)

    reconciler.reconcile_all()
    assert repository.get(TRADE_ID).state is TradeState.CLOSING

    clock.offset_seconds = GRACE_SECONDS + 1
    reconciler.reconcile_all()

    trade = repository.get(TRADE_ID)
    assert trade.state is TradeState.PROTECTED
    assert "never reached Binance" in trade.last_error


def test_a_take_profit_that_beat_a_requested_close_wins(reconciler, repository, binance):
    store_protected(repository, binance)
    repository.transition(TRADE_ID, TradeState.CLOSING, requested_close_reason=CloseReason.MANUAL)
    binance.trigger(TAKE_PROFIT_ID, fill_price="82010.5")

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).close_reason is CloseReason.TAKE_PROFIT


# --- scope and robustness -----------------------------------------------------------------------


def test_reconcile_symbol_only_touches_that_symbols_trade(reconciler, repository, binance):
    store_protected(repository, binance)
    store(repository, [], symbol="ETHUSDT", trade_id="t00000000000000000000002")
    binance.calls.clear()

    reconciler.reconcile_symbol("BTCUSDT")

    assert {arguments.get("symbol") for _name, arguments in binance.calls} - {None} == {"BTCUSDT"}


def test_reconcile_symbol_without_a_trade_does_nothing(reconciler, binance):
    reconciler.reconcile_symbol("BTCUSDT")

    assert binance.calls == []


def test_one_trade_failing_to_reconcile_does_not_stop_the_others(reconciler, repository, binance, clock):
    store(repository, ENTERING, symbol="ETHUSDT", trade_id="t00000000000000000000002")
    store_protected(repository, binance)
    binance.trigger(TAKE_PROFIT_ID, fill_price="82010.5")
    clock.offset_seconds = GRACE_SECONDS + 1
    binance.fail_next("get_order", BinanceUnavailableError("no connection"))

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.CLOSED


def test_a_binance_rejection_while_reconciling_leaves_the_trade_for_next_time(reconciler, repository, binance):
    store_protected(repository, binance)
    binance.fail_next("get_position_risk", BinanceAPIError(400, -1021, "Timestamp outside recvWindow."))

    reconciler.reconcile_all()

    assert repository.get(TRADE_ID).state is TradeState.PROTECTED
