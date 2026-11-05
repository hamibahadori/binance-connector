"""Unit tests for opening a Strategy trade: entry, then take-profit/stop-loss protection."""

from decimal import Decimal

import pytest

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
    HedgeModeNotSupportedError,
    TradeConflictError,
)
from binance_connector.persistence.repository import TradeRepository
from binance_connector.services.position_service import PositionDirection, PositionService
from binance_connector.services.trade_service import OpenTradeCommand, TradeService
from tests.fakes import FakeBinance

TRADE_ID = "t00000000000000000000001"


@pytest.fixture
def binance() -> FakeBinance:
    return FakeBinance(fill_price="78000.0")


@pytest.fixture
def repository(session_factory) -> TradeRepository:
    return TradeRepository(session_factory)


@pytest.fixture
def service(binance, repository) -> TradeService:
    return make_service(binance, repository)


def make_service(binance, repository) -> TradeService:
    return TradeService(
        repository=repository,
        client=binance,
        position_service=PositionService(binance),
        trigger_price_type="MARK_PRICE",
        generate_trade_id=lambda: TRADE_ID,
    )


def long_command(**overrides) -> OpenTradeCommand:
    fields = {
        "symbol": "BTCUSDT",
        "direction": PositionDirection.LONG,
        "quantity": Decimal("0.002"),
        "leverage": 5,
        "take_profit_price": Decimal("82000.0"),
        "stop_loss_price": Decimal("74000.0"),
        "strategy_request_id": None,
    }
    fields.update(overrides)
    return OpenTradeCommand(**fields)


# --- success ------------------------------------------------------------------------------


def test_open_with_tp_and_sl_ends_protected(service, binance):
    outcome = service.open_trade(long_command())

    trade = outcome.trade
    assert outcome.replayed is False
    assert trade.state is TradeState.PROTECTED
    assert binance.call_names() == [
        "get_position_risk",
        "change_leverage",
        "place_market_order",
        "get_order",
        "place_conditional_close",
        "place_conditional_close",
    ]
    assert trade.entry_order_id is not None
    assert trade.entry_price == Decimal("78000.0")
    assert trade.entry_quantity == Decimal("0.002")
    assert trade.opened_at is not None
    assert (trade.take_profit_status, trade.stop_loss_status) == ("NEW", "NEW")
    assert trade.take_profit_algo_id is not None and trade.stop_loss_algo_id is not None


def test_entry_uses_the_trades_own_client_order_id(service, binance):
    service.open_trade(long_command())

    entry_arguments = dict(binance.calls)["place_market_order"]
    assert entry_arguments["client_order_id"] == f"lx-{TRADE_ID}-e"
    assert entry_arguments["side"] == "BUY"
    assert entry_arguments["reduce_only"] is False


def test_stop_loss_is_placed_before_take_profit_with_close_all_semantics(service, binance):
    service.open_trade(long_command())

    protection_calls = [arguments for name, arguments in binance.calls if name == "place_conditional_close"]
    assert [call["order_type"] for call in protection_calls] == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]
    assert [call["client_algo_id"] for call in protection_calls] == [f"lx-{TRADE_ID}-sl", f"lx-{TRADE_ID}-tp"]
    assert [call["trigger_price"] for call in protection_calls] == [Decimal("74000.0"), Decimal("82000.0")]
    assert {call["side"] for call in protection_calls} == {"SELL"}
    assert {call["working_type"] for call in protection_calls} == {"MARK_PRICE"}


def test_a_short_buys_to_enter_and_its_protection_buys_to_close(service, binance):
    service.open_trade(
        long_command(direction=PositionDirection.SHORT, take_profit_price=Decimal("74000.0"),
                     stop_loss_price=Decimal("82000.0"))
    )

    assert dict(binance.calls)["place_market_order"]["side"] == "SELL"
    protection_sides = {arguments["side"] for name, arguments in binance.calls if name == "place_conditional_close"}
    assert protection_sides == {"BUY"}


def test_without_tp_or_sl_the_trade_stays_open(service, binance):
    outcome = service.open_trade(long_command(take_profit_price=None, stop_loss_price=None, leverage=None))

    assert outcome.trade.state is TradeState.OPEN
    assert "place_conditional_close" not in binance.call_names()
    assert "change_leverage" not in binance.call_names()


def test_with_only_a_stop_loss_the_trade_is_protected(service, binance):
    outcome = service.open_trade(long_command(take_profit_price=None))

    assert outcome.trade.state is TradeState.PROTECTED
    assert outcome.trade.take_profit_status is None
    assert outcome.trade.stop_loss_status == "NEW"


def test_the_history_records_every_step(service, repository):
    trade = service.open_trade(long_command()).trade

    transitions = [(event.from_state, event.to_state) for event in repository.events(trade.trade_id)
                   if event.kind == "STATE_CHANGED"]
    assert transitions == [
        (None, TradeState.RECEIVED),
        (TradeState.RECEIVED, TradeState.ENTRY_SUBMITTED),
        (TradeState.ENTRY_SUBMITTED, TradeState.OPEN),
        (TradeState.OPEN, TradeState.PROTECTED),
    ]


# --- idempotency and conflicts ---------------------------------------------------------------


def test_a_repeated_strategy_request_returns_the_same_trade_without_trading_again(service, binance):
    first = service.open_trade(long_command(strategy_request_id="strategy-7"))
    calls_after_first = len(binance.calls)

    second = service.open_trade(long_command(strategy_request_id="strategy-7"))

    assert second.replayed is True
    assert second.trade.trade_id == first.trade.trade_id
    assert len(binance.calls) == calls_after_first


def test_a_symbol_with_an_active_trade_is_refused_before_calling_binance(service, binance, repository):
    service.open_trade(long_command())
    calls_after_first = len(binance.calls)
    other_service = TradeService(repository=repository, client=binance, position_service=PositionService(binance),
                                 trigger_price_type="MARK_PRICE", generate_trade_id=lambda: "t2" + "0" * 22)

    with pytest.raises(TradeConflictError, match="BTCUSDT"):
        other_service.open_trade(long_command())

    assert len(binance.calls) == calls_after_first


def test_a_position_this_service_does_not_manage_is_refused(service, binance, repository):
    binance.positions["BTCUSDT"] = Decimal("0.010")

    with pytest.raises(TradeConflictError, match="not managed"):
        service.open_trade(long_command())

    assert repository.list_active() == []
    assert binance.call_names() == ["get_position_risk"]


def test_a_hedge_mode_account_is_refused(service, binance, repository):
    binance.hedge_mode = True

    with pytest.raises(HedgeModeNotSupportedError):
        service.open_trade(long_command())

    assert repository.list_active() == []


# --- entry failures -------------------------------------------------------------------------


def test_a_rejected_entry_ends_entry_failed_and_frees_the_symbol(service, binance, repository):
    binance.fail_next("place_market_order", BinanceAPIError(400, -2019, "Margin is insufficient."))

    trade = service.open_trade(long_command()).trade

    assert trade.state is TradeState.ENTRY_FAILED
    assert "-2019" in trade.last_error
    assert repository.find_active_by_symbol("BTCUSDT") is None
    assert "place_conditional_close" not in binance.call_names()


def test_a_rejected_leverage_change_fails_the_entry_before_any_order(service, binance):
    binance.fail_next("change_leverage", BinanceAPIError(400, -4028, "Leverage 200 is not valid"))

    trade = service.open_trade(long_command()).trade

    assert trade.state is TradeState.ENTRY_FAILED
    assert "place_market_order" not in binance.call_names()


def test_an_unreachable_binance_fails_the_entry(service, binance):
    binance.fail_next("place_market_order", BinanceUnavailableError("no connection"))

    assert service.open_trade(long_command()).trade.state is TradeState.ENTRY_FAILED


def test_an_unknown_entry_outcome_needs_attention_and_keeps_the_symbol_reserved(service, binance, repository):
    binance.fail_next("place_market_order", BinanceUnknownOutcomeError("read timeout"))

    trade = service.open_trade(long_command()).trade

    assert trade.state is TradeState.NEEDS_ATTENTION
    assert "unknown" in trade.last_error.lower()
    assert repository.find_active_by_symbol("BTCUSDT").trade_id == trade.trade_id


# --- protection failures --------------------------------------------------------------------


def test_a_rejected_stop_loss_still_places_the_take_profit_and_needs_attention(service, binance):
    binance.fail_next("place_conditional_close", BinanceAPIError(400, -2021, "Order would immediately trigger."))

    trade = service.open_trade(long_command()).trade

    assert trade.state is TradeState.NEEDS_ATTENTION
    assert "stop-loss" in trade.last_error and "-2021" in trade.last_error
    assert trade.stop_loss_status is None
    assert trade.take_profit_status == "NEW"


def test_a_trade_closed_by_the_worker_during_protection_is_returned_as_closed(binance, repository):
    """The take-profit can fill (and the worker record it) before the API finishes."""

    class TriggerDuringPlacement(FakeBinance):
        def place_conditional_close(self, **arguments):
            reply = super().place_conditional_close(**arguments)
            if arguments["order_type"] == "TAKE_PROFIT_MARKET":
                repository.transition(TRADE_ID, TradeState.CLOSED, close_reason=CloseReason.STOP_LOSS)
            return reply

    racing_binance = TriggerDuringPlacement(fill_price="78000.0")
    racing_service = make_service(racing_binance, repository)

    trade = racing_service.open_trade(long_command()).trade

    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.STOP_LOSS
