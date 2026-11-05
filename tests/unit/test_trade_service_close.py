"""Unit tests for closing a Strategy trade on request (manual or upstream)."""

from decimal import Decimal

import pytest

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnknownOutcomeError,
    TradeNotClosableError,
    TradeNotFoundError,
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
    return TradeService(repository=repository, client=binance, position_service=PositionService(binance),
                        trigger_price_type="MARK_PRICE", generate_trade_id=lambda: TRADE_ID)


@pytest.fixture
def protected_trade(service):
    return service.open_trade(OpenTradeCommand(
        symbol="BTCUSDT", direction=PositionDirection.LONG, quantity=Decimal("0.002"),
        take_profit_price=Decimal("82000.0"), stop_loss_price=Decimal("74000.0"),
    )).trade


def calls_after(binance: FakeBinance, call_count: int) -> list[str]:
    return binance.call_names()[call_count:]


def test_close_sells_reduce_only_then_records_closed(service, binance, protected_trade):
    binance.fill_price = Decimal("79000.0")
    calls_before = len(binance.calls)

    trade = service.close_trade(protected_trade.trade_id, CloseReason.MANUAL)

    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.MANUAL
    assert trade.exit_price == Decimal("79000.0")
    assert trade.exit_order_id is not None
    assert trade.closed_at is not None
    exit_order = binance.orders[f"lx-{TRADE_ID}-x"]
    assert (exit_order["side"], exit_order["reduceOnly"]) == ("SELL", True)
    assert binance.positions["BTCUSDT"] == 0
    assert calls_after(binance, calls_before)[:3] == ["get_position_risk", "place_market_order", "get_order"]


def test_protection_binance_already_expired_is_recorded_not_treated_as_an_error(service, binance, protected_trade):
    trade = service.close_trade(protected_trade.trade_id, CloseReason.UPSTREAM)

    assert trade.close_reason is CloseReason.UPSTREAM
    assert (trade.take_profit_status, trade.stop_loss_status) == ("EXPIRED", "EXPIRED")
    assert trade.last_error is None


def test_protection_still_open_after_the_close_is_cancelled(service, binance, protected_trade):
    """If Binance ever leaves a closePosition order open, the service cancels it."""

    class KeepsAlgoOrdersOpen(FakeBinance):
        def _set_position(self, symbol, amount):
            self.positions[symbol] = amount  # no automatic expiry

    stubborn_binance = KeepsAlgoOrdersOpen(fill_price="78000.0")
    stubborn_binance.positions = binance.positions
    stubborn_binance.algo_orders = binance.algo_orders
    stubborn_binance.orders = binance.orders
    stubborn_service = TradeService(repository=service._repository, client=stubborn_binance,
                                    position_service=PositionService(stubborn_binance),
                                    trigger_price_type="MARK_PRICE")

    trade = stubborn_service.close_trade(protected_trade.trade_id, CloseReason.MANUAL)

    assert (trade.take_profit_status, trade.stop_loss_status) == ("CANCELED", "CANCELED")
    assert stubborn_binance.call_names().count("cancel_algo_order") == 2


def test_a_close_binance_rejects_leaves_the_trade_as_it_was(service, binance, protected_trade):
    binance.fail_next("place_market_order", BinanceAPIError(400, -1021, "Timestamp outside recvWindow."))

    trade = service.close_trade(protected_trade.trade_id, CloseReason.MANUAL)

    assert trade.state is TradeState.PROTECTED
    assert "-1021" in trade.last_error
    assert binance.positions["BTCUSDT"] == Decimal("0.002")


def test_an_unknown_close_outcome_stays_closing_for_reconciliation(service, binance, protected_trade):
    binance.fail_next("place_market_order", BinanceUnknownOutcomeError("read timeout"))

    trade = service.close_trade(protected_trade.trade_id, CloseReason.UPSTREAM)

    assert trade.state is TradeState.CLOSING
    assert trade.requested_close_reason is CloseReason.UPSTREAM
    assert "unknown" in trade.last_error.lower()


def test_a_position_already_flat_needs_attention_instead_of_guessing_why(service, binance, protected_trade):
    binance.trigger(f"lx-{TRADE_ID}-tp", fill_price="82010.0")  # TP filled; the worker has not seen it yet

    trade = service.close_trade(protected_trade.trade_id, CloseReason.MANUAL)

    assert trade.state is TradeState.NEEDS_ATTENTION
    assert "already flat" in trade.last_error


@pytest.mark.parametrize("final_state", [TradeState.CLOSED, TradeState.ENTRY_FAILED])
def test_finished_trades_cannot_be_closed(service, repository, final_state):
    repository_trade = service.open_trade(OpenTradeCommand(
        symbol="ETHUSDT", direction=PositionDirection.LONG, quantity=Decimal("1"))).trade
    if final_state is TradeState.ENTRY_FAILED:
        repository_trade = repository.transition(repository_trade.trade_id, TradeState.NEEDS_ATTENTION)
    repository.transition(repository_trade.trade_id, final_state)

    with pytest.raises(TradeNotClosableError):
        service.close_trade(repository_trade.trade_id, CloseReason.MANUAL)


def test_a_trade_already_closing_cannot_be_closed_twice(service, binance, protected_trade):
    binance.fail_next("place_market_order", BinanceUnknownOutcomeError("read timeout"))
    service.close_trade(protected_trade.trade_id, CloseReason.MANUAL)

    with pytest.raises(TradeNotClosableError, match="CLOSING"):
        service.close_trade(protected_trade.trade_id, CloseReason.MANUAL)


def test_closing_an_unknown_trade_raises_not_found(service):
    with pytest.raises(TradeNotFoundError):
        service.close_trade("no-such-trade", CloseReason.MANUAL)
