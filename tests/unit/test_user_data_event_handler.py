"""Unit tests for turning user-data stream events into trade state changes.

Most tests replay the *real* events recorded on Demo. The spike's client ids follow the
service's own scheme (``lx-<trade_id>-sl``), so a trade with id ``spike-b`` receives exactly
the events Binance sent when that stop-loss triggered.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_connector.domain.trade_state import CloseReason, TradeState
from binance_connector.persistence.repository import NewTrade, TradeRepository
from binance_connector.services.position_service import PositionService
from binance_connector.services.stream_events import UserDataEventHandler
from binance_connector.services.trade_service import TradeService
from tests.fakes import FakeBinance
from tests.recorded import stream_events


@pytest.fixture
def repository(session_factory) -> TradeRepository:
    return TradeRepository(session_factory)


@pytest.fixture
def binance() -> FakeBinance:
    return FakeBinance(fill_price="77900.0")


@pytest.fixture
def handler(repository, binance) -> UserDataEventHandler:
    trade_service = TradeService(repository=repository, client=binance, position_service=PositionService(binance),
                                 trigger_price_type="MARK_PRICE")
    return UserDataEventHandler(repository=repository, trade_service=trade_service)


def stored_trade(repository, trade_id: str, states: list[TradeState], **fields):
    repository.create(NewTrade(trade_id=trade_id, symbol="BTCUSDT", direction="LONG", quantity=Decimal("0.002"),
                               take_profit_price=Decimal("78052.6"), stop_loss_price=Decimal("77974.6")))
    trade = None
    for state in states:
        trade = repository.transition(trade_id, state)
    if fields:
        trade = repository.update_fields(trade_id, **fields)
    return trade


def events_for(client_id_prefix: str) -> list[dict]:
    """Recorded ALGO_UPDATE / ORDER_TRADE_UPDATE events whose client id starts with the prefix."""
    selected = []
    for event in stream_events():
        if event["e"] == "ALGO_UPDATE" and event["o"]["caid"].startswith(client_id_prefix):
            selected.append(event)
        elif event["e"] == "ORDER_TRADE_UPDATE" and event["o"]["c"].startswith(client_id_prefix):
            selected.append(event)
    return selected


PROTECTED_PATH = [TradeState.ENTRY_SUBMITTED, TradeState.OPEN, TradeState.PROTECTED]


def test_the_recorded_stop_loss_trigger_closes_the_trade_as_stop_loss(handler, repository):
    stored_trade(repository, "spike-b", PROTECTED_PATH, take_profit_status="NEW", stop_loss_status="NEW")

    for event in events_for("lx-spike-b-"):
        handler.handle(event)

    trade = repository.get("spike-b")
    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.STOP_LOSS
    assert trade.exit_price == Decimal("78016.6")
    assert trade.exit_order_id == 28667219500
    assert trade.closed_at == datetime(2026, 11, 5, 15, 18, 49, 99000, tzinfo=UTC)
    assert trade.stop_loss_status == "FINISHED"
    assert trade.take_profit_status == "EXPIRED"


def test_the_trigger_sequence_is_kept_in_the_trade_history(handler, repository):
    stored_trade(repository, "spike-b", PROTECTED_PATH)

    for event in events_for("lx-spike-b-"):
        handler.handle(event)

    recorded_statuses = [event.detail for event in repository.events("spike-b") if event.kind == "FIELDS_UPDATED"]
    assert any('"stop_loss_status": "TRIGGERING"' in detail for detail in recorded_statuses)
    assert any('"stop_loss_status": "FINISHED"' in detail for detail in recorded_statuses)


def test_the_algo_update_alone_is_enough_to_close_the_trade(handler, repository):
    stored_trade(repository, "spike-b", PROTECTED_PATH)

    for event in events_for("lx-spike-b-sl"):
        if event["e"] == "ALGO_UPDATE":
            handler.handle(event)

    assert repository.get("spike-b").close_reason is CloseReason.STOP_LOSS


def test_the_order_fill_alone_is_enough_to_close_the_trade(handler, repository):
    stored_trade(repository, "spike-b", PROTECTED_PATH)

    for event in events_for("lx-spike-b-sl"):
        if event["e"] == "ORDER_TRADE_UPDATE":
            handler.handle(event)

    trade = repository.get("spike-b")
    assert trade.close_reason is CloseReason.STOP_LOSS
    assert trade.exit_price == Decimal("78016.6")


def test_a_recorded_exit_fill_completes_a_close_whose_outcome_was_unknown(handler, repository):
    stored_trade(repository, "spike-a", PROTECTED_PATH + [TradeState.CLOSING],
                 requested_close_reason=CloseReason.UPSTREAM)

    for event in events_for("lx-spike-a-x"):
        handler.handle(event)

    trade = repository.get("spike-a")
    assert trade.state is TradeState.CLOSED
    assert trade.close_reason is CloseReason.UPSTREAM
    assert trade.exit_price == Decimal("77896.4")


def test_a_recorded_entry_fill_resumes_a_trade_whose_entry_outcome_was_unknown(handler, repository, binance):
    stored_trade(repository, "spike-b", [TradeState.ENTRY_SUBMITTED, TradeState.NEEDS_ATTENTION])
    binance.positions["BTCUSDT"] = Decimal("0.002")

    for event in events_for("lx-spike-b-e"):
        handler.handle(event)

    trade = repository.get("spike-b")
    assert trade.state is TradeState.PROTECTED
    assert trade.entry_order_id == 28667219301
    assert trade.entry_price == Decimal("77900")
    assert trade.entry_quantity == Decimal("0.002")
    assert binance.call_names().count("place_conditional_close") == 2


def test_an_entry_fill_for_a_trade_the_api_is_still_entering_is_left_to_the_api(handler, repository):
    stored_trade(repository, "spike-b", [TradeState.ENTRY_SUBMITTED])

    for event in events_for("lx-spike-b-e"):
        handler.handle(event)

    assert repository.get("spike-b").state is TradeState.ENTRY_SUBMITTED


def test_an_exit_fill_for_a_trade_the_api_already_closed_changes_nothing(handler, repository):
    stored_trade(repository, "spike-a", PROTECTED_PATH + [TradeState.CLOSING])
    repository.transition("spike-a", TradeState.CLOSED, close_reason=CloseReason.MANUAL,
                          exit_price=Decimal("77896.4"))

    for event in events_for("lx-spike-a-x"):
        handler.handle(event)

    assert repository.get("spike-a").close_reason is CloseReason.MANUAL


def test_a_late_protection_fill_corrects_a_trade_reconciliation_closed_as_external(handler, repository):
    """REST can lag behind the stream: reconciliation may see 'flat, no TP/SL fill' first."""
    stored_trade(repository, "spike-b", PROTECTED_PATH)
    repository.transition("spike-b", TradeState.CLOSED, close_reason=CloseReason.EXTERNAL, last_error="unexplained")

    for event in events_for("lx-spike-b-sl"):
        handler.handle(event)

    trade = repository.get("spike-b")
    assert trade.close_reason is CloseReason.STOP_LOSS
    assert trade.exit_price == Decimal("78016.6")
    assert trade.last_error is None


def test_a_protection_fill_never_overrides_another_close_reason(handler, repository):
    stored_trade(repository, "spike-b", PROTECTED_PATH)
    repository.transition("spike-b", TradeState.CLOSED, close_reason=CloseReason.TAKE_PROFIT,
                          exit_price=Decimal("78052.6"))

    for event in events_for("lx-spike-b-sl"):
        handler.handle(event)

    assert repository.get("spike-b").close_reason is CloseReason.TAKE_PROFIT


def test_account_updates_ask_for_reconciliation_of_symbols_with_active_trades(handler, repository):
    stored_trade(repository, "spike-b", PROTECTED_PATH)
    account_update = stream_events("ACCOUNT_UPDATE")[0]

    assert handler.handle(account_update) == {"BTCUSDT"}


def test_account_updates_for_symbols_without_trades_ask_for_nothing(handler):
    assert handler.handle(stream_events("ACCOUNT_UPDATE")[0]) == set()


@pytest.mark.parametrize("event", [
    {"e": "TRADE_LITE", "s": "BTCUSDT", "c": "lx-spike-b-e"},
    {"e": "MARGIN_CALL", "E": 1},
    {"e": "SOMETHING_NEW", "E": 1},
    {"e": "ORDER_TRADE_UPDATE", "T": 1, "o": {"c": "autoclose-123", "X": "FILLED", "s": "BTCUSDT"}},
])
def test_events_that_concern_no_trade_are_ignored(handler, repository, event):
    stored_trade(repository, "spike-b", PROTECTED_PATH)

    assert handler.handle(event) == set()
    assert repository.get("spike-b").state is TradeState.PROTECTED
