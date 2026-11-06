"""Unit tests for applying user-data stream events to positions.

Most tests replay the real events recorded on Demo. The spike's client ids follow the
service's own scheme (``lx-<position_id>-sl``), so a position with id ``spike-b`` receives
exactly the events Binance sent when that stop-loss triggered.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import CloseReason, ExecutionRole, PositionState
from binance_connector.services.position_manager import PositionManager
from binance_connector.services.stream_events import UserDataEventHandler
from tests.recorded import limit_order_events, stream_events
from tests.support import store_position


@pytest.fixture
def manager(positions, clients) -> PositionManager:
    return PositionManager(positions=positions, clients=clients, trigger_price_type="MARK_PRICE",
                           limit_entry_timeout_seconds=300)


@pytest.fixture
def handler(positions, manager) -> UserDataEventHandler:
    return UserDataEventHandler(account_id="account-1", positions=positions, position_manager=manager)


def protected_position(positions, position_id, take_profit="78052.6", stop_loss="77974.6"):
    """An OPEN position with both legs placed, as the execution manager would leave it."""
    store_position(positions, position_id=position_id, take_profit_price=Decimal(take_profit),
                   stop_loss_price=Decimal(stop_loss))
    positions.transition(position_id, PositionState.OPEN, entry_average_price=Decimal("77900"),
                         entry_filled_quantity=Decimal("0.002"))
    for role, order_type in ((ExecutionRole.STOP_LOSS, "STOP_MARKET"), (ExecutionRole.TAKE_PROFIT, "TAKE_PROFIT_MARKET")):
        positions.prepare_execution(position_id, role, kind="ALGO", order_type=order_type, side="SELL")
        positions.update_execution(position_id, role, status="NEW")
    return positions.get(position_id)


def events_for(prefix: str) -> list[dict]:
    selected = []
    for event in stream_events():
        if event["e"] == "ALGO_UPDATE" and event["o"]["caid"].startswith(prefix):
            selected.append(event)
        elif event["e"] == "ORDER_TRADE_UPDATE" and event["o"]["c"].startswith(prefix):
            selected.append(event)
    return selected


# --- TP/SL triggers (recorded) ------------------------------------------------------------------


def test_the_recorded_stop_loss_trigger_closes_the_position_as_stop_loss(handler, positions):
    protected_position(positions, "spike-b")

    for event in events_for("lx-spike-b-"):
        handler.handle(event)

    position = positions.get("spike-b")
    assert position.state is PositionState.CLOSED
    assert position.close_reason is CloseReason.STOP_LOSS
    assert position.exit_average_price == Decimal("78016.6")
    assert position.realized_pnl == Decimal("0.2332")  # (78016.6 - 77900) x 0.002, as Binance's rp says
    assert position.closed_at == datetime(2026, 11, 5, 15, 18, 49, 99000, tzinfo=UTC)
    assert position.executions[ExecutionRole.STOP_LOSS].status == "FINISHED"
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "EXPIRED"


@pytest.mark.parametrize("event_type", ["ALGO_UPDATE", "ORDER_TRADE_UPDATE"])
def test_either_event_of_the_fill_alone_closes_the_position(handler, positions, event_type):
    protected_position(positions, "spike-b")

    for event in events_for("lx-spike-b-sl"):
        if event["e"] == event_type:
            handler.handle(event)

    assert positions.get("spike-b").close_reason is CloseReason.STOP_LOSS


def test_a_late_protection_fill_corrects_a_position_closed_as_external(handler, positions):
    protected_position(positions, "spike-b")
    positions.transition("spike-b", PositionState.CLOSED, close_reason=CloseReason.EXTERNAL, last_error="unexplained")

    for event in events_for("lx-spike-b-sl"):
        handler.handle(event)

    position = positions.get("spike-b")
    assert position.close_reason is CloseReason.STOP_LOSS
    assert position.exit_average_price == Decimal("78016.6")
    assert position.last_error is None


def test_a_protection_fill_never_overrides_another_close_reason(handler, positions):
    protected_position(positions, "spike-b")
    positions.transition("spike-b", PositionState.CLOSED, close_reason=CloseReason.TAKE_PROFIT)

    for event in events_for("lx-spike-b-sl"):
        handler.handle(event)

    assert positions.get("spike-b").close_reason is CloseReason.TAKE_PROFIT


# --- entry and exit fills -----------------------------------------------------------------------


def test_a_recorded_exit_fill_completes_a_close_whose_outcome_was_unknown(handler, positions):
    protected_position(positions, "spike-a")
    positions.transition("spike-a", PositionState.CLOSING)
    positions.prepare_execution("spike-a", ExecutionRole.EXIT, kind="ORDER", order_type="MARKET", side="SELL")
    positions.update_execution("spike-a", ExecutionRole.EXIT, status="SUBMITTED")

    for event in events_for("lx-spike-a-x"):
        handler.handle(event)

    position = positions.get("spike-a")
    assert position.state is PositionState.CLOSED
    assert position.close_reason is CloseReason.CLOSE_REQUESTED
    assert position.exit_average_price == Decimal("77896.4")


def test_a_recorded_entry_fill_resumes_an_entry_whose_outcome_was_unknown(handler, positions, clients):
    store_position(positions, position_id="spike-b", take_profit_price=Decimal("82000.0"),
                   stop_loss_price=Decimal("74000.0"))
    positions.prepare_execution("spike-b", ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET", side="BUY")
    positions.update_execution("spike-b", ExecutionRole.ENTRY, status="SUBMITTED")
    positions.transition("spike-b", PositionState.ERROR, last_error="entry outcome unknown")
    clients.for_account("account-1").positions["BTCUSDT"] = Decimal("0.002")

    for event in events_for("lx-spike-b-e"):
        handler.handle(event)

    position = positions.get("spike-b")
    assert position.state is PositionState.OPEN
    assert position.entry_average_price == Decimal("77900")
    assert position.entry_filled_quantity == Decimal("0.002")
    assert position.executions[ExecutionRole.STOP_LOSS].status == "NEW"


def recorded_limit_fill(client_id: str) -> list[dict]:
    """The recorded marketable-limit events (NEW, then TRADE/FILLED), re-addressed to ``client_id``."""
    events = [event for event in limit_order_events()
              if event["e"] == "ORDER_TRADE_UPDATE" and event["o"]["c"] == "lx-spike2-l2"]
    for event in events:
        event["o"]["c"] = client_id
    return events


def test_a_resting_limit_entry_that_fills_opens_and_protects_the_position(handler, positions, clients):
    store_position(positions, entry_type="LIMIT", entry_price=Decimal("76026.2"))
    execution = positions.prepare_execution("p00000000000000000000001", ExecutionRole.ENTRY, kind="ORDER",
                                            order_type="LIMIT", side="BUY")
    positions.update_execution("p00000000000000000000001", ExecutionRole.ENTRY, status="NEW")
    clients.for_account("account-1").positions["BTCUSDT"] = Decimal("0.002")

    for event in recorded_limit_fill(execution.client_id):
        handler.handle(event)

    position = positions.get("p00000000000000000000001")
    assert position.state is PositionState.OPEN
    assert position.entry_average_price == Decimal("75879.6")
    entry = position.executions[ExecutionRole.ENTRY]
    assert (entry.status, entry.executed_quantity) == ("FILLED", Decimal("0.002"))
    assert position.executions[ExecutionRole.TAKE_PROFIT].status == "NEW"


# --- scope --------------------------------------------------------------------------------------


def test_account_updates_ask_to_reconcile_this_accounts_symbols_with_active_positions(handler, positions):
    protected_position(positions, "spike-b")
    account_update = stream_events("ACCOUNT_UPDATE")[0]

    assert handler.handle(account_update) == {"BTCUSDT"}


def test_another_accounts_position_on_the_symbol_does_not_count(positions, manager):
    store_position(positions, account_id="account-2")
    account_one_handler = UserDataEventHandler(account_id="account-1", positions=positions, position_manager=manager)

    assert account_one_handler.handle(stream_events("ACCOUNT_UPDATE")[0]) == set()


@pytest.mark.parametrize("event", [
    {"e": "TRADE_LITE", "s": "BTCUSDT", "c": "lx-spike-b-e"},
    {"e": "MARGIN_CALL", "E": 1},
    {"e": "SOMETHING_NEW", "E": 1},
    {"e": "ORDER_TRADE_UPDATE", "T": 1, "o": {"c": "autoclose-123", "X": "FILLED", "s": "BTCUSDT"}},
])
def test_events_that_concern_no_position_are_ignored(handler, positions, event):
    protected_position(positions, "spike-b")

    assert handler.handle(event) == set()
    assert positions.get("spike-b").state is PositionState.OPEN
