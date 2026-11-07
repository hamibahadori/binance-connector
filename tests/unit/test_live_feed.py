"""Unit tests for the live-feed hub: the messages the WebSocket sends (spec §7)."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import ExecutionRole, PositionState
from binance_connector.persistence.market_data import MarketDataRepository
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.reports import ReportOutbox, ReportRepository
from binance_connector.services.live_feed import LiveFeedHub
from binance_connector.services.price_feed import FeedStatus, PriceTick
from tests.support import POSITION_ID, store_position

TICK_TIME = datetime(2026, 11, 7, 12, 0, 0, tzinfo=UTC)
NOW = datetime(2026, 11, 7, 12, 0, 1, tzinfo=UTC)
SECOND_POSITION_ID = "p00000000000000000000002"


class FakePriceFeed:
    def __init__(self) -> None:
        self.ticks: dict[str, PriceTick] = {}

    def latest(self, symbol: str) -> PriceTick | None:
        return self.ticks.get(symbol)

    def status(self) -> FeedStatus:
        return FeedStatus(connected=True, symbols=frozenset({"BTCUSDT"}), last_tick_at=TICK_TIME, last_error=None)


class RecordingSubscriber:
    def __init__(self, account_id: str | None = None) -> None:
        self.account_id = account_id
        self.messages: list[dict] = []

    def deliver(self, message: dict) -> None:
        self.messages.append(message)

    def of_type(self, message_type: str) -> list[dict]:
        return [message for message in self.messages if message["type"] == message_type]


@pytest.fixture
def positions(session_factory, accounts) -> PositionRepository:
    return PositionRepository(session_factory, outbox=ReportOutbox())


@pytest.fixture
def reports(session_factory) -> ReportRepository:
    return ReportRepository(session_factory)


@pytest.fixture
def price_feed() -> FakePriceFeed:
    return FakePriceFeed()


@pytest.fixture
def hub(positions, accounts, reports, session_factory, price_feed) -> LiveFeedHub:
    return LiveFeedHub(positions=positions, accounts=accounts, reports=reports,
                       market_data=MarketDataRepository(session_factory), price_feed=price_feed,
                       stream_status=lambda: {"account-1": True, "account-2": False}, clock=lambda: NOW)


def open_position(positions, position_id=POSITION_ID, account_id="account-1"):
    store_position(positions, position_id=position_id, account_id=account_id)
    positions.prepare_execution(position_id, ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET", side="BUY")
    positions.update_execution(position_id, ExecutionRole.ENTRY, status="FILLED", commission=Decimal("0.0624"))
    return positions.transition(position_id, PositionState.OPEN, entry_average_price=Decimal("78000"),
                                entry_filled_quantity=Decimal("0.002"))


def tick(price: str, symbol: str = "BTCUSDT") -> PriceTick:
    return PriceTick(symbol=symbol, mark_price=Decimal(price), event_time=TICK_TIME)


# --- snapshot -----------------------------------------------------------------------------------


def test_the_snapshot_lists_the_active_positions_with_the_latest_price(hub, positions, price_feed):
    open_position(positions)
    price_feed.ticks["BTCUSDT"] = tick("79000")

    snapshot = hub.snapshot()

    assert snapshot["type"] == "snapshot"
    assert snapshot["sent_at"] == NOW.isoformat()
    [position] = snapshot["data"]["positions"]
    assert position["position_id"] == POSITION_ID
    assert position["user_id"] == "user-of-account-1"
    assert position["prices"]["current"] == "79000"
    assert position["pnl"]["gross"] == "2"


def test_the_snapshot_can_be_limited_to_one_account(hub, positions):
    open_position(positions)
    open_position(positions, position_id=SECOND_POSITION_ID, account_id="account-2")

    snapshot = hub.snapshot(account_id="account-2")

    assert [position["position_id"] for position in snapshot["data"]["positions"]] == [SECOND_POSITION_ID]


def test_the_snapshot_leaves_out_closed_positions(hub, positions):
    store_position(positions)
    positions.transition(POSITION_ID, PositionState.FAILED, last_error="entry rejected")

    assert hub.snapshot()["data"]["positions"] == []


def test_the_snapshot_says_which_report_it_is_up_to_date_with(hub, positions, reports):
    open_position(positions)

    assert hub.snapshot()["data"]["report_sequence"] == reports.last_sequence() > 0


def test_the_snapshot_falls_back_to_the_stored_price(hub, positions, session_factory):
    open_position(positions)
    MarketDataRepository(session_factory).upsert_price("BTCUSDT", Decimal("77000"), TICK_TIME)

    [position] = hub.snapshot()["data"]["positions"]

    assert position["prices"]["current"] == "77000"


# --- price ticks --------------------------------------------------------------------------------


def test_a_price_tick_sends_each_position_on_that_symbol_its_pnl_and_distances(hub, positions):
    open_position(positions)
    hub.refresh_positions()
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)

    hub.on_price_tick(tick("80000"))

    [message] = subscriber.of_type("position_tick")
    data = message["data"]
    assert (data["position_id"], data["account_id"], data["user_id"], data["symbol"]) == (
        POSITION_ID, "account-1", "user-of-account-1", "BTCUSDT")
    assert data["state"] == "OPEN"
    assert data["prices"]["current"] == "80000"
    assert data["pnl"] == {"kind": "UNREALIZED", "gross": "4", "fees": "0.0624", "net": "3.9376"}
    assert data["take_profit"]["distance"]["absolute"] == "2000"
    assert data["stop_loss"]["distance"]["absolute"] == "6000"
    assert data["updated_at"] == TICK_TIME.isoformat()


def test_a_tick_for_another_symbol_sends_nothing(hub, positions):
    open_position(positions)
    hub.refresh_positions()
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)

    hub.on_price_tick(tick("3000", symbol="ETHUSDT"))

    assert subscriber.messages == []


def test_ticks_reach_only_subscribers_of_that_account(hub, positions):
    open_position(positions)
    hub.refresh_positions()
    own, other, everyone = RecordingSubscriber("account-1"), RecordingSubscriber("account-2"), RecordingSubscriber()
    for subscriber in (own, other, everyone):
        hub.subscribe(subscriber)

    hub.on_price_tick(tick("80000"))

    assert (len(own.messages), len(other.messages), len(everyone.messages)) == (1, 0, 1)


def test_an_unsubscribed_client_gets_nothing_more(hub, positions):
    open_position(positions)
    hub.refresh_positions()
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)
    hub.unsubscribe(subscriber)

    hub.on_price_tick(tick("80000"))

    assert subscriber.messages == []


def test_a_failing_subscriber_does_not_stop_the_others(hub, positions):
    class BrokenSubscriber(RecordingSubscriber):
        def deliver(self, message: dict) -> None:
            raise RuntimeError("socket gone")

    open_position(positions)
    hub.refresh_positions()
    healthy = RecordingSubscriber()
    hub.subscribe(BrokenSubscriber())
    hub.subscribe(healthy)

    hub.on_price_tick(tick("80000"))

    assert len(healthy.messages) == 1


# --- reports ------------------------------------------------------------------------------------


def test_new_reports_go_out_as_position_and_request_updates(hub, positions, reports):
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)
    hub.start_following_reports()
    open_position(positions)

    published = hub.publish_new_reports()

    assert published == 2
    updates = subscriber.of_type("position_update")
    assert [message["event"] for message in updates] == ["POSITION_OPENING", "POSITION_OPEN"]
    assert updates[-1]["data"]["state"] == "OPEN"
    assert updates[-1]["report_sequence"] == reports.last_sequence()


def test_a_request_report_goes_out_as_a_request_update(hub, session_factory, accounts):
    from binance_connector.domain.lifecycle import RequestAction, RequestStatus
    from binance_connector.persistence.requests import NewRequest, RequestRepository

    requests = RequestRepository(session_factory, outbox=ReportOutbox())
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)
    hub.start_following_reports()
    requests.create(NewRequest(request_id="request-1", account_id="account-1", user_id="user-of-account-1",
                               action=RequestAction.OPEN, payload={}, payload_hash="hash", requested_at=None))
    requests.transition("request-1", RequestStatus.VALIDATING)
    requests.transition("request-1", RequestStatus.REJECTED, rejection_reasons=["no such symbol"])

    hub.publish_new_reports()

    [message] = subscriber.messages
    assert (message["type"], message["event"]) == ("request_update", "REQUEST_REJECTED")
    assert message["data"]["reasons"] == ["no such symbol"]


def test_reports_written_before_the_hub_started_are_not_replayed(hub, positions):
    open_position(positions)
    hub.start_following_reports()
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)

    assert hub.publish_new_reports() == 0
    assert subscriber.messages == []


def test_report_updates_reach_only_subscribers_of_that_account(hub, positions):
    other = RecordingSubscriber("account-2")
    hub.subscribe(other)
    hub.start_following_reports()
    open_position(positions)

    hub.publish_new_reports()

    assert other.messages == []


def test_a_position_report_refreshes_the_positions_that_get_ticks(hub, positions):
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)
    hub.start_following_reports()
    open_position(positions)  # opened after the hub's last refresh

    hub.publish_new_reports()
    hub.on_price_tick(tick("80000"))

    assert len(subscriber.of_type("position_tick")) == 1


def test_a_closed_position_stops_getting_ticks(hub, positions):
    open_position(positions)
    hub.refresh_positions()
    hub.start_following_reports()
    positions.transition(POSITION_ID, PositionState.CLOSING)
    positions.transition(POSITION_ID, PositionState.CLOSED, exit_average_price=Decimal("79000"),
                         realized_pnl=Decimal("2"), fees=Decimal("0.12"))
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)

    hub.publish_new_reports()
    hub.on_price_tick(tick("80000"))

    assert subscriber.of_type("position_tick") == []


# --- service status -----------------------------------------------------------------------------


def test_the_service_status_covers_the_feed_the_account_streams_and_the_reports(hub, positions):
    open_position(positions)
    hub.subscribe(RecordingSubscriber())

    status = hub.service_status()

    assert status["type"] == "service_status"
    data = status["data"]
    assert data["price_feed"] == {"connected": True, "symbols": ["BTCUSDT"], "last_tick_at": TICK_TIME.isoformat(),
                                  "last_error": None}
    assert data["account_streams"] == {"account-1": {"connected": True}, "account-2": {"connected": False}}
    assert data["reports"] == {"PENDING": 2}
    assert data["clients"] == 1


def test_the_service_status_goes_to_every_subscriber(hub):
    own, other = RecordingSubscriber("account-1"), RecordingSubscriber("account-2")
    hub.subscribe(own)
    hub.subscribe(other)

    hub.publish_service_status()

    assert len(own.of_type("service_status")) == len(other.of_type("service_status")) == 1


# --- the loop -----------------------------------------------------------------------------------


def test_the_loop_publishes_reports_and_status_until_stopped(positions, accounts, reports, session_factory,
                                                             price_feed):
    passes = []

    def should_stop() -> bool:
        passes.append(1)
        return len(passes) > 3

    hub = LiveFeedHub(positions=positions, accounts=accounts, reports=reports,
                      market_data=MarketDataRepository(session_factory), price_feed=price_feed,
                      stream_status=dict, clock=lambda: NOW, sleep=lambda _seconds: open_position(positions)
                      if len(passes) == 1 else None)
    subscriber = RecordingSubscriber()
    hub.subscribe(subscriber)

    hub.run(should_stop)

    assert [message["event"] for message in subscriber.of_type("position_update")] == [
        "POSITION_OPENING", "POSITION_OPEN"]
    assert subscriber.of_type("service_status")
