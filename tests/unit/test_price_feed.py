"""Unit tests for the mark-price feed (messages recorded on Demo, 2026-11-07)."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_connector.persistence.market_data import MarketDataRepository
from binance_connector.services.price_feed import PriceFeed
from tests.recorded import MARKET_AND_LIMIT_CAPTURE, _capture

STREAM_URL = "wss://demo-fstream.binance.com/stream"


def recorded(label: str) -> list[str]:
    return [json.dumps(message) for message in _capture(MARKET_AND_LIMIT_CAPTURE)["market"][label]["messages"]]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class ScriptedConnection:
    def __init__(self, script, clock, seconds_per_receive):
        self.script, self.clock, self.seconds_per_receive = list(script), clock, seconds_per_receive
        self.closed = False

    def receive(self):
        self.clock.now += self.seconds_per_receive
        if not self.script:
            return None
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def close(self):
        self.closed = True


class FeedHarness:
    """Runs a PriceFeed against scripted connections; ``symbol_sets`` are returned in turn."""

    def __init__(self, market_data, scripts, symbol_sets, seconds_per_receive=1.0):
        self.clock = FakeClock()
        self.scripts = list(scripts)
        self.symbol_sets = list(symbol_sets)
        self.connections, self.urls, self.sleeps = [], [], []
        self.feed = PriceFeed(market_data=market_data, symbols=self._symbols, stream_url=STREAM_URL,
                              connect=self._connect, clock=self.clock, sleep=self._sleep, symbol_check_seconds=5)
        self.seconds_per_receive = seconds_per_receive
        self.idle_sleeps = 0

    def _symbols(self):
        return self.symbol_sets[0] if len(self.symbol_sets) == 1 else self.symbol_sets.pop(0)

    def _connect(self, url):
        self.urls.append(url)
        connection = ScriptedConnection(self.scripts.pop(0), self.clock, self.seconds_per_receive)
        self.connections.append(connection)
        return connection

    def _sleep(self, seconds):
        self.sleeps.append(seconds)
        self.clock.now += seconds

    def _should_stop(self):
        drained = self.connections and not self.connections[-1].script
        no_symbols_left = not self.symbol_sets[-1]
        return (not self.scripts and bool(drained)) or (no_symbols_left and len(self.sleeps) >= 3)

    def run(self):
        self.feed.run(should_stop=self._should_stop)


@pytest.fixture
def market_data(session_factory) -> MarketDataRepository:
    return MarketDataRepository(session_factory)


def test_without_active_symbols_nothing_is_connected(market_data):
    harness = FeedHarness(market_data, scripts=[], symbol_sets=[set()])

    harness.run()

    assert harness.urls == []


def test_one_combined_stream_carries_every_symbol(market_data):
    harness = FeedHarness(market_data, scripts=[recorded("combined stream")], symbol_sets=[{"ETHUSDT", "BTCUSDT"}])

    harness.run()

    assert harness.urls == [f"{STREAM_URL}?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s"]


def test_recorded_prices_are_cached_and_stored_with_binances_time(market_data):
    harness = FeedHarness(market_data, scripts=[recorded("combined stream")], symbol_sets=[{"BTCUSDT", "ETHUSDT"}])

    harness.run()

    last_btc_message = [json.loads(message)["data"] for message in recorded("combined stream")
                        if json.loads(message)["data"]["s"] == "BTCUSDT"][-1]
    btc = harness.feed.latest("BTCUSDT")
    assert btc.mark_price == Decimal(last_btc_message["p"])
    assert btc.event_time == datetime.fromtimestamp(last_btc_message["E"] / 1000, tz=UTC)
    stored = market_data.price("ETHUSDT")
    assert stored.mark_price == harness.feed.latest("ETHUSDT").mark_price


def test_single_stream_messages_without_a_wrapper_are_understood_too(market_data):
    harness = FeedHarness(market_data, scripts=[recorded("ws single")], symbol_sets=[{"BTCUSDT"}])

    harness.run()

    assert harness.feed.latest("BTCUSDT") is not None


def test_listeners_get_every_tick_and_a_failing_one_does_not_stop_the_feed(market_data):
    harness = FeedHarness(market_data, scripts=[recorded("combined stream")], symbol_sets=[{"BTCUSDT", "ETHUSDT"}])
    received = []
    harness.feed.add_listener(lambda tick: (_ for _ in ()).throw(RuntimeError("listener bug")))
    harness.feed.add_listener(received.append)

    harness.run()

    assert [tick.symbol for tick in received] == ["BTCUSDT", "ETHUSDT", "BTCUSDT", "ETHUSDT"]


def test_other_messages_are_ignored(market_data):
    harness = FeedHarness(market_data, scripts=[['{"result": null, "id": 7}', "not json"]], symbol_sets=[{"BTCUSDT"}])

    harness.run()

    assert harness.feed.latest("BTCUSDT") is None


def test_a_new_symbol_reconnects_with_the_larger_set(market_data):
    harness = FeedHarness(market_data, scripts=[[None] * 6, [None]],
                          symbol_sets=[{"BTCUSDT"}, {"BTCUSDT"}, {"BTCUSDT", "ETHUSDT"}, {"BTCUSDT", "ETHUSDT"}])

    harness.run()

    assert harness.urls == [f"{STREAM_URL}?streams=btcusdt@markPrice@1s",
                            f"{STREAM_URL}?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s"]
    assert harness.connections[0].closed


def test_a_failed_connection_is_retried_with_backoff(market_data):
    harness = FeedHarness(market_data, scripts=[[ConnectionError("reset")], [ConnectionError("reset")], [None]],
                          symbol_sets=[{"BTCUSDT"}])

    harness.run()

    assert len(harness.urls) == 3
    assert harness.sleeps == [1, 2]


def test_status_reports_the_connection_and_the_last_tick(market_data):
    harness = FeedHarness(market_data, scripts=[recorded("combined stream")], symbol_sets=[{"BTCUSDT", "ETHUSDT"}])

    harness.run()

    status = harness.feed.status()
    assert status.connected is False  # the run is over
    assert status.symbols == frozenset({"BTCUSDT", "ETHUSDT"})
    assert status.last_tick_at is not None
