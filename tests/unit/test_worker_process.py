"""Unit tests for the worker process loop: queue, per-account streams, limit expiry, shutdown."""

import threading
import time

import pytest

from binance_connector.domain.lifecycle import PositionState
from binance_connector.worker import WorkerProcess
from tests.support import store_position


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class RecordingExecution:
    def __init__(self) -> None:
        self.dispatches = 0
        self.recovered = False

    def recover(self) -> int:
        self.recovered = True
        return 0

    def dispatch_pending(self) -> list:
        self.dispatches += 1
        return []


class RecordingPositionManager:
    def __init__(self) -> None:
        self.expiry_runs = 0

    def expire_limit_entries(self) -> list:
        self.expiry_runs += 1
        return []


class FakeAccountStream:
    def __init__(self, account_id: str, log: list) -> None:
        self.account_id = account_id
        self.log = log

    def start(self) -> None:
        self.log.append(("start", self.account_id))

    def stop(self) -> None:
        self.log.append(("stop", self.account_id))


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def stream_log() -> list:
    return []


@pytest.fixture
def execution() -> RecordingExecution:
    return RecordingExecution()


@pytest.fixture
def position_manager() -> RecordingPositionManager:
    return RecordingPositionManager()


@pytest.fixture
def worker(positions, execution, position_manager, stream_log, clock) -> WorkerProcess:
    return WorkerProcess(
        positions=positions,
        execution_manager=execution,
        position_manager=position_manager,
        stream_factory=lambda account_id: FakeAccountStream(account_id, stream_log),
        clock=clock,
        sleep=lambda seconds: None,
        stream_sync_seconds=5,
        limit_expiry_seconds=30,
    )


def test_every_tick_dispatches_the_queue(worker, execution):
    worker.tick()
    worker.tick()

    assert execution.dispatches == 2


def test_an_account_with_a_live_position_gets_a_stream(worker, positions, stream_log):
    store_position(positions)

    worker.tick()

    assert stream_log == [("start", "account-1")]


def test_each_account_gets_its_own_stream_once(worker, positions, stream_log, clock):
    store_position(positions)
    store_position(positions, position_id="p00000000000000000000002", account_id="account-2")
    worker.tick()

    clock.now += 6
    worker.tick()

    assert sorted(stream_log) == [("start", "account-1"), ("start", "account-2")]


def test_a_stream_stops_when_its_account_has_no_live_position_left(worker, positions, stream_log, clock):
    store_position(positions)
    worker.tick()
    positions.transition("p00000000000000000000001", PositionState.FAILED)

    clock.now += 6
    worker.tick()

    assert stream_log == [("start", "account-1"), ("stop", "account-1")]


def test_streams_are_synced_only_every_few_seconds(worker, positions, stream_log, clock):
    worker.tick()
    store_position(positions)

    clock.now += 2
    worker.tick()
    assert stream_log == []

    clock.now += 4
    worker.tick()
    assert stream_log == [("start", "account-1")]


def test_limit_entries_are_checked_for_expiry_on_their_interval(worker, position_manager, clock):
    worker.tick()
    clock.now += 10
    worker.tick()
    clock.now += 25
    worker.tick()

    assert position_manager.expiry_runs == 2


def test_background_tasks_run_on_their_own_threads_until_the_worker_stops(positions, execution, position_manager,
                                                                         stream_log, clock):
    started = threading.Event()
    finished = threading.Event()

    def price_feed_like_task(should_stop):
        started.set()
        while not should_stop():
            time.sleep(0.01)
        finished.set()

    def sleep_until_task_started(seconds):
        assert started.wait(5)
        worker.stop()

    worker = WorkerProcess(positions=positions, execution_manager=execution, position_manager=position_manager,
                           stream_factory=lambda account_id: FakeAccountStream(account_id, stream_log), clock=clock,
                           sleep=sleep_until_task_started, background_tasks=[price_feed_like_task])

    worker.run()

    assert finished.wait(5)


def test_run_recovers_first_and_stops_every_stream_on_shutdown(positions, execution, position_manager, stream_log,
                                                               clock):
    store_position(positions)
    ticks = []

    def sleep_then_stop(seconds):
        ticks.append(seconds)
        if len(ticks) == 3:
            worker.stop()

    worker = WorkerProcess(positions=positions, execution_manager=execution, position_manager=position_manager,
                           stream_factory=lambda account_id: FakeAccountStream(account_id, stream_log), clock=clock,
                           sleep=sleep_then_stop)

    worker.run()

    assert execution.recovered is True
    assert stream_log == [("start", "account-1"), ("stop", "account-1")]
    assert execution.dispatches == 3
