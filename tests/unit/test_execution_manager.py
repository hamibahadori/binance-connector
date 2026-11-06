"""Unit tests for the execution manager: draining the queue, serial per account, parallel across."""

import threading
from concurrent.futures import ThreadPoolExecutor, wait

import pytest

from binance_connector.domain.lifecycle import PositionState, ProcessingState, RequestAction, RequestStatus
from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.requests import NewRequest, RequestRepository
from binance_connector.security.redaction import SecretRedactor
from binance_connector.services.execution_manager import ExecutionManager
from tests.support import store_position


class RecordingPositionManager:
    """Records what it is asked to do; can hold a call open until the test releases it."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.gates: dict[str, threading.Event] = {}
        self.started: dict[str, threading.Event] = {}
        self.failures: dict[str, Exception] = {}
        self._lock = threading.Lock()

    def hold(self, position_id: str) -> threading.Event:
        self.gates[position_id] = threading.Event()
        self.started[position_id] = threading.Event()
        return self.gates[position_id]

    def open(self, position_id: str) -> None:
        self._run("open", position_id)

    def close(self, position_id: str, close_request_id: str) -> None:
        self._run(f"close:{close_request_id}", position_id)

    def _run(self, action: str, position_id: str) -> None:
        with self._lock:
            self.calls.append((action, position_id))
        if position_id in self.started:
            self.started[position_id].set()
        if position_id in self.gates:
            assert self.gates[position_id].wait(5), "the test never released this call"
        if position_id in self.failures:
            raise self.failures[position_id]


@pytest.fixture
def requests(session_factory, accounts) -> RequestRepository:
    return RequestRepository(session_factory)


@pytest.fixture
def error_logs(session_factory) -> ErrorLogRepository:
    return ErrorLogRepository(session_factory, redactor=SecretRedactor())


@pytest.fixture
def position_manager() -> RecordingPositionManager:
    return RecordingPositionManager()


@pytest.fixture
def executor():
    pool = ThreadPoolExecutor(max_workers=4)
    yield pool
    pool.shutdown(wait=True)


@pytest.fixture
def manager(requests, positions, position_manager, error_logs, executor) -> ExecutionManager:
    return ExecutionManager(requests=requests, positions=positions, position_manager=position_manager,
                            error_logs=error_logs, executor=executor)


def accepted(requests, positions, request_id, account_id="account-1", action=RequestAction.OPEN, position_id=None):
    if position_id is None:
        position_id = store_position(positions, position_id=f"p{len(request_id):03d}{request_id[-20:]:0>20}",
                                     account_id=account_id).position_id
    requests.create(NewRequest(request_id=request_id, account_id=account_id, user_id=f"user-of-{account_id}",
                               action=action, payload={}, payload_hash=request_id, requested_at=None))
    requests.transition(request_id, RequestStatus.VALIDATING)
    requests.transition(request_id, RequestStatus.ACCEPTED, position_id=position_id)
    return position_id


def drain(manager):
    wait(manager.dispatch_pending(), timeout=5)


def test_an_open_request_is_executed_and_marked_done(manager, requests, positions, position_manager):
    position_id = accepted(requests, positions, "request-1")

    drain(manager)

    assert position_manager.calls == [("open", position_id)]
    assert requests.get("request-1").processing_state is ProcessingState.DONE


def test_a_close_request_is_executed_with_its_own_request_id(manager, requests, positions, position_manager):
    position_id = accepted(requests, positions, "request-1")
    drain(manager)
    accepted(requests, positions, "request-close-1", action=RequestAction.CLOSE, position_id=position_id)

    drain(manager)

    assert position_manager.calls[-1] == ("close:request-close-1", position_id)


def test_requests_for_one_account_run_one_at_a_time_in_arrival_order(manager, requests, positions, position_manager):
    first_position = accepted(requests, positions, "request-1")
    release_first = position_manager.hold(first_position)
    accepted(requests, positions, "request-close-1", action=RequestAction.CLOSE, position_id=first_position)

    first_futures = manager.dispatch_pending()
    assert position_manager.started[first_position].wait(5)
    assert manager.dispatch_pending() == []  # account-1 is busy: the close waits its turn
    assert requests.get("request-close-1").processing_state is ProcessingState.PENDING

    release_first.set()
    wait(first_futures, timeout=5)
    drain(manager)

    assert [action for action, _ in position_manager.calls] == ["open", "close:request-close-1"]


def test_different_accounts_run_in_parallel(manager, requests, positions, position_manager):
    slow_position = accepted(requests, positions, "request-1", account_id="account-1")
    release_slow = position_manager.hold(slow_position)
    other_position = accepted(requests, positions, "request-2", account_id="account-2")

    futures = manager.dispatch_pending()
    assert len(futures) == 2
    futures[1].result(timeout=5)  # account-2 finishes while account-1 is still held open

    assert ("open", other_position) in position_manager.calls
    assert requests.get("request-2").processing_state is ProcessingState.DONE
    assert requests.get("request-1").processing_state is ProcessingState.IN_PROGRESS
    release_slow.set()
    wait(futures, timeout=5)
    assert requests.get("request-1").processing_state is ProcessingState.DONE


def test_claims_left_by_a_crash_are_picked_up_again(manager, requests, positions, position_manager):
    position_id = accepted(requests, positions, "request-1")
    requests.claim("request-1")  # a previous worker claimed it and died

    released = manager.recover()
    drain(manager)

    assert released == 1
    assert position_manager.calls == [("open", position_id)]


def test_an_unexpected_failure_is_logged_marks_the_position_and_finishes_the_request(
    manager, requests, positions, position_manager, error_logs
):
    position_id = accepted(requests, positions, "request-1")
    position_manager.failures[position_id] = RuntimeError("bug in execution")

    drain(manager)

    assert requests.get("request-1").processing_state is ProcessingState.DONE
    position = positions.get(position_id)
    assert position.state is PositionState.ERROR
    assert "bug in execution" in position.last_error
    log_entry = error_logs.recent()[0]
    assert log_entry.component == "execution_manager"
    assert (log_entry.request_id, log_entry.position_id, log_entry.account_id) == ("request-1", position_id,
                                                                                   "account-1")
