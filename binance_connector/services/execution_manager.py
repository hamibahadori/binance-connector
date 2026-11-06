"""Draining the request queue: serial per account, parallel across accounts (spec §4).

``dispatch_pending`` (called repeatedly by the worker loop) walks the ACCEPTED requests in
arrival order and hands each to the thread pool, **unless its account already has a
request running**. So one account's requests run one at a time, in the order Django sent
them (an OPEN always before its CLOSE), while ten users' requests for the same symbol run
side by side.

A request is ``claim``ed atomically before it runs, so it can never run twice at once, and
is marked DONE afterwards whatever happens. Execution itself never raises for Binance
problems (the position manager records them on the position); anything else is a bug: it
is written to error_logs, the position is put in ERROR so it is not silently forgotten, and
the queue moves on.

Claims a crash left IN_PROGRESS are released by ``recover`` at start-up. That is safe
because every execution step checks what it already did (see the position manager).
"""

import logging
import threading
from concurrent.futures import Executor, Future
from typing import Protocol

from binance_connector.domain.lifecycle import PositionState, RequestAction
from binance_connector.errors import InvalidTransitionError
from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.requests import Request, RequestRepository

logger = logging.getLogger(__name__)


class PositionExecutor(Protocol):
    def open(self, position_id: str) -> object: ...

    def close(self, position_id: str, close_request_id: str) -> object: ...


class ExecutionManager:
    def __init__(
        self,
        *,
        requests: RequestRepository,
        positions: PositionRepository,
        position_manager: PositionExecutor,
        error_logs: ErrorLogRepository,
        executor: Executor,
    ) -> None:
        self._requests = requests
        self._positions = positions
        self._position_manager = position_manager
        self._error_logs = error_logs
        self._executor = executor
        self._busy_accounts: set[str] = set()
        self._lock = threading.Lock()

    def recover(self) -> int:
        """Return claims left IN_PROGRESS by a crash to the queue. Returns how many."""
        released = self._requests.release_in_progress()
        if released:
            logger.info("Released %s request(s) left in progress by a previous run", released)
        return released

    def dispatch_pending(self) -> list[Future]:
        """Start every pending request whose account is idle. Returns the started futures."""
        started = []
        for pending_request in self._requests.pending():
            with self._lock:
                if pending_request.account_id in self._busy_accounts:
                    continue
                self._busy_accounts.add(pending_request.account_id)
            if not self._requests.claim(pending_request.request_id):
                self._release(pending_request.account_id)
                continue
            started.append(self._executor.submit(self._process, pending_request))
        return started

    def _process(self, request: Request) -> None:
        try:
            if request.action is RequestAction.OPEN:
                self._position_manager.open(request.position_id)
            else:
                self._position_manager.close(request.position_id, request.request_id)
        except Exception as error:  # noqa: BLE001 - a bug must not stall the queue
            logger.exception("Executing request %s failed", request.request_id)
            self._error_logs.record(
                component="execution_manager",
                message=f"Executing request {request.request_id} failed: {error}",
                context={"action": request.action.value, "error_type": type(error).__name__},
                account_id=request.account_id, request_id=request.request_id, position_id=request.position_id,
            )
            self._mark_position_error(request.position_id, f"Execution failed unexpectedly: {error}")
        finally:
            try:
                self._requests.mark_processed(request.request_id)
            finally:
                self._release(request.account_id)

    def _mark_position_error(self, position_id: str | None, message: str) -> None:
        if position_id is None:
            return
        try:
            self._positions.transition(position_id, PositionState.ERROR, last_error=message)
        except InvalidTransitionError:
            pass  # already finished: nothing live to flag

    def _release(self, account_id: str) -> None:
        with self._lock:
            self._busy_accounts.discard(account_id)
