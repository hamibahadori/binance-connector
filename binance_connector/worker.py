"""The worker process: executes requests and keeps every account in step with Binance.

Run from the project folder (after ``alembic upgrade head``)::

    python -m binance_connector.worker

Threads:

* **main loop** (``WorkerProcess``), every half second: hands queued requests to the
  execution pool; every 5 s starts a user-data stream for each account that has an active
  position and stops the streams of accounts that have none left; every 30 s cancels limit
  entries past ``LIMIT_ENTRY_TIMEOUT_SECONDS``.
* **execution pool** (``EXECUTION_WORKERS`` threads): runs requests, one at a time per account.
* **one stream thread per account** (``StreamScheduler``): that account's events go to its
  event handler; a symbol whose position changed is reconciled a few seconds later; the
  whole account is reconciled after every (re)connect and every ``RECONCILE_INTERVAL_SECONDS``.
* **price feed** (``PriceFeed``): one market stream with the mark price of every symbol that
  has an active position, stored for the API and passed to listeners.

Ctrl+C or SIGTERM stops the loop, then every stream, then the pool (running requests finish).
"""

import logging
import signal
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Protocol

from binance_connector.binance.user_stream import UserDataStream
from binance_connector.composition import build_components, load_settings
from binance_connector.config import Settings
from binance_connector.persistence.positions import PositionRepository
from binance_connector.services.account_clients import AccountClients
from binance_connector.services.execution_manager import ExecutionManager
from binance_connector.services.position_manager import PositionManager
from binance_connector.services.price_feed import PriceFeed
from binance_connector.services.reconciliation import ReconciliationService
from binance_connector.services.stream_events import UserDataEventHandler

logger = logging.getLogger(__name__)

DEFAULT_SETTLE_DELAY_SECONDS = 5.0
TICK_SECONDS = 0.5
STREAM_SYNC_SECONDS = 5.0
LIMIT_EXPIRY_CHECK_SECONDS = 30.0
STREAM_STOP_TIMEOUT_SECONDS = 15.0


class Reconciler(Protocol):
    def reconcile_all(self) -> None: ...

    def reconcile_symbol(self, symbol: str) -> None: ...


class EventHandler(Protocol):
    def handle(self, event: dict[str, Any]) -> set[str]: ...


class Stream(Protocol):
    def run(self, *, on_event, on_connected, on_idle, should_stop) -> None: ...


class StreamScheduler:
    """Schedules event handling and reconciliation on one stream's thread."""

    def __init__(
        self,
        *,
        stream: Stream,
        event_handler: EventHandler,
        reconciler: Reconciler,
        reconcile_interval_seconds: float,
        settle_delay_seconds: float = DEFAULT_SETTLE_DELAY_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._stream = stream
        self._event_handler = event_handler
        self._reconciler = reconciler
        self._reconcile_interval_seconds = reconcile_interval_seconds
        self._settle_delay_seconds = settle_delay_seconds
        self._clock = clock
        self._stop_requested = threading.Event()
        self._next_full_reconciliation_at = 0.0
        self._symbols_due: dict[str, float] = {}

    def run(self) -> None:
        """Block until ``stop()`` is called."""
        self._stream.run(
            on_event=self._on_event,
            on_connected=self._reconcile_everything,
            on_idle=self._run_due_work,
            should_stop=self._stop_requested.is_set,
        )

    def stop(self) -> None:
        self._stop_requested.set()

    def _on_event(self, event: dict[str, Any]) -> None:
        for symbol in self._event_handler.handle(event):
            self._symbols_due.setdefault(symbol, self._clock() + self._settle_delay_seconds)

    def _reconcile_everything(self) -> None:
        self._symbols_due.clear()
        self._next_full_reconciliation_at = self._clock() + self._reconcile_interval_seconds
        self._reconciler.reconcile_all()

    def _run_due_work(self) -> None:
        now = self._clock()
        if now >= self._next_full_reconciliation_at:
            self._reconcile_everything()
            return
        for symbol, due_at in list(self._symbols_due.items()):
            if now >= due_at:
                del self._symbols_due[symbol]
                self._reconciler.reconcile_symbol(symbol)


class AccountReconciler:
    """Adapts the account-wide ReconciliationService to one account's StreamScheduler."""

    def __init__(self, reconciliation: ReconciliationService, account_id: str) -> None:
        self._reconciliation = reconciliation
        self._account_id = account_id

    def reconcile_all(self) -> None:
        self._reconciliation.reconcile_account(self._account_id)

    def reconcile_symbol(self, symbol: str) -> None:
        self._reconciliation.reconcile_symbol(self._account_id, symbol)


class AccountStream:
    """One account's user-data stream, running its StreamScheduler on its own thread."""

    def __init__(self, account_id: str, scheduler: StreamScheduler) -> None:
        self.account_id = account_id
        self._scheduler = scheduler
        self._thread = threading.Thread(target=scheduler.run, name=f"stream-{account_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._scheduler.stop()
        self._thread.join(timeout=STREAM_STOP_TIMEOUT_SECONDS)


class ExecutionLoop(Protocol):
    def recover(self) -> int: ...

    def dispatch_pending(self) -> list: ...


class LimitExpiry(Protocol):
    def expire_limit_entries(self) -> list: ...


class WorkerProcess:
    """The main loop: execution queue, per-account streams, limit expiry."""

    def __init__(
        self,
        *,
        positions: PositionRepository,
        execution_manager: ExecutionLoop,
        position_manager: LimitExpiry,
        stream_factory: Callable[[str], Any],
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        tick_seconds: float = TICK_SECONDS,
        stream_sync_seconds: float = STREAM_SYNC_SECONDS,
        limit_expiry_seconds: float = LIMIT_EXPIRY_CHECK_SECONDS,
        background_tasks: list[Callable[[Callable[[], bool]], None]] | None = None,
        price_feed: PriceFeed | None = None,
        on_shutdown: Callable[[], None] | None = None,
    ) -> None:
        self._positions = positions
        self._execution_manager = execution_manager
        self._position_manager = position_manager
        self._stream_factory = stream_factory
        self._clock = clock
        self._sleep = sleep
        self._tick_seconds = tick_seconds
        self._stream_sync_seconds = stream_sync_seconds
        self._limit_expiry_seconds = limit_expiry_seconds
        self._on_shutdown = on_shutdown
        self._background_tasks = list(background_tasks or [])
        self._background_threads: list[threading.Thread] = []
        self.price_feed = price_feed
        """The live price feed, for components that need prices (the WebSocket manager)."""
        self._streams: dict[str, Any] = {}
        self._stop_requested = threading.Event()
        self._next_stream_sync_at: float | None = None
        self._next_limit_expiry_at: float | None = None

    def run(self) -> None:
        """Block until ``stop()``; then stop every stream and background task."""
        self._execution_manager.recover()
        for task in self._background_tasks:
            thread = threading.Thread(target=task, args=(self._stop_requested.is_set,), daemon=True,
                                      name=getattr(task, "__name__", "background"))
            thread.start()
            self._background_threads.append(thread)
        try:
            while not self._stop_requested.is_set():
                self.tick()
                self._sleep(self._tick_seconds)
        finally:
            self._stop_requested.set()
            self._stop_streams()
            for thread in self._background_threads:
                thread.join(timeout=STREAM_STOP_TIMEOUT_SECONDS)

    def stop(self) -> None:
        self._stop_requested.set()

    def shutdown(self) -> None:
        """Stop the streams and release the execution pool (running requests finish first)."""
        self._stop_streams()
        if self._on_shutdown is not None:
            self._on_shutdown()

    def tick(self) -> None:
        """One pass of the loop. Each step is guarded so one failure never stops the others."""
        _guarded("dispatching requests", self._execution_manager.dispatch_pending)
        now = self._clock()
        if self._next_stream_sync_at is None or now >= self._next_stream_sync_at:
            self._next_stream_sync_at = now + self._stream_sync_seconds
            _guarded("syncing account streams", self.sync_streams)
        if self._next_limit_expiry_at is None or now >= self._next_limit_expiry_at:
            self._next_limit_expiry_at = now + self._limit_expiry_seconds
            _guarded("expiring limit entries", self._position_manager.expire_limit_entries)

    def sync_streams(self) -> None:
        """Run a stream for exactly the accounts that have an active position."""
        wanted_accounts = {position.account_id for position in self._positions.list_active()}
        for account_id in sorted(wanted_accounts - self._streams.keys()):
            stream = self._stream_factory(account_id)
            stream.start()
            self._streams[account_id] = stream
            logger.info("Started the user-data stream of account %s", account_id)
        for account_id in sorted(self._streams.keys() - wanted_accounts):
            self._streams.pop(account_id).stop()
            logger.info("Stopped the user-data stream of account %s (no active position)", account_id)

    def _stop_streams(self) -> None:
        while self._streams:
            _account_id, stream = self._streams.popitem()
            stream.stop()


def _guarded(description: str, step: Callable[[], Any]) -> None:
    try:
        step()
    except Exception:  # noqa: BLE001 - the loop must keep running
        logger.exception("Worker: %s failed", description)


def build_worker(settings: Settings) -> WorkerProcess:
    """Wire the worker from settings.

    Raises:
        ConfigurationError: the database schema is not current, or the encryption key is invalid.
    """
    components = build_components(settings)
    clients = AccountClients(accounts=components.accounts, settings=settings)
    position_manager = PositionManager(
        positions=components.positions,
        clients=clients,
        trigger_price_type=settings.trigger_price_type,
        limit_entry_timeout_seconds=settings.limit_entry_timeout_seconds,
        error_logs=components.error_logs,
    )
    executor = ThreadPoolExecutor(max_workers=settings.execution_workers, thread_name_prefix="execution")
    execution_manager = ExecutionManager(requests=components.requests, positions=components.positions,
                                         position_manager=position_manager, error_logs=components.error_logs,
                                         executor=executor)
    reconciliation = ReconciliationService(positions=components.positions, clients=clients,
                                           position_manager=position_manager)

    def stream_factory(account_id: str) -> AccountStream:
        scheduler = StreamScheduler(
            stream=UserDataStream(client=clients.for_account(account_id), stream_url=settings.stream_url),
            event_handler=UserDataEventHandler(account_id=account_id, positions=components.positions,
                                               position_manager=position_manager),
            reconciler=AccountReconciler(reconciliation, account_id),
            reconcile_interval_seconds=settings.reconcile_interval_seconds,
        )
        return AccountStream(account_id, scheduler)

    def release_resources() -> None:
        executor.shutdown(wait=True)
        components.engine.dispose()

    price_feed = PriceFeed(
        market_data=components.market_data,
        symbols=lambda: {position.symbol for position in components.positions.list_active()},
        stream_url=settings.market_stream_url,
    )
    return WorkerProcess(positions=components.positions, execution_manager=execution_manager,
                         position_manager=position_manager, stream_factory=stream_factory,
                         background_tasks=[price_feed.run], price_feed=price_feed, on_shutdown=release_resources)


def main() -> None:
    """Entry point: run until Ctrl+C or SIGTERM, then shut down cleanly."""
    from binance_connector import configure_logging

    configure_logging()
    worker = build_worker(load_settings())

    def request_stop(signal_number, _frame) -> None:
        logger.info("Received signal %s; stopping", signal_number)
        worker.stop()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    logger.info("Worker starting")
    try:
        worker.run()
    finally:
        worker.shutdown()
    logger.info("Worker stopped")


if __name__ == "__main__":
    main()
