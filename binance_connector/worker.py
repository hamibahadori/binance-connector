"""The worker process: listens to Binance and keeps trades in step with it.

Run from the project folder (after ``alembic upgrade head``)::

    python -m binance_connector.worker

Everything runs on one thread, inside the user-data stream's loop:

* every stream event goes to the event handler (TP/SL fills, entry/exit fills);
* a symbol the handler flags (its position changed) is reconciled a few seconds later, once
  related events have settled;
* every trade is reconciled after each (re)connect, since events may have been missed,
  and again every ``reconcile_interval_seconds``.

One thread means event handling and reconciliation never run concurrently inside the
worker; the only concurrency is with the API process, which the repository handles.
"""

import logging
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

logger = logging.getLogger(__name__)

DEFAULT_SETTLE_DELAY_SECONDS = 5.0


class Reconciler(Protocol):
    def reconcile_all(self) -> None: ...

    def reconcile_symbol(self, symbol: str) -> None: ...


class EventHandler(Protocol):
    def handle(self, event: dict[str, Any]) -> set[str]: ...


class Stream(Protocol):
    def run(self, *, on_event, on_connected, on_idle, should_stop) -> None: ...


class Worker:
    """Schedules event handling and reconciliation on the stream's thread."""

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
