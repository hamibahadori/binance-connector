"""Unit tests for the worker's scheduling: what it reconciles, and when."""

from binance_connector.worker import Worker


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class RecordingReconciler:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def reconcile_all(self) -> None:
        self.calls.append("all")

    def reconcile_symbol(self, symbol: str) -> None:
        self.calls.append(symbol)


class SymbolsFromEvents:
    """An event handler stand-in: each event names the symbols to reconcile."""

    def handle(self, event: dict) -> set[str]:
        return set(event.get("symbols", []))


class ManualStream:
    """Lets the test call the worker's callbacks directly instead of running a loop."""

    def run(self, *, on_event, on_connected, on_idle, should_stop) -> None:
        self.on_event, self.on_connected, self.on_idle, self.should_stop = on_event, on_connected, on_idle, should_stop


def make_worker(clock, reconciler):
    stream = ManualStream()
    worker = Worker(stream=stream, event_handler=SymbolsFromEvents(), reconciler=reconciler,
                    reconcile_interval_seconds=60, settle_delay_seconds=5, clock=clock)
    worker.run()
    return worker, stream


def test_every_new_connection_reconciles_everything():
    reconciler = RecordingReconciler()
    _worker, stream = make_worker(FakeClock(), reconciler)

    stream.on_connected()

    assert reconciler.calls == ["all"]


def test_a_symbol_flagged_by_an_event_is_reconciled_after_the_settle_delay():
    clock, reconciler = FakeClock(), RecordingReconciler()
    _worker, stream = make_worker(clock, reconciler)
    stream.on_connected()

    stream.on_event({"symbols": ["BTCUSDT"]})
    clock.now += 4
    stream.on_idle()
    assert reconciler.calls == ["all"]

    clock.now += 2
    stream.on_idle()
    assert reconciler.calls == ["all", "BTCUSDT"]

    clock.now += 1
    stream.on_idle()
    assert reconciler.calls == ["all", "BTCUSDT"]


def test_repeated_events_for_one_symbol_reconcile_it_once():
    clock, reconciler = FakeClock(), RecordingReconciler()
    _worker, stream = make_worker(clock, reconciler)
    stream.on_connected()

    for _ in range(5):
        stream.on_event({"symbols": ["BTCUSDT"]})
    clock.now += 6
    stream.on_idle()

    assert reconciler.calls.count("BTCUSDT") == 1


def test_everything_is_reconciled_again_every_interval():
    clock, reconciler = FakeClock(), RecordingReconciler()
    _worker, stream = make_worker(clock, reconciler)
    stream.on_connected()

    clock.now += 59
    stream.on_idle()
    clock.now += 2
    stream.on_idle()

    assert reconciler.calls == ["all", "all"]


def test_stop_makes_the_stream_loop_end():
    worker, stream = make_worker(FakeClock(), RecordingReconciler())

    assert stream.should_stop() is False
    worker.stop()
    assert stream.should_stop() is True
