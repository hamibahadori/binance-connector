"""The end-to-end harness: the whole service, for real, against Binance Futures **Demo**.

What runs, all in this process on free local ports:

* the API, served by waitress exactly as in production (``binance_connector.server``);
* the worker (``binance_connector.worker.build_worker``): execution, user streams,
  reconciliation, the price feed, report delivery and the WebSocket server;
* a **fake Django**: an HTTP server that records every report it receives, checks the
  auth header, and refuses the very first report (503) so the retry path is exercised;
* a WebSocket client that records every message the live feed sends.

Requests carry the Demo credentials from ``.env`` (``BINANCE_API_KEY`` /
``BINANCE_API_SECRET``) in their body, as Django would. A fresh SQLite database is used.

Safety rules:

* Every E2E test is skipped without Demo credentials in ``.env``.
* They refuse to run unless every Binance URL is a Demo URL.
* A test skips if the account already holds its symbol, so your own positions are never
  touched; afterwards it cancels its leftover TP/SL and flattens what it opened.

Optional ``.env`` values: ``E2E_SYMBOL`` (default BTCUSDT), ``E2E_QUANTITY`` (0.002),
``E2E_SECOND_SYMBOL`` (ETHUSDT), ``E2E_SECOND_API_KEY`` / ``E2E_SECOND_API_SECRET`` (a
second Demo account for the multi-user test), ``E2E_CAPTURE_DIR`` (write every request,
reply, report and WebSocket message there, credentials masked, for the docs' samples).
"""

import json
import os
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from decimal import ROUND_DOWN, Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
import requests
from dotenv import dotenv_values
from websockets.sync.client import connect

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import DEMO_BASE_URL, Settings
from binance_connector.errors import BinanceAPIError, ConfigurationError
from binance_connector.persistence.database import create_database_engine, create_session_factory
from binance_connector.persistence.reports import ReportRepository
from binance_connector.persistence.schema import upgrade_to_head
from binance_connector.security.credentials import ApiCredentials
from binance_connector.server import build_server
from binance_connector.worker import WorkerProcess, build_worker

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
INBOUND_TOKEN = "e2e-inbound-token-0001"
WEBSOCKET_TOKEN = "e2e-websocket-token-0001"
DJANGO_AUTH_VALUE = "Bearer e2e-django-token-0001"
STEP_TIMEOUT_SECONDS = 180.0
"""Each step may take several Binance round trips, measured at up to ~23 s each here (D-31)."""


def wait_until(description: str, check: Callable[[], Any], timeout_seconds: float = STEP_TIMEOUT_SECONDS,
               interval_seconds: float = 1.0) -> Any:
    """Poll ``check`` until it returns something truthy; return that, or fail after the timeout."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"Timed out after {timeout_seconds:g} s waiting for {description}")
        time.sleep(interval_seconds)


# --- capture for the docs -----------------------------------------------------------------------


class Capture:
    """Writes samples to ``E2E_CAPTURE_DIR`` with every credential replaced by a placeholder."""

    def __init__(self, directory: str | None, secrets: list[tuple[str, str]]) -> None:
        self._directory = Path(directory) if directory else None
        # The caller passes (value, placeholder) pairs; the first placeholder for a value wins
        # (without E2E_SECOND_* keys both accounts share one login).
        self._secrets: dict[str, str] = {}
        for value, placeholder in secrets:
            if value:
                self._secrets.setdefault(value, placeholder)
        self._lock = threading.Lock()
        if self._directory is not None:
            self._directory.mkdir(parents=True, exist_ok=True)

    def save(self, name: str, content: Any) -> None:
        if self._directory is None:
            return
        text = json.dumps(content, indent=2, default=str)
        for secret, placeholder in self._secrets.items():
            text = text.replace(secret, placeholder)
        with self._lock:
            (self._directory / f"{name}.json").write_text(text + "\n", encoding="utf-8", newline="\n")


# --- fake Django --------------------------------------------------------------------------------


@dataclass
class ReceivedReport:
    headers: dict[str, str]
    body: dict[str, Any]


@dataclass
class FakeDjango:
    """Records the reports the worker POSTs; refuses the first one to prove the retry."""

    reports: list[ReceivedReport] = field(default_factory=list)
    refused: list[dict[str, Any]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def events_for(self, position_id: str) -> list[str]:
        return [report.body["event"] for report in self.unique_reports() if report.body.get("position_id") == position_id]

    def unique_reports(self) -> list[ReceivedReport]:
        """First delivery of each report_id (delivery is at least once)."""
        with self.lock:
            seen, unique = set(), []
            for report in self.reports:
                if report.body["report_id"] not in seen:
                    seen.add(report.body["report_id"])
                    unique.append(report)
            return unique


def start_fake_django(django: FakeDjango) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - http.server's naming
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with django.lock:
                refuse = not django.refused
                if refuse:
                    django.refused.append(body)
                else:
                    django.reports.append(ReceivedReport(dict(self.headers.items()), body))
            self.send_response(503 if refuse else 200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_arguments) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# --- WebSocket client ---------------------------------------------------------------------------


class LiveFeedRecorder:
    """Keeps a WebSocket open to the worker's live feed and records every message."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def connect(self, url: str) -> None:
        self.disconnect()
        self._stop = threading.Event()
        connected = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(url, self._stop, connected), daemon=True)
        self._thread.start()
        if not connected.wait(30):
            raise AssertionError(f"Could not connect to the live feed at {url}")

    def disconnect(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def _run(self, url: str, stop: threading.Event, connected: threading.Event) -> None:
        with connect(url, additional_headers={"Authorization": f"Bearer {WEBSOCKET_TOKEN}"}, open_timeout=30) as client:
            connected.set()
            while not stop.is_set():
                try:
                    raw = client.recv(timeout=1)
                except TimeoutError:
                    continue
                except Exception:  # noqa: BLE001 - the worker stopped: this connection is over
                    return
                with self._lock:
                    self.messages.append(json.loads(raw))

    def of_type(self, message_type: str, position_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            found = [message for message in self.messages if message["type"] == message_type]
        if position_id is not None:
            found = [message for message in found if message["data"].get("position_id") == position_id]
        return found

    def snapshots(self) -> list[dict[str, Any]]:
        return self.of_type("snapshot")


# --- the service --------------------------------------------------------------------------------


@dataclass
class DemoAccount:
    user_id: str
    account_id: str
    credentials: ApiCredentials
    client: BinanceFuturesClient


class ServiceStack:
    """The API, the worker, fake Django and a live-feed client, wired together."""

    def __init__(self, settings: Settings, django: FakeDjango, capture: Capture) -> None:
        self.settings = settings
        self.django = django
        self.capture = capture
        self.live_feed = LiveFeedRecorder()
        self._api_server = build_server(settings)
        threading.Thread(target=self._api_server.run, daemon=True).start()
        self.api_url = f"http://127.0.0.1:{self._api_server.effective_port}"
        self._headers = {"Authorization": f"Bearer {INBOUND_TOKEN}"}
        self._worker: WorkerProcess | None = None
        self._worker_thread: threading.Thread | None = None
        engine = create_database_engine(settings.database_url)
        self.reports = ReportRepository(create_session_factory(engine))
        self._engine = engine

    # worker --------------------------------------------------------------------------------

    def start_worker(self) -> None:
        self._worker = build_worker(self.settings)
        self._worker_thread = threading.Thread(target=self._worker.run, daemon=True)
        self._worker_thread.start()
        port = self._worker.live_feed_server.wait_until_ready(timeout_seconds=30)
        self.live_feed.connect(f"ws://127.0.0.1:{port}/ws")

    def stop_worker(self) -> None:
        if self._worker is None:
            return
        self.live_feed.disconnect()
        self._worker.stop()
        self._worker_thread.join(timeout=60)
        self._worker.shutdown()
        self._worker = None

    def close(self) -> None:
        self.stop_worker()
        self._api_server.close()
        self._engine.dispose()

    # API -----------------------------------------------------------------------------------

    def post(self, body: dict[str, Any]) -> requests.Response:
        return requests.post(f"{self.api_url}/api/v1/requests", json=body, headers=self._headers, timeout=60)

    def get(self, path: str) -> requests.Response:
        return requests.get(f"{self.api_url}{path}", headers=self._headers, timeout=60)

    def position(self, position_id: str) -> dict[str, Any]:
        reply = self.get(f"/api/v1/positions/{position_id}")
        assert reply.status_code == 200, reply.text
        return reply.json()

    def wait_for_position(self, position_id: str, description: str, check: Callable[[dict[str, Any]], bool],
                          timeout_seconds: float = STEP_TIMEOUT_SECONDS) -> dict[str, Any]:
        return wait_until(f"position {position_id} to be {description}",
                          lambda: (lambda view: view if check(view) else None)(self.position(position_id)),
                          timeout_seconds=timeout_seconds)

    def wait_for_state(self, position_id: str, *states: str, timeout_seconds: float = STEP_TIMEOUT_SECONDS):
        return self.wait_for_position(position_id, " or ".join(states), lambda view: view["state"] in states,
                                      timeout_seconds=timeout_seconds)


# --- fixtures -----------------------------------------------------------------------------------


@pytest.fixture(scope="session")
def e2e_environment() -> dict[str, str]:
    """``.env`` values, real environment variables winning; never loaded into os.environ."""
    file_values = {key: value for key, value in dotenv_values(ENV_FILE).items() if value is not None}
    return {**file_values, **os.environ}


@pytest.fixture(scope="session")
def demo_settings(e2e_environment, tmp_path_factory) -> Settings:
    if not (e2e_environment.get("BINANCE_API_KEY") and e2e_environment.get("BINANCE_API_SECRET")):
        pytest.skip(f"E2E tests need BINANCE_API_KEY and BINANCE_API_SECRET (Demo) in {ENV_FILE}")
    try:
        settings = Settings.from_environment(e2e_environment)
    except ConfigurationError as error:
        pytest.skip(f"E2E tests need a valid {ENV_FILE}: {error}")
    urls = (settings.base_url, settings.stream_url, settings.market_stream_url)
    if settings.base_url != DEMO_BASE_URL or not all("demo" in url for url in urls):
        pytest.fail(f"E2E tests place real orders, so they only run against Binance Demo; got {urls}")
    database_url = f"sqlite:///{tmp_path_factory.mktemp('e2e') / 'e2e.db'}"
    engine = create_database_engine(database_url)
    upgrade_to_head(engine)
    engine.dispose()
    return replace(settings, database_url=database_url, api_port=0, websocket_port=0,
                   inbound_api_token=INBOUND_TOKEN, websocket_token=WEBSOCKET_TOKEN,
                   django_report_auth_header="Authorization", django_report_auth_value=DJANGO_AUTH_VALUE,
                   limit_entry_timeout_seconds=600)


@pytest.fixture(scope="session")
def primary_account(demo_settings, e2e_environment) -> DemoAccount:
    credentials = ApiCredentials(e2e_environment["BINANCE_API_KEY"], e2e_environment["BINANCE_API_SECRET"])
    return DemoAccount("e2e-user-1", "e2e-account-1", credentials, BinanceFuturesClient(demo_settings, credentials))


@pytest.fixture(scope="session")
def second_account(demo_settings, e2e_environment, primary_account) -> DemoAccount:
    """A second user and account. Without E2E_SECOND_* keys it shares the primary Binance
    login (and so must trade another symbol); the service still treats it as separate."""
    key, secret = e2e_environment.get("E2E_SECOND_API_KEY"), e2e_environment.get("E2E_SECOND_API_SECRET")
    if not (key and secret):
        return replace(primary_account, user_id="e2e-user-2", account_id="e2e-account-2")
    credentials = ApiCredentials(key, secret)
    return DemoAccount("e2e-user-2", "e2e-account-2", credentials, BinanceFuturesClient(demo_settings, credentials))


@pytest.fixture(scope="session")
def capture(e2e_environment, primary_account, second_account) -> Capture:
    return Capture(e2e_environment.get("E2E_CAPTURE_DIR"), [
        (primary_account.credentials.api_key, "<demo-api-key>"),
        (primary_account.credentials.api_secret, "<demo-api-secret>"),
        (second_account.credentials.api_key, "<second-demo-api-key>"),
        (second_account.credentials.api_secret, "<second-demo-api-secret>"),
        (INBOUND_TOKEN, "<inbound-api-token>"),
        (WEBSOCKET_TOKEN, "<websocket-token>"),
        (DJANGO_AUTH_VALUE.removeprefix("Bearer "), "<django-token>"),
    ])


@pytest.fixture(scope="session")
def stack(demo_settings, capture) -> Iterator[ServiceStack]:
    from binance_connector import configure_logging

    configure_logging()
    django = FakeDjango()
    django_server = start_fake_django(django)
    settings = replace(demo_settings, django_report_url=f"http://127.0.0.1:{django_server.server_port}/reports")
    service = ServiceStack(settings, django, capture)
    service.start_worker()
    yield service
    service.close()
    django_server.shutdown()


@pytest.fixture(scope="session")
def symbol(e2e_environment) -> str:
    return e2e_environment.get("E2E_SYMBOL", "BTCUSDT").strip().upper()


@pytest.fixture(scope="session")
def second_symbol(e2e_environment) -> str:
    return e2e_environment.get("E2E_SECOND_SYMBOL", "ETHUSDT").strip().upper()


@pytest.fixture(scope="session")
def quantity(e2e_environment) -> str:
    return e2e_environment.get("E2E_QUANTITY", "0.002").strip()


# --- market helpers -----------------------------------------------------------------------------


def mark_price(symbol: str) -> Decimal:
    reply = requests.get(f"{DEMO_BASE_URL}/fapi/v1/premiumIndex", params={"symbol": symbol}, timeout=60)
    reply.raise_for_status()
    return Decimal(reply.json()["markPrice"])


_TICK_SIZES: dict[str, Decimal] = {}


def tick_size(symbol: str) -> Decimal:
    if symbol not in _TICK_SIZES:
        reply = requests.get(f"{DEMO_BASE_URL}/fapi/v1/exchangeInfo", timeout=60)
        reply.raise_for_status()
        for entry in reply.json()["symbols"]:
            for rule in entry["filters"]:
                if rule["filterType"] == "PRICE_FILTER":
                    _TICK_SIZES[entry["symbol"]] = Decimal(rule["tickSize"]).normalize()
    return _TICK_SIZES[symbol]


def price_away(symbol: str, percent: str, reference: Decimal | None = None) -> str:
    """The price ``percent`` away from the mark (negative = below), on the tick grid."""
    reference = reference if reference is not None else mark_price(symbol)
    target = reference * (Decimal(1) + Decimal(percent) / 100)
    tick = tick_size(symbol)
    return format((target / tick).to_integral_value(rounding=ROUND_DOWN) * tick, "f")


def signed_amount(client: BinanceFuturesClient, symbol: str) -> Decimal:
    return sum((Decimal(entry["positionAmt"]) for entry in client.get_position_risk(symbol=symbol)), start=Decimal(0))


def our_open_algo_orders(client: BinanceFuturesClient, symbol: str) -> list[dict[str, Any]]:
    return [order for order in client.get_open_algo_orders(symbol=symbol)
            if str(order.get("clientAlgoId", "")).startswith("lx-")]


@pytest.fixture
def flat(stack):
    """Returns ``ensure(account, symbol)``: skip if the symbol is already held there, and clean
    up afterwards (cancel our TP/SL left open, flatten what is left)."""
    used: list[tuple[DemoAccount, str]] = []

    def ensure(account: DemoAccount, symbol: str) -> str:
        held = signed_amount(account.client, symbol)
        if held != 0:
            pytest.skip(f"The Demo account already holds {held} {symbol}; close it so tests never touch it")
        used.append((account, symbol))
        return symbol

    yield ensure

    for account, symbol in used:
        for order in our_open_algo_orders(account.client, symbol):
            try:
                account.client.cancel_algo_order(client_algo_id=order["clientAlgoId"])
            except BinanceAPIError:
                pass
        leftover = signed_amount(account.client, symbol)
        if leftover != 0:
            account.client.place_market_order(symbol=symbol, side="SELL" if leftover > 0 else "BUY",
                                              quantity=abs(leftover),
                                              client_order_id=f"lx-e2e-cleanup-{int(time.time())}", reduce_only=True)
