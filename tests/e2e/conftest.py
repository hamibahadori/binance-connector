"""Fixtures for end-to-end tests against the real Binance Futures Demo API.

The service runs for real on a local port, and tests call it over HTTP exactly
as another microservice would. Binance's state is then checked directly with
the client, independently of the service's own reply.

Safety rules built into these fixtures:

* Every test is skipped when ``.env`` has no Demo credentials.
* Tests refuse to run against anything other than the Demo base URL.
* A test is skipped if the account already holds a position in the test
  symbol, so a position you opened yourself is never touched.
* After each test, any position the test left open is closed again.
"""

import os
import threading
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
from dotenv import dotenv_values
from werkzeug.serving import make_server

from binance_connector import create_app
from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import DEMO_BASE_URL, Settings
from binance_connector.errors import ConfigurationError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

POSITION_POLL_TIMEOUT_SECONDS = 10.0
POSITION_POLL_INTERVAL_SECONDS = 0.5


@pytest.fixture(scope="session")
def e2e_environment() -> dict[str, str]:
    """``.env`` values with real environment variables taking precedence, as in the service.

    Read into a dict rather than loaded into ``os.environ`` so other tests are not affected.
    """
    file_values = {key: value for key, value in dotenv_values(ENV_FILE).items() if value is not None}
    return {**file_values, **os.environ}


@pytest.fixture(scope="session")
def e2e_settings(e2e_environment) -> Settings:
    """Demo settings, or skip every E2E test when credentials are missing."""
    try:
        settings = Settings.from_environment(e2e_environment)
    except ConfigurationError as error:
        pytest.skip(f"E2E tests need Demo API keys in {ENV_FILE}: {error}")
    if settings.base_url != DEMO_BASE_URL:
        pytest.fail(
            f"E2E tests place real orders, so they only run against {DEMO_BASE_URL}; "
            f"BINANCE_BASE_URL is {settings.base_url}."
        )
    return settings


@pytest.fixture(scope="session")
def e2e_symbol(e2e_environment) -> str:
    return e2e_environment.get("E2E_SYMBOL", "BTCUSDT").strip().upper()


@pytest.fixture(scope="session")
def e2e_quantity(e2e_environment) -> Decimal:
    """Order size for the tests.

    Opening orders must reach the symbol's minimum notional (about 100 USDT on
    BTCUSDT). Closing orders are reduce-only, which Binance exempts from it.
    """
    return Decimal(e2e_environment.get("E2E_QUANTITY", "0.002"))


@pytest.fixture(scope="session")
def service_url(e2e_settings) -> Iterator[str]:
    """Run the real service on a free local port for the whole E2E session."""
    server = make_server("127.0.0.1", 0, create_app(e2e_settings), threaded=True)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server_thread.join(timeout=5)


@pytest.fixture(scope="session")
def binance_client(e2e_settings) -> BinanceFuturesClient:
    """A direct client for checking Binance's state independently of the service."""
    return BinanceFuturesClient(e2e_settings)


def read_position_amount(binance_client: BinanceFuturesClient, symbol: str) -> Decimal:
    """The signed one-way position amount for ``symbol``; 0 when flat."""
    position_entries = binance_client.get_position_risk(symbol=symbol)
    return sum((Decimal(entry["positionAmt"]) for entry in position_entries), start=Decimal(0))


def wait_for_position_amount(
    binance_client: BinanceFuturesClient, symbol: str, expected_amount: Decimal
) -> Decimal:
    """Poll until Binance reports ``expected_amount``, or the timeout passes; return the last value."""
    deadline = time.monotonic() + POSITION_POLL_TIMEOUT_SECONDS
    position_amount = read_position_amount(binance_client, symbol)
    while position_amount != expected_amount and time.monotonic() < deadline:
        time.sleep(POSITION_POLL_INTERVAL_SECONDS)
        position_amount = read_position_amount(binance_client, symbol)
    return position_amount


@pytest.fixture
def flat_symbol(binance_client, e2e_symbol) -> Iterator[str]:
    """The test symbol, guaranteed flat at the start and flattened again at the end."""
    existing_amount = read_position_amount(binance_client, e2e_symbol)
    if existing_amount != 0:
        pytest.skip(
            f"The Demo account already holds {existing_amount} {e2e_symbol}. "
            "Close it (or set E2E_SYMBOL to another symbol) so the tests never touch your positions."
        )

    yield e2e_symbol

    leftover_amount = read_position_amount(binance_client, e2e_symbol)
    if leftover_amount != 0:
        binance_client.place_market_order(
            symbol=e2e_symbol,
            side="SELL" if leftover_amount > 0 else "BUY",
            quantity=abs(leftover_amount),
            client_order_id=f"lx-e2e-cleanup-{int(time.time())}",
            reduce_only=True,
        )
