"""Service settings, read from environment variables.

``create_app`` loads ``.env`` into the environment first (python-dotenv), so in
local development the values come from that file. See ``.env.example`` for the
full list of variables.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

from binance_connector.errors import ConfigurationError

DEMO_BASE_URL = "https://demo-fapi.binance.com"
"""Binance USD-M Futures Demo Trading REST API."""

DEFAULT_RECV_WINDOW_MS = 5000
MAX_RECV_WINDOW_MS = 60000
"""Binance rejects a recvWindow above 60 seconds."""

DEFAULT_TIMEOUT_SECONDS = 10.0

DEFAULT_DATABASE_URL = "sqlite:///binance_connector.db"
"""A SQLite file in the working directory, shared by the API and the worker."""

TRIGGER_PRICE_TYPES = ("MARK_PRICE", "CONTRACT_PRICE")
DEFAULT_TRIGGER_PRICE_TYPE = "MARK_PRICE"
"""Mark price is less prone to single-trade wicks than the last traded price (decision D-34)."""

DEMO_STREAM_URL = "wss://demo-fstream.binance.com/ws"
"""Demo user-data stream base; the listenKey is appended. Verified on Demo 2026-11-05.
Production documents ``wss://fstream.binance.com/private/ws``."""

DEFAULT_RECONCILE_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class Settings:
    """Everything the service needs to talk to Binance.

    The credentials are excluded from ``repr`` so they never end up in logs or
    tracebacks.
    """

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    base_url: str = DEMO_BASE_URL
    recv_window_ms: int = DEFAULT_RECV_WINDOW_MS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    database_url: str = DEFAULT_DATABASE_URL
    trigger_price_type: str = DEFAULT_TRIGGER_PRICE_TYPE
    stream_url: str = DEMO_STREAM_URL
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> "Settings":
        """Build settings from environment variables, failing fast on bad values.

        Raises:
            ConfigurationError: a credential is missing or blank, or a number is invalid.
        """
        return cls(
            api_key=_read_required(environment, "BINANCE_API_KEY"),
            api_secret=_read_required(environment, "BINANCE_API_SECRET"),
            base_url=environment.get("BINANCE_BASE_URL", DEMO_BASE_URL).strip().rstrip("/"),
            recv_window_ms=_read_recv_window(environment),
            timeout_seconds=_read_timeout(environment),
            database_url=environment.get("DATABASE_URL", DEFAULT_DATABASE_URL).strip() or DEFAULT_DATABASE_URL,
            trigger_price_type=_read_trigger_price_type(environment),
            stream_url=environment.get("BINANCE_STREAM_URL", DEMO_STREAM_URL).strip().rstrip("/"),
            reconcile_interval_seconds=_read_positive_number(
                environment, "RECONCILE_INTERVAL_SECONDS", DEFAULT_RECONCILE_INTERVAL_SECONDS
            ),
        )


def _read_required(environment: Mapping[str, str], variable: str) -> str:
    """Return the trimmed value of ``variable``; missing or blank is a ConfigurationError."""
    value = environment.get(variable, "").strip()
    if not value:
        raise ConfigurationError(f"{variable} is not set. Copy .env.example to .env and fill it in.")
    return value


def _read_recv_window(environment: Mapping[str, str]) -> int:
    """Read BINANCE_RECV_WINDOW_MS: a whole number of ms within Binance's 1..60000 limit."""
    variable = "BINANCE_RECV_WINDOW_MS"
    raw_value = environment.get(variable, str(DEFAULT_RECV_WINDOW_MS))
    try:
        recv_window_ms = int(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{variable} must be a whole number, got {raw_value!r}.") from error
    if not 0 < recv_window_ms <= MAX_RECV_WINDOW_MS:
        raise ConfigurationError(
            f"{variable} must be between 1 and {MAX_RECV_WINDOW_MS}, got {recv_window_ms}."
        )
    return recv_window_ms


def _read_trigger_price_type(environment: Mapping[str, str]) -> str:
    """Read BINANCE_TRIGGER_PRICE_TYPE: MARK_PRICE (default) or CONTRACT_PRICE."""
    variable = "BINANCE_TRIGGER_PRICE_TYPE"
    value = environment.get(variable, DEFAULT_TRIGGER_PRICE_TYPE).strip().upper()
    if value not in TRIGGER_PRICE_TYPES:
        raise ConfigurationError(f"{variable} must be one of {', '.join(TRIGGER_PRICE_TYPES)}, got {value!r}.")
    return value


def _read_timeout(environment: Mapping[str, str]) -> float:
    """Read BINANCE_TIMEOUT_SECONDS: a positive number of seconds."""
    return _read_positive_number(environment, "BINANCE_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)


def _read_positive_number(environment: Mapping[str, str], variable: str, default: float) -> float:
    """Read ``variable`` as a number greater than 0, or ``default`` when unset."""
    raw_value = environment.get(variable, str(default))
    try:
        number = float(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{variable} must be a number, got {raw_value!r}.") from error
    if number <= 0:
        raise ConfigurationError(f"{variable} must be greater than 0, got {number}.")
    return number
