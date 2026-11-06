"""Service settings, read from environment variables.

``load_settings`` (in ``composition``) loads ``.env`` into the environment first, so in
local development the values come from that file. ``.env.example`` lists every variable.

Binance API keys are **not** service settings any more: each account's credentials arrive
with Django's requests and are stored encrypted (see ``security.credentials``).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from binance_connector.errors import ConfigurationError
from binance_connector.security.credentials import ENCRYPTION_KEY_VARIABLE, CredentialCipher

DEMO_BASE_URL = "https://demo-fapi.binance.com"
"""Binance USD-M Futures Demo Trading REST API."""

DEMO_STREAM_URL = "wss://demo-fstream.binance.com/ws"
"""Demo user-data stream base; the listenKey is appended. Verified on Demo 2026-11-05.
Production documents ``wss://fstream.binance.com/private/ws``."""

DEMO_MARKET_STREAM_URL = "wss://demo-fstream.binance.com/stream"
"""Demo combined market stream (``?streams=...`` is appended). Verified on Demo 2026-11-07."""

DEFAULT_RECV_WINDOW_MS = 5000
MAX_RECV_WINDOW_MS = 60000
"""Binance rejects a recvWindow above 60 seconds."""

DEFAULT_TIMEOUT_SECONDS = 10.0

DEFAULT_DATABASE_URL = "sqlite:///binance_connector.db"
"""A SQLite file in the working directory, shared by the API and the worker."""

TRIGGER_PRICE_TYPES = ("MARK_PRICE", "CONTRACT_PRICE")
DEFAULT_TRIGGER_PRICE_TYPE = "MARK_PRICE"
"""Mark price is less prone to single-trade wicks than the last traded price (decision D-34)."""

DEFAULT_RECONCILE_INTERVAL_SECONDS = 60.0
DEFAULT_LIMIT_ENTRY_TIMEOUT_SECONDS = 300.0
DEFAULT_EXECUTION_WORKERS = 4


@dataclass(frozen=True)
class Settings:
    """Everything the service needs, apart from per-account credentials.

    Secrets (the encryption key, the inbound API token) are excluded from ``repr`` so they
    never end up in logs or tracebacks.
    """

    credentials_encryption_key: str = field(repr=False)
    base_url: str = DEMO_BASE_URL
    recv_window_ms: int = DEFAULT_RECV_WINDOW_MS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    database_url: str = DEFAULT_DATABASE_URL
    trigger_price_type: str = DEFAULT_TRIGGER_PRICE_TYPE
    stream_url: str = DEMO_STREAM_URL
    market_stream_url: str = DEMO_MARKET_STREAM_URL
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    limit_entry_timeout_seconds: float = DEFAULT_LIMIT_ENTRY_TIMEOUT_SECONDS
    price_guard_max_deviation_percent: Decimal | None = None
    execution_workers: int = DEFAULT_EXECUTION_WORKERS
    inbound_api_token: str | None = field(default=None, repr=False)

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> "Settings":
        """Build settings from environment variables, failing fast on bad values.

        Raises:
            ConfigurationError: a required value is missing, or any value is invalid.
        """
        return cls(
            credentials_encryption_key=_read_encryption_key(environment),
            base_url=_read_url(environment, "BINANCE_BASE_URL", DEMO_BASE_URL),
            recv_window_ms=_read_recv_window(environment),
            timeout_seconds=_read_positive_number(environment, "BINANCE_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
            database_url=environment.get("DATABASE_URL", DEFAULT_DATABASE_URL).strip() or DEFAULT_DATABASE_URL,
            trigger_price_type=_read_trigger_price_type(environment),
            stream_url=_read_url(environment, "BINANCE_STREAM_URL", DEMO_STREAM_URL),
            market_stream_url=_read_url(environment, "BINANCE_MARKET_STREAM_URL", DEMO_MARKET_STREAM_URL),
            reconcile_interval_seconds=_read_positive_number(
                environment, "RECONCILE_INTERVAL_SECONDS", DEFAULT_RECONCILE_INTERVAL_SECONDS
            ),
            limit_entry_timeout_seconds=_read_positive_number(
                environment, "LIMIT_ENTRY_TIMEOUT_SECONDS", DEFAULT_LIMIT_ENTRY_TIMEOUT_SECONDS
            ),
            price_guard_max_deviation_percent=_read_optional_percent(environment, "PRICE_GUARD_MAX_DEVIATION_PERCENT"),
            execution_workers=_read_positive_integer(environment, "EXECUTION_WORKERS", DEFAULT_EXECUTION_WORKERS),
            inbound_api_token=environment.get("INBOUND_API_TOKEN", "").strip() or None,
        )


def _read_encryption_key(environment: Mapping[str, str]) -> str:
    """Required, and must be a valid Fernet key (checked now, not on the first request)."""
    key = environment.get(ENCRYPTION_KEY_VARIABLE, "").strip()
    if not key:
        raise ConfigurationError(
            f"{ENCRYPTION_KEY_VARIABLE} is not set. Generate one with "
            "`python -m binance_connector.security.credentials` and put it in .env."
        )
    CredentialCipher(key)
    return key


def _read_url(environment: Mapping[str, str], variable: str, default: str) -> str:
    return environment.get(variable, default).strip().rstrip("/") or default


def _read_recv_window(environment: Mapping[str, str]) -> int:
    """Read BINANCE_RECV_WINDOW_MS: a whole number of ms within Binance's 1..60000 limit."""
    variable = "BINANCE_RECV_WINDOW_MS"
    recv_window_ms = _read_positive_integer(environment, variable, DEFAULT_RECV_WINDOW_MS)
    if recv_window_ms > MAX_RECV_WINDOW_MS:
        raise ConfigurationError(f"{variable} must be between 1 and {MAX_RECV_WINDOW_MS}, got {recv_window_ms}.")
    return recv_window_ms


def _read_trigger_price_type(environment: Mapping[str, str]) -> str:
    """Read BINANCE_TRIGGER_PRICE_TYPE: MARK_PRICE (default) or CONTRACT_PRICE."""
    variable = "BINANCE_TRIGGER_PRICE_TYPE"
    value = environment.get(variable, DEFAULT_TRIGGER_PRICE_TYPE).strip().upper()
    if value not in TRIGGER_PRICE_TYPES:
        raise ConfigurationError(f"{variable} must be one of {', '.join(TRIGGER_PRICE_TYPES)}, got {value!r}.")
    return value


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


def _read_positive_integer(environment: Mapping[str, str], variable: str, default: int) -> int:
    raw_value = environment.get(variable, str(default)).strip()
    try:
        number = int(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{variable} must be a whole number, got {raw_value!r}.") from error
    if number <= 0:
        raise ConfigurationError(f"{variable} must be greater than 0, got {number}.")
    return number


def _read_optional_percent(environment: Mapping[str, str], variable: str) -> Decimal | None:
    """An optional percentage greater than 0; unset or blank means "off"."""
    raw_value = environment.get(variable, "").strip()
    if not raw_value:
        return None
    try:
        percent = Decimal(raw_value)
    except InvalidOperation as error:
        raise ConfigurationError(f"{variable} must be a number, got {raw_value!r}.") from error
    if not percent.is_finite() or percent <= 0:
        raise ConfigurationError(f"{variable} must be greater than 0, got {raw_value!r}.")
    return percent
