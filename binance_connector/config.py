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
        )


def _read_required(environment: Mapping[str, str], variable: str) -> str:
    value = environment.get(variable, "").strip()
    if not value:
        raise ConfigurationError(f"{variable} is not set. Copy .env.example to .env and fill it in.")
    return value


def _read_recv_window(environment: Mapping[str, str]) -> int:
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


def _read_timeout(environment: Mapping[str, str]) -> float:
    variable = "BINANCE_TIMEOUT_SECONDS"
    raw_value = environment.get(variable, str(DEFAULT_TIMEOUT_SECONDS))
    try:
        timeout_seconds = float(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{variable} must be a number, got {raw_value!r}.") from error
    if timeout_seconds <= 0:
        raise ConfigurationError(f"{variable} must be greater than 0, got {timeout_seconds}.")
    return timeout_seconds
