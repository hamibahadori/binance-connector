"""The Binance user-data stream: one WebSocket per listenKey, kept alive and renewed.

Facts this relies on (Binance docs, and verified on Demo on 2026-11-05):

* ``POST /fapi/v1/listenKey`` gives a key valid for 60 minutes; ``PUT`` extends it. The
  loop sends the PUT every 30 minutes, well inside the window.
* A connection lives at most 24 hours. The loop reconnects at 23 hours on its own terms
  rather than being cut off.
* ``listenKeyExpired`` means no further events arrive on this key, so reconnect with a new one.
* On Demo, ``wss://demo-fstream.binance.com/ws/<key>`` and ``/private/ws/<key>`` both work.

Events can be missed while disconnected, so the caller's ``on_connected`` runs after
*every* (re)connect. The worker uses it to reconcile against Binance's REST state.

The loop never gives up: any failure (Binance unreachable, a dropped socket) leads to a
reconnect after an exponential backoff (1, 2, 4 … capped at 60 seconds). A callback that
raises is logged and does not end the loop either.
"""

import json
import logging
import time
from collections.abc import Callable
from typing import Any, Protocol

import websocket

logger = logging.getLogger(__name__)

KEEPALIVE_INTERVAL_SECONDS = 30 * 60
MAX_CONNECTION_AGE_SECONDS = 23 * 60 * 60
RECEIVE_TIMEOUT_SECONDS = 1.0
"""How long one receive waits before the loop gets a turn (keepalive, scheduled work)."""
CONNECT_TIMEOUT_SECONDS = 30.0
"""How long the WebSocket handshake may take. Separate from the receive timeout: a 1 s
handshake limit failed on a slow link (Demo smoke run, 2026-11-07)."""
MAX_BACKOFF_SECONDS = 60


class StreamConnection(Protocol):
    def receive(self) -> str | None:
        """The next message, or None if nothing arrived within the receive timeout."""

    def close(self) -> None: ...


class ListenKeyClient(Protocol):
    def start_user_stream(self) -> str: ...

    def keepalive_user_stream(self) -> None: ...


class WebSocketConnection:
    """``StreamConnection`` over websocket-client. Pings from Binance are answered automatically."""

    def __init__(
        self,
        url: str,
        connect_timeout_seconds: float = CONNECT_TIMEOUT_SECONDS,
        receive_timeout_seconds: float = RECEIVE_TIMEOUT_SECONDS,
    ) -> None:
        self._socket = websocket.create_connection(url, timeout=connect_timeout_seconds)
        self._socket.settimeout(receive_timeout_seconds)

    def receive(self) -> str | None:
        try:
            return self._socket.recv()
        except websocket.WebSocketTimeoutException:
            return None

    def close(self) -> None:
        self._socket.close()


class UserDataStream:
    """Delivers user-data events to callbacks until ``should_stop`` returns True.

    Args:
        client: creates and keeps alive the listenKey.
        stream_url: base WebSocket URL; the listenKey is appended as ``<stream_url>/<key>``.
        connect: opens a ``StreamConnection`` for a URL (injectable for tests).
        clock: monotonic seconds (injectable for tests).
        sleep: waits between reconnect attempts (injectable for tests).
    """

    def __init__(
        self,
        *,
        client: ListenKeyClient,
        stream_url: str,
        connect: Callable[[str], StreamConnection] = WebSocketConnection,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        keepalive_interval_seconds: float = KEEPALIVE_INTERVAL_SECONDS,
        max_connection_age_seconds: float = MAX_CONNECTION_AGE_SECONDS,
        max_backoff_seconds: float = MAX_BACKOFF_SECONDS,
    ) -> None:
        self._client = client
        self._stream_url = stream_url.rstrip("/")
        self._connect = connect
        self._clock = clock
        self._sleep = sleep
        self._keepalive_interval_seconds = keepalive_interval_seconds
        self._max_connection_age_seconds = max_connection_age_seconds
        self._max_backoff_seconds = max_backoff_seconds

    def run(
        self,
        *,
        on_event: Callable[[dict[str, Any]], None],
        on_connected: Callable[[], None],
        on_idle: Callable[[], None],
        should_stop: Callable[[], bool],
    ) -> None:
        """Connect, deliver events, reconnect on any failure; return once ``should_stop()``.

        ``on_idle`` runs after every receive attempt (about once per second when quiet),
        so periodic work can share this one thread.
        """
        consecutive_failures = 0
        while not should_stop():
            try:
                self._run_one_connection(on_event, on_connected, on_idle, should_stop)
                consecutive_failures = 0
            except Exception as error:  # noqa: BLE001 - any failure means "reconnect"
                consecutive_failures += 1
                delay_seconds = min(2 ** (consecutive_failures - 1), self._max_backoff_seconds)
                logger.warning("User-data stream failed (%s: %s); reconnecting in %s s",
                               type(error).__name__, error, delay_seconds)
                self._sleep(delay_seconds)

    def _run_one_connection(self, on_event, on_connected, on_idle, should_stop) -> None:
        listen_key = self._client.start_user_stream()
        connection = self._connect(f"{self._stream_url}/{listen_key}")
        logger.info("User-data stream connected")
        try:
            connected_at = last_keepalive_at = self._clock()
            _call_safely("on_connected", on_connected)
            while not should_stop():
                now = self._clock()
                if now - connected_at >= self._max_connection_age_seconds:
                    logger.info("User-data stream: renewing the connection before Binance's 24 h limit")
                    return
                if now - last_keepalive_at >= self._keepalive_interval_seconds:
                    self._client.keepalive_user_stream()
                    last_keepalive_at = now
                message = connection.receive()
                if message is not None and self._deliver(message, on_event) == "listen_key_expired":
                    logger.info("User-data stream: listenKey expired; reconnecting with a new one")
                    return
                _call_safely("on_idle", on_idle)
        finally:
            connection.close()

    def _deliver(self, message: str, on_event) -> str | None:
        try:
            event = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("User-data stream: skipped a message that is not JSON")
            return None
        if event.get("e") == "listenKeyExpired":
            return "listen_key_expired"
        _call_safely("on_event", on_event, event)
        return None


def _call_safely(name: str, callback: Callable, *arguments) -> None:
    try:
        callback(*arguments)
    except Exception:  # noqa: BLE001 - a callback bug must not stop the stream
        logger.exception("User-data stream: %s failed", name)
