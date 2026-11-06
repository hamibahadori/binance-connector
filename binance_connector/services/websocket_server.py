"""The live-feed WebSocket server (spec §7), run by the worker on its own thread.

Clients connect to ``ws://WEBSOCKET_HOST:WEBSOCKET_PORT/ws``, optionally with
``?account_id=<id>`` to follow one account. If ``WEBSOCKET_TOKEN`` is set, the handshake
must carry it as ``Authorization: Bearer <token>`` or ``?token=<token>`` (browsers cannot
set headers on a WebSocket); otherwise it is refused with HTTP 401. Any other path is 404.

What a client receives is described in ``services.live_feed``. In short: a ``snapshot``
first, then ticks, updates and the service status. The only message a client may send is
``{"type": "resync"}``, which is answered with a fresh snapshot; anything else is ignored.

**Slow clients:** each client has a bounded queue (``QUEUE_SIZE`` messages). A client that
falls that far behind is closed with code 1013 ("try again later"): it reconnects and
starts again from a snapshot, rather than the worker buffering without limit or the
client silently missing updates.

**Consistency:** a client is subscribed *before* its snapshot is read, so no update falls
between the two. Updates the snapshot already reflects (``report_sequence`` up to the
snapshot's) are skipped.

The server uses the ``websockets`` library's asyncio implementation, on an event loop of
its own; the hub delivers from other threads through ``call_soon_threadsafe``. Pings are
sent every 20 s, so dead connections are noticed and dropped.
"""

import asyncio
import hmac
import json
import logging
import threading
import time
from collections.abc import Callable
from http import HTTPStatus
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

logger = logging.getLogger(__name__)

PATH = "/ws"
QUEUE_SIZE = 1000
"""Messages a client may fall behind by before it is closed (a few minutes of ticks)."""
OVERFLOW_CLOSE_CODE = 1013
OVERFLOW_CLOSE_REASON = "too far behind; reconnect for a fresh snapshot"
MAX_CLIENT_MESSAGE_BYTES = 64 * 1024
STOP_CHECK_SECONDS = 0.2
BIND_RETRY_SECONDS = 5.0
_RESYNC = object()
"""Queued in place of a message: send a fresh snapshot."""


class Hub(Protocol):
    def subscribe(self, subscriber) -> None: ...

    def unsubscribe(self, subscriber) -> None: ...

    def snapshot(self, account_id: str | None = None) -> dict[str, Any]: ...


class WebSocketClient:
    """One connected client: the hub's ``Subscriber``, with a bounded queue on the server's loop."""

    def __init__(self, connection, *, account_id: str | None, loop: asyncio.AbstractEventLoop,
                 queue_size: int = QUEUE_SIZE) -> None:
        self.account_id = account_id
        self.connection = connection
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._loop = loop
        self._closing = False
        self._close_task: asyncio.Task | None = None

    def deliver(self, message: dict[str, Any]) -> None:
        """Called by the hub from any thread."""
        try:
            self._loop.call_soon_threadsafe(self.offer, message)
        except RuntimeError:
            pass  # the loop is closed: the server has stopped

    def offer(self, message: Any) -> None:
        """Queue a message; runs on the server's loop."""
        if self._closing:
            return
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            self._closing = True
            logger.warning("Live feed: a client fell %s messages behind; closing it so it resyncs",
                           self.queue.maxsize)
            self._close_task = self._loop.create_task(
                self.connection.close(OVERFLOW_CLOSE_CODE, OVERFLOW_CLOSE_REASON))


class LiveFeedServer:
    def __init__(
        self,
        *,
        hub: Hub,
        host: str,
        port: int,
        token: str | None = None,
        queue_size: int = QUEUE_SIZE,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._hub = hub
        self._host = host
        self._requested_port = port
        self._token = token
        self._queue_size = queue_size
        self._sleep = sleep
        self._ready = threading.Event()
        self.port: int | None = None
        """The port actually listened on (differs from the setting when that is 0)."""

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Serve until ``should_stop()``. A port that cannot be bound is retried every few seconds."""
        while not should_stop():
            try:
                asyncio.run(self._serve(should_stop))
                return
            except OSError as error:
                logger.error("Live feed: cannot listen on %s:%s (%s); retrying in %s s",
                             self._host, self._requested_port, error, BIND_RETRY_SECONDS)
                self._sleep(BIND_RETRY_SECONDS)

    def wait_until_ready(self, timeout_seconds: float) -> int:
        """Block until the server listens; return its port."""
        if not self._ready.wait(timeout_seconds) or self.port is None:
            raise TimeoutError("the live-feed server did not start")
        return self.port

    async def _serve(self, should_stop: Callable[[], bool]) -> None:
        async with serve(self._handle, self._host, self._requested_port, process_request=self._check_request,
                         max_size=MAX_CLIENT_MESSAGE_BYTES) as server:
            self.port = next(iter(server.sockets)).getsockname()[1]
            self._ready.set()
            logger.info("Live feed listening on ws://%s:%s%s", self._host, self.port, PATH)
            while not should_stop():
                await asyncio.sleep(STOP_CHECK_SECONDS)
        logger.info("Live feed stopped")

    # --- handshake ----------------------------------------------------------------------------

    def _check_request(self, connection: ServerConnection, request: Request) -> Response | None:
        address = urlsplit(request.path)
        if address.path != PATH:
            return connection.respond(HTTPStatus.NOT_FOUND, "Not found\n")
        if self._token is not None and not self._authorised(request, parse_qs(address.query)):
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        return None

    def _authorised(self, request: Request, query: dict[str, list[str]]) -> bool:
        header = request.headers.get("Authorization", "")
        offered = header.removeprefix("Bearer ").strip() if header.startswith("Bearer ") else None
        if offered is None:
            offered = (query.get("token") or [""])[0]
        return hmac.compare_digest(offered.encode(), self._token.encode())

    # --- one client ---------------------------------------------------------------------------

    async def _handle(self, connection: ServerConnection) -> None:
        query = parse_qs(urlsplit(connection.request.path).query)
        account_id = (query.get("account_id") or [None])[0]
        client = WebSocketClient(connection, account_id=account_id, loop=asyncio.get_running_loop(),
                                 queue_size=self._queue_size)
        self._hub.subscribe(client)  # before the snapshot, so no update falls in between
        try:
            tasks = {asyncio.create_task(self._write(client)), asyncio.create_task(self._read(client))}
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                error = task.exception()
                if error is not None and not isinstance(error, ConnectionClosed):
                    logger.error("Live feed: a client connection failed", exc_info=error)
        finally:
            self._hub.unsubscribe(client)

    async def _write(self, client: WebSocketClient) -> None:
        snapshot_sequence = await self._send_snapshot(client)
        while True:
            message = await client.queue.get()
            if message is _RESYNC:
                snapshot_sequence = await self._send_snapshot(client)
                continue
            report_sequence = message.get("report_sequence")
            if report_sequence is not None and report_sequence <= snapshot_sequence:
                continue  # the snapshot already reflects it
            await client.connection.send(json.dumps(message))

    async def _send_snapshot(self, client: WebSocketClient) -> int:
        snapshot = await asyncio.to_thread(self._hub.snapshot, client.account_id)  # database reads
        await client.connection.send(json.dumps(snapshot))
        return snapshot["data"].get("report_sequence", 0)

    async def _read(self, client: WebSocketClient) -> None:
        async for raw_message in client.connection:
            try:
                request = json.loads(raw_message)
            except (TypeError, ValueError):
                continue
            if isinstance(request, dict) and request.get("type") == "resync":
                client.offer(_RESYNC)
