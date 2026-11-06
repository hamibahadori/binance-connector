"""Container health checks: ``python -m binance_connector.healthcheck api|worker``.

Exit code 0 means healthy, 1 unhealthy (what Docker's ``HEALTHCHECK`` expects).

* ``api``: ``GET http://127.0.0.1:$API_PORT/health`` answers 200.
* ``worker``: the live-feed WebSocket on ``127.0.0.1:$WEBSOCKET_PORT`` completes a handshake.
  A 401 (``WEBSOCKET_TOKEN`` is set) also counts: the server is up and answering, and the
  check does not need to know the token.

Only the port variables are read, so a check never needs the service's secrets.
"""

import os
import sys
import urllib.error
import urllib.request

from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect

from binance_connector.config import DEFAULT_API_PORT, DEFAULT_WEBSOCKET_PORT

TIMEOUT_SECONDS = 3


def api_is_healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 - local URL
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def live_feed_is_healthy(url: str) -> bool:
    try:
        with connect(url, open_timeout=TIMEOUT_SECONDS):
            return True
    except InvalidStatus as refusal:
        return refusal.response.status_code == 401
    except Exception:  # noqa: BLE001 - refused, timed out, or any other handshake failure
        return False


def main(arguments: list[str]) -> int:
    target = arguments[0] if arguments else ""
    if target == "api":
        port = os.environ.get("API_PORT") or DEFAULT_API_PORT
        return 0 if api_is_healthy(f"http://127.0.0.1:{port}/health") else 1
    if target == "worker":
        port = os.environ.get("WEBSOCKET_PORT") or DEFAULT_WEBSOCKET_PORT
        return 0 if live_feed_is_healthy(f"ws://127.0.0.1:{port}/ws") else 1
    print("usage: python -m binance_connector.healthcheck api|worker", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
