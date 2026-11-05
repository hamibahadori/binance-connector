"""HTTP client for the Binance USD-M Futures REST API.

Only the signed endpoints this service needs are implemented:

* ``POST /fapi/v1/leverage``     change a symbol's initial leverage
* ``POST /fapi/v1/order``        place a MARKET order
* ``GET  /fapi/v1/order``        look up an order (used for its average fill price)
* ``GET  /fapi/v3/positionRisk`` read the current position for a symbol
* ``POST /fapi/v1/algoOrder``    place a take-profit / stop-loss (conditional algo order)
* ``DELETE /fapi/v1/algoOrder``  cancel one algo order
* ``GET  /fapi/v1/algoOrder``    look up one algo order (status, fill price once triggered)
* ``GET  /fapi/v1/openAlgoOrders`` list a symbol's untriggered algo orders

Algo-order behaviour verified on Demo (2026-11-05) is described in
``tests/fixtures/demo_capture_2026_11_05.json`` and the docstrings below.

Every parameter, including ``signature``, goes in the query string. Binance
allows this for POST, and building the URL ourselves keeps the parameter order
we signed identical to the order we send.

The client returns Binance's JSON unchanged. Interpreting it is the service
layer's job.
"""

import logging
import time
import urllib.parse
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Literal

import requests

from binance_connector.binance.signing import sign_query
from binance_connector.config import Settings
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
)

OrderSide = Literal["BUY", "SELL"]

API_KEY_HEADER = "X-MBX-APIKEY"

LISTEN_KEY_PATH = "/fapi/v1/listenKey"

logger = logging.getLogger(__name__)


def current_time_ms() -> int:
    """Current Unix time in milliseconds, which is what Binance's ``timestamp`` expects."""
    return time.time_ns() // 1_000_000


def format_decimal(value: Decimal) -> str:
    """Render a Decimal in plain notation (``0.001``, never ``1E-3``)."""
    return format(value, "f")


class BinanceFuturesClient:
    """Signs and sends requests to the Binance USD-M Futures REST API.

    Args:
        settings: credentials, base URL, recvWindow and timeout.
        session: optional ``requests.Session``, for connection reuse or tests.
        clock: returns the current time in ms. Injectable so tests get stable signatures.
    """

    def __init__(
        self,
        settings: Settings,
        session: requests.Session | None = None,
        clock: Callable[[], int] = current_time_ms,
    ) -> None:
        self._settings = settings
        self._session = session or requests.Session()
        self._session.headers[API_KEY_HEADER] = settings.api_key
        self._clock = clock

    def change_leverage(self, *, symbol: str, leverage: int) -> dict[str, Any]:
        """Set the initial leverage for ``symbol``; applies to positions opened afterwards."""
        return self._send_signed(
            "POST",
            "/fapi/v1/leverage",
            [("symbol", symbol), ("leverage", str(leverage))],
        )

    def place_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        client_order_id: str,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        """Place a MARKET order and return Binance's order response.

        ``newOrderRespType=RESULT`` makes Binance reply with the final ``status`` and
        ``executedQty`` instead of an acknowledgement. The reply does **not** include
        ``avgPrice`` (verified on Demo, 2026-10-16); use ``get_order`` for that.
        ``reduce_only`` stops the order from ever increasing or flipping a position.
        """
        parameters = [
            ("symbol", symbol),
            ("side", side),
            ("type", "MARKET"),
            ("quantity", format_decimal(quantity)),
        ]
        if reduce_only:
            parameters.append(("reduceOnly", "true"))
        parameters += [
            ("newClientOrderId", client_order_id),
            ("newOrderRespType", "RESULT"),
        ]
        return self._send_signed("POST", "/fapi/v1/order", parameters)

    def get_order(self, *, symbol: str, client_order_id: str) -> dict[str, Any]:
        """Return Binance's record of an order, including ``avgPrice``, found by our client order id."""
        return self._send_signed(
            "GET",
            "/fapi/v1/order",
            [("symbol", symbol), ("origClientOrderId", client_order_id)],
        )

    def get_position_risk(self, *, symbol: str) -> list[dict[str, Any]]:
        """Return the position entries for ``symbol`` (empty when there is nothing open)."""
        return self._send_signed("GET", "/fapi/v3/positionRisk", [("symbol", symbol)])

    def place_conditional_close(
        self,
        *,
        symbol: str,
        side: OrderSide,
        order_type: Literal["TAKE_PROFIT_MARKET", "STOP_MARKET"],
        trigger_price: Decimal,
        client_algo_id: str,
        working_type: Literal["MARK_PRICE", "CONTRACT_PRICE"],
    ) -> dict[str, Any]:
        """Place a take-profit or stop-loss that closes the whole position when triggered.

        ``closePosition=true`` (no quantity) makes it close whatever is open at trigger time.
        Binance expires such orders by itself once the position is flat, and rejects a
        trigger price that is already crossed with ``-2021``.
        """
        return self._send_signed(
            "POST",
            "/fapi/v1/algoOrder",
            [
                ("algoType", "CONDITIONAL"),
                ("symbol", symbol),
                ("side", side),
                ("type", order_type),
                ("triggerPrice", format_decimal(trigger_price)),
                ("closePosition", "true"),
                ("workingType", working_type),
                ("clientAlgoId", client_algo_id),
            ],
        )

    def cancel_algo_order(self, *, client_algo_id: str) -> dict[str, Any]:
        """Cancel an algo order. ``-2011 Unknown order sent`` means it is no longer open."""
        return self._send_signed("DELETE", "/fapi/v1/algoOrder", [("clientAlgoId", client_algo_id)])

    def get_algo_order(self, *, client_algo_id: str) -> dict[str, Any]:
        """Look up an algo order. Once triggered it reports ``actualOrderId``/``actualPrice``/``actualQty``."""
        return self._send_signed("GET", "/fapi/v1/algoOrder", [("clientAlgoId", client_algo_id)])

    def get_open_algo_orders(self, *, symbol: str) -> list[dict[str, Any]]:
        """The symbol's algo orders that have not triggered, been cancelled or expired."""
        return self._send_signed("GET", "/fapi/v1/openAlgoOrders", [("symbol", symbol)])

    # --- user-data stream (API key only, no signature) ------------------------------------------

    def start_user_stream(self) -> str:
        """Create (or extend) the account's listenKey for the user-data stream; valid 60 minutes."""
        return self._send("POST", LISTEN_KEY_PATH, f"{self._settings.base_url}{LISTEN_KEY_PATH}")["listenKey"]

    def keepalive_user_stream(self) -> None:
        """Extend the listenKey by another 60 minutes."""
        self._send("PUT", LISTEN_KEY_PATH, f"{self._settings.base_url}{LISTEN_KEY_PATH}")

    def close_user_stream(self) -> None:
        """Invalidate the listenKey."""
        self._send("DELETE", LISTEN_KEY_PATH, f"{self._settings.base_url}{LISTEN_KEY_PATH}")

    def _send_signed(self, method: str, path: str, parameters: list[tuple[str, str]]) -> Any:
        """Add timestamp and recvWindow, sign, send, and turn failures into typed errors."""
        signed_parameters = [
            *parameters,
            ("recvWindow", str(self._settings.recv_window_ms)),
            ("timestamp", str(self._clock())),
        ]
        query_string = urllib.parse.urlencode(signed_parameters)
        signature = sign_query(query_string, self._settings.api_secret)
        url = f"{self._settings.base_url}{path}?{query_string}&signature={signature}"
        return self._send(method, path, url)

    def _send(self, method: str, path: str, url: str) -> Any:
        """Send one request and turn failures into typed errors (the API key header is always set)."""
        try:
            response = self._session.request(method, url, timeout=self._settings.timeout_seconds)
        except requests.exceptions.ConnectTimeout as error:
            # The connection was never established, so Binance never saw the request.
            raise BinanceUnavailableError(f"Could not connect to Binance for {method} {path}.") from error
        except requests.exceptions.RequestException as error:
            # Anything else (read timeout, reset, DNS and so on) may have happened after
            # Binance received the request. Treat the outcome as unknown, never as a failure.
            raise BinanceUnknownOutcomeError(
                f"No reliable reply from Binance for {method} {path}: {error.__class__.__name__}."
            ) from error

        logger.info("Binance %s %s -> HTTP %s", method, path, response.status_code)

        if response.status_code >= 500:
            raise BinanceUnknownOutcomeError(
                f"Binance returned HTTP {response.status_code} for {method} {path}; "
                "the execution status is unknown."
            )
        if response.status_code >= 400:
            raise _api_error_from(response)
        return response.json()


def _api_error_from(response: requests.Response) -> BinanceAPIError:
    """Build a BinanceAPIError from a 4xx reply, tolerating non-JSON bodies."""
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and "msg" in body:
        return BinanceAPIError(response.status_code, body.get("code"), str(body["msg"]))
    return BinanceAPIError(response.status_code, None, response.text or response.reason or "")
