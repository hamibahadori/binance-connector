"""Binance's public USD-M Futures endpoints: market data that needs no account keys.

* ``GET /fapi/v1/exchangeInfo``  trading rules for every symbol
* ``GET /fapi/v1/premiumIndex``  the current mark price of one symbol

Used to validate and size requests at intake, before any account credentials are involved.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import requests

from binance_connector.binance.client import send_request
from binance_connector.config import Settings


class BinancePublicClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self._settings = settings
        self._session = session or requests.Session()

    def get_exchange_info(self) -> dict[str, Any]:
        return self._get("/fapi/v1/exchangeInfo")

    def get_mark_price(self, symbol: str) -> tuple[Decimal, datetime]:
        """The symbol's current mark price and Binance's time for it."""
        reply = self._get(f"/fapi/v1/premiumIndex?symbol={symbol}")
        return Decimal(reply["markPrice"]), datetime.fromtimestamp(reply["time"] / 1000, tz=UTC)

    def _get(self, path_and_query: str) -> Any:
        path = path_and_query.split("?")[0]
        url = f"{self._settings.base_url}{path_and_query}"
        return send_request(self._session, "GET", path, url, self._settings.timeout_seconds)
