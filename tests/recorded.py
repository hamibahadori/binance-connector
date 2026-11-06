"""Access to the real Binance Demo replies and events recorded on 2026-11-05.

Tests take Binance's reply shapes from here rather than writing them by hand, because
the documented shapes and the real ones have differed before (``avgPrice``).
"""

import copy
import json
from functools import cache
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).parent / "fixtures"
TP_SL_CAPTURE = "demo_capture_2026_11_05.json"
"""Spike 1: market entry, TP/SL algo orders, manual close, a triggered stop-loss."""
MARKET_AND_LIMIT_CAPTURE = "demo_capture_2026_11_07.json"
"""Spike 2: mark-price streams, exchange rules, resting and marketable limit orders."""


@cache
def _capture(file_name: str = TP_SL_CAPTURE) -> dict[str, Any]:
    return json.loads((FIXTURES / file_name).read_text(encoding="utf-8"))


def rest_reply(label: str, capture: str = TP_SL_CAPTURE) -> Any:
    """A recorded REST reply (or error) by its spike label, as a fresh copy."""
    entry = next(entry for entry in _capture(capture)["rest"] if entry["label"] == label)
    return copy.deepcopy(entry.get("reply", entry.get("error")))


def exchange_info_reply() -> dict[str, Any]:
    """An exchangeInfo-shaped reply holding the recorded BTCUSDT and ETHUSDT entries."""
    symbols = [rest_reply(f"exchangeInfo {symbol}", MARKET_AND_LIMIT_CAPTURE) for symbol in ("BTCUSDT", "ETHUSDT")]
    return {"symbols": symbols}


def mark_price_messages() -> list[dict[str, Any]]:
    """Recorded ``markPriceUpdate`` messages from the single-symbol stream."""
    return copy.deepcopy(_capture(MARKET_AND_LIMIT_CAPTURE)["market"]["ws single"]["messages"])


def limit_order_events() -> list[dict[str, Any]]:
    """User-data events recorded around the limit orders, in arrival order."""
    return copy.deepcopy(_capture(MARKET_AND_LIMIT_CAPTURE)["user_events"])


def stream_events(event_type: str | None = None) -> list[dict[str, Any]]:
    """Recorded user-data events in arrival order (one stream path only, so no duplicates)."""
    events = [entry["message"] for entry in _capture()["events"] if entry["path"] == "/ws/<key>"]
    return copy.deepcopy([event for event in events if event_type is None or event["e"] == event_type])
