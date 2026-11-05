"""Access to the real Binance Demo replies and events recorded on 2026-11-05.

Tests take Binance's reply shapes from here rather than writing them by hand, because
the documented shapes and the real ones have differed before (``avgPrice``).
"""

import copy
import json
from functools import cache
from pathlib import Path
from typing import Any

CAPTURE_PATH = Path(__file__).parent / "fixtures" / "demo_capture_2026_11_05.json"


@cache
def _capture() -> dict[str, Any]:
    return json.loads(CAPTURE_PATH.read_text(encoding="utf-8"))


def rest_reply(label: str) -> Any:
    """A recorded REST reply (or error) by its spike label, as a fresh copy."""
    entry = next(entry for entry in _capture()["rest"] if entry["label"] == label)
    return copy.deepcopy(entry.get("reply", entry.get("error")))


def stream_events(event_type: str | None = None) -> list[dict[str, Any]]:
    """Recorded user-data events in arrival order (one stream path only, so no duplicates)."""
    events = [entry["message"] for entry in _capture()["events"] if entry["path"] == "/ws/<key>"]
    return copy.deepcopy([event for event in events if event_type is None or event["e"] == event_type])
