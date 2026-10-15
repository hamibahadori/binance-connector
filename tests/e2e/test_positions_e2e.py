"""End-to-end tests: real HTTP to the running service, real orders on Binance Futures Demo.

Run with::

    .venv\\Scripts\\python -m pytest -m e2e

These tests open and close small positions on the Demo account and change the
test symbol's leverage. Demo funds are virtual, but the orders are real orders
on Binance's Demo exchange.
"""

import uuid
from decimal import Decimal

import pytest
import requests

from tests.e2e.conftest import read_position_amount, wait_for_position_amount

pytestmark = pytest.mark.e2e

# One open can make three Binance calls (leverage, order, price lookup), each up to
# BINANCE_TIMEOUT_SECONDS, so the caller must wait longer than any single call.
REQUEST_TIMEOUT_SECONDS = 120


def unique_client_order_id(label: str) -> str:
    """A fresh id per order, so reruns never collide in Binance's order history."""
    return f"lx-e2e-{label}-{uuid.uuid4().hex[:12]}"


def call_service(service_url: str, path: str, body: dict | None = None) -> requests.Response:
    if body is None:
        return requests.get(f"{service_url}{path}", timeout=REQUEST_TIMEOUT_SECONDS)
    return requests.post(f"{service_url}{path}", json=body, timeout=REQUEST_TIMEOUT_SECONDS)


def test_health_over_real_http(service_url):
    response = call_service(service_url, "/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_open_long_then_close_it(service_url, binance_client, flat_symbol, e2e_quantity):
    open_order_id = unique_client_order_id("open-long")

    open_response = call_service(
        service_url,
        "/api/v1/positions/open",
        {
            "symbol": flat_symbol,
            "direction": "LONG",
            "quantity": str(e2e_quantity),
            "client_order_id": open_order_id,
        },
    )

    assert open_response.status_code == 200, open_response.text
    open_body = open_response.json()
    assert open_body["status"] == "FILLED"
    assert open_body["side"] == "BUY"
    assert open_body["client_order_id"] == open_order_id
    assert Decimal(open_body["executed_quantity"]) == e2e_quantity
    assert Decimal(open_body["average_price"]) > 0
    assert wait_for_position_amount(binance_client, flat_symbol, e2e_quantity) == e2e_quantity

    close_response = call_service(service_url, "/api/v1/positions/close", {"symbol": flat_symbol})

    assert close_response.status_code == 200, close_response.text
    close_body = close_response.json()
    assert close_body["status"] == "FILLED"
    assert close_body["direction"] == "LONG"
    assert close_body["side"] == "SELL"
    assert Decimal(close_body["executed_quantity"]) == e2e_quantity
    assert wait_for_position_amount(binance_client, flat_symbol, Decimal(0)) == 0


def test_open_short_with_leverage_then_close_in_two_steps(
    service_url, binance_client, flat_symbol, e2e_quantity
):
    position_quantity = e2e_quantity * 2

    open_response = call_service(
        service_url,
        "/api/v1/positions/open",
        {
            "symbol": flat_symbol,
            "direction": "SHORT",
            "quantity": str(position_quantity),
            "leverage": 5,
            "client_order_id": unique_client_order_id("open-short"),
        },
    )

    assert open_response.status_code == 200, open_response.text
    assert open_response.json()["leverage"] == 5
    assert open_response.json()["side"] == "SELL"
    assert wait_for_position_amount(binance_client, flat_symbol, -position_quantity) == -position_quantity

    # Asking to close more than is open is refused before any order is sent.
    oversized_close = call_service(
        service_url,
        "/api/v1/positions/close",
        {"symbol": flat_symbol, "quantity": str(position_quantity * 2)},
    )
    assert oversized_close.status_code == 409, oversized_close.text
    assert read_position_amount(binance_client, flat_symbol) == -position_quantity

    partial_close = call_service(
        service_url,
        "/api/v1/positions/close",
        {"symbol": flat_symbol, "quantity": str(e2e_quantity)},
    )
    assert partial_close.status_code == 200, partial_close.text
    assert partial_close.json()["direction"] == "SHORT"
    assert partial_close.json()["side"] == "BUY"
    assert wait_for_position_amount(binance_client, flat_symbol, -e2e_quantity) == -e2e_quantity

    final_close = call_service(service_url, "/api/v1/positions/close", {"symbol": flat_symbol})
    assert final_close.status_code == 200, final_close.text
    assert Decimal(final_close.json()["executed_quantity"]) == e2e_quantity
    assert wait_for_position_amount(binance_client, flat_symbol, Decimal(0)) == 0


def test_close_with_no_open_position_returns_404(service_url, flat_symbol):
    response = call_service(service_url, "/api/v1/positions/close", {"symbol": flat_symbol})

    assert response.status_code == 404, response.text
    assert response.json()["error"] == "no_open_position"


def test_unknown_symbol_is_rejected_by_binance_with_422(service_url):
    response = call_service(
        service_url,
        "/api/v1/positions/open",
        {"symbol": "NOTAREALSYMBOLUSDT", "direction": "LONG", "quantity": "1"},
    )

    assert response.status_code == 422, response.text
    response_body = response.json()
    assert response_body["error"] == "binance_rejected"
    assert response_body["binance"]["code"] is not None
