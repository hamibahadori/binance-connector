"""Functional tests: HTTP request in, Binance calls out, HTTP response back.

Everything runs for real except Binance itself, which is replaced by the
``binance`` fixture (``responses``).
"""

import urllib.parse

import pytest
import requests

from binance_connector.config import DEMO_BASE_URL

OPEN_URL = "/api/v1/positions/open"
CLOSE_URL = "/api/v1/positions/close"

BINANCE_ORDER_URL = f"{DEMO_BASE_URL}/fapi/v1/order"
BINANCE_LEVERAGE_URL = f"{DEMO_BASE_URL}/fapi/v1/leverage"
BINANCE_POSITION_RISK_URL = f"{DEMO_BASE_URL}/fapi/v3/positionRisk"


def binance_order_reply(side: str, quantity: str, client_order_id: str) -> dict:
    """A FILLED reply in the shape Binance Demo really sends: note there is no avgPrice."""
    return {
        "orderId": 4000000001,
        "symbol": "BTCUSDT",
        "status": "FILLED",
        "clientOrderId": client_order_id,
        "side": side,
        "origQty": quantity,
        "executedQty": quantity,
        "cumQty": quantity,
    }


def register_order_lookup(binance, average_price: str = "65000.10") -> None:
    """Mock the follow-up GET /fapi/v1/order the service makes to learn the fill price."""
    binance.get(BINANCE_ORDER_URL, json={"symbol": "BTCUSDT", "status": "FILLED", "avgPrice": average_price})


def sent_parameters(mocked_call) -> dict[str, str]:
    """The query parameters of a request the service sent to Binance."""
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(mocked_call.request.url).query))


# --- health -----------------------------------------------------------------


def test_health_reports_ok_without_calling_binance(http_client, binance):
    response = http_client.get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
    assert len(binance.calls) == 0


# --- open: success ----------------------------------------------------------


def test_open_long_places_a_buy_market_order(http_client, binance):
    binance.post(BINANCE_ORDER_URL, json=binance_order_reply("BUY", "0.002", "open-1"))
    register_order_lookup(binance)

    response = http_client.post(
        OPEN_URL,
        json={"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "client_order_id": "open-1"},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "side": "BUY",
        "order_id": 4000000001,
        "client_order_id": "open-1",
        "status": "FILLED",
        "requested_quantity": "0.002",
        "executed_quantity": "0.002",
        "average_price": "65000.10",
        "leverage": None,
    }
    order_parameters = sent_parameters(binance.calls[0])
    assert order_parameters["side"] == "BUY"
    assert order_parameters["type"] == "MARKET"
    assert order_parameters["quantity"] == "0.002"
    assert "reduceOnly" not in order_parameters


def test_open_short_with_leverage_sets_leverage_then_sells(http_client, binance):
    binance.post(BINANCE_LEVERAGE_URL, json={"symbol": "BTCUSDT", "leverage": 5})
    binance.post(BINANCE_ORDER_URL, json=binance_order_reply("SELL", "0.002", "open-2"))
    register_order_lookup(binance)

    response = http_client.post(
        OPEN_URL,
        json={
            "symbol": "btcusdt",
            "direction": "short",
            "quantity": 0.002,
            "leverage": 5,
            "client_order_id": "open-2",
        },
    )

    assert response.status_code == 200
    assert response.get_json()["leverage"] == 5
    assert response.get_json()["direction"] == "SHORT"
    assert [
        (mocked_call.request.method, mocked_call.request.url.split("?")[0]) for mocked_call in binance.calls
    ] == [
        ("POST", BINANCE_LEVERAGE_URL),
        ("POST", BINANCE_ORDER_URL),
        ("GET", BINANCE_ORDER_URL),
    ]
    assert sent_parameters(binance.calls[0])["leverage"] == "5"
    assert sent_parameters(binance.calls[1])["symbol"] == "BTCUSDT"
    assert sent_parameters(binance.calls[1])["quantity"] == "0.002"


def test_open_generates_a_client_order_id_when_none_is_given(http_client, binance):
    binance.post(BINANCE_ORDER_URL, json=binance_order_reply("BUY", "0.002", "echoed"))
    register_order_lookup(binance)

    http_client.post(OPEN_URL, json={"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002"})

    assert sent_parameters(binance.calls[0])["newClientOrderId"].startswith("lx-")


def test_open_succeeds_with_null_average_price_when_the_lookup_fails(http_client, binance):
    binance.post(BINANCE_ORDER_URL, json=binance_order_reply("BUY", "0.002", "open-3"))
    binance.get(BINANCE_ORDER_URL, status=503, json={"code": -1000, "msg": "Unknown error"})

    response = http_client.post(
        OPEN_URL,
        json={"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "client_order_id": "open-3"},
    )

    assert response.status_code == 200
    assert response.get_json()["status"] == "FILLED"
    assert response.get_json()["average_price"] is None


# --- open: bad requests -----------------------------------------------------


@pytest.mark.parametrize(
    ("body", "bad_field"),
    [
        ({"direction": "LONG", "quantity": "0.002"}, "symbol"),
        ({"symbol": "BTCUSDT", "direction": "UP", "quantity": "0.002"}, "direction"),
        ({"symbol": "BTCUSDT", "direction": "LONG", "quantity": "-1"}, "quantity"),
        ({"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "leverage": 200}, "leverage"),
        ({"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "price": "1"}, "price"),
    ],
)
def test_open_with_invalid_body_returns_400_and_calls_nothing(http_client, binance, body, bad_field):
    response = http_client.post(OPEN_URL, json=body)

    assert response.status_code == 400
    response_body = response.get_json()
    assert response_body["error"] == "validation_error"
    assert bad_field in [detail["field"] for detail in response_body["details"]]
    assert len(binance.calls) == 0


@pytest.mark.parametrize(
    "request_arguments",
    [
        {"data": "not json", "content_type": "application/json"},
        {"data": "symbol=BTCUSDT", "content_type": "application/x-www-form-urlencoded"},
        {"json": ["BTCUSDT", "LONG", "0.002"]},
    ],
    ids=["malformed-json", "form-encoded", "json-array"],
)
def test_open_with_a_body_that_is_not_a_json_object_returns_400(http_client, binance, request_arguments):
    response = http_client.post(OPEN_URL, **request_arguments)

    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_request_body"


# --- open: Binance failures -------------------------------------------------


def test_binance_rejection_returns_422_with_binance_code(http_client, binance):
    binance.post(BINANCE_ORDER_URL, status=400, json={"code": -2019, "msg": "Margin is insufficient."})

    response = http_client.post(OPEN_URL, json={"symbol": "BTCUSDT", "direction": "LONG", "quantity": "100"})

    assert response.status_code == 422
    assert response.get_json() == {
        "error": "binance_rejected",
        "message": "Binance rejected the request.",
        "binance": {"http_status": 400, "code": -2019, "message": "Margin is insufficient."},
    }


def test_binance_server_error_returns_504_with_unknown_status(http_client, binance):
    binance.post(BINANCE_ORDER_URL, status=503, json={"code": -1000, "msg": "Unknown error"})

    response = http_client.post(
        OPEN_URL,
        json={"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "client_order_id": "maybe-1"},
    )

    assert response.status_code == 504
    response_body = response.get_json()
    assert response_body["error"] == "binance_outcome_unknown"
    assert response_body["execution_status"] == "unknown"
    assert response_body["client_order_id"] == "maybe-1"


def test_binance_unreachable_returns_502(http_client, binance):
    binance.post(BINANCE_ORDER_URL, body=requests.exceptions.ConnectTimeout())

    response = http_client.post(OPEN_URL, json={"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002"})

    assert response.status_code == 502
    assert response.get_json()["error"] == "binance_unavailable"


# --- close ------------------------------------------------------------------


def test_close_long_sells_the_whole_position_reduce_only(http_client, binance):
    binance.get(
        BINANCE_POSITION_RISK_URL,
        json=[{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.005"}],
    )
    binance.post(BINANCE_ORDER_URL, json=binance_order_reply("SELL", "0.005", "close-1"))
    register_order_lookup(binance)

    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT", "client_order_id": "close-1"})

    assert response.status_code == 200
    response_body = response.get_json()
    assert response_body["direction"] == "LONG"
    assert response_body["side"] == "SELL"
    assert response_body["executed_quantity"] == "0.005"
    order_parameters = sent_parameters(binance.calls[1])
    assert order_parameters["side"] == "SELL"
    assert order_parameters["quantity"] == "0.005"
    assert order_parameters["reduceOnly"] == "true"


def test_partial_close_of_a_short_buys_the_requested_quantity(http_client, binance):
    binance.get(
        BINANCE_POSITION_RISK_URL,
        json=[{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "-0.005"}],
    )
    binance.post(BINANCE_ORDER_URL, json=binance_order_reply("BUY", "0.002", "close-2"))
    register_order_lookup(binance)

    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT", "quantity": "0.002"})

    assert response.status_code == 200
    assert response.get_json()["direction"] == "SHORT"
    assert sent_parameters(binance.calls[1])["side"] == "BUY"
    assert sent_parameters(binance.calls[1])["quantity"] == "0.002"


def test_close_without_an_open_position_returns_404(http_client, binance):
    binance.get(BINANCE_POSITION_RISK_URL, json=[])

    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT"})

    assert response.status_code == 404
    assert response.get_json()["error"] == "no_open_position"


def test_close_more_than_is_open_returns_409(http_client, binance):
    binance.get(
        BINANCE_POSITION_RISK_URL,
        json=[{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.001"}],
    )

    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT", "quantity": "0.002"})

    assert response.status_code == 409
    response_body = response.get_json()
    assert response_body["error"] == "close_quantity_exceeds_position"
    assert response_body["open_quantity"] == "0.001"


def test_close_on_a_hedge_mode_account_returns_409(http_client, binance):
    binance.get(
        BINANCE_POSITION_RISK_URL,
        json=[{"symbol": "BTCUSDT", "positionSide": "LONG", "positionAmt": "0.001"}],
    )

    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT"})

    assert response.status_code == 409
    assert response.get_json()["error"] == "hedge_mode_not_supported"


def test_close_with_invalid_body_returns_400(http_client, binance):
    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT", "quantity": "0"})

    assert response.status_code == 400
    assert response.get_json()["error"] == "validation_error"


# --- generic errors ---------------------------------------------------------


def test_unknown_route_returns_json_404(http_client):
    response = http_client.get("/api/v1/does-not-exist")

    assert response.status_code == 404
    assert response.get_json()["error"] == "not_found"


def test_wrong_method_returns_json_405(http_client):
    response = http_client.get(OPEN_URL)

    assert response.status_code == 405
    assert response.get_json()["error"] == "method_not_allowed"


def test_unexpected_failure_returns_json_500_without_internals(http_client, binance):
    # A reply missing positionAmt is something the code does not expect.
    binance.get(BINANCE_POSITION_RISK_URL, json=[{"symbol": "BTCUSDT", "positionSide": "BOTH"}])

    response = http_client.post(CLOSE_URL, json={"symbol": "BTCUSDT"})

    assert response.status_code == 500
    assert response.get_json() == {
        "error": "internal_error",
        "message": "An unexpected error occurred. See the service logs.",
    }
