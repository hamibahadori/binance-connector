"""Unit tests for the Binance USD-M Futures REST client.

HTTP is intercepted with ``responses``, so these tests check exactly what the
client puts on the wire and how it interprets each kind of reply, without any
network access.
"""

import urllib.parse
from decimal import Decimal

import pytest
import requests
import responses

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.binance.signing import sign_query
from binance_connector.config import DEMO_BASE_URL
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
)
from tests.conftest import TEST_API_KEY, TEST_API_SECRET, TEST_CREDENTIALS

FIXED_TIMESTAMP_MS = 1_700_000_000_000
ORDER_URL = f"{DEMO_BASE_URL}/fapi/v1/order"
LEVERAGE_URL = f"{DEMO_BASE_URL}/fapi/v1/leverage"
POSITION_RISK_URL = f"{DEMO_BASE_URL}/fapi/v3/positionRisk"

# Shape of a real Demo reply to POST /fapi/v1/order (captured 2026-10-16): no avgPrice.
ORDER_RESPONSE = {
    "orderId": 4000000001,
    "symbol": "BTCUSDT",
    "status": "FILLED",
    "clientOrderId": "order-123",
    "side": "BUY",
    "origQty": "0.0020",
    "executedQty": "0.0020",
    "cumQty": "0.0020",
}


@pytest.fixture
def client(settings) -> BinanceFuturesClient:
    """A client whose clock always returns the same timestamp."""
    return BinanceFuturesClient(settings, TEST_CREDENTIALS, clock=lambda: FIXED_TIMESTAMP_MS)


def split_signed_query(request_url: str) -> tuple[str, str]:
    """Return (everything before the signature, the signature) of a request URL."""
    query_string = urllib.parse.urlsplit(request_url).query
    unsigned_query, _, signature = query_string.rpartition("&signature=")
    return unsigned_query, signature


def query_parameters(request_url: str) -> dict[str, str]:
    """Return the request's query parameters as a flat dictionary."""
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request_url).query))


@responses.activate
def test_place_market_order_sends_a_correctly_signed_request(client):
    responses.post(ORDER_URL, json=ORDER_RESPONSE)

    result = client.place_market_order(
        symbol="BTCUSDT",
        side="BUY",
        quantity=Decimal("0.002"),
        client_order_id="order-123",
    )

    sent_request = responses.calls[0].request
    unsigned_query, signature = split_signed_query(sent_request.url)
    assert sent_request.method == "POST"
    assert sent_request.headers["X-MBX-APIKEY"] == TEST_API_KEY
    assert unsigned_query == (
        "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.002"
        "&newClientOrderId=order-123&newOrderRespType=RESULT"
        f"&recvWindow=5000&timestamp={FIXED_TIMESTAMP_MS}"
    )
    assert signature == sign_query(unsigned_query, TEST_API_SECRET)
    assert result == ORDER_RESPONSE


@responses.activate
def test_reduce_only_flag_is_sent_only_when_requested(client):
    responses.post(ORDER_URL, json=ORDER_RESPONSE)

    client.place_market_order(
        symbol="BTCUSDT", side="SELL", quantity=Decimal("0.002"), client_order_id="first"
    )
    client.place_market_order(
        symbol="BTCUSDT",
        side="SELL",
        quantity=Decimal("0.002"),
        client_order_id="second",
        reduce_only=True,
    )

    assert "reduceOnly" not in query_parameters(responses.calls[0].request.url)
    assert query_parameters(responses.calls[1].request.url)["reduceOnly"] == "true"


@responses.activate
def test_quantity_is_sent_in_plain_notation_never_scientific(client):
    responses.post(ORDER_URL, json=ORDER_RESPONSE)

    client.place_market_order(
        symbol="BTCUSDT", side="BUY", quantity=Decimal("1E-3"), client_order_id="order-123"
    )

    assert query_parameters(responses.calls[0].request.url)["quantity"] == "0.001"


@responses.activate
def test_change_leverage_sends_symbol_and_leverage(client):
    responses.post(LEVERAGE_URL, json={"symbol": "BTCUSDT", "leverage": 10})

    result = client.change_leverage(symbol="BTCUSDT", leverage=10)

    sent_request = responses.calls[0].request
    unsigned_query, signature = split_signed_query(sent_request.url)
    assert sent_request.method == "POST"
    assert unsigned_query == (
        f"symbol=BTCUSDT&leverage=10&recvWindow=5000&timestamp={FIXED_TIMESTAMP_MS}"
    )
    assert signature == sign_query(unsigned_query, TEST_API_SECRET)
    assert result == {"symbol": "BTCUSDT", "leverage": 10}


@responses.activate
def test_get_order_looks_up_by_client_order_id(client):
    order_details = {"symbol": "BTCUSDT", "clientOrderId": "order-123", "avgPrice": "82206.60"}
    responses.get(ORDER_URL, json=order_details)

    result = client.get_order(symbol="BTCUSDT", client_order_id="order-123")

    sent_request = responses.calls[0].request
    unsigned_query, signature = split_signed_query(sent_request.url)
    assert sent_request.method == "GET"
    assert unsigned_query == (
        "symbol=BTCUSDT&origClientOrderId=order-123"
        f"&recvWindow=5000&timestamp={FIXED_TIMESTAMP_MS}"
    )
    assert signature == sign_query(unsigned_query, TEST_API_SECRET)
    assert result == order_details


@responses.activate
def test_get_position_risk_returns_binance_list(client):
    position_entries = [{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.002"}]
    responses.get(POSITION_RISK_URL, json=position_entries)

    result = client.get_position_risk(symbol="BTCUSDT")

    sent_request = responses.calls[0].request
    assert sent_request.method == "GET"
    assert query_parameters(sent_request.url)["symbol"] == "BTCUSDT"
    assert result == position_entries


@responses.activate
def test_requests_use_the_configured_timeout(client, settings):
    responses.get(POSITION_RISK_URL, json=[])

    client.get_position_risk(symbol="BTCUSDT")

    assert responses.calls[0].request.req_kwargs["timeout"] == settings.timeout_seconds


@responses.activate
def test_binance_rejection_raises_api_error_with_binance_code(client):
    responses.post(ORDER_URL, status=400, json={"code": -2019, "msg": "Margin is insufficient."})

    with pytest.raises(BinanceAPIError) as raised:
        client.place_market_order(
            symbol="BTCUSDT", side="BUY", quantity=Decimal("1"), client_order_id="order-123"
        )

    assert raised.value.http_status == 400
    assert raised.value.code == -2019
    assert raised.value.message == "Margin is insufficient."


@responses.activate
def test_rejection_without_json_body_still_raises_api_error(client):
    responses.post(ORDER_URL, status=403, body="Forbidden")

    with pytest.raises(BinanceAPIError) as raised:
        client.place_market_order(
            symbol="BTCUSDT", side="BUY", quantity=Decimal("1"), client_order_id="order-123"
        )

    assert raised.value.http_status == 403
    assert raised.value.code is None
    assert raised.value.message == "Forbidden"


@pytest.mark.parametrize("server_status", [500, 502, 503])
@responses.activate
def test_binance_server_error_means_outcome_unknown(client, server_status):
    responses.post(ORDER_URL, status=server_status, json={"code": -1000, "msg": "Unknown error"})

    with pytest.raises(BinanceUnknownOutcomeError):
        client.place_market_order(
            symbol="BTCUSDT", side="BUY", quantity=Decimal("1"), client_order_id="order-123"
        )


@pytest.mark.parametrize(
    "network_error",
    [requests.exceptions.ReadTimeout(), requests.exceptions.ConnectionError()],
)
@responses.activate
def test_network_failure_after_connecting_means_outcome_unknown(client, network_error):
    responses.post(ORDER_URL, body=network_error)

    with pytest.raises(BinanceUnknownOutcomeError):
        client.place_market_order(
            symbol="BTCUSDT", side="BUY", quantity=Decimal("1"), client_order_id="order-123"
        )


@responses.activate
def test_connect_timeout_means_binance_unavailable(client):
    responses.post(ORDER_URL, body=requests.exceptions.ConnectTimeout())

    with pytest.raises(BinanceUnavailableError):
        client.place_market_order(
            symbol="BTCUSDT", side="BUY", quantity=Decimal("1"), client_order_id="order-123"
        )
