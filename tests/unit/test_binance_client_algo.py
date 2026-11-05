"""Unit tests for the client's algo-order (TP/SL) and order-lookup calls.

Replies come from the recorded Demo capture, so the tests also pin the shapes we rely on.
"""

import urllib.parse
from decimal import Decimal

import pytest
import responses

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.binance.signing import sign_query
from binance_connector.config import DEMO_BASE_URL
from binance_connector.errors import BinanceAPIError
from tests.conftest import TEST_API_SECRET
from tests.recorded import rest_reply

FIXED_TIMESTAMP_MS = 1_700_000_000_000
ALGO_ORDER_URL = f"{DEMO_BASE_URL}/fapi/v1/algoOrder"
OPEN_ALGO_ORDERS_URL = f"{DEMO_BASE_URL}/fapi/v1/openAlgoOrders"


@pytest.fixture
def client(settings) -> BinanceFuturesClient:
    return BinanceFuturesClient(settings, clock=lambda: FIXED_TIMESTAMP_MS)


def signed_parts(request_url: str) -> tuple[str, str]:
    query_string = urllib.parse.urlsplit(request_url).query
    unsigned_query, _, signature = query_string.rpartition("&signature=")
    return unsigned_query, signature


@responses.activate
def test_place_conditional_close_sends_a_close_all_algo_order(client):
    recorded_reply = rest_reply("A SL algo (-5%)")
    responses.post(ALGO_ORDER_URL, json=recorded_reply)

    reply = client.place_conditional_close(
        symbol="BTCUSDT",
        side="SELL",
        order_type="STOP_MARKET",
        trigger_price=Decimal("73843.7"),
        client_algo_id="lx-spike-a-sl",
        working_type="MARK_PRICE",
    )

    sent_request = responses.calls[0].request
    unsigned_query, signature = signed_parts(sent_request.url)
    assert sent_request.method == "POST"
    assert unsigned_query == (
        "algoType=CONDITIONAL&symbol=BTCUSDT&side=SELL&type=STOP_MARKET&triggerPrice=73843.7"
        "&closePosition=true&workingType=MARK_PRICE&clientAlgoId=lx-spike-a-sl"
        f"&recvWindow=5000&timestamp={FIXED_TIMESTAMP_MS}"
    )
    assert signature == sign_query(unsigned_query, TEST_API_SECRET)
    assert reply["algoId"] == 1000000274196254
    assert reply["algoStatus"] == "NEW"


@responses.activate
def test_get_algo_order_by_client_algo_id(client):
    responses.get(ALGO_ORDER_URL, json=rest_reply("B query lx-spike-b-sl (after trigger)"))

    reply = client.get_algo_order(client_algo_id="lx-spike-b-sl")

    unsigned_query, _ = signed_parts(responses.calls[0].request.url)
    assert unsigned_query.startswith("clientAlgoId=lx-spike-b-sl&")
    assert reply["algoStatus"] == "FINISHED"
    assert reply["actualPrice"] == "78016.600000"


@responses.activate
def test_cancel_algo_order_by_client_algo_id(client):
    responses.delete(ALGO_ORDER_URL, json={"algoId": 1, "clientAlgoId": "lx-x-tp", "code": "200", "msg": "success"})

    client.cancel_algo_order(client_algo_id="lx-x-tp")

    sent_request = responses.calls[0].request
    assert sent_request.method == "DELETE"
    assert signed_parts(sent_request.url)[0].startswith("clientAlgoId=lx-x-tp&")


@responses.activate
def test_cancelling_an_expired_algo_order_raises_binance_error_2011(client):
    recorded_error = rest_reply("A cancel TP by clientAlgoId")
    responses.delete(ALGO_ORDER_URL, status=400, json={"code": recorded_error["code"], "msg": "Unknown order sent."})

    with pytest.raises(BinanceAPIError) as raised:
        client.cancel_algo_order(client_algo_id="lx-spike-a-tp")

    assert raised.value.code == -2011


@responses.activate
def test_get_open_algo_orders_for_a_symbol(client):
    responses.get(OPEN_ALGO_ORDERS_URL, json=rest_reply("openAlgoOrders (A placed)"))

    reply = client.get_open_algo_orders(symbol="BTCUSDT")

    assert signed_parts(responses.calls[0].request.url)[0].startswith("symbol=BTCUSDT&")
    assert {algo_order["clientAlgoId"] for algo_order in reply} == {"lx-spike-a-tp", "lx-spike-a-sl"}
