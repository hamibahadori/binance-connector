"""Unit tests for limit orders and order cancellation (replies recorded on Demo, 2026-11-07)."""

import urllib.parse
from decimal import Decimal

import pytest
import responses

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import DEMO_BASE_URL
from binance_connector.errors import BinanceAPIError
from tests.conftest import TEST_CREDENTIALS
from tests.recorded import MARKET_AND_LIMIT_CAPTURE, rest_reply

FIXED_TIMESTAMP_MS = 1_700_000_000_000
ORDER_URL = f"{DEMO_BASE_URL}/fapi/v1/order"


@pytest.fixture
def client(settings) -> BinanceFuturesClient:
    return BinanceFuturesClient(settings, TEST_CREDENTIALS, clock=lambda: FIXED_TIMESTAMP_MS)


def unsigned_query(request_url: str) -> str:
    return urllib.parse.urlsplit(request_url).query.rpartition("&signature=")[0]


@responses.activate
def test_place_limit_order_sends_a_gtc_limit_with_its_price(client):
    responses.post(ORDER_URL, json=rest_reply("L1 place resting LIMIT BUY", MARKET_AND_LIMIT_CAPTURE))

    reply = client.place_limit_order(symbol="BTCUSDT", side="BUY", quantity=Decimal("0.002"),
                                     price=Decimal("68291.6"), client_order_id="lx-spike2-l1")

    assert unsigned_query(responses.calls[0].request.url) == (
        "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.002&price=68291.6"
        f"&newClientOrderId=lx-spike2-l1&newOrderRespType=RESULT&recvWindow=5000&timestamp={FIXED_TIMESTAMP_MS}"
    )
    assert reply["status"] == "NEW"


@responses.activate
def test_cancel_order_by_client_order_id(client):
    responses.delete(ORDER_URL, json=rest_reply("L1 cancel", MARKET_AND_LIMIT_CAPTURE))

    reply = client.cancel_order(symbol="BTCUSDT", client_order_id="lx-spike2-l1")

    assert responses.calls[0].request.method == "DELETE"
    assert unsigned_query(responses.calls[0].request.url).startswith("symbol=BTCUSDT&origClientOrderId=lx-spike2-l1&")
    assert reply["status"] == "CANCELED"


@responses.activate
def test_cancelling_a_filled_or_cancelled_order_raises_2011(client):
    responses.delete(ORDER_URL, status=400, json={"code": -2011, "msg": "Unknown order sent."})

    with pytest.raises(BinanceAPIError) as raised:
        client.cancel_order(symbol="BTCUSDT", client_order_id="lx-spike2-l2")

    assert raised.value.code == -2011
