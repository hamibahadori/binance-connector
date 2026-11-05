"""Helpers that make ``responses`` answer like Binance Demo did, echoing our client ids.

The service generates its trade ids (and therefore its client order ids) itself, so the
mocked replies are built per request: the recorded Demo reply, with the ids the request
actually sent.
"""

import json
import urllib.parse
from collections.abc import Callable

from binance_connector.config import DEMO_BASE_URL
from tests.recorded import rest_reply

ORDER_URL = f"{DEMO_BASE_URL}/fapi/v1/order"
LEVERAGE_URL = f"{DEMO_BASE_URL}/fapi/v1/leverage"
POSITION_RISK_URL = f"{DEMO_BASE_URL}/fapi/v3/positionRisk"
ALGO_ORDER_URL = f"{DEMO_BASE_URL}/fapi/v1/algoOrder"


def sent_parameters(request) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request.url).query))


def _echo(build_reply: Callable[[dict[str, str]], dict]) -> Callable:
    def callback(request):
        return 200, {"Content-Type": "application/json"}, json.dumps(build_reply(sent_parameters(request)))

    return callback


# ``upsert`` replaces an earlier registration for the same method and URL. With plain
# ``add``, responses would keep answering with the older one first.


def register_flat_position(binance) -> None:
    binance.upsert("GET", POSITION_RISK_URL, json=[])


def register_position(binance, position_amount: str) -> None:
    binance.upsert(
        "GET", POSITION_RISK_URL,
        json=[{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": position_amount}],
    )


def register_order_rejection(binance, code: int, message: str) -> None:
    binance.upsert("POST", ORDER_URL, status=400, json={"code": code, "msg": message})


def register_order_server_error(binance) -> None:
    binance.upsert("POST", ORDER_URL, status=503, json={"code": -1000, "msg": "Unknown error"})


def register_leverage(binance) -> None:
    binance.post(LEVERAGE_URL, json={"symbol": "BTCUSDT", "leverage": 5, "maxNotionalValue": "100000000"})


def register_market_order(binance) -> None:
    """POST /fapi/v1/order answered with the recorded entry reply (no avgPrice), our ids echoed."""

    def build_reply(parameters):
        reply = rest_reply("A entry MARKET BUY")
        reply.update(clientOrderId=parameters["newClientOrderId"], side=parameters["side"],
                     reduceOnly=parameters.get("reduceOnly") == "true")
        return reply

    binance.add_callback("POST", ORDER_URL, callback=_echo(build_reply))


def register_order_lookup(binance) -> None:
    """GET /fapi/v1/order answered with the recorded lookup (avgPrice 77890.500000)."""

    def build_reply(parameters):
        reply = rest_reply("A entry lookup")
        reply.update(clientOrderId=parameters["origClientOrderId"])
        return reply

    binance.add_callback("GET", ORDER_URL, callback=_echo(build_reply))


def register_algo_placement(binance) -> None:
    """POST /fapi/v1/algoOrder answered with the recorded placement reply, our ids echoed."""

    def build_reply(parameters):
        reply = rest_reply("A SL algo (-5%)")
        reply.update(clientAlgoId=parameters["clientAlgoId"], orderType=parameters["type"],
                     side=parameters["side"], triggerPrice=parameters["triggerPrice"])
        return reply

    binance.add_callback("POST", ALGO_ORDER_URL, callback=_echo(build_reply))


def register_algo_cancel_already_gone(binance) -> None:
    binance.delete(ALGO_ORDER_URL, status=400, json={"code": -2011, "msg": "Unknown order sent."})


def calls_summary(binance) -> list[tuple[str, str]]:
    """(method, path) of every call the service made to Binance, in order."""
    return [
        (mocked_call.request.method, urllib.parse.urlsplit(mocked_call.request.url).path)
        for mocked_call in binance.calls
    ]
