"""Functional tests: the Strategy trades API end to end, with Binance mocked at HTTP level."""

import pytest

from tests.functional.binance_mock import (
    calls_summary,
    register_algo_cancel_already_gone,
    register_algo_placement,
    register_flat_position,
    register_leverage,
    register_market_order,
    register_order_lookup,
    register_order_rejection,
    register_order_server_error,
    register_position,
    sent_parameters,
)

TRADES_URL = "/api/v1/strategy/trades"

LONG_WITH_PROTECTION = {
    "symbol": "BTCUSDT",
    "direction": "LONG",
    "quantity": "0.002",
    "leverage": 5,
    "take_profit_price": "82000.0",
    "stop_loss_price": "74000.0",
    "strategy_request_id": "strategy-7:signal-42",
}


def register_successful_open(binance) -> None:
    register_flat_position(binance)
    register_leverage(binance)
    register_market_order(binance)
    register_order_lookup(binance)
    register_algo_placement(binance)


def open_protected_trade(http_client, binance) -> dict:
    register_successful_open(binance)
    response = http_client.post(TRADES_URL, json=LONG_WITH_PROTECTION)
    assert response.status_code == 201, response.get_json()
    return response.get_json()


# --- open -------------------------------------------------------------------------------------


def test_open_with_tp_and_sl_returns_201_and_the_protected_trade(http_client, binance):
    trade = open_protected_trade(http_client, binance)

    assert trade["state"] == "PROTECTED"
    assert trade["symbol"] == "BTCUSDT"
    assert trade["direction"] == "LONG"
    assert trade["quantity"] == "0.002"
    assert trade["leverage"] == 5
    assert trade["strategy_request_id"] == "strategy-7:signal-42"
    assert trade["entry"]["price"] == "77890.500000"
    assert trade["entry"]["client_order_id"] == f"lx-{trade['trade_id']}-e"
    assert trade["take_profit"] == {
        "trigger_price": "82000.0",
        "client_algo_id": f"lx-{trade['trade_id']}-tp",
        "algo_id": 1000000274196254,
        "status": "NEW",
    }
    assert trade["stop_loss"]["status"] == "NEW"
    assert trade["exit"]["price"] is None
    assert trade["close_reason"] is None
    assert trade["last_error"] is None


def test_open_makes_exactly_the_expected_binance_calls(http_client, binance):
    open_protected_trade(http_client, binance)

    assert calls_summary(binance) == [
        ("GET", "/fapi/v3/positionRisk"),
        ("POST", "/fapi/v1/leverage"),
        ("POST", "/fapi/v1/order"),
        ("GET", "/fapi/v1/order"),
        ("POST", "/fapi/v1/algoOrder"),
        ("POST", "/fapi/v1/algoOrder"),
    ]
    stop_loss_parameters = sent_parameters(binance.calls[4].request)
    assert stop_loss_parameters["type"] == "STOP_MARKET"
    assert stop_loss_parameters["triggerPrice"] == "74000.0"
    assert stop_loss_parameters["closePosition"] == "true"
    assert stop_loss_parameters["workingType"] == "MARK_PRICE"
    assert sent_parameters(binance.calls[5].request)["type"] == "TAKE_PROFIT_MARKET"


def test_a_repeated_strategy_request_returns_200_and_the_same_trade(http_client, binance):
    first_trade = open_protected_trade(http_client, binance)
    calls_after_first = len(binance.calls)

    response = http_client.post(TRADES_URL, json=LONG_WITH_PROTECTION)

    assert response.status_code == 200
    assert response.get_json()["trade_id"] == first_trade["trade_id"]
    assert len(binance.calls) == calls_after_first


def test_a_second_trade_on_the_same_symbol_returns_409(http_client, binance):
    open_protected_trade(http_client, binance)

    response = http_client.post(TRADES_URL, json={**LONG_WITH_PROTECTION, "strategy_request_id": "another"})

    assert response.status_code == 409
    assert response.get_json()["error"] == "trade_conflict"


def test_an_unmanaged_position_on_binance_returns_409(http_client, binance):
    register_position(binance, "0.010")

    response = http_client.post(TRADES_URL, json=LONG_WITH_PROTECTION)

    assert response.status_code == 409
    assert "not managed" in response.get_json()["message"]


def test_a_rejected_entry_returns_422_with_the_failed_trade(http_client, binance):
    register_flat_position(binance)
    register_leverage(binance)
    register_order_rejection(binance, -2019, "Margin is insufficient.")

    response = http_client.post(TRADES_URL, json=LONG_WITH_PROTECTION)

    assert response.status_code == 422
    body = response.get_json()
    assert body["state"] == "ENTRY_FAILED"
    assert "-2019" in body["last_error"]


def test_an_unknown_entry_outcome_returns_202_needs_attention(http_client, binance):
    register_flat_position(binance)
    register_leverage(binance)
    register_order_server_error(binance)

    response = http_client.post(TRADES_URL, json=LONG_WITH_PROTECTION)

    assert response.status_code == 202
    assert response.get_json()["state"] == "NEEDS_ATTENTION"


@pytest.mark.parametrize(
    "body",
    [
        {**LONG_WITH_PROTECTION, "take_profit_price": "70000"},
        {**LONG_WITH_PROTECTION, "quantity": "0"},
        {key: value for key, value in LONG_WITH_PROTECTION.items() if key != "symbol"},
    ],
)
def test_an_invalid_body_returns_400_and_calls_nothing(http_client, binance, body):
    response = http_client.post(TRADES_URL, json=body)

    assert response.status_code == 400
    assert response.get_json()["error"] == "validation_error"
    assert len(binance.calls) == 0


# --- close ------------------------------------------------------------------------------------


def test_close_returns_200_closed_and_cancels_nothing_binance_already_expired(http_client, binance):
    trade = open_protected_trade(http_client, binance)
    register_position(binance, "0.0020")
    register_algo_cancel_already_gone(binance)

    response = http_client.post(f"{TRADES_URL}/{trade['trade_id']}/close", json={"reason": "UPSTREAM"})

    assert response.status_code == 200
    closed_trade = response.get_json()
    assert closed_trade["state"] == "CLOSED"
    assert closed_trade["close_reason"] == "UPSTREAM"
    assert closed_trade["exit"]["client_order_id"] == f"lx-{trade['trade_id']}-x"
    assert closed_trade["exit"]["price"] == "77890.500000"
    assert closed_trade["take_profit"]["status"] == "EXPIRED"
    assert closed_trade["stop_loss"]["status"] == "EXPIRED"
    close_calls = calls_summary(binance)[6:]
    assert close_calls[:3] == [("GET", "/fapi/v3/positionRisk"), ("POST", "/fapi/v1/order"), ("GET", "/fapi/v1/order")]
    assert sent_parameters(binance.calls[7].request)["reduceOnly"] == "true"


def test_close_without_a_body_defaults_to_manual(http_client, binance):
    trade = open_protected_trade(http_client, binance)
    register_position(binance, "0.0020")
    register_algo_cancel_already_gone(binance)

    response = http_client.post(f"{TRADES_URL}/{trade['trade_id']}/close")

    assert response.status_code == 200
    assert response.get_json()["close_reason"] == "MANUAL"


def test_closing_an_unknown_trade_returns_404(http_client):
    response = http_client.post(f"{TRADES_URL}/does-not-exist/close", json={})

    assert response.status_code == 404
    assert response.get_json()["error"] == "trade_not_found"


def test_closing_a_failed_trade_returns_409(http_client, binance):
    register_flat_position(binance)
    register_leverage(binance)
    register_order_rejection(binance, -2019, "Margin is insufficient.")
    failed_trade = http_client.post(TRADES_URL, json=LONG_WITH_PROTECTION).get_json()

    response = http_client.post(f"{TRADES_URL}/{failed_trade['trade_id']}/close", json={})

    assert response.status_code == 409
    assert response.get_json()["error"] == "trade_not_closable"


def test_a_close_binance_rejects_returns_422_and_the_trade_stays_protected(http_client, binance):
    trade = open_protected_trade(http_client, binance)
    register_position(binance, "0.0020")
    register_order_rejection(binance, -1021, "Timestamp for this request is outside of the recvWindow.")

    response = http_client.post(f"{TRADES_URL}/{trade['trade_id']}/close", json={})

    assert response.status_code == 422
    assert response.get_json()["state"] == "PROTECTED"
    assert "-1021" in response.get_json()["last_error"]


# --- retired endpoints ------------------------------------------------------------------------


@pytest.mark.parametrize("retired_path", ["/api/v1/positions/open", "/api/v1/positions/close"])
def test_the_retired_position_endpoints_are_gone(http_client, retired_path):
    assert http_client.post(retired_path, json={}).status_code == 404
