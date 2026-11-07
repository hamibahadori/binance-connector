"""End-to-end tests on Binance Futures Demo: the spec §14 acceptance criteria, for real.

Run with ``.venv\\Scripts\\python -m pytest -m e2e -v`` (Demo keys in ``.env``). They place
real orders on the Demo account; every test cleans up after itself. Each test names the
§14 criteria it covers:

1 receive requests · 2 right user and account · 3 several users · 4 manage a position ·
5 live price · 6 live WebSocket · 7 CLOSE from Django · 8 close on TP/SL · 9 every event
and state recorded · 10 no duplicate execution · 11 restart recovery · 12 errors and
reconnects · 13 the final report to Django
"""

import threading
import time
from decimal import Decimal

import pytest

from tests.e2e.conftest import our_open_algo_orders, price_away, signed_amount, wait_until

pytestmark = pytest.mark.e2e

RUN = str(int(time.time()))
"""Makes request ids unique per run (the database is fresh anyway; this keeps logs readable)."""
TERMINAL = ("CLOSED", "FAILED")


def request_id(name: str) -> str:
    return f"e2e-{RUN}-{name}"


def open_body(account, name: str, symbol: str, **fields) -> dict:
    body = {
        "request_id": request_id(name), "user_id": account.user_id, "account_id": account.account_id,
        "api_key": account.credentials.api_key, "secret_key": account.credentials.api_secret,
        "action": "OPEN", "timestamp": int(time.time() * 1000), "symbol": symbol, "direction": "LONG",
    }
    body.update(fields)
    return body


def close_body(account, name: str, **target) -> dict:
    return {
        "request_id": request_id(name), "user_id": account.user_id, "account_id": account.account_id,
        "api_key": account.credentials.api_key, "secret_key": account.credentials.api_secret,
        "action": "CLOSE", "timestamp": int(time.time() * 1000), **target,
    }


def protected(view: dict) -> bool:
    return view["state"] == "OPEN" and all(
        (view[leg] or {}).get("status") == "NEW" for leg in ("take_profit", "stop_loss"))


# --- 1, 2, 4, 5, 6, 7, 9, 10, 12, 13 -------------------------------------------------------------


def test_a_position_is_opened_protected_watched_closed_and_reported(stack, primary_account, flat, symbol, quantity):
    flat(primary_account, symbol)
    client = primary_account.client
    body = open_body(primary_account, "lifecycle-open", symbol, quantity=quantity, leverage=5,
                     entry_price=price_away(symbol, "0"),
                     take_profit_price=price_away(symbol, "3"), stop_loss_price=price_away(symbol, "-3"))

    # 1, 2: received, validated and accepted for the right user and account
    receipt = stack.post(body)
    assert receipt.status_code == 202, receipt.text
    assert (receipt.json()["status"], receipt.json()["user_id"], receipt.json()["account_id"]) == (
        "ACCEPTED", primary_account.user_id, primary_account.account_id)
    position_id = receipt.json()["position_id"]
    stack.capture.save("open-request", body)
    stack.capture.save("open-receipt-202", receipt.json())

    # 10: the same request again returns the original receipt and executes nothing new
    duplicate = stack.post(body)
    assert duplicate.status_code == 200 and duplicate.json()["duplicate"] is True
    stack.capture.save("open-duplicate-200", duplicate.json())

    # 4: open on Binance, with TP and SL resting there
    opened = stack.wait_for_position(position_id, "OPEN with TP and SL on Binance", protected)
    assert signed_amount(client, symbol) == Decimal(quantity)
    assert {order["clientAlgoId"] for order in our_open_algo_orders(client, symbol)} == {
        f"lx-{position_id}-tp", f"lx-{position_id}-sl"}
    assert opened["entry"]["average_price"] is not None and opened["entry"]["slippage"] is not None
    assert opened["pnl"]["fees"] is not None
    stack.capture.save("position-open", opened)

    # 5, 6: the live price and P&L arrive over the WebSocket
    ticks = wait_until("a position_tick with a price and P&L",
                       lambda: [tick for tick in stack.live_feed.of_type("position_tick", position_id)
                                if tick["data"]["prices"]["current"] and tick["data"]["pnl"]["gross"]])
    assert ticks[-1]["data"]["take_profit"]["distance"]["percent"] is not None
    assert stack.live_feed.snapshots(), "a snapshot is sent on connect"
    stack.capture.save("websocket-position-tick", ticks[-1])
    with_price = stack.wait_for_position(position_id, "showing the live price",
                                         lambda view: view["prices"]["current"] is not None)
    assert with_price["pnl"]["kind"] == "UNREALIZED"

    # 7: Django closes it
    close = close_body(primary_account, "lifecycle-close", position_id=position_id)
    close_receipt = stack.post(close)
    assert close_receipt.status_code == 202, close_receipt.text
    stack.capture.save("close-request", close)
    stack.capture.save("close-receipt-202", close_receipt.json())
    closed = stack.wait_for_state(position_id, *TERMINAL)
    assert closed["state"] == "CLOSED" and closed["close_reason"] == "CLOSE_REQUESTED"
    assert closed["pnl"]["kind"] == "REALIZED" and closed["pnl"]["fees"] is not None
    assert signed_amount(client, symbol) == 0
    wait_until("TP/SL to be gone from Binance", lambda: our_open_algo_orders(client, symbol) == [])
    stack.capture.save("position-closed", closed)

    # 10: repeating the CLOSE executes nothing; a new CLOSE of a closed position says so
    assert stack.post(close).json()["duplicate"] is True
    late_close = stack.post(close_body(primary_account, "lifecycle-close-again", position_id=position_id))
    assert late_close.status_code == 200 and late_close.json()["already_closed"] is True
    stack.capture.save("close-already-closed-200", late_close.json())

    # 9: every step is in the position's history, and in the WebSocket's updates
    events = [event["data"]["state"] for event in stack.live_feed.of_type("position_update", position_id)]
    assert events[0] in ("OPENING", "OPEN") and events[-1] == "CLOSED"

    # 13, 12: Django got every report, in order, authenticated, despite refusing the first one
    received = wait_until("POSITION_CLOSED at Django",
                          lambda: (lambda found: found if "POSITION_CLOSED" in found else None)(
                              stack.django.events_for(position_id)))
    stored = [report.event_type for report in stack.reports.all() if report.position_id == position_id]
    assert received == stored[:len(received)] and received[-1] == "POSITION_CLOSED"
    assert received.index("POSITION_OPEN") < received.index("POSITION_CLOSING") < received.index("POSITION_CLOSED")
    assert stack.django.refused, "the first report was refused and must have been retried"
    final = [report for report in stack.django.unique_reports()
             if report.body.get("position_id") == position_id and report.body["event"] == "POSITION_CLOSED"][0]
    assert final.headers["Authorization"] == stack.settings.django_report_auth_value
    assert final.headers["X-Report-Event"] == "POSITION_CLOSED"
    assert final.body["pnl"]["net"] is not None and final.body["close_reason"] == "CLOSE_REQUESTED"
    every_body = repr([report.body for report in stack.django.reports])
    assert primary_account.credentials.api_secret not in every_body
    assert primary_account.credentials.api_key not in every_body
    stack.capture.save("django-report-position-closed", {"headers": {
        key: value for key, value in final.headers.items() if key.startswith(("X-", "Content-Type", "Authorization"))},
        "body": final.body})


# --- 1, 2, 10 -----------------------------------------------------------------------------------


def test_invalid_requests_are_rejected_and_reported(stack, primary_account, symbol, quantity):
    # below the minimum notional: stored REJECTED, reported, nothing sent to Binance
    too_small = open_body(primary_account, "rejected-too-small", symbol, quantity="0.0001")
    rejected = stack.post(too_small)
    assert rejected.status_code == 422 and rejected.json()["status"] == "REJECTED"
    assert rejected.json()["reasons"]
    stack.capture.save("open-rejected-422", rejected.json())

    # the same request id with a different body: refused, the original stands
    conflict = stack.post({**too_small, "quantity": quantity})
    assert conflict.status_code == 409
    stack.capture.save("request-id-conflict-409", conflict.json())

    # an account claimed by another user: rejected
    stolen = open_body(primary_account, "rejected-other-user", symbol, quantity=quantity, user_id="someone-else")
    assert stack.post(stolen).json()["status"] == "REJECTED"

    reported = wait_until("REQUEST_REJECTED at Django", lambda: [
        report for report in stack.django.unique_reports()
        if report.body["event"] == "REQUEST_REJECTED" and report.body["request_id"] == request_id("rejected-too-small")])
    assert reported[0].body["reasons"] == rejected.json()["reasons"]


# --- 4, 7 ---------------------------------------------------------------------------------------


def test_a_limit_entry_rests_until_django_cancels_it(stack, primary_account, flat, symbol, quantity):
    flat(primary_account, symbol)
    body = open_body(primary_account, "limit-open", symbol, quantity=quantity, entry_type="LIMIT",
                     entry_price=price_away(symbol, "-10"))
    position_id = stack.post(body).json()["position_id"]
    stack.capture.save("limit-open-request", body)

    resting = stack.wait_for_position(position_id, "resting on Binance",
                                      lambda view: view["state"] == "OPENING" and view["times"]["started"] is None
                                      and stack.get(f"/api/v1/requests/{request_id('limit-open')}").json()[
                                          "processing_state"] == "DONE")
    assert resting["entry"]["type"] == "LIMIT"
    assert primary_account.client.get_order(symbol=symbol, client_order_id=f"lx-{position_id}-e")["status"] == "NEW"

    stack.post(close_body(primary_account, "limit-close", position_id=position_id))
    failed = stack.wait_for_state(position_id, *TERMINAL)
    assert failed["state"] == "FAILED" and "before it filled" in failed["error"]
    order = primary_account.client.get_order(symbol=symbol, client_order_id=f"lx-{position_id}-e")
    assert order["status"] == "CANCELED"
    stack.capture.save("limit-cancelled-failed", failed)


# --- 6, 11, 12 ----------------------------------------------------------------------------------


def test_requests_survive_a_worker_restart_and_the_feed_resyncs(stack, primary_account, flat, symbol, quantity):
    flat(primary_account, symbol)
    stack.stop_worker()
    body = open_body(primary_account, "restart-open", symbol, quantity=quantity,
                     take_profit_price=price_away(symbol, "3"), stop_loss_price=price_away(symbol, "-3"))
    receipt = stack.post(body)  # the API keeps taking requests while the worker is down
    assert receipt.status_code == 202
    position_id = receipt.json()["position_id"]
    time.sleep(3)
    assert stack.get(f"/api/v1/requests/{request_id('restart-open')}").json()["processing_state"] == "PENDING"
    assert signed_amount(primary_account.client, symbol) == 0

    stack.start_worker()  # picks up the queued request
    stack.wait_for_position(position_id, "OPEN after the restart", protected)

    stack.stop_worker()  # and again, with a live position this time
    stack.start_worker()
    snapshot = wait_until("a snapshot from the new worker", stack.live_feed.snapshots)[-1]
    assert position_id in [position["position_id"] for position in snapshot["data"]["positions"]]
    stack.capture.save("websocket-snapshot", snapshot)
    status = wait_until("a service_status", lambda: stack.live_feed.of_type("service_status"))[-1]
    stack.capture.save("websocket-service-status", status)

    stack.post(close_body(primary_account, "restart-close", symbol=symbol))
    closed = stack.wait_for_state(position_id, *TERMINAL)
    assert closed["state"] == "CLOSED"
    assert signed_amount(primary_account.client, symbol) == 0


# --- 3 ------------------------------------------------------------------------------------------


def test_two_users_trade_at_once_without_mixing(stack, primary_account, second_account, flat, symbol,
                                                second_symbol):
    """With a second Demo account (E2E_SECOND_*), both users trade the same symbol; without
    one, the second user shares the first's Binance login and so trades another symbol."""
    separate_logins = second_account.credentials.api_key != primary_account.credentials.api_key
    plans = [(primary_account, flat(primary_account, symbol)),
             (second_account, flat(second_account, symbol if separate_logins else second_symbol))]
    receipts: dict[str, dict] = {}

    def send(account, account_symbol) -> None:
        body = open_body(account, f"multi-{account.user_id}", account_symbol, quantity="100",
                         quantity_type="NOTIONAL", leverage=5)
        receipts[account.user_id] = stack.post(body).json()

    threads = [threading.Thread(target=send, args=plan) for plan in plans]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    for account, account_symbol in plans:
        receipt = receipts[account.user_id]
        assert receipt["status"] == "ACCEPTED", receipt
        view = stack.wait_for_state(receipt["position_id"], "OPEN", *TERMINAL)
        assert view["state"] == "OPEN"
        assert (view["user_id"], view["account_id"], view["symbol"]) == (
            account.user_id, account.account_id, account_symbol)
        listed = stack.get(f"/api/v1/accounts/{account.account_id}/positions?state=OPEN").json()["positions"]
        assert [position["position_id"] for position in listed] == [receipt["position_id"]]
    stack.capture.save("account-positions", stack.get(
        f"/api/v1/accounts/{primary_account.account_id}/positions").json())

    for account, _account_symbol in plans:
        stack.post(close_body(account, f"multi-close-{account.user_id}", position_id=receipts[account.user_id][
            "position_id"]))
    for account, account_symbol in plans:
        assert stack.wait_for_state(receipts[account.user_id]["position_id"], *TERMINAL)["state"] == "CLOSED"
        assert signed_amount(account.client, account_symbol) == 0


# --- 8 ------------------------------------------------------------------------------------------


def test_a_take_profit_or_stop_loss_closes_the_position_on_binance(stack, primary_account, flat, symbol, quantity,
                                                                   e2e_environment):
    """TP and SL E2E_TRIGGER_PERCENT (default 0.05 %) either side of the mark price: whichever
    the market reaches first closes it. Binance executes the trigger; the service records it,
    closes the position and reports it.

    The band must be wide enough not to be crossed before the orders are placed (a few
    seconds), and narrow enough for the market to reach it: a first run at 0.15 % saw the
    mark price stay inside the band for 30 minutes (2026-11-07, late evening UTC)."""
    flat(primary_account, symbol)
    band = e2e_environment.get("E2E_TRIGGER_PERCENT", "0.05").strip()
    reference = Decimal(price_away(symbol, "0"))
    body = open_body(primary_account, "trigger-open", symbol, quantity=quantity,
                     take_profit_price=price_away(symbol, band, reference),
                     stop_loss_price=price_away(symbol, f"-{band}", reference))
    position_id = stack.post(body).json()["position_id"]
    stack.wait_for_position(position_id, "OPEN with TP and SL", protected)

    closed = stack.wait_for_state(position_id, *TERMINAL, timeout_seconds=45 * 60)
    assert closed["state"] == "CLOSED"
    assert closed["close_reason"] in ("TAKE_PROFIT", "STOP_LOSS")
    triggered = "take_profit" if closed["close_reason"] == "TAKE_PROFIT" else "stop_loss"
    # the fill can arrive before the algo order's own updates; either way the leg ends FINISHED
    assert closed[triggered]["status"] == "FINISHED"
    other = "stop_loss" if triggered == "take_profit" else "take_profit"
    assert closed[other]["status"] in ("CANCELED", "EXPIRED")  # removed before the close was recorded
    assert our_open_algo_orders(primary_account.client, symbol) == []
    assert closed["pnl"]["kind"] == "REALIZED" and closed["pnl"]["fees"] is not None
    assert signed_amount(primary_account.client, symbol) == 0
    report = wait_until("POSITION_CLOSED at Django", lambda: [
        received for received in stack.django.unique_reports()
        if received.body.get("position_id") == position_id and received.body["event"] == "POSITION_CLOSED"])[0]
    assert report.body["close_reason"] == closed["close_reason"]
    stack.capture.save("position-closed-by-trigger", closed)
