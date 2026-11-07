"""Functional tests: the intake and read API over HTTP, Binance's public endpoints mocked."""

from dataclasses import replace

import pytest

from binance_connector import create_app
from binance_connector.config import DEMO_BASE_URL
from tests.recorded import exchange_info_reply

REQUESTS_URL = "/api/v1/requests"
EXCHANGE_INFO_URL = f"{DEMO_BASE_URL}/fapi/v1/exchangeInfo"
PREMIUM_INDEX_URL = f"{DEMO_BASE_URL}/fapi/v1/premiumIndex"


def open_body(**overrides) -> dict:
    body = {
        "request_id": "request-1", "user_id": "user-1", "account_id": "account-1",
        "api_key": "demo-api-key-0001", "secret_key": "demo-api-secret-0001",
        "action": "OPEN", "timestamp": 1794000000000,
        "symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "leverage": 5,
        "take_profit_price": "82000.0", "stop_loss_price": "74000.0",
    }
    body.update(overrides)
    return body


@pytest.fixture
def market(binance):
    """Binance's public market data, as recorded on Demo."""
    binance.get(EXCHANGE_INFO_URL, json=exchange_info_reply())
    binance.get(PREMIUM_INDEX_URL, json={"symbol": "BTCUSDT", "markPrice": "75000.00000000", "time": 1794000000000})
    return binance


def test_an_accepted_open_returns_202_and_its_receipt(http_client, market):
    response = http_client.post(REQUESTS_URL, json=open_body())

    assert response.status_code == 202
    receipt = response.get_json()
    assert receipt["status"] == "ACCEPTED"
    assert receipt["action"] == "OPEN"
    assert receipt["account_id"] == "account-1"
    assert receipt["user_id"] == "user-1"
    assert len(receipt["position_id"]) == 24
    assert receipt["position"]["state"] == "OPENING"


def test_an_accepted_open_leaves_its_reports_in_the_outbox_for_django(app, http_client, market):
    position_id = http_client.post(REQUESTS_URL, json=open_body()).get_json()["position_id"]

    stored_reports = app.extensions["components"].reports.all()

    assert [report.event_type for report in stored_reports] == ["POSITION_OPENING", "REQUEST_ACCEPTED"]
    assert {report.position_id for report in stored_reports} == {position_id}
    assert {report.status for report in stored_reports} == {"PENDING"}
    assert "demo-api-secret-0001" not in "".join(report.payload for report in stored_reports)


def test_a_rejected_open_returns_422_with_reasons(http_client, market):
    response = http_client.post(REQUESTS_URL, json=open_body(quantity="0.0005"))

    assert response.status_code == 422
    assert response.get_json()["status"] == "REJECTED"


def test_a_duplicate_returns_200_and_a_conflict_returns_409(http_client, market):
    http_client.post(REQUESTS_URL, json=open_body())

    assert http_client.post(REQUESTS_URL, json=open_body()).status_code == 200
    assert http_client.post(REQUESTS_URL, json=open_body(quantity="0.003")).status_code == 409


def test_a_body_that_is_not_a_json_object_returns_400(http_client):
    response = http_client.post(REQUESTS_URL, data="nope", content_type="application/json")

    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_request_body"


def test_the_request_can_be_read_back_without_its_credentials(http_client, market):
    http_client.post(REQUESTS_URL, json=open_body())

    response = http_client.get(f"{REQUESTS_URL}/request-1")

    assert response.status_code == 200
    request_view = response.get_json()
    assert request_view["status"] == "ACCEPTED"
    assert request_view["processing_state"] == "PENDING"
    assert request_view["requested_at"] == "2026-11-06T21:20:00+00:00"  # epoch ms 1794000000000
    assert "demo-api-secret-0001" not in response.get_data(as_text=True)
    assert "demo-api-key-0001" not in response.get_data(as_text=True)


def test_a_position_can_be_read_with_the_spec_6_fields(http_client, market):
    position_id = http_client.post(REQUESTS_URL, json=open_body()).get_json()["position_id"]

    position_view = http_client.get(f"/api/v1/positions/{position_id}").get_json()

    for field_name in ("position_id", "request_id", "user_id", "account_id", "symbol", "direction", "state",
                       "times", "prices", "pnl", "take_profit", "stop_loss", "close_reason", "error"):
        assert field_name in position_view, field_name
    assert position_view["user_id"] == "user-1"
    assert position_view["take_profit"]["price"] == "82000"  # exact, without trailing zeros
    assert position_view["take_profit"]["status"] is None


def test_an_accounts_positions_can_be_listed_and_filtered(http_client, market):
    http_client.post(REQUESTS_URL, json=open_body())

    all_positions = http_client.get("/api/v1/accounts/account-1/positions").get_json()["positions"]
    closed_positions = http_client.get("/api/v1/accounts/account-1/positions?state=CLOSED").get_json()["positions"]

    assert len(all_positions) == 1
    assert closed_positions == []


@pytest.mark.parametrize("path", ["/api/v1/requests/nope", "/api/v1/positions/nope",
                                  "/api/v1/accounts/nope/positions"])
def test_unknown_ids_return_404(http_client, path):
    assert http_client.get(path).status_code == 404


def test_an_invalid_state_filter_returns_400(http_client, market):
    http_client.post(REQUESTS_URL, json=open_body())

    assert http_client.get("/api/v1/accounts/account-1/positions?state=WHATEVER").status_code == 400


# --- inbound token ------------------------------------------------------------------------------


@pytest.fixture
def protected_client(settings):
    return create_app(replace(settings, inbound_api_token="shared-token-123")).test_client()


def test_with_a_token_configured_calls_without_it_are_refused(protected_client):
    response = protected_client.get(f"{REQUESTS_URL}/request-1")

    assert response.status_code == 401
    assert response.get_json()["error"] == "unauthorized"


def test_with_a_token_configured_the_right_bearer_token_gets_through(protected_client):
    response = protected_client.get(f"{REQUESTS_URL}/request-1", headers={"Authorization": "Bearer shared-token-123"})

    assert response.status_code == 404  # authorised; the request just does not exist


def test_health_never_needs_the_token(protected_client):
    assert protected_client.get("/health").status_code == 200


# --- database trouble ---------------------------------------------------------------------------


def test_an_unavailable_database_answers_503_so_django_can_retry(app, http_client, market, monkeypatch):
    from sqlalchemy.exc import OperationalError

    def locked(*_arguments, **_keywords):
        raise OperationalError("INSERT INTO requests ...", {"secret": "demo-api-secret-0001"},
                               Exception("database is locked"))

    monkeypatch.setattr(app.extensions["components"].requests, "create", locked)

    response = http_client.post(REQUESTS_URL, json=open_body())

    assert response.status_code == 503
    assert response.get_json()["error"] == "database_unavailable"
    assert "demo-api-secret-0001" not in response.get_data(as_text=True)
    assert "INSERT" not in response.get_data(as_text=True)