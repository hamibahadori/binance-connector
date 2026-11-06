"""The secrets audit (spec §10): credentials never appear in logs, replies, error logs or reports.

One request carries real-looking credentials through the whole service: intake over HTTP,
the worker's position manager with the real per-account Binance client (signing included),
and a Binance error that echoes the API key back, as some Binance errors quote the request.
Then every place a secret could leak is searched for both values.
"""

import json
import logging
import re
from pathlib import Path

import pytest
from sqlalchemy import inspect, text

from binance_connector.config import DEMO_BASE_URL
from binance_connector.services.account_clients import AccountClients
from binance_connector.services.position_manager import PositionManager
from tests.functional.binance_mock import POSITION_RISK_URL
from tests.recorded import exchange_info_reply

AUDIT_API_KEY = "auditApiKey0123456789abcdefABCDEF"
AUDIT_SECRET = "auditSecret9876543210zyxwvuZYXWVU"
SECRETS = (AUDIT_API_KEY, AUDIT_SECRET)


def open_body() -> dict:
    return {
        "request_id": "audit-request-1", "user_id": "audit-user", "account_id": "audit-account",
        "api_key": AUDIT_API_KEY, "secret_key": AUDIT_SECRET, "action": "OPEN", "timestamp": 1794000000000,
        "symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002", "leverage": 5,
        "take_profit_price": "82000.0", "stop_loss_price": "74000.0",
    }


def leaks(text_to_search: str) -> list[str]:
    return [secret for secret in SECRETS if secret in text_to_search]


@pytest.fixture
def audited_run(app, http_client, binance, settings, caplog):
    """Intake, then an execution that Binance refuses with the API key quoted in its message."""
    from binance_connector import configure_logging

    configure_logging()  # installs the redacting filter on the root handlers, as in production
    caplog.set_level(logging.DEBUG)
    binance.get(f"{DEMO_BASE_URL}/fapi/v1/exchangeInfo", json=exchange_info_reply())
    binance.get(f"{DEMO_BASE_URL}/fapi/v1/premiumIndex",
                json={"symbol": "BTCUSDT", "markPrice": "75000.00000000", "time": 1794000000000})
    binance.get(re.compile(re.escape(POSITION_RISK_URL) + ".*"), status=401,
                json={"code": -2015, "msg": f"Invalid API-key {AUDIT_API_KEY}, IP, or permissions for action."})

    intake_reply = http_client.post("/api/v1/requests", json=open_body())
    components = app.extensions["components"]
    manager = PositionManager(positions=components.positions,
                              clients=AccountClients(accounts=components.accounts, settings=settings),
                              trigger_price_type="MARK_PRICE", limit_entry_timeout_seconds=300,
                              error_logs=components.error_logs, sleep=lambda _seconds: None)
    position_id = intake_reply.get_json()["position_id"]
    failed = manager.open(position_id)
    replies = [intake_reply,
               http_client.get("/api/v1/requests/audit-request-1"),
               http_client.get(f"/api/v1/positions/{position_id}"),
               http_client.get("/api/v1/accounts/audit-account/positions")]
    return {"components": components, "failed": failed, "replies": replies, "caplog": caplog}


def test_the_run_really_went_through_binance_and_failed(audited_run):
    assert audited_run["failed"].state.value == "FAILED"
    assert "-2015" in audited_run["failed"].last_error


def test_no_secret_in_the_log_output(audited_run):
    handler_output = "\n".join(audited_run["caplog"].handler.format(record)
                               for record in audited_run["caplog"].records)

    assert leaks(handler_output) == []


def test_no_secret_in_any_http_reply(audited_run):
    for reply in audited_run["replies"]:
        assert leaks(reply.get_data(as_text=True)) == [], reply.request.path


def test_no_secret_in_any_database_row(audited_run):
    engine = audited_run["components"].engine
    with engine.connect() as connection:
        for table in inspect(engine).get_table_names():
            rows = connection.execute(text(f'SELECT * FROM "{table}"')).fetchall()  # noqa: S608 - our own names
            assert leaks(repr(rows)) == [], table


def test_no_secret_in_the_database_files(audited_run, settings):
    database_file = Path(settings.database_url.removeprefix("sqlite:///"))
    for path in (database_file, database_file.with_name(database_file.name + "-wal")):
        if path.exists():
            raw_bytes = path.read_bytes()
            assert [secret for secret in SECRETS if secret.encode() in raw_bytes] == [], path.name


def test_no_secret_in_the_reports_for_django(audited_run):
    payloads = [report.payload for report in audited_run["components"].reports.all()]

    assert payloads, "the run should have produced reports"
    assert leaks("".join(payloads)) == []
    failed_report = json.loads(payloads[-1])
    assert failed_report["event"] == "POSITION_FAILED"
    assert "***" in failed_report["error"]  # Binance's message is kept, with the key masked
