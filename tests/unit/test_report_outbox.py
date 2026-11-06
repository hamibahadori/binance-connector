"""Unit tests for the report outbox: reports written in the same transaction as each change."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import ExecutionRole, PositionState, RequestAction, RequestStatus
from binance_connector.errors import InvalidTransitionError
from binance_connector.persistence.market_data import MarketDataRepository
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.reports import ReportOutbox, ReportRepository
from binance_connector.persistence.requests import NewRequest, RequestRepository
from tests.support import POSITION_ID, store_position


@pytest.fixture
def reports(session_factory) -> ReportRepository:
    return ReportRepository(session_factory)


@pytest.fixture
def positions(session_factory, accounts) -> PositionRepository:
    return PositionRepository(session_factory, outbox=ReportOutbox())


@pytest.fixture
def requests(session_factory, accounts) -> RequestRepository:
    return RequestRepository(session_factory, outbox=ReportOutbox())


def events(reports) -> list[str]:
    return [report.event_type for report in reports.all()]


def payload(report) -> dict:
    return json.loads(report.payload)


# --- requests -----------------------------------------------------------------------------------


def new_request(requests, request_id="request-1"):
    requests.create(NewRequest(request_id=request_id, account_id="account-1", user_id="user-of-account-1",
                               action=RequestAction.OPEN, payload={"symbol": "BTCUSDT"}, payload_hash="hash",
                               requested_at=None))
    requests.transition(request_id, RequestStatus.VALIDATING)


def test_an_accepted_request_is_reported(requests, reports):
    new_request(requests)

    requests.transition("request-1", RequestStatus.ACCEPTED, position_id=POSITION_ID)

    report = reports.all()[-1]
    assert report.event_type == "REQUEST_ACCEPTED"
    body = payload(report)
    assert (body["request_id"], body["user_id"], body["account_id"], body["position_id"]) == (
        "request-1", "user-of-account-1", "account-1", POSITION_ID)
    assert body["status"] == "ACCEPTED"
    assert body["report_id"] == report.report_id
    assert body["event"] == "REQUEST_ACCEPTED"


def test_a_rejected_request_is_reported_with_its_reasons(requests, reports):
    new_request(requests)

    requests.transition("request-1", RequestStatus.REJECTED, rejection_reasons=["quantity below the minimum"])

    body = payload(reports.all()[-1])
    assert body["event"] == "REQUEST_REJECTED"
    assert body["reasons"] == ["quantity below the minimum"]


def test_receiving_and_validating_are_not_reported(requests, reports):
    new_request(requests)

    assert events(reports) == []


# --- positions ----------------------------------------------------------------------------------


def test_a_new_position_is_reported_as_opening(positions, reports):
    store_position(positions)

    assert events(reports) == ["POSITION_OPENING"]


def test_every_state_change_is_reported_with_the_spec_6_fields(positions, reports, session_factory):
    store_position(positions)
    MarketDataRepository(session_factory).upsert_price("BTCUSDT", Decimal("79000"), datetime.now(UTC))

    positions.transition(POSITION_ID, PositionState.OPEN, entry_average_price=Decimal("78000"),
                         entry_filled_quantity=Decimal("0.002"))

    body = payload(reports.all()[-1])
    assert body["event"] == "POSITION_OPEN"
    for field_name in ("report_id", "occurred_at", "request_id", "user_id", "account_id", "position_id", "symbol",
                       "direction", "state", "times", "prices", "pnl", "take_profit", "stop_loss", "close_reason",
                       "error"):
        assert field_name in body, field_name
    assert body["user_id"] == "user-of-account-1"
    assert body["prices"]["current"] == "79000"
    assert body["pnl"]["kind"] == "UNREALIZED"


def test_the_whole_lifecycle_produces_one_report_per_step(positions, reports):
    store_position(positions)
    for state in (PositionState.OPEN, PositionState.CLOSING, PositionState.CLOSED):
        positions.transition(POSITION_ID, state)

    assert events(reports) == ["POSITION_OPENING", "POSITION_OPEN", "POSITION_CLOSING", "POSITION_CLOSED"]


def test_a_refused_transition_writes_no_report(positions, reports):
    store_position(positions)

    with pytest.raises(InvalidTransitionError):
        positions.transition(POSITION_ID, PositionState.CLOSED)

    assert events(reports) == ["POSITION_OPENING"]


def test_a_protection_status_change_is_reported_but_a_repeat_is_not(positions, reports):
    store_position(positions)
    positions.prepare_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, kind="ALGO", order_type="TAKE_PROFIT_MARKET",
                                side="SELL")

    positions.update_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, status="NEW")
    positions.update_execution(POSITION_ID, ExecutionRole.TAKE_PROFIT, status="NEW", binance_id=5)

    assert events(reports) == ["POSITION_OPENING", "PROTECTION_UPDATED"]
    assert payload(reports.all()[-1])["take_profit"]["status"] == "NEW"


def test_entry_and_exit_order_updates_are_not_reported_on_their_own(positions, reports):
    store_position(positions)
    positions.prepare_execution(POSITION_ID, ExecutionRole.ENTRY, kind="ORDER", order_type="MARKET", side="BUY")

    positions.update_execution(POSITION_ID, ExecutionRole.ENTRY, status="FILLED")

    assert events(reports) == ["POSITION_OPENING"]


def test_a_correction_can_ask_for_a_report(positions, reports):
    store_position(positions)
    positions.transition(POSITION_ID, PositionState.OPEN)
    positions.transition(POSITION_ID, PositionState.CLOSED)

    positions.update_fields(POSITION_ID, report_event="POSITION_CLOSED", close_reason="TAKE_PROFIT")

    assert events(reports)[-1] == "POSITION_CLOSED"
    assert payload(reports.all()[-1])["close_reason"] == "TAKE_PROFIT"


def test_new_reports_start_pending_with_no_attempts(positions, reports):
    store_position(positions)

    report = reports.all()[0]
    assert (report.status, report.attempts) == ("PENDING", 0)
    assert report.position_id == POSITION_ID
    assert report.account_id == "account-1"
