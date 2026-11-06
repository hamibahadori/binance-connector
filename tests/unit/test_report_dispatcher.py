"""Unit tests for delivering reports to Django: order, retries, failure, auth."""

from dataclasses import replace
from datetime import timedelta

import pytest
import requests
import responses

from binance_connector.domain.lifecycle import PositionState
from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.reports import ReportOutbox, ReportRepository
from binance_connector.persistence.writes import utc_now
from binance_connector.security.redaction import SecretRedactor
from binance_connector.services.reporting import ReportDispatcher
from tests.support import POSITION_ID, store_position

DJANGO_URL = "https://django.test/api/execution-reports"


class ShiftedClock:
    def __init__(self) -> None:
        self.offset = timedelta()

    def __call__(self):
        return utc_now() + self.offset


@pytest.fixture
def clock() -> ShiftedClock:
    return ShiftedClock()


@pytest.fixture
def reports(session_factory) -> ReportRepository:
    return ReportRepository(session_factory)


@pytest.fixture
def error_logs(session_factory) -> ErrorLogRepository:
    return ErrorLogRepository(session_factory, redactor=SecretRedactor())


@pytest.fixture
def positions(session_factory, accounts) -> PositionRepository:
    return PositionRepository(session_factory, outbox=ReportOutbox())


@pytest.fixture
def report_settings(settings):
    return replace(settings, django_report_url=DJANGO_URL, django_report_auth_header="Authorization",
                   django_report_auth_value="Bearer django-token-123", django_report_max_attempts=3)


@pytest.fixture
def dispatcher(reports, error_logs, report_settings, clock) -> ReportDispatcher:
    return ReportDispatcher(reports=reports, error_logs=error_logs, settings=report_settings, clock=clock)


def lifecycle(positions, position_id=POSITION_ID, account_id="account-1"):
    store_position(positions, position_id=position_id, account_id=account_id)
    positions.transition(position_id, PositionState.OPEN)


@responses.activate
def test_pending_reports_are_posted_in_order_with_auth_and_marked_sent(dispatcher, positions, reports):
    responses.post(DJANGO_URL, status=200)
    lifecycle(positions)

    sent_count = dispatcher.dispatch_due()

    assert sent_count == 2
    sent_events = [call.request for call in responses.calls]
    assert [request.headers["X-Report-Event"] for request in sent_events] == ["POSITION_OPENING", "POSITION_OPEN"]
    assert sent_events[0].headers["Authorization"] == "Bearer django-token-123"
    assert sent_events[0].headers["X-Report-Id"] == reports.all()[0].report_id
    assert sent_events[0].headers["Content-Type"] == "application/json"
    assert {report.status for report in reports.all()} == {"SENT"}
    assert all(report.sent_at is not None for report in reports.all())


@responses.activate
def test_a_rejected_delivery_is_retried_later_with_backoff(dispatcher, positions, reports, clock):
    responses.post(DJANGO_URL, status=503)
    store_position(positions)

    dispatcher.dispatch_due()
    report = reports.all()[0]
    assert (report.status, report.attempts) == ("PENDING", 1)
    assert "503" in report.last_error
    assert report.next_attempt_at > utc_now()

    assert dispatcher.dispatch_due() == 0  # not due yet
    assert len(responses.calls) == 1

    clock.offset = timedelta(minutes=10)
    responses.upsert("POST", DJANGO_URL, status=200)
    assert dispatcher.dispatch_due() == 1


@responses.activate
def test_a_network_failure_is_retried_too(dispatcher, positions, reports):
    responses.post(DJANGO_URL, body=requests.exceptions.ConnectionError("refused"))
    store_position(positions)

    dispatcher.dispatch_due()

    assert reports.all()[0].attempts == 1
    assert "ConnectionError" in reports.all()[0].last_error


@responses.activate
def test_after_the_last_attempt_a_report_fails_and_is_logged(dispatcher, positions, reports, error_logs, clock):
    responses.post(DJANGO_URL, status=500)
    store_position(positions)

    for attempt in range(3):
        clock.offset = timedelta(hours=attempt)
        dispatcher.dispatch_due()

    report = reports.all()[0]
    assert (report.status, report.attempts) == ("FAILED", 3)
    log_entry = error_logs.recent()[0]
    assert log_entry.component == "reporting_service"
    assert log_entry.position_id == POSITION_ID


@responses.activate
def test_a_stuck_report_holds_back_its_own_position_only(dispatcher, positions, reports):
    responses.post(DJANGO_URL, status=200)
    lifecycle(positions, position_id="p00000000000000000000001", account_id="account-1")
    lifecycle(positions, position_id="p00000000000000000000002", account_id="account-2")
    first_report = reports.all()[0]
    reports.mark_retry(first_report.report_id, error="earlier failure", next_attempt_at=utc_now() + timedelta(hours=1))

    dispatcher.dispatch_due()

    sent = [(report.position_id, report.event_type) for report in reports.all() if report.status == "SENT"]
    assert sent == [("p00000000000000000000002", "POSITION_OPENING"), ("p00000000000000000000002", "POSITION_OPEN")]


@responses.activate
def test_without_a_django_url_nothing_is_sent_and_reports_wait(reports, error_logs, settings, positions, clock):
    dispatcher = ReportDispatcher(reports=reports, error_logs=error_logs, settings=settings, clock=clock)
    store_position(positions)

    assert dispatcher.dispatch_due() == 0
    assert reports.all()[0].status == "PENDING"
    assert len(responses.calls) == 0
