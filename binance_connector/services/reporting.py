"""Delivering reports to Django (spec §6, §8, §9): at least once, in order per position.

The worker runs ``ReportDispatcher.run`` as a background task. Every second it walks the
PENDING reports, oldest first, and POSTs each one to ``DJANGO_REPORT_URL``:

* body: the report's JSON (``report_id``, ``event``, ``occurred_at``, then spec §6's fields);
* headers: ``Content-Type: application/json``, ``X-Report-Id``, ``X-Report-Event``, and the
  configured auth header (``DJANGO_REPORT_AUTH_HEADER: DJANGO_REPORT_AUTH_VALUE``);
* any 2xx means delivered. Anything else (another status, a timeout, a refused connection)
  is retried after 5, 10, 20 … s (capped at 5 minutes). After ``DJANGO_REPORT_MAX_ATTEMPTS``
  attempts the report is FAILED and written to error_logs.

**Order per position:** a report waiting for a retry holds back the later reports of the
same position (or request), so Django never sees a position's CLOSED before its OPEN.
Other positions are not held back. Delivery is at least once: a report can arrive twice
(e.g. Django stored it but its 2xx was lost), so Django should drop repeats by ``report_id``.

Without ``DJANGO_REPORT_URL`` the dispatcher stays idle and reports wait, still stored.
"""

import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta

import requests

from binance_connector.config import Settings
from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.reports import Report, ReportRepository
from binance_connector.persistence.writes import utc_now

logger = logging.getLogger(__name__)

POLL_SECONDS = 1.0
FIRST_RETRY_SECONDS = 5
MAX_RETRY_SECONDS = 300


class ReportDispatcher:
    def __init__(
        self,
        *,
        reports: ReportRepository,
        error_logs: ErrorLogRepository,
        settings: Settings,
        session: requests.Session | None = None,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._reports = reports
        self._error_logs = error_logs
        self._settings = settings
        self._session = session or requests.Session()
        self._clock = clock
        self._sleep = sleep
        self._warned_about_missing_url = False

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Deliver due reports every second until ``should_stop()``."""
        while not should_stop():
            try:
                self.dispatch_due()
            except Exception:  # noqa: BLE001 - delivery must keep going
                logger.exception("Report delivery pass failed")
            self._sleep(POLL_SECONDS)

    def dispatch_due(self) -> int:
        """Deliver every report that is due and not held back. Returns how many were delivered."""
        if not self._settings.django_report_url:
            if not self._warned_about_missing_url:
                logger.warning("DJANGO_REPORT_URL is not set: reports are stored but not sent")
                self._warned_about_missing_url = True
            return 0
        now = self._clock()
        held_back: set[str] = set()
        delivered = 0
        for report in self._reports.pending():
            ordering_key = report.position_id or report.request_id or report.report_id
            if ordering_key in held_back:
                continue
            if report.next_attempt_at is not None and report.next_attempt_at > now:
                held_back.add(ordering_key)
                continue
            if self._deliver(report):
                delivered += 1
            else:
                held_back.add(ordering_key)
        return delivered

    def _deliver(self, report: Report) -> bool:
        headers = {
            "Content-Type": "application/json",
            "X-Report-Id": report.report_id,
            "X-Report-Event": report.event_type,
        }
        if self._settings.django_report_auth_value:
            headers[self._settings.django_report_auth_header] = self._settings.django_report_auth_value
        try:
            response = self._session.post(self._settings.django_report_url, data=report.payload.encode("utf-8"),
                                          headers=headers, timeout=self._settings.django_report_timeout_seconds)
        except requests.RequestException as error:
            return self._failed(report, f"{type(error).__name__}: {error}")
        if 200 <= response.status_code < 300:
            self._reports.mark_sent(report.report_id)
            return True
        return self._failed(report, f"Django answered HTTP {response.status_code}: {response.text[:200]}")

    def _failed(self, report: Report, error: str) -> bool:
        attempts = report.attempts + 1
        if attempts >= self._settings.django_report_max_attempts:
            self._reports.mark_failed(report.report_id, error=error)
            logger.error("Report %s (%s) failed after %s attempts: %s", report.report_id, report.event_type,
                         attempts, error)
            self._error_logs.record(
                component="reporting_service",
                message=f"Report {report.event_type} could not be delivered after {attempts} attempts: {error}",
                context={"report_id": report.report_id, "event": report.event_type},
                account_id=report.account_id, request_id=report.request_id, position_id=report.position_id,
            )
        else:
            delay = min(FIRST_RETRY_SECONDS * 2 ** (attempts - 1), MAX_RETRY_SECONDS)
            self._reports.mark_retry(report.report_id, error=error,
                                     next_attempt_at=self._clock() + timedelta(seconds=delay))
        return False
