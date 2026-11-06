"""Reports for Django (spec §6): a transactional outbox, and its delivery queue.

``ReportOutbox`` writes a report row **in the same database transaction** as the change it
reports, so a report can never be lost to a crash, and a change that is rolled back never
reports anything. The repositories call it:

* requests: REQUEST_ACCEPTED, REQUEST_REJECTED
* positions: POSITION_OPENING / _OPEN / _CLOSING / _CLOSED / _FAILED / _ERROR on every state
  change, and PROTECTION_UPDATED when a take-profit or stop-loss changes status

Report payloads carry spec §6's fields (see ``presentation.position_view``) plus
``report_id`` (for Django to drop duplicates), ``event`` and ``occurred_at``.

``ReportRepository`` is what the dispatcher (``services.reporting``) works from:
PENDING → SENT, or → FAILED after the last attempt.
"""

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from binance_connector.domain.lifecycle import RequestStatus
from binance_connector.persistence.market_data import MarketPrice
from binance_connector.persistence.models import AccountRecord, MarketPriceRecord, ReportRecord
from binance_connector.persistence.positions import Position
from binance_connector.persistence.requests import Request
from binance_connector.persistence.writes import utc_now
from binance_connector.presentation import position_view

REPORTED_REQUEST_STATUSES = frozenset({RequestStatus.ACCEPTED, RequestStatus.REJECTED})


@dataclass(frozen=True)
class Report:
    sequence: int
    """Database order; the WebSocket manager follows new reports by it."""
    report_id: str
    account_id: str | None
    request_id: str | None
    position_id: str | None
    event_type: str
    payload: str
    status: str
    attempts: int
    next_attempt_at: datetime | None
    last_error: str | None
    created_at: datetime
    sent_at: datetime | None


class ReportOutbox:
    """Adds report rows to the caller's session. Never commits: the caller's change does."""

    def __init__(self, clock: Callable[[], datetime] = utc_now) -> None:
        self._clock = clock

    def request_decided(self, session: Session, request: Request) -> None:
        if request.status not in REPORTED_REQUEST_STATUSES:
            return
        body = {
            "request_id": request.request_id,
            "user_id": request.user_id,
            "account_id": request.account_id,
            "action": request.action.value,
            "status": request.status.value,
            "position_id": request.position_id,
            "reasons": request.rejection_reasons or [],
            "times": {"received": _time(request.received_at), "decided": _time(request.decided_at)},
        }
        self._add(session, f"REQUEST_{request.status.value}", body, account_id=request.account_id,
                  request_id=request.request_id, position_id=request.position_id)

    def position_changed(self, session: Session, position: Position, event_type: str) -> None:
        user_id = session.scalar(select(AccountRecord.user_id).where(AccountRecord.account_id == position.account_id))
        price_record = session.get(MarketPriceRecord, position.symbol)
        market_price = None if price_record is None else MarketPrice(
            price_record.symbol, price_record.mark_price, price_record.event_time, price_record.updated_at)
        body = position_view(position, user_id=user_id, market_price=market_price)
        self._add(session, event_type, body, account_id=position.account_id, request_id=position.open_request_id,
                  position_id=position.position_id)

    def _add(self, session: Session, event_type: str, body: dict[str, Any], **ids: str | None) -> None:
        report_id = str(uuid.uuid4())
        now = self._clock()
        payload = {"report_id": report_id, "event": event_type, "occurred_at": now.isoformat(), **body}
        session.add(ReportRecord(report_id=report_id, event_type=event_type, payload=json.dumps(payload, default=str),
                                 status="PENDING", attempts=0, created_at=now, **ids))


class ReportRepository:
    def __init__(self, session_factory: sessionmaker[Session], clock: Callable[[], datetime] = utc_now) -> None:
        self._session_factory = session_factory
        self._clock = clock

    def pending(self, limit: int = 500) -> list[Report]:
        """PENDING reports, oldest first (the dispatcher decides which are due)."""
        return self._query(select(ReportRecord).where(ReportRecord.status == "PENDING")
                           .order_by(ReportRecord.id).limit(limit))

    def all(self) -> list[Report]:
        return self._query(select(ReportRecord).order_by(ReportRecord.id))

    def after(self, sequence: int, limit: int = 500) -> list[Report]:
        """Reports newer than ``sequence``, oldest first (whatever their delivery status)."""
        return self._query(select(ReportRecord).where(ReportRecord.id > sequence).order_by(ReportRecord.id)
                           .limit(limit))

    def last_sequence(self) -> int:
        with self._session_factory() as session:
            return session.scalar(select(func.max(ReportRecord.id))) or 0

    def counts(self) -> dict[str, int]:
        with self._session_factory() as session:
            rows = session.execute(select(ReportRecord.status, func.count()).group_by(ReportRecord.status))
            return {status: count for status, count in rows}

    def mark_sent(self, report_id: str) -> None:
        self._update(report_id, status="SENT", sent_at=self._clock(), next_attempt_at=None, attempts_increment=1)

    def mark_retry(self, report_id: str, *, error: str, next_attempt_at: datetime) -> None:
        self._update(report_id, last_error=error, next_attempt_at=next_attempt_at, attempts_increment=1)

    def mark_failed(self, report_id: str, *, error: str) -> None:
        self._update(report_id, status="FAILED", last_error=error, next_attempt_at=None, attempts_increment=1)

    def _update(self, report_id: str, attempts_increment: int = 0, **values: Any) -> None:
        with self._session_factory() as session:
            record = session.scalar(select(ReportRecord).where(ReportRecord.report_id == report_id))
            for field_name, value in values.items():
                setattr(record, field_name, value)
            record.attempts += attempts_increment
            session.commit()

    def _query(self, statement) -> list[Report]:
        with self._session_factory() as session:
            return [_snapshot(record) for record in session.scalars(statement)]


def _snapshot(record: ReportRecord) -> Report:
    values = {field.name: getattr(record, field.name) for field in fields(Report) if field.name != "sequence"}
    return Report(sequence=record.id, **values)


def _time(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()
