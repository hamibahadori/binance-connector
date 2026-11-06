"""Requests from Django: their lifecycle, duplicate detection, and the worker's queue.

* ``create`` stores a request as RECEIVED. A request_id seen before raises
  DuplicateRequestError carrying the original, so the API can apply duplicate option 2
  (same body → original result; different body → 409).
* ACCEPTED requests enter the queue (``processing_state`` PENDING). The worker ``claim``s
  one atomically, so two execution threads can never run the same request, and marks it
  DONE when finished. Claims left IN_PROGRESS by a crash are released on start-up; the
  execution steps are written to be safe to run again (see the execution manager).
* The stored ``payload`` never contains credentials: those live encrypted on the account.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from binance_connector.domain.lifecycle import (
    ProcessingState,
    RequestAction,
    RequestStatus,
    check_request_transition,
)
from binance_connector.errors import DuplicateRequestError, RequestNotFoundError
from binance_connector.persistence.models import RequestRecord
from binance_connector.persistence.writes import from_json, to_json, utc_now, write_with_retry

DECIDED_STATUSES = frozenset({RequestStatus.ACCEPTED, RequestStatus.REJECTED})


@dataclass(frozen=True)
class NewRequest:
    request_id: str
    account_id: str
    user_id: str
    action: RequestAction
    payload: dict[str, Any]
    payload_hash: str
    requested_at: datetime | None


@dataclass(frozen=True)
class Request:
    request_id: str
    account_id: str
    user_id: str
    action: RequestAction
    status: RequestStatus
    processing_state: ProcessingState | None
    payload: dict[str, Any]
    payload_hash: str
    rejection_reasons: list[str] | None
    position_id: str | None
    requested_at: datetime | None
    received_at: datetime
    decided_at: datetime | None
    processed_at: datetime | None
    response: dict[str, Any] | None


class RequestRepository:
    def __init__(self, session_factory: sessionmaker[Session], clock: Callable[[], datetime] = utc_now) -> None:
        self._session_factory = session_factory
        self._clock = clock

    def create(self, new_request: NewRequest) -> Request:
        """Store a new request as RECEIVED.

        Raises:
            DuplicateRequestError: the request_id was used before (``.existing`` is the original).
        """
        record = RequestRecord(
            request_id=new_request.request_id,
            account_id=new_request.account_id,
            user_id=new_request.user_id,
            action=new_request.action.value,
            status=RequestStatus.RECEIVED.value,
            payload=to_json(new_request.payload),
            payload_hash=new_request.payload_hash,
            requested_at=new_request.requested_at,
            received_at=self._clock(),
        )
        with self._session_factory() as session:
            session.add(record)
            try:
                session.commit()
            except IntegrityError as error:
                session.rollback()
                existing = self.find(new_request.request_id)
                if existing is None:
                    raise
                raise DuplicateRequestError(existing) from error
            return _snapshot(record)

    def get(self, request_id: str) -> Request:
        request = self.find(request_id)
        if request is None:
            raise RequestNotFoundError(request_id)
        return request

    def find(self, request_id: str) -> Request | None:
        with self._session_factory() as session:
            record = session.scalar(select(RequestRecord).where(RequestRecord.request_id == request_id))
            return None if record is None else _snapshot(record)

    def transition(
        self,
        request_id: str,
        target_status: RequestStatus,
        *,
        rejection_reasons: list[str] | None = None,
        position_id: str | None = None,
        response: dict[str, Any] | None = None,
    ) -> Request:
        """Move the request to ``target_status``; ACCEPTED also queues it for the worker.

        Raises:
            InvalidTransitionError: the current status does not allow it.
        """

        def change(_session: Session, record: RequestRecord) -> None:
            check_request_transition(RequestStatus(record.status), target_status)
            record.status = target_status.value
            if target_status in DECIDED_STATUSES:
                record.decided_at = self._clock()
            if target_status is RequestStatus.ACCEPTED:
                record.processing_state = ProcessingState.PENDING.value
            if rejection_reasons is not None:
                record.rejection_reasons = to_json(rejection_reasons)
            if position_id is not None:
                record.position_id = position_id
            if response is not None:
                record.response = to_json(response)

        return write_with_retry(self._session_factory, lambda session: self._load(session, request_id), change,
                                lambda _session, record: _snapshot(record))

    def set_response(self, request_id: str, response: dict[str, Any]) -> Request:
        """Replace the stored response (returned again for duplicates)."""

        def change(_session: Session, record: RequestRecord) -> None:
            record.response = to_json(response)

        return write_with_retry(self._session_factory, lambda session: self._load(session, request_id), change,
                                lambda _session, record: _snapshot(record))

    # --- worker queue -------------------------------------------------------------------------

    def pending(self, limit: int = 100) -> list[Request]:
        """ACCEPTED requests not yet claimed, oldest first."""
        with self._session_factory() as session:
            records = session.scalars(
                select(RequestRecord)
                .where(RequestRecord.processing_state == ProcessingState.PENDING.value)
                .order_by(RequestRecord.id)
                .limit(limit)
            )
            return [_snapshot(record) for record in records]

    def claim(self, request_id: str) -> bool:
        """Atomically move PENDING → IN_PROGRESS. False if someone else already claimed it."""
        return self._compare_and_set(request_id, ProcessingState.PENDING, ProcessingState.IN_PROGRESS)

    def mark_processed(self, request_id: str) -> Request:
        def change(_session: Session, record: RequestRecord) -> None:
            record.processing_state = ProcessingState.DONE.value
            record.processed_at = self._clock()

        return write_with_retry(self._session_factory, lambda session: self._load(session, request_id), change,
                                lambda _session, record: _snapshot(record))

    def release_in_progress(self) -> int:
        """Return requests a crashed worker left IN_PROGRESS to the queue. Returns how many."""
        with self._session_factory() as session:
            result = session.execute(
                update(RequestRecord)
                .where(RequestRecord.processing_state == ProcessingState.IN_PROGRESS.value)
                .values(processing_state=ProcessingState.PENDING.value, version=RequestRecord.version + 1)
            )
            session.commit()
            return result.rowcount

    def _compare_and_set(self, request_id: str, expected: ProcessingState, new: ProcessingState) -> bool:
        with self._session_factory() as session:
            result = session.execute(
                update(RequestRecord)
                .where(RequestRecord.request_id == request_id, RequestRecord.processing_state == expected.value)
                .values(processing_state=new.value, version=RequestRecord.version + 1)
            )
            session.commit()
            return result.rowcount == 1

    @staticmethod
    def _load(session: Session, request_id: str) -> RequestRecord:
        record = session.scalar(select(RequestRecord).where(RequestRecord.request_id == request_id))
        if record is None:
            raise RequestNotFoundError(request_id)
        return record


def _snapshot(record: RequestRecord) -> Request:
    return Request(
        request_id=record.request_id,
        account_id=record.account_id,
        user_id=record.user_id,
        action=RequestAction(record.action),
        status=RequestStatus(record.status),
        processing_state=ProcessingState(record.processing_state) if record.processing_state else None,
        payload=from_json(record.payload),
        payload_hash=record.payload_hash,
        rejection_reasons=from_json(record.rejection_reasons),
        position_id=record.position_id,
        requested_at=record.requested_at,
        received_at=record.received_at,
        decided_at=record.decided_at,
        processed_at=record.processed_at,
        response=from_json(record.response),
    )
