"""Error logs (spec §10): what went wrong, where, for which account/request/position.

Messages and context are passed through the secret redactor before they are stored, so a
credential that slipped into an exception message never reaches the database.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from binance_connector.persistence.models import ErrorLogRecord
from binance_connector.persistence.writes import to_json, utc_now
from binance_connector.security.redaction import REDACTOR, SecretRedactor

MAX_MESSAGE_LENGTH = 4000


@dataclass(frozen=True)
class ErrorLogEntry:
    occurred_at: datetime
    component: str
    message: str
    context: str | None
    account_id: str | None
    request_id: str | None
    position_id: str | None


class ErrorLogRepository:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        redactor: SecretRedactor = REDACTOR,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._session_factory = session_factory
        self._redactor = redactor
        self._clock = clock

    def record(
        self,
        *,
        component: str,
        message: str,
        context: dict[str, Any] | None = None,
        account_id: str | None = None,
        request_id: str | None = None,
        position_id: str | None = None,
    ) -> None:
        context_text = to_json(context)
        with self._session_factory() as session:
            session.add(ErrorLogRecord(
                occurred_at=self._clock(),
                component=component,
                message=self._redactor.redact(message)[:MAX_MESSAGE_LENGTH],
                context=None if context_text is None else self._redactor.redact(context_text),
                account_id=account_id,
                request_id=request_id,
                position_id=position_id,
            ))
            session.commit()

    def recent(self, limit: int = 50) -> list[ErrorLogEntry]:
        """Newest first."""
        with self._session_factory() as session:
            records = session.scalars(select(ErrorLogRecord).order_by(ErrorLogRecord.id.desc()).limit(limit))
            return [
                ErrorLogEntry(
                    occurred_at=record.occurred_at,
                    component=record.component,
                    message=record.message,
                    context=record.context,
                    account_id=record.account_id,
                    request_id=record.request_id,
                    position_id=record.position_id,
                )
                for record in records
            ]
