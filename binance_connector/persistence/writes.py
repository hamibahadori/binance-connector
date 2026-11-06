"""The write pattern every repository uses.

Each write opens its own short transaction, loads the row **fresh**, applies the change
(which may check a state transition against that fresh state), and commits. If another
thread or process committed in between (SQLAlchemy's version counter raises
``StaleDataError``), the whole read-check-write is retried on the new state.
"""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import StaleDataError

MAX_WRITE_ATTEMPTS = 3

RecordType = TypeVar("RecordType")
SnapshotType = TypeVar("SnapshotType")


def utc_now() -> datetime:
    return datetime.now(UTC)


def write_with_retry(
    session_factory: sessionmaker[Session],
    load: Callable[[Session], RecordType],
    change: Callable[[Session, RecordType], None],
    snapshot: Callable[[Session, RecordType], SnapshotType],
) -> SnapshotType:
    """Load, change and commit, retrying on a concurrent write; return ``snapshot(...)``."""
    for attempt in range(1, MAX_WRITE_ATTEMPTS + 1):
        with session_factory() as session:
            record = load(session)
            change(session, record)
            try:
                session.commit()
            except StaleDataError:
                session.rollback()
                if attempt == MAX_WRITE_ATTEMPTS:
                    raise
                continue
            return snapshot(session, record)
    raise AssertionError("unreachable")


def to_json(value: Any) -> str | None:
    """JSON text for storage; Decimals and datetimes become strings."""
    return None if value is None else json.dumps(value, default=str, sort_keys=True)


def from_json(text: str | None) -> Any:
    return None if text is None else json.loads(text)
