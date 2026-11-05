"""Column types that keep values exact across databases.

* ``DecimalString`` stores a Decimal as its exact text. SQLite has no decimal type and
  would otherwise round through a float; ``"0.0020"`` must come back as ``0.0020``.
* ``UtcDateTime`` stores UTC and always returns timezone-aware UTC datetimes. SQLite drops
  the timezone, so it is re-attached on load.
"""

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import DateTime, String
from sqlalchemy.types import TypeDecorator


class DecimalString(TypeDecorator):
    impl = String(40)
    cache_ok = True

    def process_bind_param(self, value: Decimal | None, dialect) -> str | None:
        return None if value is None else format(Decimal(value), "f")

    def process_result_value(self, value: str | None, dialect) -> Decimal | None:
        return None if value is None else Decimal(value)


class UtcDateTime(TypeDecorator):
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("UtcDateTime needs a timezone-aware datetime")
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect) -> datetime | None:
        return None if value is None else value.replace(tzinfo=UTC)
