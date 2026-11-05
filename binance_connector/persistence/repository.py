"""Reading and changing trades.

Every write opens its own short transaction on a **freshly loaded** row, checks the state
transition against that fresh state, records a ``trade_events`` row, and commits. Callers
get back an immutable ``Trade`` snapshot, never a live ORM object, so nothing can be
changed by accident outside this module.

Two processes (API and worker) write the same rows. The fresh read plus the state check
decides most races ("the worker already closed it"); SQLAlchemy's version counter catches
the rest (two writers that read the same version), and the write is retried on the new
state a few times before giving up.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import StaleDataError

from binance_connector.domain.trade_state import CloseReason, TradeState, check_transition, is_active
from binance_connector.errors import TradeConflictError, TradeNotFoundError
from binance_connector.persistence.models import TradeEventRecord, TradeRecord

MAX_WRITE_ATTEMPTS = 3

CLIENT_ID_ROLES = {
    "entry_client_order_id": "entry",
    "exit_client_order_id": "exit",
    "take_profit_client_algo_id": "take_profit",
    "stop_loss_client_algo_id": "stop_loss",
}


@dataclass(frozen=True)
class NewTrade:
    """What the API layer knows when a Strategy request arrives."""

    trade_id: str
    symbol: str
    direction: str
    quantity: Decimal
    leverage: int | None = None
    take_profit_price: Decimal | None = None
    stop_loss_price: Decimal | None = None
    strategy_request_id: str | None = None


@dataclass(frozen=True)
class Trade:
    """An immutable snapshot of one trade row."""

    trade_id: str
    strategy_request_id: str | None
    symbol: str
    direction: str
    quantity: Decimal
    leverage: int | None
    take_profit_price: Decimal | None
    stop_loss_price: Decimal | None
    state: TradeState
    close_reason: CloseReason | None
    requested_close_reason: CloseReason | None
    last_error: str | None
    entry_client_order_id: str
    entry_order_id: int | None
    entry_price: Decimal | None
    entry_quantity: Decimal | None
    opened_at: datetime | None
    exit_client_order_id: str
    exit_order_id: int | None
    exit_price: Decimal | None
    closed_at: datetime | None
    take_profit_client_algo_id: str
    take_profit_algo_id: int | None
    take_profit_status: str | None
    stop_loss_client_algo_id: str
    stop_loss_algo_id: int | None
    stop_loss_status: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def wants_take_profit(self) -> bool:
        return self.take_profit_price is not None

    @property
    def wants_stop_loss(self) -> bool:
        return self.stop_loss_price is not None


@dataclass(frozen=True)
class TradeEvent:
    """One row of a trade's history."""

    occurred_at: datetime
    kind: str
    from_state: TradeState | None
    to_state: TradeState | None
    detail: str | None


def client_ids_for(trade_id: str) -> dict[str, str]:
    """The Binance client ids of a trade's four orders, derived from its id.

    Deriving them (instead of generating them at send time) means they are stored before
    anything is sent, so reconciliation can always ask Binance about them, even after a
    crash between "send" and "store the reply". All fit Binance's 36-character limit.
    """
    return {
        "entry_client_order_id": f"lx-{trade_id}-e",
        "exit_client_order_id": f"lx-{trade_id}-x",
        "take_profit_client_algo_id": f"lx-{trade_id}-tp",
        "stop_loss_client_algo_id": f"lx-{trade_id}-sl",
    }


def utc_now() -> datetime:
    return datetime.now(UTC)


class TradeRepository:
    """All database access for trades. Safe to use from several processes at once."""

    def __init__(self, session_factory: sessionmaker[Session], clock: Callable[[], datetime] = utc_now) -> None:
        self._session_factory = session_factory
        self._clock = clock

    # --- creating ---------------------------------------------------------------------------

    def create(self, new_trade: NewTrade) -> Trade:
        """Store a new trade as RECEIVED.

        Raises:
            TradeConflictError: the symbol already has an active trade, or the
                strategy_request_id was already used.
        """
        now = self._clock()
        record = TradeRecord(
            **{field.name: getattr(new_trade, field.name) for field in fields(NewTrade)},
            **client_ids_for(new_trade.trade_id),
            active_symbol=new_trade.symbol,
            state=TradeState.RECEIVED.value,
            created_at=now,
            updated_at=now,
        )
        with self._session_factory() as session:
            try:
                session.add(record)
                # Flush the trade first: the history row references it, and without an ORM
                # relationship SQLAlchemy would not order the two inserts by themselves.
                session.flush()
                session.add(_event_record(new_trade.trade_id, now, "STATE_CHANGED", None, TradeState.RECEIVED, None))
                session.commit()
            except IntegrityError as error:
                session.rollback()
                raise self._conflict_for(new_trade) from error
            return _snapshot(record)

    def _conflict_for(self, new_trade: NewTrade) -> TradeConflictError:
        if new_trade.strategy_request_id and self.find_by_strategy_request_id(new_trade.strategy_request_id):
            return TradeConflictError(f"strategy_request_id {new_trade.strategy_request_id} was already used.")
        return TradeConflictError(f"{new_trade.symbol} already has an active trade.")

    # --- reading ----------------------------------------------------------------------------

    def get(self, trade_id: str) -> Trade:
        """Raises TradeNotFoundError when there is no such trade."""
        with self._session_factory() as session:
            return _snapshot(self._load(session, trade_id))

    def find_by_strategy_request_id(self, strategy_request_id: str) -> Trade | None:
        return self._find_one(TradeRecord.strategy_request_id == strategy_request_id)

    def find_active_by_symbol(self, symbol: str) -> Trade | None:
        return self._find_one(TradeRecord.active_symbol == symbol)

    def list_active(self) -> list[Trade]:
        with self._session_factory() as session:
            records = session.scalars(
                select(TradeRecord).where(TradeRecord.active_symbol.is_not(None)).order_by(TradeRecord.id)
            )
            return [_snapshot(record) for record in records]

    def find_by_client_id(self, client_id: str) -> tuple[Trade, str] | None:
        """Find the trade one of whose four orders has ``client_id``, and that order's role.

        Roles: ``entry``, ``exit``, ``take_profit``, ``stop_loss``.
        """
        columns = [getattr(TradeRecord, column_name) for column_name in CLIENT_ID_ROLES]
        trade = self._find_one(or_(*(column == client_id for column in columns)))
        if trade is None:
            return None
        role = next(role for column_name, role in CLIENT_ID_ROLES.items() if getattr(trade, column_name) == client_id)
        return trade, role

    def events(self, trade_id: str) -> list[TradeEvent]:
        """The trade's history, oldest first."""
        with self._session_factory() as session:
            records = session.scalars(
                select(TradeEventRecord).where(TradeEventRecord.trade_id == trade_id).order_by(TradeEventRecord.id)
            )
            return [
                TradeEvent(
                    occurred_at=record.occurred_at,
                    kind=record.kind,
                    from_state=TradeState(record.from_state) if record.from_state else None,
                    to_state=TradeState(record.to_state) if record.to_state else None,
                    detail=record.detail,
                )
                for record in records
            ]

    # --- changing ---------------------------------------------------------------------------

    def transition(
        self,
        trade_id: str,
        target_state: TradeState,
        event_detail: dict[str, Any] | None = None,
        **field_updates: Any,
    ) -> Trade:
        """Move the trade to ``target_state`` and set ``field_updates`` in one transaction.

        Raises:
            InvalidTransitionError: the trade's current (fresh) state does not allow it.
            AttributeError: a field name does not exist.
        """

        def change(record: TradeRecord) -> TradeEventRecord:
            current_state = TradeState(record.state)
            check_transition(current_state, target_state)
            _apply_fields(record, field_updates)
            record.state = target_state.value
            if not is_active(target_state):
                record.active_symbol = None
            return _event_record(trade_id, self._clock(), "STATE_CHANGED", current_state, target_state, event_detail)

        return self._write(trade_id, change)

    def update_fields(self, trade_id: str, event_detail: dict[str, Any] | None = None, **field_updates: Any) -> Trade:
        """Set fields without a state change (e.g. an algo order's status). Recorded as an event."""

        def change(record: TradeRecord) -> TradeEventRecord:
            _apply_fields(record, field_updates)
            detail = {"fields": field_updates, **(event_detail or {})}
            return _event_record(trade_id, self._clock(), "FIELDS_UPDATED", None, None, detail)

        return self._write(trade_id, change)

    def record_event(self, trade_id: str, kind: str, detail: dict[str, Any]) -> None:
        """Append a history entry without changing the trade (e.g. a Binance event received)."""
        with self._session_factory() as session:
            session.add(_event_record(trade_id, self._clock(), kind, None, None, detail))
            session.commit()

    def _write(self, trade_id: str, change: Callable[[TradeRecord], TradeEventRecord]) -> Trade:
        for attempt in range(1, MAX_WRITE_ATTEMPTS + 1):
            with self._session_factory() as session:
                record = self._load(session, trade_id)
                event_record = change(record)
                record.updated_at = self._clock()
                session.add(event_record)
                try:
                    session.commit()
                except StaleDataError:
                    # Another process committed between our read and our write: re-read and
                    # re-check against its result.
                    session.rollback()
                    if attempt == MAX_WRITE_ATTEMPTS:
                        raise
                    continue
                return _snapshot(record)
        raise AssertionError("unreachable")

    def _load(self, session: Session, trade_id: str) -> TradeRecord:
        record = session.scalar(select(TradeRecord).where(TradeRecord.trade_id == trade_id))
        if record is None:
            raise TradeNotFoundError(trade_id)
        return record

    def _find_one(self, condition) -> Trade | None:
        with self._session_factory() as session:
            record = session.scalar(select(TradeRecord).where(condition))
            return None if record is None else _snapshot(record)


def _apply_fields(record: TradeRecord, field_updates: dict[str, Any]) -> None:
    for field_name, value in field_updates.items():
        if field_name not in TradeRecord.__mapper__.columns or field_name in ("id", "trade_id", "version"):
            raise AttributeError(f"Trades have no writable field {field_name!r}")
        setattr(record, field_name, value.value if isinstance(value, (TradeState, CloseReason)) else value)


def _event_record(
    trade_id: str,
    occurred_at: datetime,
    kind: str,
    from_state: TradeState | None,
    to_state: TradeState | None,
    detail: dict[str, Any] | None,
) -> TradeEventRecord:
    return TradeEventRecord(
        trade_id=trade_id,
        occurred_at=occurred_at,
        kind=kind,
        from_state=from_state.value if from_state else None,
        to_state=to_state.value if to_state else None,
        detail=None if detail is None else json.dumps(detail, default=str),
    )


def _snapshot(record: TradeRecord) -> Trade:
    values = {field.name: getattr(record, field.name) for field in fields(Trade)}
    values["state"] = TradeState(record.state)
    values["close_reason"] = CloseReason(record.close_reason) if record.close_reason else None
    values["requested_close_reason"] = (
        CloseReason(record.requested_close_reason) if record.requested_close_reason else None
    )
    return Trade(**values)
