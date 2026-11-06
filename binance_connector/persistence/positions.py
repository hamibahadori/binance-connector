"""Positions, their Binance execution records, and their history.

Ported from v1's TradeRepository, now scoped to accounts:

* Every write runs on a **freshly loaded** row; state transitions are checked against that
  fresh state; each change appends a ``position_events`` row (see ``writes``).
* One active position per symbol **per account** (UNIQUE account_id + active_symbol).
* Binance orders live in ``execution_records``, one per role (entry, exit, take-profit,
  stop-loss). Their client ids are derived from the position id and stored *before* the
  order is sent (``prepare_execution``), so reconciliation can always ask Binance about
  them, even after a crash between "send" and "record the reply".
"""

import json
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from binance_connector.domain.lifecycle import (
    CloseReason,
    ExecutionRole,
    PositionState,
    check_position_transition,
    is_active,
)
from binance_connector.errors import PositionConflictError, PositionNotFoundError
from binance_connector.persistence.models import ExecutionRecordRow, PositionEventRecord, PositionRecord
from binance_connector.persistence.writes import utc_now, write_with_retry

NOT_WRITABLE = frozenset({"id", "position_id", "account_id", "version"})


def client_id_for(position_id: str, role: ExecutionRole) -> str:
    """``lx-<position_id>-<e|x|tp|sl>``: unique, and within Binance's 36-character limit."""
    return f"lx-{position_id}-{role.value}"


@dataclass(frozen=True)
class NewPosition:
    position_id: str
    account_id: str
    symbol: str
    direction: str
    quantity: Decimal
    requested_quantity: Decimal
    quantity_type: str
    leverage: int | None
    entry_type: str
    entry_price: Decimal | None
    take_profit_price: Decimal | None
    stop_loss_price: Decimal | None
    open_request_id: str


@dataclass(frozen=True)
class Execution:
    """One Binance order or algo order belonging to a position."""

    role: ExecutionRole
    kind: str
    client_id: str
    binance_id: int | None
    order_type: str
    side: str
    status: str
    price: Decimal | None
    quantity: Decimal | None
    executed_quantity: Decimal | None
    average_price: Decimal | None
    commission: Decimal | None
    commission_asset: str | None
    submitted_at: datetime | None
    updated_at: datetime
    last_error: str | None


@dataclass(frozen=True)
class Position:
    position_id: str
    account_id: str
    symbol: str
    direction: str
    quantity_type: str
    requested_quantity: Decimal
    quantity: Decimal
    leverage: int | None
    entry_type: str
    entry_price: Decimal | None
    take_profit_price: Decimal | None
    stop_loss_price: Decimal | None
    state: PositionState
    close_reason: CloseReason | None
    last_error: str | None
    open_request_id: str
    close_request_id: str | None
    entry_average_price: Decimal | None
    entry_filled_quantity: Decimal | None
    opened_at: datetime | None
    exit_average_price: Decimal | None
    closed_at: datetime | None
    realized_pnl: Decimal | None
    fees: Decimal | None
    created_at: datetime
    updated_at: datetime
    executions: dict[ExecutionRole, Execution]

    @property
    def commissions_so_far(self) -> Decimal | None:
        """The sum of every commission recorded on this position's orders; None if none is known."""
        commissions = [execution.commission for execution in self.executions.values()
                       if execution.commission is not None]
        return sum(commissions, Decimal(0)) if commissions else None


@dataclass(frozen=True)
class PositionEvent:
    occurred_at: datetime
    kind: str
    from_state: PositionState | None
    to_state: PositionState | None
    detail: str | None


class PositionRepository:
    """All database access for positions. Safe to use from several threads and processes."""

    def __init__(self, session_factory: sessionmaker[Session], clock: Callable[[], datetime] = utc_now) -> None:
        self._session_factory = session_factory
        self._clock = clock

    # --- creating -----------------------------------------------------------------------------

    def create(self, new_position: NewPosition) -> Position:
        """Store a new position as OPENING.

        Raises:
            PositionConflictError: the account already has an active position on the symbol.
        """
        now = self._clock()
        record = PositionRecord(
            **{field.name: getattr(new_position, field.name) for field in fields(NewPosition)},
            active_symbol=new_position.symbol,
            state=PositionState.OPENING.value,
            created_at=now,
            updated_at=now,
        )
        with self._session_factory() as session:
            try:
                session.add(record)
                session.flush()  # the history row references the position
                session.add(_event(new_position.position_id, now, "STATE_CHANGED", None, PositionState.OPENING, None))
                session.commit()
            except IntegrityError as error:
                session.rollback()
                raise PositionConflictError(new_position.account_id, new_position.symbol) from error
            return _snapshot(session, record)

    # --- reading ------------------------------------------------------------------------------

    def get(self, position_id: str) -> Position:
        with self._session_factory() as session:
            return _snapshot(session, self._load(session, position_id))

    def find_active(self, account_id: str, symbol: str) -> Position | None:
        return self._find_one(PositionRecord.account_id == account_id, PositionRecord.active_symbol == symbol)

    def list_active(self, account_id: str | None = None) -> list[Position]:
        conditions = [PositionRecord.active_symbol.is_not(None)]
        if account_id is not None:
            conditions.append(PositionRecord.account_id == account_id)
        with self._session_factory() as session:
            records = session.scalars(select(PositionRecord).where(*conditions).order_by(PositionRecord.id))
            return [_snapshot(session, record) for record in records]

    def list_for_account(self, account_id: str, state: PositionState | None = None) -> list[Position]:
        conditions = [PositionRecord.account_id == account_id]
        if state is not None:
            conditions.append(PositionRecord.state == state.value)
        with self._session_factory() as session:
            records = session.scalars(select(PositionRecord).where(*conditions).order_by(PositionRecord.id))
            return [_snapshot(session, record) for record in records]

    def find_by_client_id(self, client_id: str) -> tuple[Position, ExecutionRole] | None:
        """The position one of whose orders has ``client_id``, and that order's role."""
        with self._session_factory() as session:
            execution = session.scalar(select(ExecutionRecordRow).where(ExecutionRecordRow.client_id == client_id))
            if execution is None:
                return None
            record = self._load(session, execution.position_id)
            return _snapshot(session, record), ExecutionRole(execution.role)

    def events(self, position_id: str) -> list[PositionEvent]:
        with self._session_factory() as session:
            records = session.scalars(
                select(PositionEventRecord)
                .where(PositionEventRecord.position_id == position_id)
                .order_by(PositionEventRecord.id)
            )
            return [
                PositionEvent(
                    occurred_at=record.occurred_at,
                    kind=record.kind,
                    from_state=PositionState(record.from_state) if record.from_state else None,
                    to_state=PositionState(record.to_state) if record.to_state else None,
                    detail=record.detail,
                )
                for record in records
            ]

    # --- changing the position ----------------------------------------------------------------

    def transition(
        self,
        position_id: str,
        target_state: PositionState,
        event_detail: dict[str, Any] | None = None,
        **field_updates: Any,
    ) -> Position:
        """Move to ``target_state`` and set ``field_updates`` in one transaction.

        Raises:
            InvalidTransitionError: the current (fresh) state does not allow it.
        """

        def change(session: Session, record: PositionRecord) -> None:
            current_state = PositionState(record.state)
            check_position_transition(current_state, target_state)
            _apply(record, field_updates)
            record.state = target_state.value
            if not is_active(target_state):
                record.active_symbol = None
            record.updated_at = self._clock()
            session.add(_event(position_id, self._clock(), "STATE_CHANGED", current_state, target_state, event_detail))

        return self._write(position_id, change)

    def update_fields(self, position_id: str, event_detail: dict[str, Any] | None = None, **field_updates: Any) -> Position:
        """Set fields without a state change. Recorded as an event."""

        def change(session: Session, record: PositionRecord) -> None:
            _apply(record, field_updates)
            record.updated_at = self._clock()
            detail = {"fields": field_updates, **(event_detail or {})}
            session.add(_event(position_id, self._clock(), "FIELDS_UPDATED", None, None, detail))

        return self._write(position_id, change)

    def record_event(self, position_id: str, kind: str, detail: dict[str, Any]) -> None:
        """Append a history entry without changing the position (e.g. a Binance event)."""
        with self._session_factory() as session:
            session.add(_event(position_id, self._clock(), kind, None, None, detail))
            session.commit()

    # --- execution records --------------------------------------------------------------------

    def prepare_execution(
        self,
        position_id: str,
        role: ExecutionRole,
        *,
        kind: str,
        order_type: str,
        side: str,
        price: Decimal | None = None,
        quantity: Decimal | None = None,
    ) -> Execution:
        """Record an order that is about to be sent (status PENDING). Returns the existing
        record if this role was prepared before, so a retried step reuses the same client id."""
        with self._session_factory() as session:
            existing = self._find_execution(session, position_id, role)
            if existing is not None:
                return _execution(existing)
            row = ExecutionRecordRow(
                position_id=position_id,
                role=role.value,
                kind=kind,
                client_id=client_id_for(position_id, role),
                order_type=order_type,
                side=side,
                status="PENDING",
                price=price,
                quantity=quantity,
                updated_at=self._clock(),
            )
            session.add(row)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                return _execution(self._find_execution(session, position_id, role))
            return _execution(row)

    def update_execution(
        self,
        position_id: str,
        role: ExecutionRole,
        *,
        only_if_unset: tuple[str, ...] = (),
        status_only_if: str | None = None,
        event_detail: dict[str, Any] | None = None,
        **field_updates: Any,
    ) -> Position:
        """Update one execution record and return the position.

        ``only_if_unset`` fields are written only while still NULL. ``status`` is written only
        while the current status equals ``status_only_if`` (when given). Both let a later,
        more advanced status written by another thread win over an earlier one.
        """

        def change(session: Session, record: PositionRecord) -> None:
            row = self._find_execution(session, position_id, role)
            if row is None:
                raise KeyError(f"Position {position_id} has no {role.name} execution")
            updates = dict(field_updates)
            for field_name in only_if_unset:
                if getattr(row, field_name) is not None:
                    updates.pop(field_name, None)
            if status_only_if is not None and row.status != status_only_if:
                updates.pop("status", None)
            for field_name, value in updates.items():
                if field_name not in ExecutionRecordRow.__mapper__.columns or field_name in ("id", "position_id",
                                                                                             "role", "client_id"):
                    raise AttributeError(f"Executions have no writable field {field_name!r}")
                setattr(row, field_name, value)
            row.updated_at = self._clock()
            detail = {"execution": role.name, "fields": updates, **(event_detail or {})}
            session.add(_event(position_id, self._clock(), "EXECUTION_UPDATED", None, None, detail))

        return self._write(position_id, change)

    # --- helpers ------------------------------------------------------------------------------

    def _write(self, position_id: str, change: Callable[[Session, PositionRecord], None]) -> Position:
        return write_with_retry(self._session_factory, lambda session: self._load(session, position_id), change,
                                _snapshot)

    @staticmethod
    def _load(session: Session, position_id: str) -> PositionRecord:
        record = session.scalar(select(PositionRecord).where(PositionRecord.position_id == position_id))
        if record is None:
            raise PositionNotFoundError(position_id)
        return record

    @staticmethod
    def _find_execution(session: Session, position_id: str, role: ExecutionRole) -> ExecutionRecordRow | None:
        return session.scalar(
            select(ExecutionRecordRow).where(
                ExecutionRecordRow.position_id == position_id, ExecutionRecordRow.role == role.value
            )
        )

    def _find_one(self, *conditions) -> Position | None:
        with self._session_factory() as session:
            record = session.scalar(select(PositionRecord).where(*conditions))
            return None if record is None else _snapshot(session, record)


def _apply(record: PositionRecord, field_updates: dict[str, Any]) -> None:
    for field_name, value in field_updates.items():
        if field_name not in PositionRecord.__mapper__.columns or field_name in NOT_WRITABLE:
            raise AttributeError(f"Positions have no writable field {field_name!r}")
        setattr(record, field_name, value.value if isinstance(value, (PositionState, CloseReason)) else value)


def _event(position_id, occurred_at, kind, from_state, to_state, detail) -> PositionEventRecord:
    return PositionEventRecord(
        position_id=position_id,
        occurred_at=occurred_at,
        kind=kind,
        from_state=from_state.value if from_state else None,
        to_state=to_state.value if to_state else None,
        detail=None if detail is None else json.dumps(detail, default=str),
    )


def _execution(row: ExecutionRecordRow) -> Execution:
    values = {field.name: getattr(row, field.name) for field in fields(Execution)}
    values["role"] = ExecutionRole(row.role)
    return Execution(**values)


def _snapshot(session: Session, record: PositionRecord) -> Position:
    rows = session.scalars(select(ExecutionRecordRow).where(ExecutionRecordRow.position_id == record.position_id))
    executions = {ExecutionRole(row.role): _execution(row) for row in rows}
    values = {field.name: getattr(record, field.name) for field in fields(Position) if field.name != "executions"}
    values["state"] = PositionState(record.state)
    values["close_reason"] = CloseReason(record.close_reason) if record.close_reason else None
    return Position(**values, executions=executions)
