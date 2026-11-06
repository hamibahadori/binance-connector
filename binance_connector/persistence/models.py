"""ORM models for the multi-account execution service (spec §10).

=====================  ==================================================================
Table                  Holds
=====================  ==================================================================
accounts               one row per Django account: owning user, credentials (encrypted)
requests               every request Django sent, its status, and the stored response
positions              one row per position: what was asked, what was held, its state
execution_records      every Binance order / algo order of a position (entry, exit, TP, SL)
position_events        append-only history of every position change
reports                outbox of reports for Django (delivered in phase 6)
error_logs             errors with their account/request/position ids, secrets redacted
market_prices          latest mark price per symbol (written by the worker)
exchange_rules         cached Binance trading rules per symbol
=====================  ==================================================================

Concurrency rules carried over from v1:

* ``positions.active_symbol`` equals ``symbol`` while the position is active and NULL once
  terminal; UNIQUE (account_id, active_symbol) enforces one active position per symbol
  per account in the database itself.
* ``version`` columns are SQLAlchemy's optimistic-locking counters.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from binance_connector.persistence.types import DecimalString, UtcDateTime


class Base(DeclarativeBase):
    pass


class AccountRecord(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    api_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    api_key_fingerprint: Mapped[str] = mapped_column(String(12), nullable=False)
    credentials_updated_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    __mapper_args__ = {"version_id_col": version}


class RequestRecord(Base):
    __tablename__ = "requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.account_id"), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(15), nullable=False)
    processing_state: Mapped[str | None] = mapped_column(String(15), index=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rejection_reasons: Mapped[str | None] = mapped_column(Text)
    position_id: Mapped[str | None] = mapped_column(String(24), index=True)
    requested_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    received_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    decided_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    processed_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    response: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    __mapper_args__ = {"version_id_col": version}


class PositionRecord(Base):
    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("account_id", "active_symbol", name="uq_positions_account_active_symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[str] = mapped_column(String(24), unique=True, nullable=False)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.account_id"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    active_symbol: Mapped[str | None] = mapped_column(String(20))
    direction: Mapped[str] = mapped_column(String(5), nullable=False)
    quantity_type: Mapped[str] = mapped_column(String(10), nullable=False)
    requested_quantity: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    leverage: Mapped[int | None] = mapped_column(Integer)
    entry_type: Mapped[str] = mapped_column(String(10), nullable=False)
    entry_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    take_profit_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    stop_loss_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    state: Mapped[str] = mapped_column(String(10), nullable=False)
    close_reason: Mapped[str | None] = mapped_column(String(20))
    last_error: Mapped[str | None] = mapped_column(Text)
    open_request_id: Mapped[str] = mapped_column(String(64), nullable=False)
    close_request_id: Mapped[str | None] = mapped_column(String(64))
    entry_average_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    entry_filled_quantity: Mapped[Decimal | None] = mapped_column(DecimalString)
    opened_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    exit_average_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    closed_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    realized_pnl: Mapped[Decimal | None] = mapped_column(DecimalString)
    fees: Mapped[Decimal | None] = mapped_column(DecimalString)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    __mapper_args__ = {"version_id_col": version}


class ExecutionRecordRow(Base):
    __tablename__ = "execution_records"
    __table_args__ = (UniqueConstraint("position_id", "role", name="uq_execution_records_position_role"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[str] = mapped_column(ForeignKey("positions.position_id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(12), nullable=False)
    kind: Mapped[str] = mapped_column(String(5), nullable=False)
    client_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    binance_id: Mapped[int | None] = mapped_column(BigInteger)
    order_type: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    price: Mapped[Decimal | None] = mapped_column(DecimalString)
    quantity: Mapped[Decimal | None] = mapped_column(DecimalString)
    executed_quantity: Mapped[Decimal | None] = mapped_column(DecimalString)
    average_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    commission: Mapped[Decimal | None] = mapped_column(DecimalString)
    commission_asset: Mapped[str | None] = mapped_column(String(10))
    submitted_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    __mapper_args__ = {"version_id_col": version}


class PositionEventRecord(Base):
    __tablename__ = "position_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[str] = mapped_column(ForeignKey("positions.position_id"), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    from_state: Mapped[str | None] = mapped_column(String(10))
    to_state: Mapped[str | None] = mapped_column(String(10))
    detail: Mapped[str | None] = mapped_column(Text)


class ReportRecord(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    account_id: Mapped[str | None] = mapped_column(String(64), index=True)
    request_id: Mapped[str | None] = mapped_column(String(64))
    position_id: Mapped[str | None] = mapped_column(String(24), index=True)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(UtcDateTime)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(UtcDateTime)


class ErrorLogRecord(Base):
    __tablename__ = "error_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    occurred_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, index=True)
    component: Mapped[str] = mapped_column(String(40), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    context: Mapped[str | None] = mapped_column(Text)
    account_id: Mapped[str | None] = mapped_column(String(64))
    request_id: Mapped[str | None] = mapped_column(String(64))
    position_id: Mapped[str | None] = mapped_column(String(24))


class MarketPriceRecord(Base):
    __tablename__ = "market_prices"

    symbol: Mapped[str] = mapped_column(String(20), primary_key=True)
    mark_price: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    event_time: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)


class ExchangeRuleRecord(Base):
    __tablename__ = "exchange_rules"

    symbol: Mapped[str] = mapped_column(String(20), primary_key=True)
    tick_size: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    step_size: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    min_quantity: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    market_step_size: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    market_min_quantity: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    market_max_quantity: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    min_notional: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
