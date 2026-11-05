"""ORM models.

One ``trades`` row per Strategy trade holds its current state and the ids of the four
Binance orders that belong to it (entry, exit, take-profit, stop-loss). ``trade_events``
is an append-only history of every change, kept so nothing is lost across restarts and
so a trade's story can be reconstructed.

``active_symbol`` equals ``symbol`` while the trade is active and NULL once it is terminal.
A UNIQUE constraint on it enforces "one active trade per symbol" in the database itself,
which also covers two requests racing each other. (Several NULLs are allowed by both
SQLite and Postgres.)

``version`` is SQLAlchemy's optimistic-locking counter: a write based on a stale read fails
instead of silently overwriting what the other process wrote.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from binance_connector.persistence.types import DecimalString, UtcDateTime


class Base(DeclarativeBase):
    pass


class TradeRecord(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(24), unique=True, nullable=False)
    strategy_request_id: Mapped[str | None] = mapped_column(String(64), unique=True)

    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    active_symbol: Mapped[str | None] = mapped_column(String(20), unique=True)
    direction: Mapped[str] = mapped_column(String(5), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(DecimalString, nullable=False)
    leverage: Mapped[int | None] = mapped_column(Integer)
    take_profit_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    stop_loss_price: Mapped[Decimal | None] = mapped_column(DecimalString)

    state: Mapped[str] = mapped_column(String(20), nullable=False)
    close_reason: Mapped[str | None] = mapped_column(String(20))
    requested_close_reason: Mapped[str | None] = mapped_column(String(20))
    last_error: Mapped[str | None] = mapped_column(Text)

    entry_client_order_id: Mapped[str] = mapped_column(String(36), nullable=False)
    entry_order_id: Mapped[int | None] = mapped_column(BigInteger)
    entry_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    entry_quantity: Mapped[Decimal | None] = mapped_column(DecimalString)
    opened_at: Mapped[datetime | None] = mapped_column(UtcDateTime)

    exit_client_order_id: Mapped[str] = mapped_column(String(36), nullable=False)
    exit_order_id: Mapped[int | None] = mapped_column(BigInteger)
    exit_price: Mapped[Decimal | None] = mapped_column(DecimalString)
    closed_at: Mapped[datetime | None] = mapped_column(UtcDateTime)

    take_profit_client_algo_id: Mapped[str] = mapped_column(String(36), nullable=False)
    take_profit_algo_id: Mapped[int | None] = mapped_column(BigInteger)
    take_profit_status: Mapped[str | None] = mapped_column(String(20))

    stop_loss_client_algo_id: Mapped[str] = mapped_column(String(36), nullable=False)
    stop_loss_algo_id: Mapped[int | None] = mapped_column(BigInteger)
    stop_loss_status: Mapped[str | None] = mapped_column(String(20))

    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    __mapper_args__ = {"version_id_col": version}


class TradeEventRecord(Base):
    __tablename__ = "trade_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(ForeignKey("trades.trade_id"), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    from_state: Mapped[str | None] = mapped_column(String(20))
    to_state: Mapped[str | None] = mapped_column(String(20))
    detail: Mapped[str | None] = mapped_column(Text)
