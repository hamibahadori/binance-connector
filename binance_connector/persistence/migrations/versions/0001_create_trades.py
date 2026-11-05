"""Create the trades and trade_events tables.

Revision ID: 0001
Revises:
Create Date: 2026-11-05

Decimal columns are String(40) (see ``persistence.types.DecimalString``) and datetimes are
stored as UTC (``UtcDateTime``).
"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("trade_id", sa.String(24), nullable=False, unique=True),
        sa.Column("strategy_request_id", sa.String(64), unique=True),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("active_symbol", sa.String(20), unique=True),
        sa.Column("direction", sa.String(5), nullable=False),
        sa.Column("quantity", sa.String(40), nullable=False),
        sa.Column("leverage", sa.Integer),
        sa.Column("take_profit_price", sa.String(40)),
        sa.Column("stop_loss_price", sa.String(40)),
        sa.Column("state", sa.String(20), nullable=False),
        sa.Column("close_reason", sa.String(20)),
        sa.Column("requested_close_reason", sa.String(20)),
        sa.Column("last_error", sa.Text),
        sa.Column("entry_client_order_id", sa.String(36), nullable=False),
        sa.Column("entry_order_id", sa.BigInteger),
        sa.Column("entry_price", sa.String(40)),
        sa.Column("entry_quantity", sa.String(40)),
        sa.Column("opened_at", sa.DateTime(timezone=True)),
        sa.Column("exit_client_order_id", sa.String(36), nullable=False),
        sa.Column("exit_order_id", sa.BigInteger),
        sa.Column("exit_price", sa.String(40)),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("take_profit_client_algo_id", sa.String(36), nullable=False),
        sa.Column("take_profit_algo_id", sa.BigInteger),
        sa.Column("take_profit_status", sa.String(20)),
        sa.Column("stop_loss_client_algo_id", sa.String(36), nullable=False),
        sa.Column("stop_loss_algo_id", sa.BigInteger),
        sa.Column("stop_loss_status", sa.String(20)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
    )
    op.create_table(
        "trade_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("trade_id", sa.String(24), sa.ForeignKey("trades.trade_id"), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kind", sa.String(30), nullable=False),
        sa.Column("from_state", sa.String(20)),
        sa.Column("to_state", sa.String(20)),
        sa.Column("detail", sa.Text),
    )
    op.create_index("ix_trade_events_trade_id", "trade_events", ["trade_id"])


def downgrade() -> None:
    op.drop_index("ix_trade_events_trade_id", table_name="trade_events")
    op.drop_table("trade_events")
    op.drop_table("trades")
