"""Replace the single-account v1 tables with the multi-account model (plan v2).

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-07

v1 (``trades``, ``trade_events``) was never released and only ever held test data, so it is
replaced rather than converted. As a safeguard the upgrade **refuses to run while v1 holds
any trade**, instead of dropping data silently.
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

DECIMAL = sa.String(40)
UTC_DATETIME = sa.DateTime(timezone=True)


def upgrade() -> None:
    connection = op.get_bind()
    trade_count = connection.execute(sa.text("SELECT COUNT(*) FROM trades")).scalar_one()
    if trade_count:
        raise RuntimeError(
            f"The v1 'trades' table holds {trade_count} trade(s). Export or delete them before "
            "upgrading: this migration replaces the v1 tables."
        )
    op.drop_index("ix_trade_events_trade_id", table_name="trade_events")
    op.drop_table("trade_events")
    op.drop_table("trades")

    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(64), nullable=False, unique=True),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("api_key_encrypted", sa.Text, nullable=False),
        sa.Column("api_secret_encrypted", sa.Text, nullable=False),
        sa.Column("api_key_fingerprint", sa.String(12), nullable=False),
        sa.Column("credentials_updated_at", UTC_DATETIME),
        sa.Column("created_at", UTC_DATETIME, nullable=False),
        sa.Column("updated_at", UTC_DATETIME, nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
    )
    op.create_index("ix_accounts_user_id", "accounts", ["user_id"])

    op.create_table(
        "requests",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("request_id", sa.String(64), nullable=False, unique=True),
        sa.Column("account_id", sa.String(64), sa.ForeignKey("accounts.account_id"), nullable=False),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("action", sa.String(10), nullable=False),
        sa.Column("status", sa.String(15), nullable=False),
        sa.Column("processing_state", sa.String(15)),
        sa.Column("payload", sa.Text, nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("rejection_reasons", sa.Text),
        sa.Column("position_id", sa.String(24)),
        sa.Column("requested_at", UTC_DATETIME),
        sa.Column("received_at", UTC_DATETIME, nullable=False),
        sa.Column("decided_at", UTC_DATETIME),
        sa.Column("processed_at", UTC_DATETIME),
        sa.Column("response", sa.Text),
        sa.Column("version", sa.Integer, nullable=False),
    )
    op.create_index("ix_requests_account_id", "requests", ["account_id"])
    op.create_index("ix_requests_processing_state", "requests", ["processing_state"])
    op.create_index("ix_requests_position_id", "requests", ["position_id"])

    op.create_table(
        "positions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("position_id", sa.String(24), nullable=False, unique=True),
        sa.Column("account_id", sa.String(64), sa.ForeignKey("accounts.account_id"), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("active_symbol", sa.String(20)),
        sa.Column("direction", sa.String(5), nullable=False),
        sa.Column("quantity_type", sa.String(10), nullable=False),
        sa.Column("requested_quantity", DECIMAL, nullable=False),
        sa.Column("quantity", DECIMAL, nullable=False),
        sa.Column("leverage", sa.Integer),
        sa.Column("entry_type", sa.String(10), nullable=False),
        sa.Column("entry_price", DECIMAL),
        sa.Column("take_profit_price", DECIMAL),
        sa.Column("stop_loss_price", DECIMAL),
        sa.Column("state", sa.String(10), nullable=False),
        sa.Column("close_reason", sa.String(20)),
        sa.Column("last_error", sa.Text),
        sa.Column("open_request_id", sa.String(64), nullable=False),
        sa.Column("close_request_id", sa.String(64)),
        sa.Column("entry_average_price", DECIMAL),
        sa.Column("entry_filled_quantity", DECIMAL),
        sa.Column("opened_at", UTC_DATETIME),
        sa.Column("exit_average_price", DECIMAL),
        sa.Column("closed_at", UTC_DATETIME),
        sa.Column("realized_pnl", DECIMAL),
        sa.Column("fees", DECIMAL),
        sa.Column("created_at", UTC_DATETIME, nullable=False),
        sa.Column("updated_at", UTC_DATETIME, nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.UniqueConstraint("account_id", "active_symbol", name="uq_positions_account_active_symbol"),
    )
    op.create_index("ix_positions_account_id", "positions", ["account_id"])

    op.create_table(
        "execution_records",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("position_id", sa.String(24), sa.ForeignKey("positions.position_id"), nullable=False),
        sa.Column("role", sa.String(12), nullable=False),
        sa.Column("kind", sa.String(5), nullable=False),
        sa.Column("client_id", sa.String(36), nullable=False, unique=True),
        sa.Column("binance_id", sa.BigInteger),
        sa.Column("order_type", sa.String(20), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("price", DECIMAL),
        sa.Column("quantity", DECIMAL),
        sa.Column("executed_quantity", DECIMAL),
        sa.Column("average_price", DECIMAL),
        sa.Column("commission", DECIMAL),
        sa.Column("commission_asset", sa.String(10)),
        sa.Column("submitted_at", UTC_DATETIME),
        sa.Column("updated_at", UTC_DATETIME, nullable=False),
        sa.Column("last_error", sa.Text),
        sa.Column("version", sa.Integer, nullable=False),
        sa.UniqueConstraint("position_id", "role", name="uq_execution_records_position_role"),
    )
    op.create_index("ix_execution_records_position_id", "execution_records", ["position_id"])

    op.create_table(
        "position_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("position_id", sa.String(24), sa.ForeignKey("positions.position_id"), nullable=False),
        sa.Column("occurred_at", UTC_DATETIME, nullable=False),
        sa.Column("kind", sa.String(30), nullable=False),
        sa.Column("from_state", sa.String(10)),
        sa.Column("to_state", sa.String(10)),
        sa.Column("detail", sa.Text),
    )
    op.create_index("ix_position_events_position_id", "position_events", ["position_id"])

    op.create_table(
        "reports",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("report_id", sa.String(36), nullable=False, unique=True),
        sa.Column("account_id", sa.String(64)),
        sa.Column("request_id", sa.String(64)),
        sa.Column("position_id", sa.String(24)),
        sa.Column("event_type", sa.String(30), nullable=False),
        sa.Column("payload", sa.Text, nullable=False),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
        sa.Column("next_attempt_at", UTC_DATETIME),
        sa.Column("last_error", sa.Text),
        sa.Column("created_at", UTC_DATETIME, nullable=False),
        sa.Column("sent_at", UTC_DATETIME),
    )
    op.create_index("ix_reports_account_id", "reports", ["account_id"])
    op.create_index("ix_reports_position_id", "reports", ["position_id"])
    op.create_index("ix_reports_status", "reports", ["status"])

    op.create_table(
        "error_logs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("occurred_at", UTC_DATETIME, nullable=False),
        sa.Column("component", sa.String(40), nullable=False),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("context", sa.Text),
        sa.Column("account_id", sa.String(64)),
        sa.Column("request_id", sa.String(64)),
        sa.Column("position_id", sa.String(24)),
    )
    op.create_index("ix_error_logs_occurred_at", "error_logs", ["occurred_at"])

    op.create_table(
        "market_prices",
        sa.Column("symbol", sa.String(20), primary_key=True),
        sa.Column("mark_price", DECIMAL, nullable=False),
        sa.Column("event_time", UTC_DATETIME, nullable=False),
        sa.Column("updated_at", UTC_DATETIME, nullable=False),
    )

    op.create_table(
        "exchange_rules",
        sa.Column("symbol", sa.String(20), primary_key=True),
        sa.Column("tick_size", DECIMAL, nullable=False),
        sa.Column("step_size", DECIMAL, nullable=False),
        sa.Column("min_quantity", DECIMAL, nullable=False),
        sa.Column("market_step_size", DECIMAL, nullable=False),
        sa.Column("market_min_quantity", DECIMAL, nullable=False),
        sa.Column("market_max_quantity", DECIMAL, nullable=False),
        sa.Column("min_notional", DECIMAL, nullable=False),
        sa.Column("fetched_at", UTC_DATETIME, nullable=False),
    )


def downgrade() -> None:
    raise NotImplementedError("0002 replaces the unreleased v1 schema; there is nothing to go back to.")
