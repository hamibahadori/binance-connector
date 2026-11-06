"""Unit tests for the Alembic migrations and the start-up schema check."""

import pytest
from alembic import command
from sqlalchemy import inspect, text

from binance_connector.errors import ConfigurationError
from binance_connector.persistence.database import create_database_engine
from binance_connector.persistence.schema import alembic_config, ensure_schema_is_current, upgrade_to_head

PLAN_V2_TABLES = {
    "accounts",
    "requests",
    "positions",
    "position_events",
    "execution_records",
    "reports",
    "error_logs",
    "market_prices",
    "exchange_rules",
    "alembic_version",
}


def test_upgrade_creates_every_table_of_the_multi_account_model(tmp_path):
    engine = create_database_engine(f"sqlite:///{tmp_path / 'fresh.db'}")

    upgrade_to_head(engine)

    table_names = set(inspect(engine).get_table_names())
    assert PLAN_V2_TABLES <= table_names
    assert not {"trades", "trade_events"} & table_names


def test_the_v1_tables_are_only_replaced_when_empty(tmp_path):
    engine = create_database_engine(f"sqlite:///{tmp_path / 'v1.db'}")
    config = alembic_config()
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0001")
        connection.execute(text(
            "INSERT INTO trades (trade_id, symbol, direction, quantity, state, entry_client_order_id, "
            "exit_client_order_id, take_profit_client_algo_id, stop_loss_client_algo_id, created_at, updated_at, "
            "version) VALUES ('t1', 'BTCUSDT', 'LONG', '0.002', 'OPEN', 'e', 'x', 'tp', 'sl', "
            "'2026-11-05 00:00:00', '2026-11-05 00:00:00', 1)"
        ))

    with pytest.raises(RuntimeError, match="1 trade"):
        upgrade_to_head(engine)


def test_an_unmigrated_database_is_refused_at_start_up(tmp_path):
    engine = create_database_engine(f"sqlite:///{tmp_path / 'empty.db'}")

    with pytest.raises(ConfigurationError, match="alembic upgrade head"):
        ensure_schema_is_current(engine)


def test_a_migrated_database_passes_the_check(tmp_path):
    engine = create_database_engine(f"sqlite:///{tmp_path / 'migrated.db'}")
    upgrade_to_head(engine)

    ensure_schema_is_current(engine)


def test_sqlite_runs_in_wal_mode_with_a_busy_timeout(tmp_path):
    engine = create_database_engine(f"sqlite:///{tmp_path / 'pragmas.db'}")

    with engine.connect() as connection:
        journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar()
        busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar()

    assert journal_mode == "wal"
    assert busy_timeout >= 5000
