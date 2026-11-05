"""Unit tests for the Alembic migrations and the start-up schema check."""

import pytest
from sqlalchemy import inspect

from binance_connector.errors import ConfigurationError
from binance_connector.persistence.database import create_database_engine
from binance_connector.persistence.schema import ensure_schema_is_current, upgrade_to_head


def test_upgrade_creates_the_tables(tmp_path):
    engine = create_database_engine(f"sqlite:///{tmp_path / 'fresh.db'}")

    upgrade_to_head(engine)

    assert {"trades", "trade_events", "alembic_version"} <= set(inspect(engine).get_table_names())


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
