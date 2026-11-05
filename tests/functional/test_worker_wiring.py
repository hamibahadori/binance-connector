"""Functional tests for building the worker process from settings (no network involved)."""

import pytest

from binance_connector.errors import ConfigurationError
from binance_connector.worker import Worker, build_worker


def test_the_worker_is_wired_from_settings(settings):
    worker = build_worker(settings)

    assert isinstance(worker, Worker)


def test_the_worker_refuses_to_start_on_an_unmigrated_database(settings, tmp_path):
    from dataclasses import replace

    unmigrated = replace(settings, database_url=f"sqlite:///{tmp_path / 'never-migrated.db'}")

    with pytest.raises(ConfigurationError, match="alembic upgrade head"):
        build_worker(unmigrated)
