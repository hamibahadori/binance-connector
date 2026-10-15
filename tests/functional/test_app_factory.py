"""Functional tests for building the app from environment variables."""

import pytest

from binance_connector import create_app
from binance_connector.errors import ConfigurationError


@pytest.fixture
def clean_environment(monkeypatch, tmp_path):
    """Run from an empty directory (so no .env is found), with no Binance variables set."""
    monkeypatch.chdir(tmp_path)
    for variable in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "BINANCE_BASE_URL"):
        monkeypatch.delenv(variable, raising=False)
    return monkeypatch


def test_app_builds_from_environment_variables(clean_environment):
    clean_environment.setenv("BINANCE_API_KEY", "env-key")
    clean_environment.setenv("BINANCE_API_SECRET", "env-secret")

    response = create_app().test_client().get("/health")

    assert response.status_code == 200


def test_app_reads_dotenv_file_in_working_directory(clean_environment, tmp_path):
    (tmp_path / ".env").write_bytes(b"BINANCE_API_KEY=file-key\nBINANCE_API_SECRET=file-secret\n")

    response = create_app().test_client().get("/health")

    assert response.status_code == 200


def test_app_refuses_to_start_without_credentials(clean_environment):
    with pytest.raises(ConfigurationError, match="BINANCE_API_KEY"):
        create_app()
