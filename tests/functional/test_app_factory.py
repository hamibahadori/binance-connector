"""Functional tests for building the app from environment variables."""

import pytest

from binance_connector import create_app
from binance_connector.errors import ConfigurationError
from tests.conftest import TEST_ENCRYPTION_KEY

SERVICE_VARIABLES = (
    "CREDENTIALS_ENCRYPTION_KEY",
    "BINANCE_BASE_URL",
    "DATABASE_URL",
    "BINANCE_TRIGGER_PRICE_TYPE",
    "INBOUND_API_TOKEN",
)


@pytest.fixture
def clean_environment(monkeypatch, tmp_path, database_url):
    """Run from an empty directory (so no .env is found), pointing at a migrated database."""
    monkeypatch.chdir(tmp_path)
    for variable in SERVICE_VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("DATABASE_URL", database_url)
    return monkeypatch


def test_app_builds_from_environment_variables(clean_environment):
    clean_environment.setenv("CREDENTIALS_ENCRYPTION_KEY", TEST_ENCRYPTION_KEY)

    response = create_app().test_client().get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_app_reads_dotenv_file_in_working_directory(clean_environment, tmp_path):
    (tmp_path / ".env").write_bytes(f"CREDENTIALS_ENCRYPTION_KEY={TEST_ENCRYPTION_KEY}\n".encode())

    assert create_app().test_client().get("/health").status_code == 200


def test_app_refuses_to_start_without_an_encryption_key(clean_environment):
    with pytest.raises(ConfigurationError, match="CREDENTIALS_ENCRYPTION_KEY"):
        create_app()


def test_app_refuses_to_start_on_an_unmigrated_database(clean_environment, tmp_path):
    clean_environment.setenv("CREDENTIALS_ENCRYPTION_KEY", TEST_ENCRYPTION_KEY)
    clean_environment.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'never-migrated.db'}")

    with pytest.raises(ConfigurationError, match="alembic upgrade head"):
        create_app()


def test_unknown_routes_return_json_404(clean_environment):
    clean_environment.setenv("CREDENTIALS_ENCRYPTION_KEY", TEST_ENCRYPTION_KEY)

    response = create_app().test_client().get("/api/v1/strategy/trades")

    assert response.status_code == 404
    assert response.get_json()["error"] == "not_found"
