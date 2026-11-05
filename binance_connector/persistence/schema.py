"""Run Alembic migrations from code, and refuse to start on an outdated schema.

Migrations are applied explicitly (``alembic upgrade head``), never automatically at
start-up: the API and the worker both start, and two processes migrating the same file
at once would race. Instead both refuse to start until the schema is current.
"""

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine

from binance_connector.errors import ConfigurationError

MIGRATIONS_FOLDER = Path(__file__).with_name("migrations")


def alembic_config() -> Config:
    """An Alembic config pointing at this package's migrations."""
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_FOLDER))
    return config


def upgrade_to_head(engine: Engine) -> None:
    """Apply every pending migration to the database behind ``engine``."""
    config = alembic_config()
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")


def ensure_schema_is_current(engine: Engine) -> None:
    """Raise ConfigurationError unless the database is at the latest migration."""
    head_revision = ScriptDirectory.from_config(alembic_config()).get_current_head()
    with engine.connect() as connection:
        current_revision = MigrationContext.configure(connection).get_current_revision()
    if current_revision != head_revision:
        raise ConfigurationError(
            f"The database schema is at {current_revision or 'nothing'}, expected {head_revision}. "
            "Run `alembic upgrade head` from the project folder first."
        )
