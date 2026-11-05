"""Alembic environment.

Two ways in:

* From code (``schema.upgrade_to_head``): the caller passes an open connection in
  ``config.attributes["connection"]``; tests use this.
* From the command line (``alembic upgrade head``): the database comes from
  ``DATABASE_URL`` in the environment or ``.env``, with the same default as the service.

``render_as_batch=True`` lets future migrations alter SQLite tables, which SQLite only
supports by copying the table.
"""

import os

from alembic import context
from dotenv import find_dotenv, load_dotenv

from binance_connector.config import DEFAULT_DATABASE_URL
from binance_connector.persistence.database import create_database_engine
from binance_connector.persistence.models import Base

target_metadata = Base.metadata


def run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, render_as_batch=True)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    raise SystemExit("Offline (--sql) migrations are not supported; run against a database.")

provided_connection = context.config.attributes.get("connection")
if provided_connection is not None:
    run_migrations(provided_connection)
else:
    load_dotenv(find_dotenv(usecwd=True))
    engine = create_database_engine(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    with engine.begin() as new_connection:
        run_migrations(new_connection)
    engine.dispose()
