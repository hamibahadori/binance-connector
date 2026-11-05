"""Engine and session factory.

SQLite is configured for two processes (API and worker) sharing one file:

* ``journal_mode=WAL`` lets readers proceed while a writer commits.
* ``busy_timeout`` makes a writer wait for the other process's lock instead of failing.
* ``foreign_keys=ON`` because SQLite leaves foreign keys off by default.

Any other database URL (e.g. Postgres) is used as is.
"""

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

SQLITE_BUSY_TIMEOUT_MS = 10000


def create_database_engine(database_url: str) -> Engine:
    """Create an engine for ``database_url`` with the SQLite settings above when relevant."""
    engine = create_engine(database_url)
    if engine.dialect.name == "sqlite":
        event.listen(engine, "connect", _configure_sqlite_connection)
    return engine


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Sessions whose objects stay readable after commit (we hand out snapshots)."""
    return sessionmaker(engine, expire_on_commit=False)


def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
