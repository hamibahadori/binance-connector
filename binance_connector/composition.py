"""Wiring: build the shared components from settings, the same way for the API and the worker."""

import os
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import Engine

from binance_connector.config import Settings
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.database import create_database_engine, create_session_factory
from binance_connector.persistence.error_logs import ErrorLogRepository
from binance_connector.persistence.market_data import MarketDataRepository
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.reports import ReportOutbox, ReportRepository
from binance_connector.persistence.requests import RequestRepository
from binance_connector.persistence.schema import ensure_schema_is_current
from binance_connector.security.credentials import CredentialCipher


@dataclass(frozen=True)
class Components:
    """The long-lived objects both processes need."""

    settings: Settings
    engine: Engine
    accounts: AccountRepository
    requests: RequestRepository
    positions: PositionRepository
    error_logs: ErrorLogRepository
    market_data: MarketDataRepository
    reports: ReportRepository


def load_settings() -> Settings:
    """Load ``.env`` from the working directory (real environment variables win), then read it."""
    load_dotenv(find_dotenv(usecwd=True))
    return Settings.from_environment(os.environ)


def build_components(settings: Settings) -> Components:
    """Connect to the database and build the repositories.

    Raises:
        ConfigurationError: the database schema is not current (run ``alembic upgrade head``),
            or the encryption key is invalid.
    """
    engine = create_database_engine(settings.database_url)
    ensure_schema_is_current(engine)
    session_factory = create_session_factory(engine)
    outbox = ReportOutbox()  # every reported change writes its report in the same transaction
    return Components(
        settings=settings,
        engine=engine,
        accounts=AccountRepository(session_factory, cipher=CredentialCipher(settings.credentials_encryption_key)),
        requests=RequestRepository(session_factory, outbox=outbox),
        positions=PositionRepository(session_factory, outbox=outbox),
        error_logs=ErrorLogRepository(session_factory),
        market_data=MarketDataRepository(session_factory),
        reports=ReportRepository(session_factory),
    )
