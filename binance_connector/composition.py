"""Wiring: build the shared components from settings, the same way for the API and the worker."""

import os
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import Settings
from binance_connector.persistence.database import create_database_engine, create_session_factory
from binance_connector.persistence.repository import TradeRepository
from binance_connector.persistence.schema import ensure_schema_is_current
from binance_connector.services.position_service import PositionService
from binance_connector.services.trade_service import TradeService


@dataclass(frozen=True)
class Components:
    """The long-lived objects both processes need."""

    settings: Settings
    repository: TradeRepository
    client: BinanceFuturesClient
    position_service: PositionService
    trade_service: TradeService


def load_settings() -> Settings:
    """Load ``.env`` from the working directory (real environment variables win), then read it."""
    load_dotenv(find_dotenv(usecwd=True))
    return Settings.from_environment(os.environ)


def build_components(settings: Settings) -> Components:
    """Connect to the database and build the services.

    Raises:
        ConfigurationError: the database schema is not current (run ``alembic upgrade head``).
    """
    engine = create_database_engine(settings.database_url)
    ensure_schema_is_current(engine)
    repository = TradeRepository(create_session_factory(engine))
    client = BinanceFuturesClient(settings)
    position_service = PositionService(client)
    trade_service = TradeService(
        repository=repository,
        client=client,
        position_service=position_service,
        trigger_price_type=settings.trigger_price_type,
    )
    return Components(settings, repository, client, position_service, trade_service)
