"""binance-connector: execute Strategy trades on Binance USD-M futures.

Two processes share one database:

* the API (this Flask app)::

      flask --app binance_connector run --port 5001

* the worker, which listens to Binance and reconciles::

      python -m binance_connector.worker

Flask finds ``create_app`` below and calls it with no arguments, which loads settings
from ``.env`` and the environment. Run ``alembic upgrade head`` once before either starts.
"""

import logging
import os

from dotenv import find_dotenv, load_dotenv
from flask import Flask

from binance_connector.api.error_handlers import register_error_handlers
from binance_connector.api.routes import health_blueprint
from binance_connector.api.trade_routes import TRADE_SERVICE_KEY, trades_blueprint
from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import Settings
from binance_connector.persistence.database import create_database_engine, create_session_factory
from binance_connector.persistence.repository import TradeRepository
from binance_connector.persistence.schema import ensure_schema_is_current
from binance_connector.services.position_service import PositionService
from binance_connector.services.trade_service import TradeService


def load_settings() -> Settings:
    """Load ``.env`` from the working directory (real environment variables win), then read it."""
    load_dotenv(find_dotenv(usecwd=True))
    return Settings.from_environment(os.environ)


def build_trade_service(settings: Settings) -> TradeService:
    """Wire the trade service from settings. Shared by the API and the worker.

    Raises:
        ConfigurationError: the database schema is not current.
    """
    engine = create_database_engine(settings.database_url)
    ensure_schema_is_current(engine)
    client = BinanceFuturesClient(settings)
    return TradeService(
        repository=TradeRepository(create_session_factory(engine)),
        client=client,
        position_service=PositionService(client),
        trigger_price_type=settings.trigger_price_type,
    )


def create_app(settings: Settings | None = None) -> Flask:
    """Build the Flask application.

    Args:
        settings: explicit settings (tests pass these); loaded from ``.env``/environment when omitted.

    Raises:
        ConfigurationError: settings are missing or invalid, or the database schema is not
            current. The service refuses to start rather than fail on the first request.
    """
    settings = settings or load_settings()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    app = Flask(__name__, static_folder=None)  # a JSON API serves no static files
    app.json.sort_keys = False  # keep response fields in the documented order

    app.extensions[TRADE_SERVICE_KEY] = build_trade_service(settings)
    app.register_blueprint(health_blueprint)
    app.register_blueprint(trades_blueprint)
    register_error_handlers(app)
    return app
