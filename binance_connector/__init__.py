"""binance-connector: open and close Binance USD-M futures positions over HTTP.

Run locally with::

    flask --app binance_connector run --port 5001

Flask finds ``create_app`` below and calls it with no arguments, which loads
settings from ``.env`` and the environment.
"""

import logging
import os

from dotenv import find_dotenv, load_dotenv
from flask import Flask

from binance_connector.api.error_handlers import register_error_handlers
from binance_connector.api.routes import POSITION_SERVICE_KEY, health_blueprint, positions_blueprint
from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import Settings
from binance_connector.services.position_service import PositionService


def create_app(settings: Settings | None = None) -> Flask:
    """Build the Flask application.

    Args:
        settings: explicit settings (tests pass these). When omitted, ``.env`` in the
            current working directory is loaded into the environment (real
            environment variables take precedence) and settings are read from it.

    Raises:
        ConfigurationError: settings are missing or invalid. The service refuses to start
            rather than fail on the first request.
    """
    if settings is None:
        load_dotenv(find_dotenv(usecwd=True))
        settings = Settings.from_environment(os.environ)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    app = Flask(__name__, static_folder=None)  # a JSON API serves no static files
    app.json.sort_keys = False  # keep response fields in the documented order

    app.extensions[POSITION_SERVICE_KEY] = PositionService(BinanceFuturesClient(settings))
    app.register_blueprint(health_blueprint)
    app.register_blueprint(positions_blueprint)
    register_error_handlers(app)
    return app
