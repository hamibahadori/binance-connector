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

from flask import Flask

from binance_connector.api.error_handlers import register_error_handlers
from binance_connector.api.routes import health_blueprint
from binance_connector.api.trade_routes import TRADE_SERVICE_KEY, trades_blueprint
from binance_connector.composition import build_components, load_settings
from binance_connector.config import Settings

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def create_app(settings: Settings | None = None) -> Flask:
    """Build the Flask application.

    Args:
        settings: explicit settings (tests pass these); loaded from ``.env``/environment when omitted.

    Raises:
        ConfigurationError: settings are missing or invalid, or the database schema is not
            current. The service refuses to start rather than fail on the first request.
    """
    settings = settings or load_settings()

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    app = Flask(__name__, static_folder=None)  # a JSON API serves no static files
    app.json.sort_keys = False  # keep response fields in the documented order

    app.extensions[TRADE_SERVICE_KEY] = build_components(settings).trade_service
    app.register_blueprint(health_blueprint)
    app.register_blueprint(trades_blueprint)
    register_error_handlers(app)
    return app
