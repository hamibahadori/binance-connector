"""binance-connector: a multi-account Strategy execution service on Binance USD-M futures.

Two processes share one database:

* the API (this Flask app): intake of Django's requests and their latest state::

      flask --app binance_connector run --port 5001

* the worker: execution, Binance streams, reconciliation, reports, live WebSocket::

      python -m binance_connector.worker

Flask finds ``create_app`` below and calls it with no arguments, which loads settings
from ``.env`` and the environment. Run ``alembic upgrade head`` once before either starts.
"""

import logging

from flask import Flask

from binance_connector.api.error_handlers import register_error_handlers
from binance_connector.api.routes import health_blueprint
from binance_connector.composition import build_components, load_settings
from binance_connector.config import Settings
from binance_connector.security.redaction import install_log_redaction

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

COMPONENTS_KEY = "components"
"""Key under ``app.extensions`` where ``create_app`` stores the shared components."""


def configure_logging() -> None:
    """INFO-level logs with every registered secret masked (spec §10)."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    install_log_redaction()


def create_app(settings: Settings | None = None) -> Flask:
    """Build the Flask application.

    Args:
        settings: explicit settings (tests pass these); loaded from ``.env``/environment when omitted.

    Raises:
        ConfigurationError: settings are missing or invalid, or the database schema is not
            current. The service refuses to start rather than fail on the first request.
    """
    settings = settings or load_settings()
    configure_logging()

    app = Flask(__name__, static_folder=None)  # a JSON API serves no static files
    app.json.sort_keys = False  # keep response fields in the documented order

    app.extensions[COMPONENTS_KEY] = build_components(settings)
    app.register_blueprint(health_blueprint)
    register_error_handlers(app)
    return app
