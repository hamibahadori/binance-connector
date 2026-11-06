"""The API process for production: the Flask app served by waitress.

Run from the project folder (after ``alembic upgrade head``)::

    python -m binance_connector.server

It listens on ``API_HOST:API_PORT`` (default 127.0.0.1:5000) with ``API_THREADS`` worker
threads (default 8). waitress is a pure-Python production WSGI server that runs the same on
Windows and Linux; ``flask run`` remains for development only.

The API only validates, stores and acknowledges requests; the worker
(``python -m binance_connector.worker``) executes them. Both must run.
"""

import logging
import signal

from waitress import create_server
from waitress.server import BaseWSGIServer, MultiSocketServer

from binance_connector import configure_logging, create_app
from binance_connector.composition import load_settings
from binance_connector.config import Settings

logger = logging.getLogger(__name__)


def build_server(settings: Settings) -> BaseWSGIServer | MultiSocketServer:
    """Build (but do not start) the waitress server for the API.

    Raises:
        ConfigurationError: the database schema is not current, or the settings are invalid.
    """
    app = create_app(settings)
    return create_server(app, host=settings.api_host, port=settings.api_port, threads=settings.api_threads,
                         ident="binance-connector")


def main() -> None:
    """Entry point: serve until Ctrl+C or SIGTERM, then close cleanly (exit code 0)."""
    configure_logging()
    settings = load_settings()
    server = build_server(settings)

    def request_stop(signal_number, _frame) -> None:
        logger.info("Received signal %s; stopping", signal_number)
        server.close()  # stops accepting; the serving loop ends once open connections finish

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    logger.info("API listening on http://%s:%s with %s threads", settings.api_host, server.effective_port,
                settings.api_threads)
    try:
        server.run()
    except OSError:
        pass  # select() on the just-closed listening socket, on some platforms
    logger.info("API stopped")


if __name__ == "__main__":
    main()
