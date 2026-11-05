"""Shared HTTP pieces: the health check and JSON-body parsing.

The Strategy trade endpoints are in ``trade_routes``. The original stateless
``/api/v1/positions/open|close`` endpoints were retired (decision D-34): they changed
positions without the trade state machine knowing.
"""

from typing import Any

from flask import Blueprint, jsonify, request
from flask.typing import ResponseReturnValue

health_blueprint = Blueprint("health", __name__)


class InvalidRequestBodyError(Exception):
    """The request body is missing, is not JSON, or is not a JSON object."""


@health_blueprint.get("/health")
def health() -> ResponseReturnValue:
    """Report that the process is up. Says nothing about Binance's availability."""
    return jsonify(status="ok")


def json_object_body(allow_empty: bool = False) -> dict[str, Any]:
    """Return the request body as a dict, or raise InvalidRequestBodyError.

    ``silent=True`` makes malformed JSON and non-JSON content types return None instead
    of Flask's HTML 400 page, so every error stays JSON. With ``allow_empty``, a request
    with no body at all counts as ``{}``.
    """
    if allow_empty and not request.get_data():
        return {}
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        raise InvalidRequestBodyError("The request body must be a JSON object.")
    return body
