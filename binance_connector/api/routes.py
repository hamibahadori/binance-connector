"""HTTP routes.

* ``GET  /health``                 liveness check; does not call Binance.
* ``POST /api/v1/positions/open``  open (or add to) a position with a MARKET order.
* ``POST /api/v1/positions/close`` close all or part of a position with a reduce-only MARKET order.

Routes only parse input, call the service, and serialise the result. Failures
are raised as exceptions and turned into responses in ``error_handlers``.
"""

from typing import Any

from flask import Blueprint, current_app, jsonify, request
from flask.typing import ResponseReturnValue

from binance_connector.api.schemas import ClosePositionRequest, OpenPositionRequest
from binance_connector.services.position_service import OrderResult, PositionService

POSITION_SERVICE_KEY = "position_service"
"""Key under ``app.extensions`` where ``create_app`` stores the PositionService."""

health_blueprint = Blueprint("health", __name__)
positions_blueprint = Blueprint("positions", __name__, url_prefix="/api/v1/positions")


class InvalidRequestBodyError(Exception):
    """The request body is missing, is not JSON, or is not a JSON object."""


@health_blueprint.get("/health")
def health() -> ResponseReturnValue:
    """Report that the process is up. Says nothing about Binance's availability."""
    return jsonify(status="ok")


@positions_blueprint.post("/open")
def open_position() -> ResponseReturnValue:
    """Open a position. See README.md for the body and response format."""
    request_body = OpenPositionRequest.model_validate(_json_object_body())
    result = _position_service().open_position(request_body.to_command())
    return jsonify(_serialise_order_result(result))


@positions_blueprint.post("/close")
def close_position() -> ResponseReturnValue:
    """Close all or part of a position. See README.md for the body and response format."""
    request_body = ClosePositionRequest.model_validate(_json_object_body())
    result = _position_service().close_position(request_body.to_command())
    return jsonify(_serialise_order_result(result))


def _json_object_body() -> dict[str, Any]:
    """Return the request body as a dict, or raise InvalidRequestBodyError.

    ``silent=True`` makes malformed JSON and non-JSON content types return None
    instead of Flask's HTML 400 page, so every error stays JSON.
    """
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        raise InvalidRequestBodyError("The request body must be a JSON object.")
    return body


def _position_service() -> PositionService:
    """The PositionService that ``create_app`` attached to the current app."""
    return current_app.extensions[POSITION_SERVICE_KEY]


def _serialise_order_result(result: OrderResult) -> dict[str, Any]:
    """Convert to JSON-safe values. Decimals become strings so no precision is lost."""
    return {
        "symbol": result.symbol,
        "direction": result.direction.value,
        "side": result.side,
        "order_id": result.order_id,
        "client_order_id": result.client_order_id,
        "status": result.status,
        "requested_quantity": format(result.requested_quantity, "f"),
        "executed_quantity": format(result.executed_quantity, "f"),
        "average_price": format(result.average_price, "f"),
        "leverage": result.leverage,
    }
