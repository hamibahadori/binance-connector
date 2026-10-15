"""Turn exceptions into JSON error responses.

Every error response has the shape ``{"error": "<machine_code>", "message": "<text>", ...}``.
Some errors add fields; see the README's error table.

| Exception                          | HTTP | ``error``                         |
|------------------------------------|------|-----------------------------------|
| InvalidRequestBodyError            | 400  | invalid_request_body              |
| pydantic ValidationError           | 400  | validation_error                  |
| NoOpenPositionError                | 404  | no_open_position                  |
| CloseQuantityExceedsPositionError  | 409  | close_quantity_exceeds_position   |
| HedgeModeNotSupportedError         | 409  | hedge_mode_not_supported          |
| BinanceAPIError                    | 422  | binance_rejected                  |
| BinanceUnavailableError            | 502  | binance_unavailable               |
| BinanceUnknownOutcomeError         | 504  | binance_outcome_unknown           |
| werkzeug HTTPException (404, 405)  | as is| e.g. not_found                    |
| anything else                      | 500  | internal_error                    |
"""

import logging

from flask import Flask, Response, jsonify
from pydantic import ValidationError
from werkzeug.exceptions import HTTPException, MethodNotAllowed

from binance_connector.api.routes import InvalidRequestBodyError
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
    CloseQuantityExceedsPositionError,
    HedgeModeNotSupportedError,
    NoOpenPositionError,
)

logger = logging.getLogger(__name__)


def register_error_handlers(app: Flask) -> None:
    """Attach every handler in this module to ``app``."""
    app.register_error_handler(InvalidRequestBodyError, _invalid_request_body)
    app.register_error_handler(ValidationError, _validation_error)
    app.register_error_handler(NoOpenPositionError, _no_open_position)
    app.register_error_handler(CloseQuantityExceedsPositionError, _close_quantity_exceeds_position)
    app.register_error_handler(HedgeModeNotSupportedError, _hedge_mode_not_supported)
    app.register_error_handler(BinanceAPIError, _binance_rejected)
    app.register_error_handler(BinanceUnavailableError, _binance_unavailable)
    app.register_error_handler(BinanceUnknownOutcomeError, _binance_outcome_unknown)
    app.register_error_handler(HTTPException, _http_exception)
    app.register_error_handler(Exception, _unexpected_error)


def _error_response(status: int, error_code: str, message: str, **extra_fields: object) -> tuple[Response, int]:
    return jsonify(error=error_code, message=message, **extra_fields), status


def _invalid_request_body(error: InvalidRequestBodyError) -> tuple[Response, int]:
    return _error_response(400, "invalid_request_body", str(error))


def _validation_error(error: ValidationError) -> tuple[Response, int]:
    details = [
        {
            "field": ".".join(str(location_part) for location_part in field_error["loc"]) or "body",
            "message": field_error["msg"],
        }
        for field_error in error.errors(include_url=False)
    ]
    return _error_response(400, "validation_error", "The request body is invalid.", details=details)


def _no_open_position(error: NoOpenPositionError) -> tuple[Response, int]:
    return _error_response(404, "no_open_position", str(error), symbol=error.symbol)


def _close_quantity_exceeds_position(error: CloseQuantityExceedsPositionError) -> tuple[Response, int]:
    return _error_response(
        409,
        "close_quantity_exceeds_position",
        str(error),
        symbol=error.symbol,
        requested_quantity=format(error.requested_quantity, "f"),
        open_quantity=format(error.open_quantity, "f"),
    )


def _hedge_mode_not_supported(error: HedgeModeNotSupportedError) -> tuple[Response, int]:
    return _error_response(409, "hedge_mode_not_supported", str(error), symbol=error.symbol)


def _binance_rejected(error: BinanceAPIError) -> tuple[Response, int]:
    logger.warning("Binance rejected a request: %s", error)
    return _error_response(
        422,
        "binance_rejected",
        "Binance rejected the request.",
        binance={"http_status": error.http_status, "code": error.code, "message": error.message},
    )


def _binance_unavailable(error: BinanceUnavailableError) -> tuple[Response, int]:
    logger.error("Binance unavailable: %s", error)
    return _error_response(502, "binance_unavailable", str(error))


def _binance_outcome_unknown(error: BinanceUnknownOutcomeError) -> tuple[Response, int]:
    logger.error("Binance outcome unknown (client_order_id=%s): %s", error.client_order_id, error)
    return _error_response(
        504,
        "binance_outcome_unknown",
        f"{error} Check the order by client_order_id before retrying.",
        execution_status="unknown",
        client_order_id=error.client_order_id,
    )


def _http_exception(error: HTTPException) -> Response:
    error_code = (error.name or "http_error").lower().replace(" ", "_")
    response = jsonify(error=error_code, message=error.description)
    response.status_code = error.code or 500
    if isinstance(error, MethodNotAllowed) and error.valid_methods:
        response.headers["Allow"] = ", ".join(error.valid_methods)
    return response


def _unexpected_error(error: Exception) -> tuple[Response, int]:
    logger.exception("Unhandled error while serving a request")
    return _error_response(500, "internal_error", "An unexpected error occurred. See the service logs.")
