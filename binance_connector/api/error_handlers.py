"""Turn exceptions into JSON error responses.

Every error response has the shape ``{"error": "<machine_code>", "message": "<text>", ...}``.
Some errors add fields; see the README's error table.

| Exception                          | HTTP | ``error``                         |
|------------------------------------|------|-----------------------------------|
| InvalidRequestBodyError            | 400  | invalid_request_body              |
| pydantic ValidationError           | 400  | validation_error                  |
| RequestNotFoundError               | 404  | request_not_found                 |
| PositionNotFoundError              | 404  | position_not_found                |
| AccountNotFoundError               | 404  | account_not_found                 |
| BinanceAPIError                    | 422  | binance_rejected                  |
| BinanceUnavailableError            | 502  | binance_unavailable               |
| BinanceUnknownOutcomeError         | 504  | binance_outcome_unknown           |
| sqlalchemy OperationalError        | 503  | database_unavailable              |
| werkzeug HTTPException (404, 405)  | as is| e.g. not_found                    |
| anything else                      | 500  | internal_error                    |

Business outcomes of a request (rejected, duplicate, …) are not errors here: the intake
endpoint answers with the request's receipt (plan v2 phase 2).
"""

import logging

from flask import Flask, Response, jsonify
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from werkzeug.exceptions import HTTPException, MethodNotAllowed

from binance_connector.api.routes import InvalidRequestBodyError
from binance_connector.errors import (
    BinanceAPIError,
    BinanceUnavailableError,
    BinanceUnknownOutcomeError,
    AccountNotFoundError,
    PositionNotFoundError,
    RequestNotFoundError,
)

logger = logging.getLogger(__name__)


def register_error_handlers(app: Flask) -> None:
    """Attach every handler in this module to ``app``."""
    app.register_error_handler(InvalidRequestBodyError, _invalid_request_body)
    app.register_error_handler(ValidationError, _validation_error)
    app.register_error_handler(RequestNotFoundError, _not_found("request_not_found", "request_id"))
    app.register_error_handler(PositionNotFoundError, _not_found("position_not_found", "position_id"))
    app.register_error_handler(AccountNotFoundError, _not_found("account_not_found", "account_id"))
    app.register_error_handler(BinanceAPIError, _binance_rejected)
    app.register_error_handler(BinanceUnavailableError, _binance_unavailable)
    app.register_error_handler(BinanceUnknownOutcomeError, _binance_outcome_unknown)
    app.register_error_handler(OperationalError, _database_unavailable)
    app.register_error_handler(HTTPException, _http_exception)
    app.register_error_handler(Exception, _unexpected_error)


def _error_response(status: int, error_code: str, message: str, **extra_fields: object) -> tuple[Response, int]:
    """Build the standard error body ``{"error", "message", **extra_fields}`` with ``status``."""
    return jsonify(error=error_code, message=message, **extra_fields), status


def _invalid_request_body(error: InvalidRequestBodyError) -> tuple[Response, int]:
    return _error_response(400, "invalid_request_body", str(error))


def _validation_error(error: ValidationError) -> tuple[Response, int]:
    """List every invalid field as ``{"field": "quantity", "message": "..."}``.

    A nested location such as ``("leverage", "int")`` is joined with dots; a
    whole-body error with no location is reported as field ``"body"``.
    """
    details = [
        {
            "field": ".".join(str(location_part) for location_part in field_error["loc"]) or "body",
            "message": field_error["msg"],
        }
        for field_error in error.errors(include_url=False)
    ]
    return _error_response(400, "validation_error", "The request body is invalid.", details=details)


def _not_found(error_code: str, id_field: str):
    """A 404 handler that echoes the missing id under ``id_field``."""

    def handler(error: Exception) -> tuple[Response, int]:
        return _error_response(404, error_code, str(error), **{id_field: getattr(error, id_field)})

    return handler


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


def _database_unavailable(error: OperationalError) -> tuple[Response, int]:
    """The database could not be used (locked past the busy timeout, file unreachable, ...).

    The caller may retry with the same request_id: if the request was not stored, it is
    processed then; if it was, the duplicate rule returns its stored receipt. Only the
    driver's message is logged: the SQL statement and its parameters stay out of the logs.
    """
    logger.error("Database unavailable: %s", error.orig)
    return _error_response(503, "database_unavailable",
                           "The service's database is unavailable. Retry the same request shortly.")


def _http_exception(error: HTTPException) -> Response:
    """Render Flask/werkzeug errors (404, 405, ...) as JSON, e.g. "Not Found" -> ``not_found``.

    A 405 keeps its ``Allow`` header so callers can see which methods are valid.
    """
    error_code = (error.name or "http_error").lower().replace(" ", "_")
    response = jsonify(error=error_code, message=error.description)
    response.status_code = error.code or 500
    if isinstance(error, MethodNotAllowed) and error.valid_methods:
        response.headers["Allow"] = ", ".join(error.valid_methods)
    return response


def _unexpected_error(error: Exception) -> tuple[Response, int]:
    """Log the full traceback, but never send internals (stack traces, values) to the caller."""
    logger.exception("Unhandled error while serving a request")
    return _error_response(500, "internal_error", "An unexpected error occurred. See the service logs.")
