"""The API Django calls (spec §3, §6, §8).

* ``POST /api/v1/requests``                       submit an OPEN or CLOSE request
* ``GET  /api/v1/requests/<request_id>``          latest state of a request
* ``GET  /api/v1/positions/<position_id>``        latest state of a position (spec §6 fields)
* ``GET  /api/v1/accounts/<account_id>/positions`` an account's positions (``?state=`` filters)

``POST /api/v1/requests`` status codes:

======  ============================================================================
202     ACCEPTED; queued for execution. The receipt carries the position_id.
200     a duplicate (same request_id and body): the original receipt, ``duplicate: true``;
        or a CLOSE for a position that already finished (``already_closed: true``)
422     REJECTED; ``reasons`` says why. Stored, so a retry gets the same answer.
400     the request cannot be identified (bad envelope or not JSON); nothing stored
409     the request_id was already used for a different body
503     Binance market data unavailable; nothing stored, retry with the same request_id
======  ============================================================================

When ``INBOUND_API_TOKEN`` is set, every route here needs ``Authorization: Bearer <token>``
(``/health`` never does).
"""

import hmac
from typing import Any

from flask import Blueprint, current_app, jsonify, request
from flask.typing import ResponseReturnValue

from binance_connector.api.routes import json_object_body
from binance_connector.composition import Components
from binance_connector.domain.lifecycle import PositionState
from binance_connector.errors import PositionNotFoundError
from binance_connector.presentation import position_view, request_view
from binance_connector.services.intake import IntakeService

COMPONENTS_KEY = "components"
INTAKE_KEY = "intake"

requests_blueprint = Blueprint("requests_api", __name__, url_prefix="/api/v1")


@requests_blueprint.before_request
def require_token() -> ResponseReturnValue | None:
    """Refuse the call unless it carries the configured bearer token (if one is configured)."""
    expected_token = _components().settings.inbound_api_token
    if expected_token is None:
        return None
    header = request.headers.get("Authorization", "")
    supplied_token = header.removeprefix("Bearer ").strip() if header.startswith("Bearer ") else ""
    if not hmac.compare_digest(supplied_token.encode(), expected_token.encode()):
        return jsonify(error="unauthorized", message="A valid bearer token is required."), 401
    return None


@requests_blueprint.post("/requests")
def submit_request() -> ResponseReturnValue:
    outcome = _intake().submit(json_object_body())
    return jsonify(outcome.body), outcome.http_status


@requests_blueprint.get("/requests/<request_id>")
def get_request(request_id: str) -> ResponseReturnValue:
    components = _components()
    stored_request = components.requests.get(request_id)
    position = _optional_position(components, stored_request.position_id)
    market_price = components.market_data.price(position.symbol) if position is not None else None
    return jsonify(request_view(stored_request, position, market_price))


@requests_blueprint.get("/positions/<position_id>")
def get_position(position_id: str) -> ResponseReturnValue:
    components = _components()
    position = components.positions.get(position_id)
    return jsonify(_position_json(components, position))


@requests_blueprint.get("/accounts/<account_id>/positions")
def list_account_positions(account_id: str) -> ResponseReturnValue:
    components = _components()
    account = components.accounts.get(account_id)
    state_filter = request.args.get("state")
    state = None
    if state_filter:
        try:
            state = PositionState(state_filter.upper())
        except ValueError:
            allowed = ", ".join(state.value for state in PositionState)
            return jsonify(error="invalid_state", message=f"state must be one of {allowed}."), 400
    positions = components.positions.list_for_account(account_id, state)
    return jsonify(account_id=account.account_id, user_id=account.user_id,
                   positions=[_position_json(components, position, account.user_id) for position in positions])


def _position_json(components: Components, position, user_id: str | None = None) -> dict[str, Any]:
    user_id = user_id or components.accounts.get(position.account_id).user_id
    return position_view(position, user_id=user_id, market_price=components.market_data.price(position.symbol))


def _optional_position(components: Components, position_id: str | None):
    if position_id is None:
        return None
    try:
        return components.positions.get(position_id)
    except PositionNotFoundError:
        return None


def _components() -> Components:
    return current_app.extensions[COMPONENTS_KEY]


def _intake() -> IntakeService:
    return current_app.extensions[INTAKE_KEY]
