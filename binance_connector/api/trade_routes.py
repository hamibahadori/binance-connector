"""Strategy trade endpoints.

* ``POST /api/v1/strategy/trades``                  open a trade (entry, then TP/SL)
* ``POST /api/v1/strategy/trades/<trade_id>/close`` close it (manual or upstream request)

Both answer with the trade as it stands after the call. The HTTP status says how the call
went; the trade's ``state`` says where the trade is:

=========================  ======  =====================================================
Outcome                    Status  Trade state
=========================  ======  =====================================================
opened                     201     OPEN (no TP/SL asked for) or PROTECTED
repeated request id        200     whatever the existing trade's state is
outcome not known yet      202     NEEDS_ATTENTION or CLOSING; reconciliation resolves it
Binance rejected it        422     ENTRY_FAILED (open), or unchanged (close)
closed                     200     CLOSED
=========================  ======  =====================================================

There is deliberately no endpoint for reading trades or pushing status yet: reporting
waits for the customer's requirements (decision D-35).
"""

from typing import Any

from flask import Blueprint, current_app, jsonify
from flask.typing import ResponseReturnValue

from binance_connector.api.routes import json_object_body
from binance_connector.api.trade_schemas import CloseTradeRequest, OpenTradeRequest
from binance_connector.domain.trade_state import TradeState
from binance_connector.persistence.repository import Trade
from binance_connector.services.trade_service import TradeService

TRADE_SERVICE_KEY = "trade_service"
"""Key under ``app.extensions`` where ``create_app`` stores the TradeService."""

trades_blueprint = Blueprint("strategy_trades", __name__, url_prefix="/api/v1/strategy/trades")

OPEN_STATUS_BY_STATE = {
    TradeState.OPEN: 201,
    TradeState.PROTECTED: 201,
    TradeState.ENTRY_FAILED: 422,
    TradeState.NEEDS_ATTENTION: 202,
    TradeState.ENTRY_SUBMITTED: 202,
    TradeState.CLOSED: 201,  # a TP/SL filled before the call even returned
}

CLOSE_STATUS_BY_STATE = {
    TradeState.CLOSED: 200,
    TradeState.CLOSING: 202,
    TradeState.NEEDS_ATTENTION: 202,
    TradeState.OPEN: 422,
    TradeState.PROTECTED: 422,
}


@trades_blueprint.post("")
def open_trade() -> ResponseReturnValue:
    """Open a trade. See README.md for the body and the response."""
    request_body = OpenTradeRequest.model_validate(json_object_body())
    outcome = _trade_service().open_trade(request_body.to_command())
    status = 200 if outcome.replayed else OPEN_STATUS_BY_STATE.get(outcome.trade.state, 202)
    return jsonify(serialise_trade(outcome.trade)), status


@trades_blueprint.post("/<trade_id>/close")
def close_trade(trade_id: str) -> ResponseReturnValue:
    """Close a trade. The body is optional: ``{"reason": "MANUAL" | "UPSTREAM"}``."""
    request_body = CloseTradeRequest.model_validate(json_object_body(allow_empty=True))
    trade = _trade_service().close_trade(trade_id, request_body.reason)
    return jsonify(serialise_trade(trade)), CLOSE_STATUS_BY_STATE.get(trade.state, 202)


def _trade_service() -> TradeService:
    return current_app.extensions[TRADE_SERVICE_KEY]


def serialise_trade(trade: Trade) -> dict[str, Any]:
    """JSON-safe view of a trade. Decimals are strings; times are ISO 8601 UTC."""
    return {
        "trade_id": trade.trade_id,
        "strategy_request_id": trade.strategy_request_id,
        "symbol": trade.symbol,
        "direction": trade.direction,
        "quantity": _decimal(trade.quantity),
        "leverage": trade.leverage,
        "state": trade.state.value,
        "close_reason": trade.close_reason.value if trade.close_reason else None,
        "last_error": trade.last_error,
        "entry": {
            "client_order_id": trade.entry_client_order_id,
            "order_id": trade.entry_order_id,
            "price": _decimal(trade.entry_price),
            "quantity": _decimal(trade.entry_quantity),
            "time": _time(trade.opened_at),
        },
        "exit": {
            "client_order_id": trade.exit_client_order_id,
            "order_id": trade.exit_order_id,
            "price": _decimal(trade.exit_price),
            "time": _time(trade.closed_at),
        },
        "take_profit": _protection(trade, "take_profit"),
        "stop_loss": _protection(trade, "stop_loss"),
        "created_at": _time(trade.created_at),
        "updated_at": _time(trade.updated_at),
    }


def _protection(trade: Trade, role: str) -> dict[str, Any] | None:
    trigger_price = getattr(trade, f"{role}_price")
    if trigger_price is None:
        return None
    return {
        "trigger_price": _decimal(trigger_price),
        "client_algo_id": getattr(trade, f"{role}_client_algo_id"),
        "algo_id": getattr(trade, f"{role}_algo_id"),
        "status": getattr(trade, f"{role}_status"),
    }


def _decimal(value) -> str | None:
    return None if value is None else format(value, "f")


def _time(value) -> str | None:
    return None if value is None else value.isoformat()
