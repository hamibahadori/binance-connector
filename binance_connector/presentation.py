"""JSON views of requests and positions, shared by the API, reports and the WebSocket.

``position_view`` carries spec §6's fields: request/user/account/position ids, symbol,
direction, state, times (received, started, ended), entry/current/exit prices, P&L,
TP/SL status, close reason and error. Decimals are strings (no precision is lost) and
times are ISO 8601 UTC. Credentials never appear in any view.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from binance_connector.domain.lifecycle import ExecutionRole, PositionState
from binance_connector.domain.pnl import price_distance, unrealized_pnl
from binance_connector.persistence.market_data import MarketPrice
from binance_connector.persistence.positions import Position
from binance_connector.persistence.requests import Request


def receipt(
    request: Request,
    position: Position | None,
    *,
    duplicate: bool = False,
    already_closed: bool = False,
) -> dict[str, Any]:
    """What intake answers: whether the request was accepted, and why not if it was not."""
    return {
        "request_id": request.request_id,
        "status": request.status.value,
        "action": request.action.value,
        "user_id": request.user_id,
        "account_id": request.account_id,
        "position_id": request.position_id,
        "received_at": _time(request.received_at),
        "decided_at": _time(request.decided_at),
        "reasons": request.rejection_reasons or [],
        "duplicate": duplicate,
        "already_closed": already_closed,
        "position": None if position is None else position_view(position, user_id=request.user_id),
    }


def request_view(request: Request, position: Position | None, market_price: MarketPrice | None = None) -> dict[str, Any]:
    """The latest state of a request (``GET /api/v1/requests/<id>``)."""
    return {
        "request_id": request.request_id,
        "status": request.status.value,
        "action": request.action.value,
        "processing_state": request.processing_state.value if request.processing_state else None,
        "user_id": request.user_id,
        "account_id": request.account_id,
        "position_id": request.position_id,
        "requested_at": _time(request.requested_at),
        "received_at": _time(request.received_at),
        "decided_at": _time(request.decided_at),
        "processed_at": _time(request.processed_at),
        "reasons": request.rejection_reasons or [],
        "payload": request.payload,
        "position": None if position is None else position_view(position, user_id=request.user_id,
                                                                market_price=market_price),
    }


def position_view(position: Position, *, user_id: str, market_price: MarketPrice | None = None) -> dict[str, Any]:
    """The latest state of a position, with spec §6's fields."""
    current_price = market_price.mark_price if market_price is not None else None
    entry_average = position.entry_average_price
    filled_quantity = position.entry_filled_quantity
    is_open = position.state in (PositionState.OPEN, PositionState.CLOSING, PositionState.ERROR)

    gross_pnl: Decimal | None = None
    pnl_kind = None
    if position.realized_pnl is not None:
        gross_pnl, pnl_kind = position.realized_pnl, "REALIZED"
    elif is_open and current_price is not None and entry_average is not None and filled_quantity:
        gross_pnl = unrealized_pnl(direction=position.direction, entry_price=entry_average,
                                   quantity=filled_quantity, current_price=current_price)
        pnl_kind = "UNREALIZED"
    fees = position.fees
    net_pnl = None if gross_pnl is None else gross_pnl - (fees or Decimal(0))

    slippage = None
    if position.entry_type == "MARKET" and position.entry_price is not None and entry_average is not None:
        slippage = entry_average - position.entry_price

    return {
        "position_id": position.position_id,
        "request_id": position.open_request_id,
        "close_request_id": position.close_request_id,
        "user_id": user_id,
        "account_id": position.account_id,
        "symbol": position.symbol,
        "direction": position.direction,
        "state": position.state.value,
        "leverage": position.leverage,
        "quantity": {
            "requested": _decimal(position.requested_quantity),
            "type": position.quantity_type,
            "ordered": _decimal(position.quantity),
            "filled": _decimal(filled_quantity),
        },
        "entry": {
            "type": position.entry_type,
            "requested_price": _decimal(position.entry_price),
            "average_price": _decimal(entry_average),
            "slippage": _decimal(slippage),
        },
        "times": {
            "received": _time(position.created_at),
            "started": _time(position.opened_at),
            "ended": _time(position.closed_at),
            "updated": _time(position.updated_at),
        },
        "prices": {
            "entry": _decimal(entry_average),
            "current": _decimal(current_price),
            "current_at": _time(market_price.event_time) if market_price is not None else None,
            "exit": _decimal(position.exit_average_price),
        },
        "pnl": {
            "kind": pnl_kind,
            "gross": _decimal(gross_pnl),
            "fees": _decimal(fees),
            "net": _decimal(net_pnl),
        },
        "take_profit": _protection(position, ExecutionRole.TAKE_PROFIT, position.take_profit_price, current_price),
        "stop_loss": _protection(position, ExecutionRole.STOP_LOSS, position.stop_loss_price, current_price),
        "close_reason": position.close_reason.value if position.close_reason else None,
        "error": position.last_error,
    }


def _protection(position: Position, role: ExecutionRole, trigger_price: Decimal | None,
                current_price: Decimal | None) -> dict[str, Any] | None:
    if trigger_price is None:
        return None
    execution = position.executions.get(role)
    distance = None if current_price is None else price_distance(current_price=current_price,
                                                                 target_price=trigger_price)
    return {
        "price": _decimal(trigger_price),
        "status": execution.status if execution is not None else None,
        "distance": None if distance is None else {"absolute": _decimal(distance.absolute),
                                                   "percent": _decimal(distance.percent)},
    }


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _time(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()
