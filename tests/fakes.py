"""An in-memory Binance account for service-level unit tests.

``FakeBinance`` implements the same methods as ``BinanceFuturesClient`` and simulates
just enough of an exchange to test the open/close/reconcile logic. Every behaviour
mirrors something recorded on Demo (``tests/fixtures/demo_capture_2026_11_05.json``):

* market-order replies have no ``avgPrice`` (it comes from ``get_order``);
* a reduce-only order can only shrink a position;
* TP/SL algo orders with ``closePosition`` start ``NEW``;
* when a position goes flat, Binance expires every ``closePosition`` algo order on that
  symbol by itself, and cancelling one of those then fails with ``-2011``;
* a triggered algo order ends ``FINISHED`` with ``actualOrderId``/``actualPrice``/``actualQty``,
  and the order it creates carries the algo's client id;
* unknown order / algo ids fail with ``-2013``.
"""

from decimal import Decimal
from itertools import count
from typing import Any

from binance_connector.errors import BinanceAPIError

UNKNOWN_ORDER = (400, -2013, "Order does not exist.")
UNKNOWN_ORDER_SENT = (400, -2011, "Unknown order sent.")


class FakeBinance:
    def __init__(self, fill_price: str = "78000.0") -> None:
        self.fill_price = Decimal(fill_price)
        self.positions: dict[str, Decimal] = {}
        self.orders: dict[str, dict[str, Any]] = {}
        self.algo_orders: dict[str, dict[str, Any]] = {}
        self.leverage: dict[str, int] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.hedge_mode = False
        self._failures: dict[str, list[Exception]] = {}
        self._order_ids = count(28667218850)
        self._algo_ids = count(1000000274196247)

    # --- test controls ----------------------------------------------------------------------

    def fail_next(self, method_name: str, error: Exception) -> None:
        """Make the next call of ``method_name`` raise ``error`` (nothing happens on Binance)."""
        self._failures.setdefault(method_name, []).append(error)

    def call_names(self) -> list[str]:
        return [call_name for call_name, _ in self.calls]

    def trigger(self, client_algo_id: str, fill_price: str) -> None:
        """Simulate Binance triggering a TP/SL: it fills, closes the position, expires siblings."""
        algo_order = self.algo_orders[client_algo_id]
        symbol = algo_order["symbol"]
        filled_quantity = abs(self.positions.get(symbol, Decimal(0)))
        order_id = next(self._order_ids)
        self.orders[client_algo_id] = self._order_record(
            order_id, symbol, algo_order["side"], filled_quantity, client_algo_id, Decimal(fill_price), True
        )
        algo_order.update(
            algoStatus="FINISHED",
            actualOrderId=str(order_id),
            actualPrice=format(Decimal(fill_price), "f"),
            actualQty=format(filled_quantity, "f"),
            triggerTime=1793891929077,
        )
        self._set_position(symbol, Decimal(0))

    def flatten_externally(self, symbol: str) -> None:
        """Simulate the position disappearing without any of our orders (e.g. liquidation)."""
        self._set_position(symbol, Decimal(0))

    # --- BinanceFuturesClient methods ----------------------------------------------------------

    def change_leverage(self, *, symbol: str, leverage: int) -> dict[str, Any]:
        self._record_call("change_leverage", symbol=symbol, leverage=leverage)
        self.leverage[symbol] = leverage
        return {"symbol": symbol, "leverage": leverage, "maxNotionalValue": "100000000"}

    def place_market_order(self, *, symbol, side, quantity, client_order_id, reduce_only=False) -> dict[str, Any]:
        self._record_call(
            "place_market_order", symbol=symbol, side=side, quantity=quantity,
            client_order_id=client_order_id, reduce_only=reduce_only,
        )
        signed_quantity = quantity if side == "BUY" else -quantity
        current_amount = self.positions.get(symbol, Decimal(0))
        if reduce_only and (current_amount == 0 or abs(current_amount + signed_quantity) > abs(current_amount)):
            raise BinanceAPIError(400, -2022, "ReduceOnly Order is rejected.")
        order_id = next(self._order_ids)
        self.orders[client_order_id] = self._order_record(
            order_id, symbol, side, quantity, client_order_id, self.fill_price, reduce_only
        )
        self._set_position(symbol, current_amount + signed_quantity)
        reply = dict(self.orders[client_order_id])
        reply.pop("avgPrice")
        reply.pop("cumQuote")
        return reply

    def get_order(self, *, symbol: str, client_order_id: str) -> dict[str, Any]:
        self._record_call("get_order", symbol=symbol, client_order_id=client_order_id)
        if client_order_id not in self.orders:
            raise BinanceAPIError(*UNKNOWN_ORDER)
        return dict(self.orders[client_order_id])

    def get_position_risk(self, *, symbol: str) -> list[dict[str, Any]]:
        self._record_call("get_position_risk", symbol=symbol)
        amount = self.positions.get(symbol, Decimal(0))
        if self.hedge_mode:
            return [{"symbol": symbol, "positionSide": "LONG", "positionAmt": format(amount, "f")}]
        if amount == 0:
            return []
        return [{"symbol": symbol, "positionSide": "BOTH", "positionAmt": format(amount, "f"),
                 "entryPrice": format(self.fill_price, "f")}]

    def place_conditional_close(self, *, symbol, side, order_type, trigger_price, client_algo_id,
                                working_type) -> dict[str, Any]:
        self._record_call(
            "place_conditional_close", symbol=symbol, side=side, order_type=order_type,
            trigger_price=trigger_price, client_algo_id=client_algo_id, working_type=working_type,
        )
        self.algo_orders[client_algo_id] = {
            "algoId": next(self._algo_ids), "clientAlgoId": client_algo_id, "algoType": "CONDITIONAL",
            "orderType": order_type, "symbol": symbol, "side": side, "positionSide": "BOTH",
            "timeInForce": "GTE_GTC", "quantity": "0.0000", "algoStatus": "NEW",
            "actualOrderId": "", "actualPrice": "0.000000",
            "triggerPrice": format(trigger_price, "f"), "workingType": working_type,
            "closePosition": True, "reduceOnly": True, "triggerTime": 0,
        }
        reply = dict(self.algo_orders[client_algo_id])
        reply.pop("actualOrderId")
        reply.pop("actualPrice")
        return reply

    def cancel_algo_order(self, *, client_algo_id: str) -> dict[str, Any]:
        self._record_call("cancel_algo_order", client_algo_id=client_algo_id)
        algo_order = self.algo_orders.get(client_algo_id)
        if algo_order is None or algo_order["algoStatus"] != "NEW":
            raise BinanceAPIError(*UNKNOWN_ORDER_SENT)
        algo_order["algoStatus"] = "CANCELED"
        return {"algoId": algo_order["algoId"], "clientAlgoId": client_algo_id, "code": "200", "msg": "success"}

    def get_algo_order(self, *, client_algo_id: str) -> dict[str, Any]:
        self._record_call("get_algo_order", client_algo_id=client_algo_id)
        if client_algo_id not in self.algo_orders:
            raise BinanceAPIError(*UNKNOWN_ORDER)
        return dict(self.algo_orders[client_algo_id])

    def get_open_algo_orders(self, *, symbol: str) -> list[dict[str, Any]]:
        self._record_call("get_open_algo_orders", symbol=symbol)
        return [dict(algo) for algo in self.algo_orders.values()
                if algo["symbol"] == symbol and algo["algoStatus"] == "NEW"]

    # --- internals ----------------------------------------------------------------------------

    def _record_call(self, method_name: str, **arguments: Any) -> None:
        self.calls.append((method_name, arguments))
        pending_failures = self._failures.get(method_name)
        if pending_failures:
            raise pending_failures.pop(0)

    def _set_position(self, symbol: str, amount: Decimal) -> None:
        self.positions[symbol] = amount
        if amount == 0:
            for algo_order in self.algo_orders.values():
                if algo_order["symbol"] == symbol and algo_order["algoStatus"] == "NEW":
                    algo_order["algoStatus"] = "EXPIRED"

    def _order_record(self, order_id, symbol, side, quantity, client_order_id, price, reduce_only) -> dict[str, Any]:
        return {
            "orderId": order_id, "symbol": symbol, "status": "FILLED", "clientOrderId": client_order_id,
            "side": side, "type": "MARKET", "reduceOnly": reduce_only,
            "origQty": format(quantity, "f"), "executedQty": format(quantity, "f"), "cumQty": format(quantity, "f"),
            "avgPrice": format(price, "f"), "cumQuote": format(price * quantity, "f"),
            "updateTime": 1793891906543,
        }
