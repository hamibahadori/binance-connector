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
        self.trades: dict[int, list[dict[str, Any]]] = {}
        """Fills per order id, each charged Binance Demo's 0.04% taker commission."""
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

    def place_limit_order(self, *, symbol, side, quantity, price, client_order_id) -> dict[str, Any]:
        """A marketable limit fills at once at ``fill_price``; otherwise it rests as NEW."""
        self._record_call("place_limit_order", symbol=symbol, side=side, quantity=quantity, price=price,
                          client_order_id=client_order_id)
        marketable = price >= self.fill_price if side == "BUY" else price <= self.fill_price
        order_id = next(self._order_ids)
        order = self._order_record(order_id, symbol, side, quantity, client_order_id, self.fill_price, False)
        order.update(type="LIMIT", price=format(price, "f"))
        if marketable:
            self._set_position(symbol, self.positions.get(symbol, Decimal(0)) + (quantity if side == "BUY" else -quantity))
        else:
            order.update(status="NEW", executedQty="0.0000", cumQty="0.0000", avgPrice="0.00", cumQuote="0")
            self.trades.pop(order_id, None)  # nothing has filled yet
        self.orders[client_order_id] = order
        reply = dict(order)
        reply.pop("avgPrice")
        reply.pop("cumQuote")
        return reply

    def fill_resting(self, client_order_id: str, quantity: str | None = None) -> None:
        """Simulate the market reaching a resting limit order: fill all of it, or ``quantity``."""
        order = self.orders[client_order_id]
        total = Decimal(order["origQty"])
        already_filled = Decimal(order["executedQty"])
        filled_now = Decimal(quantity) if quantity is not None else total - already_filled
        executed = already_filled + filled_now
        order.update(executedQty=format(executed, "f"), cumQty=format(executed, "f"),
                     avgPrice=order["price"], status="FILLED" if executed == total else "PARTIALLY_FILLED")
        self._record_trade(order["orderId"], filled_now, Decimal(order["price"]))
        signed = filled_now if order["side"] == "BUY" else -filled_now
        self._set_position(order["symbol"], self.positions.get(order["symbol"], Decimal(0)) + signed)

    def cancel_order(self, *, symbol: str, client_order_id: str) -> dict[str, Any]:
        self._record_call("cancel_order", symbol=symbol, client_order_id=client_order_id)
        order = self.orders.get(client_order_id)
        if order is None or order["status"] not in ("NEW", "PARTIALLY_FILLED"):
            raise BinanceAPIError(*UNKNOWN_ORDER_SENT)
        order["status"] = "CANCELED"
        return dict(order)

    def get_order_trades(self, *, symbol: str, order_id: int) -> list[dict[str, Any]]:
        self._record_call("get_order_trades", symbol=symbol, order_id=order_id)
        return [dict(trade) for trade in self.trades.get(order_id, [])]

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

    def _record_trade(self, order_id: int, quantity: Decimal, price: Decimal) -> None:
        self.trades.setdefault(order_id, []).append({
            "orderId": order_id, "qty": format(quantity, "f"), "price": format(price, "f"),
            "commission": format(quantity * price * Decimal("0.0004"), "f"), "commissionAsset": "USDT",
        })

    def _order_record(self, order_id, symbol, side, quantity, client_order_id, price, reduce_only) -> dict[str, Any]:
        if quantity > 0:
            self._record_trade(order_id, quantity, price)
        return {
            "orderId": order_id, "symbol": symbol, "status": "FILLED", "clientOrderId": client_order_id,
            "side": side, "type": "MARKET", "reduceOnly": reduce_only,
            "origQty": format(quantity, "f"), "executedQty": format(quantity, "f"), "cumQty": format(quantity, "f"),
            "avgPrice": format(price, "f"), "cumQuote": format(price * quantity, "f"),
            "updateTime": 1793891906543,
        }
