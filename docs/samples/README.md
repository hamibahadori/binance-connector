# Sample requests and responses

Every file here was **recorded on Binance Demo** by the end-to-end tests
(`tests/e2e/test_demo_service.py`, with `E2E_CAPTURE_DIR` set). They are the service's real
output, not hand-written examples:
- the request ids carry the run's timestamp;
- the position ids are the service's own;
- the prices, fills and commissions are Binance Demo's.

**Credentials are replaced by placeholders** (`<demo-api-key>`, `<demo-api-secret>`,
`<django-token>`, …). To send a request yourself, use your own Demo keys and a new
`request_id`. The fields are explained in [../api-contract.md](../api-contract.md).

## Requests Django sends (`POST /api/v1/requests`)

| File | What |
|---|---|
| [open-request.json](open-request.json) | OPEN, MARKET, 0.002 BTC, leverage 5, TP/SL ±3 %, with `entry_price` as Django's reference |
| [limit-open-request.json](limit-open-request.json) | OPEN with a LIMIT entry 10 % below the market |
| [close-request.json](close-request.json) | CLOSE by `position_id` |

## Answers

| File | HTTP | What |
|---|---|---|
| [open-receipt-202.json](open-receipt-202.json) | 202 | accepted: the receipt with the new `position_id`, state OPENING |
| [open-duplicate-200.json](open-duplicate-200.json) | 200 | the same request again: the original receipt, `duplicate: true`, nothing executed |
| [open-rejected-422.json](open-rejected-422.json) | 422 | rejected (below the minimum notional), with `reasons` |
| [request-id-conflict-409.json](request-id-conflict-409.json) | 409 | the same `request_id` with a different body |
| [close-receipt-202.json](close-receipt-202.json) | 202 | CLOSE accepted |
| [close-already-closed-200.json](close-already-closed-200.json) | 200 | CLOSE of a position that already closed: `already_closed: true`, final result |

## Position views (`GET /api/v1/positions/{id}`, also inside every report)

| File | What |
|---|---|
| [position-open.json](position-open.json) | OPEN: entry fill, slippage, live price, unrealized P&L, fees so far, TP/SL resting with their distances |
| [position-closed.json](position-closed.json) | CLOSED by Django's CLOSE: exit price, realized P&L, fees, net; both TP/SL removed |
| [position-closed-by-trigger.json](position-closed-by-trigger.json) | CLOSED by a real TP/SL trigger on Binance: the triggered leg FINISHED, the other removed |
| [limit-cancelled-failed.json](limit-cancelled-failed.json) | a LIMIT entry cancelled by CLOSE before it filled: FAILED |
| [account-positions.json](account-positions.json) | `GET /api/v1/accounts/{id}/positions` |

## Pushed to Django and the WebSocket

| File | What |
|---|---|
| [django-report-position-closed.json](django-report-position-closed.json) | the final report POSTed to `DJANGO_REPORT_URL`: headers and body |
| [websocket-snapshot.json](websocket-snapshot.json) | `snapshot`: sent on every (re)connect; here after a worker restart, with the open position |
| [websocket-position-tick.json](websocket-position-tick.json) | `position_tick`: live price, P&L and TP/SL distance, about every second |
| [websocket-service-status.json](websocket-service-status.json) | `service_status`: price feed, account streams, report delivery, clients |
