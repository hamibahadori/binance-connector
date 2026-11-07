# API and the Django ↔ Python contract

This is the contract between Django and the Strategy execution service. It covers what
Django sends, what it gets back, the reports the service pushes, and the live WebSocket.
Real samples, recorded on Binance Demo by the E2E tests, are in [samples/](samples/).

```
Django ──POST /api/v1/requests──► API (validate, store, acknowledge: 202 / 422 / 200 / 409)
                                     │ database
Django ◄──POST DJANGO_REPORT_URL──  Worker (execute on Binance Demo, track, reconcile)
Django ◄══ ws://…/ws ════════════   Worker (live prices, P&L, updates, status)
Django ──GET …───────────────────► API (latest state of a request, a position, an account)
```

- **Intake is asynchronous.** The API validates, stores and answers within milliseconds.
  The worker then executes in the background, one request at a time per account and in
  parallel across accounts. Results reach Django as reports and over the WebSocket, and can
  be read at any time with the GET endpoints.
- All bodies are JSON, UTF-8. **Decimals are strings** (e.g. `"0.002"`, `"75900.1"`):
  send them as strings, and read them as decimals, not floats. Times are ISO 8601 UTC.
- Version 1 covers one exchange, Binance USDⓈ-M futures, on **Demo** only.

## Authentication

| Direction | Setting | How |
|---|---|---|
| Django → API | `INBOUND_API_TOKEN` | `Authorization: Bearer <token>` on every `/api/v1/*` call. 401 otherwise. Unset = no check. `/health` never needs it |
| Worker → Django | `DJANGO_REPORT_AUTH_HEADER`, `DJANGO_REPORT_AUTH_VALUE` | sent as is on every report, e.g. `Authorization: Bearer …` or `X-Api-Key: …` |
| Django → WebSocket | `WEBSOCKET_TOKEN` | `Authorization: Bearer <token>`, or `?token=<token>` for browsers. 401 otherwise. Unset = no check |

The tokens are compared in constant time and are masked in every log line.

---

## Submitting a request: `POST /api/v1/requests`

### Fields every request carries

| Field | Type | Rules |
|---|---|---|
| `request_id` | string | `[A-Za-z0-9._:-]{1,64}`, **unique across all accounts**. Use a new one for every request, CLOSEs included |
| `user_id` | string | same pattern; the owner of the account |
| `account_id` | string | same pattern; the user's trading account. Bound to its first `user_id`: a request naming another user is rejected |
| `api_key` | string | the account's Binance Demo API key (8–256 characters) |
| `secret_key` | string | its secret (8–256 characters) |
| `action` | string | `OPEN` or `CLOSE` (case-insensitive) |
| `timestamp` | string or integer | when Django created the request: ISO 8601 (`2026-11-07T12:00:00Z`) or Unix epoch **milliseconds** |

Credentials are stored encrypted and never returned, logged or reported. The latest
credentials sent for an account replace the stored ones, so a key rotation just works.

### OPEN

| Field | Required | Type | Meaning |
|---|---|---|---|
| `symbol` | yes | string | e.g. `BTCUSDT` (case-insensitive) |
| `direction` | yes | string | `LONG` or `SHORT` |
| `quantity` | yes | decimal | the size, in the unit of `quantity_type` |
| `quantity_type` | no | string | `BASE` (default): coins. `NOTIONAL`: USDT value. `MARGIN`: USDT margin, multiplied by `leverage` |
| `leverage` | no (yes for MARGIN) | integer | 1–125, set on Binance before the entry. Omitted = the account's current setting |
| `entry_type` | no | string | `MARKET` (default) or `LIMIT` |
| `entry_price` | LIMIT only | decimal | **LIMIT:** the limit price. **MARKET:** Django's reference price. It is informational: the fill's slippage is reported against it. It also feeds the optional price guard |
| `take_profit_price` | no | decimal | trigger price of the take-profit |
| `stop_loss_price` | no | decimal | trigger price of the stop-loss |

Unknown fields are rejected, so a typo can't be silently ignored.

**How the quantity becomes an order.** The reference price is the limit price for LIMIT,
and the current mark price for MARKET.

- `BASE`: used as is. It must already be a multiple of the symbol's step size: it is
  refused, not rounded, because an explicit coin amount shouldn't change silently.
- `NOTIONAL`: coins = quantity ÷ reference price, **rounded down** to the step size.
- `MARGIN`: coins = quantity × leverage ÷ reference price, **rounded down**.
- The result must reach the symbol's minimum quantity and minimum notional (BTCUSDT on
  Demo: 0.001 BTC and 50 USDT). MARKET orders use Binance's MARKET_LOT_SIZE rules and LIMIT
  orders use LOT_SIZE.

**Price rules.**
- Every price must be a multiple of the symbol's tick size (BTCUSDT: 0.1). Prices are
  refused, not rounded.
- The take-profit must sit on the profitable side of the reference price, and the
  stop-loss on the protective side. For a LONG that is TP above and SL below; a SHORT is
  the reverse.
- **Price guard:** when `PRICE_GUARD_MAX_DEVIATION_PERCENT` is set, a MARKET OPEN whose
  `entry_price` is further than that from the mark price is rejected.

**One position per symbol per account.** An OPEN for a symbol on which the account already
has an active position is rejected. Different accounts can hold the same symbol at once.

### CLOSE

| Field | Required | Meaning |
|---|---|---|
| `position_id` | one of the two | the position to close |
| `symbol` | one of the two | the account's active position on that symbol |

What the close does depends on the position's state:

| Position is… | The CLOSE… |
|---|---|
| OPEN (or ERROR) | sends a reduce-only market order for the whole position, then removes any TP/SL left. Ends **CLOSED**, `close_reason: CLOSE_REQUESTED` |
| OPENING, limit entry unfilled | cancels the entry. Nothing filled ends **FAILED**; a partial fill closes what filled |
| CLOSING | nothing more (a close is already running) |
| already CLOSED or FAILED, e.g. its TP fired first | executes nothing. It answers **200** with the final result and `already_closed: true` |

### Answers

| HTTP | When | Body |
|---|---|---|
| **202** | accepted and queued for execution | receipt, `status: ACCEPTED`, with the `position_id` |
| **200** | the same `request_id` with the same body again | the original receipt, `duplicate: true`. **Nothing is executed again** |
| **200** | a CLOSE for a position that already finished | receipt with `already_closed: true` and the final position |
| **422** | rejected by a business rule | receipt, `status: REJECTED`, `reasons: [...]`. Stored, so a retry gets the same answer |
| **409** | the `request_id` was already used for a **different** body | `{"error": "request_id_conflict", "message", "request_id"}` |
| **400** | the request can't be identified: not JSON, or a bad envelope field | `{"error": "validation_error" \| "invalid_request_body", "message", "details": [{"field", "message"}]}`. Nothing stored |
| **401** | missing or wrong bearer token | `{"error": "unauthorized", "message"}` |
| **503** | Binance market data, or the service's database, is unavailable | `{"error": "market_data_unavailable" \| "database_unavailable", "message"}`. Retry with the same `request_id` |

Duplicates are compared on the normalised body without the credentials, so a retry with
rotated keys is still the same request.

**The receipt:**

```json
{
  "request_id": "…", "status": "ACCEPTED", "action": "OPEN",
  "user_id": "…", "account_id": "…", "position_id": "6624ac9fffd641098472996a",
  "received_at": "2026-11-06T22:51:25.034328+00:00", "decided_at": "2026-11-06T22:51:25.043083+00:00",
  "reasons": [], "duplicate": false, "already_closed": false,
  "position": { …the position view, state OPENING… }
}
```

---

## Reading the latest state

| Endpoint | Returns |
|---|---|
| `GET /api/v1/requests/{request_id}` | the request view (below) |
| `GET /api/v1/positions/{position_id}` | the position view |
| `GET /api/v1/accounts/{account_id}/positions?state=OPEN` | `{account_id, user_id, positions: [position view…]}`, optionally filtered by state |

Unknown ids answer 404 (`request_not_found`, `position_not_found`, `account_not_found`).
An invalid `state` answers 400 `invalid_state`.

**Request view:** `request_id, status, action, processing_state` (`PENDING`,
`IN_PROGRESS` or `DONE` in the worker's queue), plus:
- `user_id, account_id, position_id`;
- `requested_at, received_at, decided_at, processed_at`;
- `reasons`;
- `payload`: the body as received, without credentials;
- `position`: the position view.

### The position view (spec §6)

The same object appears everywhere: in receipts, in GET answers, in every report to
Django, and in WebSocket snapshots and updates.

| Field | Meaning |
|---|---|
| `position_id`, `request_id`, `close_request_id`, `user_id`, `account_id` | the ids: the position, the OPEN that created it, and the CLOSE that ended it |
| `symbol`, `direction`, `leverage` | |
| `state` | `OPENING`, `OPEN`, `CLOSING`, `CLOSED`, `FAILED`, `ERROR` (below) |
| `quantity` | `{requested, type, ordered, filled}`: as sent, its unit, the coins ordered, the coins filled |
| `entry` | `{type, requested_price, average_price, slippage}`; slippage = average fill − `entry_price`, for MARKET |
| `times` | `{received, started, ended, updated}`: created, first fill, closed, last change |
| `prices` | `{entry, current, current_at, exit}`; `current` is the latest mark price, from the live feed |
| `pnl` | `{kind, gross, fees, net}`. `kind` is `UNREALIZED` (while open, at the current price) or `REALIZED` (once closed). `fees` are Binance's commissions on every fill, so far; `net = gross − fees` |
| `take_profit`, `stop_loss` | `{price, status, distance: {absolute, percent}}` or null when not requested. `status` is Binance's: `NEW` resting, `TRIGGERED` / `FINISHED` fired, `EXPIRED` / `CANCELED` removed, `REJECTED` |
| `close_reason` | `CLOSE_REQUESTED`, `TAKE_PROFIT`, `STOP_LOSS`, `EXTERNAL` (gone without one of our orders, e.g. liquidation or a manual close) |
| `error` | the latest problem, for people; credentials are always masked |

**States:**

| State | Meaning | Terminal |
|---|---|---|
| `OPENING` | the entry is being placed, or a LIMIT entry waits for its fill | |
| `OPEN` | held on Binance, with its TP/SL resting there | |
| `CLOSING` | a close is being executed | |
| `CLOSED` | closed; see `close_reason` and the realized P&L | yes |
| `FAILED` | nothing was ever held: the entry was rejected, or a LIMIT entry expired (`LIMIT_ENTRY_TIMEOUT_SECONDS`) or was cancelled unfilled | yes |
| `ERROR` | something needs attention: an unknown outcome, or protection that could not be placed. Reconciliation with Binance moves it on by itself | |

### TP/SL

Take-profit and stop-loss are placed **on Binance** as conditional, close-position orders
triggered by the mark price (`BINANCE_TRIGGER_PRICE_TYPE`). Binance executes them even if
the service is down. The service watches the price and the orders. When one fires, it
records the fill and closes the position as `TAKE_PROFIT` or `STOP_LOSS`, removes the other
leg and reports the result. This meets spec §9's outcome: closed at the threshold, with the
reason recorded, the result reported and the related orders updated. It does so more
reliably than a service-side price watcher, which would leave positions unprotected
whenever the service is not running.

---

## Reports to Django

Every important step is POSTed to `DJANGO_REPORT_URL`.

- **Body:** the report JSON, below.
- **Headers:**
  - `Content-Type: application/json`;
  - `X-Report-Id` (the report's UUID);
  - `X-Report-Event` (e.g. `POSITION_CLOSED`);
  - the configured auth header.
- **Delivered** means Django answered any 2xx.
- **Retried** on anything else (another status, a timeout, a refused connection) after 5,
  10, 20 … s, capped at 5 minutes. After `DJANGO_REPORT_MAX_ATTEMPTS` (default 10) the
  report is marked FAILED and written to the error log.
- **In order per position:** a report waiting for a retry holds back the later reports of
  the same position (or request), so Django never sees CLOSED before OPEN. Other positions
  are not held back.
- **At least once:** a report can arrive twice, for example when Django stored it but its
  2xx was lost. **Django should ignore a `report_id` it has already processed.**
- Reports are written in the same database transaction as the change they report, so none
  is lost to a crash. Without `DJANGO_REPORT_URL` they are kept and wait.

| Event | When | Body |
|---|---|---|
| `REQUEST_ACCEPTED` | a request passed validation | request report |
| `REQUEST_REJECTED` | a request was rejected | request report, with `reasons` |
| `POSITION_OPENING` | a position was created (or went back to waiting for its limit entry) | position report |
| `POSITION_OPEN` | the entry filled; also after a rejected close leaves it open (`error` says why) | position report |
| `PROTECTION_UPDATED` | a take-profit or stop-loss changed status (placed, fired, removed) | position report |
| `POSITION_CLOSING` | a close started | position report |
| `POSITION_CLOSED` | **the final result**: `close_reason`, exit price, realized P&L, fees | position report |
| `POSITION_FAILED` | the entry never executed | position report, with `error` |
| `POSITION_ERROR` | the position needs attention | position report, with `error` |
| `CLOSE_FAILED` | a CLOSE could not start (e.g. Binance unreachable after retries); the position is still open | position report, with `error` |

**Position report** = `{report_id, event, occurred_at}` + the position view. See
[samples/django-report-position-closed.json](samples/django-report-position-closed.json).

**Request report:**

```json
{"report_id": "…", "event": "REQUEST_REJECTED", "occurred_at": "…",
 "request_id": "…", "user_id": "…", "account_id": "…", "action": "OPEN", "status": "REJECTED",
 "position_id": null, "reasons": ["…"], "times": {"received": "…", "decided": "…"}}
```

---

## The live WebSocket

`ws://WEBSOCKET_HOST:WEBSOCKET_PORT/ws`, served by the worker (default `127.0.0.1:8765`).

- `?account_id=<id>` follows one account: its snapshot, ticks and updates only.
- With `WEBSOCKET_TOKEN` set, send `Authorization: Bearer <token>` or `?token=<token>`.
- Any other path answers 404, and a wrong token answers 401.
- The server pings every 20 s.

Every message is `{"type": "...", "sent_at": "<ISO time>", "data": {...}}`:

| `type` | When | `data` |
|---|---|---|
| `snapshot` | **on every connect and reconnect**, and on request | `{report_sequence, positions: [position view…]}`: every active position, with the latest price |
| `position_tick` | every mark-price tick (about 1 s), per active position on that symbol | `{position_id, account_id, user_id, symbol, direction, state, prices, pnl, take_profit, stop_loss, updated_at}` |
| `position_update` | every position report (the same events as Django's) | the position report; the message also carries `event` and `report_sequence` |
| `request_update` | every request report | the request report; also `event`, `report_sequence` |
| `service_status` | every 5 s | `{price_feed: {connected, symbols, last_tick_at, last_error}, account_streams: {<account_id>: {connected}}, reports: {PENDING, SENT, FAILED counts}, clients}` |

**Resync (spec §7).** After a disconnect, reconnect: the first message is a fresh
`snapshot`, which is the current state. A client can also send `{"type": "resync"}` at any
time to get one. Updates the snapshot already reflects (`report_sequence` ≤ the
snapshot's) are not sent again.

**Slow clients.** A client that falls 1000 messages behind is closed with code **1013**.
It reconnects and resyncs from a snapshot. It never silently misses updates.

---

## Health

- `GET /health` → `200 {"status": "ok"}`. Liveness only; it never calls Binance.
- `python -m binance_connector.healthcheck api|worker` exits 0 when the API, or the
  worker's WebSocket, answers. It is used by docker-compose.

## Error bodies

Every error is `{"error": "<code>", "message": "<text>", …}`:

| HTTP | `error` |
|---|---|
| 400 | `invalid_request_body`, `validation_error`, `invalid_state` |
| 401 | `unauthorized` |
| 404 | `request_not_found`, `position_not_found`, `account_not_found`, `not_found` |
| 405 | `method_not_allowed` |
| 409 | `request_id_conflict` |
| 503 | `market_data_unavailable`, `database_unavailable` |
| 500 | `internal_error`: no internals are ever included; see the service logs |
