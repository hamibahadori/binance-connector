# binance-connector

A small Flask microservice that **opens and closes USDⓈ-M futures positions on Binance**. Other Live EX microservices call it over HTTP. It handles Binance's request signing, picks the right order side, and makes sure a close can never accidentally open a position the other way.

It talks to the **Binance Futures Demo Trading** REST API (`https://demo-fapi.binance.com`). Demo funds are virtual, but the orders are real orders on Binance's Demo exchange.

```
other microservice ──HTTP/JSON──► binance-connector ──signed REST──► Binance Futures Demo
```

---

## Contents

- [What it does](#what-it-does)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API reference](#api-reference)
  - [GET /health](#get-health)
  - [POST /api/v1/positions/open](#post-apiv1positionsopen)
  - [POST /api/v1/positions/close](#post-apiv1positionsclose)
  - [Errors](#errors)
- [Calling it from another service](#calling-it-from-another-service)
- [How it works](#how-it-works)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Limitations](#limitations)

---

## What it does

| You call | The service does on Binance |
|---|---|
| `POST /api/v1/positions/open` with `direction: LONG` | (optionally) set leverage, then **BUY** at market |
| `POST /api/v1/positions/open` with `direction: SHORT` | (optionally) set leverage, then **SELL** at market |
| `POST /api/v1/positions/close` | read the current position, then send an opposite-side market order flagged **reduce-only**, for all of the position or the quantity you ask for |

Rules it follows:

- **One-way position mode.** Each symbol has a single net position: long or short, never both. This is Binance's default.
- **Market orders only.** A call either opens or closes the position immediately, so there is never a resting order to track.
- **Closing is reduce-only.** A close can shrink a position but can never flip it into the opposite direction.
- **Stateless.** No database. Binance is the source of truth for positions.

---

## Quick start

Requirements: **Python 3.13** and a Binance account. Demo Trading is free and needs no KYC.

**1. Create Demo API keys.** Log in to Binance, switch to Demo Trading, and create a key at
<https://demo.binance.com/en/my/settings/api-management>.

**2. Install.** From the project folder:

```bash
py -3.13 -m venv .venv
```

```bash
.venv\Scripts\python -m pip install -e ".[dev]"
```

On macOS/Linux use `python3.13 -m venv .venv` and `.venv/bin/python` instead.

**3. Configure.** Copy `.env.example` to `.env` and fill in `BINANCE_API_KEY` and `BINANCE_API_SECRET`. `.env` is git-ignored; never commit it, and never put live (production) keys in it.

**4. Run** (from the project folder, so `.env` is found):

```bash
.venv\Scripts\flask --app binance_connector run --port 5001
```

**5. Try it:**

```bash
curl -s http://127.0.0.1:5001/health
```

```bash
curl -s -X POST http://127.0.0.1:5001/api/v1/positions/open -H "Content-Type: application/json" -d "{\"symbol\":\"BTCUSDT\",\"direction\":\"LONG\",\"quantity\":\"0.002\"}"
```

```bash
curl -s -X POST http://127.0.0.1:5001/api/v1/positions/close -H "Content-Type: application/json" -d "{\"symbol\":\"BTCUSDT\"}"
```

In Windows PowerShell 5.1, `curl` is an alias for `Invoke-WebRequest`. Type `curl.exe` to get real curl.

The service **refuses to start** when the API key or secret is missing, and prints which variable is absent.

---

## Configuration

Settings come from environment variables. On startup, `.env` in the current working directory is loaded first. A variable already set in the real environment wins over `.env`.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `BINANCE_API_KEY` | yes | — | Demo Trading API key |
| `BINANCE_API_SECRET` | yes | — | Demo Trading API secret (HMAC) |
| `BINANCE_BASE_URL` | no | `https://demo-fapi.binance.com` | Binance USD-M Futures REST base URL |
| `BINANCE_RECV_WINDOW_MS` | no | `5000` | How long (1–60000 ms) Binance accepts a signed request after its timestamp |
| `BINANCE_TIMEOUT_SECONDS` | no | `10` | HTTP timeout for each call to Binance |
| `E2E_SYMBOL` | no | `BTCUSDT` | E2E tests only: symbol to trade |
| `E2E_QUANTITY` | no | `0.002` | E2E tests only: order size; must reach the symbol's minimum order value (about 100 USDT on BTCUSDT) |

### Slow networks

The defaults (`recvWindow` 5000 ms, timeout 10 s) follow Binance's recommendation and suit a normal connection. If requests to Binance are slow from your machine, you will see two symptoms:

- `binance_rejected` with code **`-1021`** ("Timestamp for this request is outside of the recvWindow"). Requests are arriving too long after they were signed.
- **`504 binance_outcome_unknown`** caused by timeouts.

Measure the round trip, then raise both values in `.env`. For example, on a link with multi-second round trips:

```
BINANCE_RECV_WINDOW_MS=20000
BINANCE_TIMEOUT_SECONDS=30
```

The trade-off: with a larger `recvWindow`, a delayed market order can still execute that much later, at whatever the price is by then.

---

## API reference

All request and response bodies are JSON. Send `Content-Type: application/json`.

**Quantities and prices are decimal strings** in responses (`"0.002"`, `"65000.10"`), so no precision is lost to floating point. Send quantities as strings too. JSON numbers are accepted, but strings are exact.

### GET /health

Liveness check. It reports that the process is up and **does not call Binance**.

```http
GET /health
```

```json
{"status": "ok"}
```

### POST /api/v1/positions/open

Opens a position, or adds to one in the same direction, with a MARKET order.

**Request body**

| Field | Type | Required | Rules |
|---|---|---|---|
| `symbol` | string | yes | Binance futures symbol, e.g. `"BTCUSDT"`. Case-insensitive; 2–20 letters/digits. |
| `direction` | string | yes | `"LONG"` (buys) or `"SHORT"` (sells). Case-insensitive. |
| `quantity` | string or number | yes | Greater than 0, in the base asset (BTC for BTCUSDT). Must match the symbol's step size and minimum notional; Binance checks this. |
| `leverage` | integer | no | 1–125 (Binance may allow less for a symbol). If present, set **before** the order. It **stays set** on the account for that symbol afterwards. |
| `client_order_id` | string | no | Your id for this order: 1–36 chars from `A–Z a–z 0–9 . : / _ -`. Generated (`lx-…`) when omitted. Use it to reconcile; see [Unknown outcomes](#unknown-outcomes-504). |

Unknown fields are rejected, so a misspelling like `levrage` fails loudly instead of being ignored.

**Example**

```json
{
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "quantity": "0.002",
  "leverage": 10,
  "client_order_id": "strategy-7-entry-42"
}
```

**Success: `200 OK`**

```json
{
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "side": "BUY",
  "order_id": 4000000001,
  "client_order_id": "strategy-7-entry-42",
  "status": "FILLED",
  "requested_quantity": "0.002",
  "executed_quantity": "0.002",
  "average_price": "65000.10",
  "leverage": 10
}
```

| Field | Meaning |
|---|---|
| `direction` | Direction of the position opened |
| `side` | Order side sent to Binance (`BUY` for LONG, `SELL` for SHORT) |
| `order_id` | Binance's order id |
| `client_order_id` | Yours, or the generated one |
| `status` | Binance order status, normally `FILLED` for a market order |
| `requested_quantity` | Quantity asked for |
| `executed_quantity` | Quantity Binance filled |
| `average_price` | Average fill price, or `null` if it couldn't be read (see below) |
| `leverage` | Leverage applied by this call, or `null` if none was requested |

**About `average_price`.** Binance's reply to a new order doesn't include the fill price, so the service looks the order up once it has filled (`GET /fapi/v1/order` by `client_order_id`).

If that lookup fails, **the order has still been placed**. The service then returns `200` with `"average_price": null` rather than reporting an error for a trade that happened. A caller that needs the price can look the order up later by `client_order_id`.

### POST /api/v1/positions/close

Closes all or part of the open position for a symbol. The service reads the position from Binance, then sends an opposite-side MARKET order with `reduceOnly=true`. A long is closed by selling and a short by buying.

**Request body**

| Field | Type | Required | Rules |
|---|---|---|---|
| `symbol` | string | yes | As for open. |
| `quantity` | string or number | no | Greater than 0. **Omit it to close the whole position.** Must not exceed the open quantity (that returns `409`). |
| `client_order_id` | string | no | As for open. |

**Examples**

```json
{"symbol": "BTCUSDT"}
```

```json
{"symbol": "BTCUSDT", "quantity": "0.001", "client_order_id": "strategy-7-exit-42a"}
```

**Success: `200 OK`.** Same shape as open. `direction` is the direction of the position that was closed (or reduced), and `leverage` is always `null`.

```json
{
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "side": "SELL",
  "order_id": 4000000002,
  "client_order_id": "lx-3f2a9c...",
  "status": "FILLED",
  "requested_quantity": "0.002",
  "executed_quantity": "0.002",
  "average_price": "65110.40",
  "leverage": null
}
```

### Errors

Every error is JSON with at least `error` (a stable machine-readable code) and `message` (human-readable text).

| HTTP | `error` | When | Extra fields |
|---|---|---|---|
| 400 | `invalid_request_body` | Body missing, not JSON, or not a JSON object | — |
| 400 | `validation_error` | A field is missing, has the wrong type, breaks a rule, or is unknown | `details: [{field, message}]` |
| 404 | `no_open_position` | Close requested but the symbol has no open position | `symbol` |
| 404 | `not_found` | Unknown URL | — |
| 405 | `method_not_allowed` | Wrong HTTP method (e.g. GET on `/open`) | `Allow` header |
| 409 | `close_quantity_exceeds_position` | Partial close asks for more than is open; **no order is sent** | `symbol`, `requested_quantity`, `open_quantity` |
| 409 | `hedge_mode_not_supported` | The account is in hedge mode; switch it to one-way mode | `symbol` |
| 422 | `binance_rejected` | Binance refused the request (insufficient margin, bad step size, unknown symbol, invalid key...) | `binance: {http_status, code, message}` |
| 502 | `binance_unavailable` | Could not connect to Binance; **nothing was executed** | — |
| 504 | `binance_outcome_unknown` | Binance timed out or returned 5xx; **the order may or may not have executed** | `execution_status: "unknown"`, `client_order_id` |
| 500 | `internal_error` | Unexpected bug; details are in the service log, never in the response | — |

**Validation error example**

```json
{
  "error": "validation_error",
  "message": "The request body is invalid.",
  "details": [{"field": "quantity", "message": "Input should be greater than 0"}]
}
```

**Binance rejection example.** The `binance.code` values are listed in [Binance's error codes](https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code).

```json
{
  "error": "binance_rejected",
  "message": "Binance rejected the request.",
  "binance": {"http_status": 400, "code": -2019, "message": "Margin is insufficient."}
}
```

Common codes:

| Code | Meaning |
|---|---|
| `-2019` | Insufficient margin |
| `-1111` | Quantity has too many decimals for the symbol |
| `-4164` | Order value below the symbol's minimum notional (reduce-only orders are exempt) |
| `-1121` | Invalid symbol |
| `-4061` | The account is in hedge mode |
| `-1021` | Timestamp outside recvWindow; the machine clock is off |
| `-2015` | Invalid API key or permissions |

#### Unknown outcomes (504)

When Binance times out or answers with a 5xx, Binance documents that the request **may have been executed**. The service therefore **never retries an order by itself**. A blind retry could open the same position twice.

If you get `504 binance_outcome_unknown`:

1. Keep the `client_order_id` from the response. Sending your own id on every call makes this easy.
2. Check what actually happened before doing anything else, for example in Binance's order history for that client order id.
3. Only then decide whether to send the request again.

A `502 binance_unavailable` is different: the connection was never made, so nothing happened and it is safe to retry.

---

## Calling it from another service

Python with `requests`:

```python
import uuid
import requests

CONNECTOR_URL = "http://binance-connector:5001"

def open_long(symbol: str, quantity: str, leverage: int | None = None) -> dict:
    client_order_id = f"svc-{uuid.uuid4().hex[:20]}"
    body = {"symbol": symbol, "direction": "LONG", "quantity": quantity,
            "client_order_id": client_order_id}
    if leverage is not None:
        body["leverage"] = leverage

    response = requests.post(f"{CONNECTOR_URL}/api/v1/positions/open", json=body, timeout=30)
    if response.status_code == 504:
        # Outcome unknown: reconcile by client_order_id before any retry.
        raise RuntimeError(f"Order {client_order_id} outcome unknown")
    response.raise_for_status()
    return response.json()

def close_position(symbol: str, quantity: str | None = None) -> dict:
    body = {"symbol": symbol} if quantity is None else {"symbol": symbol, "quantity": quantity}
    response = requests.post(f"{CONNECTOR_URL}/api/v1/positions/close", json=body, timeout=30)
    response.raise_for_status()
    return response.json()
```

Use a client timeout well above `BINANCE_TIMEOUT_SECONDS`. An open with leverage makes up to three Binance calls (leverage, order, price lookup), so allow at least three times that.

---

## How it works

```
binance_connector/
├── api/          HTTP: validate the JSON body → call the service → serialise; map exceptions → status codes
├── services/     business rules: sides, reduce-only, partial close, client order ids
└── binance/      signed REST calls to Binance; typed errors for rejected / unreachable / unknown
```

Dependencies only point downwards: api → services → binance. The service layer depends on an interface, not on HTTP, so it is unit-tested with a fake Binance client.

**Binance endpoints used**

| Purpose | Endpoint |
|---|---|
| Set leverage | `POST /fapi/v1/leverage` |
| Place a market order | `POST /fapi/v1/order` (`type=MARKET`, `newOrderRespType=RESULT`, `reduceOnly` on close) |
| Read the order's average fill price | `GET /fapi/v1/order` (by `origClientOrderId`) |
| Read the position | `GET /fapi/v3/positionRisk` |

**Signing.** Each request gets `timestamp` and `recvWindow`. Then the exact URL-encoded parameter string is signed with HMAC-SHA256 using the API secret, and sent with the API key in the `X-MBX-APIKEY` header. All parameters go in the query string, so the string that is signed is byte-for-byte the string that is sent. Reference: [Binance USD-M Futures general info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info).

**Why some things are the way they are**

- **A small hand-written client instead of Binance's official SDK.** Only three endpoints are needed, and the SDK has had many breaking major releases. A thin, fully tested client is easier to keep working.
- **Market orders only.** A limit order can sit unfilled, and tracking it would need state, which means a database. That is out of scope.
- **Reduce-only closes.** Even if the position changed between reading it and closing it, the close can never flip into a new position.
- **Decimals, never floats.** A float cannot represent `0.1` exactly, and an order quantity must be exact.
- **No automatic retries.** See [Unknown outcomes](#unknown-outcomes-504).

---

## Testing

The service was built test-first, and the tests come in three layers:

| Layer | Folder | What it tests | Network |
|---|---|---|---|
| Unit | `tests/unit` | Signing (against Binance's documented example), config, the Binance client, the open/close rules, request validation | none; HTTP mocked with [`responses`](https://github.com/getsentry/responses) |
| Functional | `tests/functional` | The whole app through Flask's test client: routes, validation, error mapping, the real client | none; Binance mocked, and every expected call must happen |
| End-to-end | `tests/e2e` | The real service on a local port, called over HTTP, placing **real orders on Binance Demo**; results checked on Binance directly | **Binance Demo** |

**Unit and functional tests** (the default; they never touch the network):

```bash
.venv\Scripts\python -m pytest
```

**End-to-end tests** are opt-in, because they place orders:

```bash
.venv\Scripts\python -m pytest -m e2e -v
```

Before running the E2E tests:

- Put Demo keys in `.env`. Without them, the E2E tests are skipped with a message.
- The Demo futures account must be in **one-way** mode and hold some USDT.
- `E2E_QUANTITY` × the price of `E2E_SYMBOL` must reach the symbol's minimum order value: the `MIN_NOTIONAL` filter in `GET /fapi/v1/exchangeInfo`, about 100 USDT for BTCUSDT. Closing orders are reduce-only, and Binance exempts those from the minimum.
- The tests set the test symbol's leverage to 5, and it stays that way afterwards.

The E2E tests protect your account:

- They refuse to run against any base URL other than Demo.
- They skip if the account already holds a position in the test symbol, so your own positions are never touched.
- They close anything a test leaves open.

---

## Project layout

```
binance-connector/
├── binance_connector/
│   ├── __init__.py            create_app() — the Flask app factory
│   ├── config.py              Settings, read and validated from the environment
│   ├── errors.py              every exception type the service raises
│   ├── api/
│   │   ├── routes.py          /health, /api/v1/positions/open, /api/v1/positions/close
│   │   ├── schemas.py         request validation (pydantic)
│   │   └── error_handlers.py  exception → JSON error + status code
│   ├── services/
│   │   └── position_service.py  open/close business rules
│   └── binance/
│       ├── client.py          signed REST client for Binance USD-M Futures
│       └── signing.py         HMAC-SHA256 signing
├── tests/
│   ├── unit/  functional/  e2e/
│   └── conftest.py
├── .env.example               template for .env (copy and fill in)
├── pyproject.toml             dependencies and pytest settings
└── README.md
```

---

## Limitations

These are deliberate, to keep the service small. Each could be added later.

| Not supported | Notes |
|---|---|
| Authentication and TLS on the service's own API | Meant for an internal network only. Do not expose it publicly. |
| Rate-limit handling | Binance answers `429` when overloaded; it surfaces as a `422 binance_rejected` with that status. |
| Hedge mode | One-way mode only. Close returns `409 hedge_mode_not_supported` in hedge mode. |
| Limit, stop-loss and take-profit orders | Market orders only. Binance moved stop/TP orders to separate Algo endpoints (`/fapi/v1/algoOrder`) in December 2025. |
| Margin type (isolated/cross) | The account's current setting applies. |
| Clock sync with Binance | Uses the local clock. Keep the machine's clock synced, or Binance returns `-1021`. Slow networks cause `-1021` too; see [Slow networks](#slow-networks). |
| Production web server | `flask run` is a development server. Use a WSGI server such as waitress for anything long-running. |
| Live (real-money) trading | Built and tested for Demo only. |
