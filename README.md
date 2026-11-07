# binance-connector: Strategy execution service

An independent Python service that receives trade requests from **Django**, executes them
on **Binance USDⓈ-M futures Demo** (each user on their own Demo account), manages every
position's state, and reports every step back to Django: by HTTP push, and live over a
WebSocket.

```
Django ──POST /api/v1/requests──►  API process ── validate · store · acknowledge (202)
                                       │
                                    database (SQLite, WAL)
                                       │
                                   worker process
                                     ├─ execution_manager   one queue per account, accounts in parallel
                                     ├─ position_manager    entry (market / limit), leverage, TP/SL, close
                                     ├─ user-data streams   one Binance stream per account with a position
                                     ├─ reconciliation      Binance's real state wins, after every reconnect
                                     ├─ price_feed          live mark prices for the symbols in use
                                     ├─ reporting_service   outbox → POST to Django, retried, in order
                                     └─ websocket_manager   ws://…/ws: snapshot, ticks, updates, status
                                       │
Binance Demo ◄─────────────────────────┘   REST (signed per account) + WebSocket streams
```

- **Many users at once.** Every request names its user and account, and carries that
  account's Demo API key and secret. Ten users can open BTCUSDT at the same moment.
  - Each user's requests run in order in their own queue.
  - Accounts run in parallel.
  - No data is shared between them.
- **Nothing lost on a restart.** Everything lives in the database: requests, positions,
  orders, history, reports and errors. Orders are recorded *before* they are sent and
  identified by derived client ids, so a restarted worker looks them up instead of sending
  them twice. Positions are reconciled with Binance on every reconnect.
- **Never executed twice.** A repeated `request_id` returns the original result.
- **Secrets stay secret.** Credentials are stored Fernet-encrypted and masked in every log
  line, error, reply and report. A dedicated test audits this end to end.

## Documentation

| Document | Contents |
|---|---|
| [docs/api-contract.md](docs/api-contract.md) | **The Django ↔ Python contract**: request fields and rules, answers, the position view, reports to Django, the WebSocket |
| [docs/samples/](docs/samples/) | Real requests, replies, reports and WebSocket messages, recorded on Binance Demo |
| [docs/database.md](docs/database.md) | Every table and column, the states, how recovery works |
| [docs/install.md](docs/install.md) | Install, configure, run (Docker or venv), every setting, troubleshooting |
| [docs/test-results.md](docs/test-results.md) | The latest test run, mapped to the spec's acceptance criteria |

## Quick start

```bash
cp .env.example .env                     # then set CREDENTIALS_ENCRYPTION_KEY (see install.md)
docker compose up --build -d             # migrates, then starts the API (:5000) and the worker (:8765)
curl http://127.0.0.1:5000/health
```

Without Docker (Python 3.13):

```bash
python -m venv .venv && .venv\Scripts\activate
pip install -e ".[dev]"
alembic upgrade head
python -m binance_connector.server       # terminal 1: the API
python -m binance_connector.worker       # terminal 2: the worker
```

Details, the full configuration and Django's settings are in [docs/install.md](docs/install.md).

## How a request flows

1. **Django POSTs** an OPEN or CLOSE to `/api/v1/requests`.
2. **The API** validates every field:
   - the envelope;
   - the account ↔ user binding;
   - the symbol;
   - the quantity in coins, USDT notional or margin, converted with Binance's step size and
     minimum notional;
   - tick sizes;
   - which side of the price TP and SL sit on;
   - the optional price guard;
   - one active position per symbol per account.

   It stores the request and answers immediately: **202** accepted, **422** rejected with
   reasons, **200** for a repeat, **409** for a reused id with a different body.
3. **The worker** picks the request up. For an OPEN it:
   - sets the leverage;
   - sends the entry (market, or a resting limit order that expires after
     `LIMIT_ENTRY_TIMEOUT_SECONDS`);
   - records the fill and Binance's commission;
   - places the stop-loss, then the take-profit, **on Binance** as close-position orders
     triggered by the mark price.

   A CLOSE sends a reduce-only market order, or cancels a limit entry still waiting, then
   removes the remaining TP/SL.
4. **While it is open,** the account's user-data stream reports fills and triggers. When TP
   or SL fires, the position is closed as `TAKE_PROFIT` or `STOP_LOSS`. The live price feed
   gives the current price, unrealized P&L and the distance to TP and SL. Reconciliation
   checks everything against Binance after each reconnect and every
   `RECONCILE_INTERVAL_SECONDS`.
5. **Every step is reported**:
   - `REQUEST_ACCEPTED`;
   - `POSITION_OPENING`, then `OPEN`;
   - `PROTECTION_UPDATED`;
   - `POSITION_CLOSING`;
   - `POSITION_CLOSED` with the realized P&L, fees and close reason.

   Each report is POSTed to Django, retried until delivered, and published on the
   WebSocket. Django can also read the latest state at any time with `GET`.

Position states: `OPENING → OPEN → CLOSING → CLOSED`, plus `FAILED` (the entry never
executed) and `ERROR` (needs attention; reconciliation resolves it).

## Where each part of the spec lives

| Spec §11 module | Code |
|---|---|
| api | `binance_connector/api/` (routes, error handling), `server.py` (waitress) |
| request_validator | `api/request_schemas.py` (fields), `services/intake.py` (market rules, duplicates, decisions) |
| account_manager | `persistence/accounts.py` (accounts, encrypted credentials), `services/account_clients.py` (one Binance client per account) |
| execution_manager | `services/execution_manager.py` |
| position_manager | `services/position_manager.py`, plus `services/stream_events.py` and `services/reconciliation.py` |
| price_feed | `services/price_feed.py` |
| websocket_manager | `services/live_feed.py` (messages), `services/websocket_server.py` (server) |
| reporting_service | `persistence/reports.py` (outbox), `services/reporting.py` (delivery) |
| database | `persistence/` (models, repositories, Alembic migrations) |
| event_logger | `position_events` (in `persistence/positions.py`), `persistence/error_logs.py`, `security/redaction.py` |

Also: `binance/` (signed REST client, user-data stream), `domain/` (states, sizing, P&L),
`worker.py` (the worker process), `healthcheck.py`, `composition.py` (wiring).

## Testing

```bash
pytest              # ~430 unit and functional tests, no network, ~30 s
pytest -m e2e -v    # 6 end-to-end scenarios on Binance Demo (needs Demo keys in .env)
```

- **Unit tests** cover each component against simulated Binance accounts. The simulations
  are built from replies and stream events recorded on Demo (`tests/fixtures/`).
- **Functional tests** run the real Flask app, waitress, the WebSocket server and the
  wired worker over real local sockets, with Binance mocked at the HTTP layer. They include
  the secrets audit.
- **End-to-end tests** run the whole service against Binance Demo, with a fake Django
  receiving the reports and a WebSocket client watching:
  - open, protect, watch, close and report;
  - rejections and duplicates;
  - a limit entry cancelled by Django;
  - a worker restart with WebSocket resync;
  - two users at once;
  - a real TP/SL trigger.

The latest results, mapped to the spec's acceptance criteria, are in
[docs/test-results.md](docs/test-results.md).

## Scope and limits (version 1)

- **One exchange:** Binance USDⓈ-M futures, **Demo only.** Every URL is a setting, and the
  E2E tests refuse non-Demo URLs.
- **One-way position mode,** one active position per symbol per account (spec §4).
- **One instance.** SQLite on one host, and the API and worker run once each. There is no
  failover or horizontal scaling (out of scope, spec §12).
- **TP/SL execute on Binance,** not by a service-side price watcher. Protection therefore
  holds even while the service is down (see the contract's TP/SL section).
- **Fees** are Binance's reported commissions, and P&L is reported gross, fees and net.
  Funding payments are not included.
