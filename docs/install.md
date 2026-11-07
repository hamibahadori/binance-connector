# Install, set up and run

The service has two processes that share one database. Both must run:

| Process | Command | Does |
|---|---|---|
| **API** | `python -m binance_connector.server` | takes Django's requests (validates, stores, answers) and serves the GET endpoints |
| **worker** | `python -m binance_connector.worker` | executes requests on Binance Demo, follows every account's stream, reconciles, delivers reports to Django, serves the WebSocket |

Nothing depends on a particular machine or person. Everything is configured through
environment variables, or a `.env` file in the project folder. You can run it with Docker
(quickest) or in a Python virtual environment.

## 1. What you need

- **Binance Demo trading credentials** for each user account. Django sends them with every
  request, so the service needs none of its own to run.
  - Create them at [demo.binance.com → API management](https://demo.binance.com/en/my/settings/api-management).
  - Demo uses virtual funds and is free.
  - The account must be in **one-way** position mode (Binance's default).
- Either **Docker** with Compose v2, or **Python 3.13**.
- Network access to `demo-fapi.binance.com` and `demo-fstream.binance.com`, and to Django
  (for reports).

## 2. Configure

```bash
cp .env.example .env
```

(On Windows: `copy .env.example .env`.) Then edit `.env`. Only one value is required:

```bash
python -m binance_connector.security.credentials
```

prints a new encryption key. Put it in `CREDENTIALS_ENCRYPTION_KEY`. It encrypts the
credentials stored in the database. Keep it safe: if it is lost, stored credentials become
unreadable. That isn't fatal, because Django resends them with the next request. (With
Docker, run it as `docker compose run --rm api python -m binance_connector.security.credentials`
once the image is built.)

To connect Django, also set:

| Variable | Example | Purpose |
|---|---|---|
| `INBOUND_API_TOKEN` | a long random string | Django must send `Authorization: Bearer <it>` |
| `DJANGO_REPORT_URL` | `https://django.example/api/execution-reports` | where reports are POSTed |
| `DJANGO_REPORT_AUTH_HEADER` / `DJANGO_REPORT_AUTH_VALUE` | `Authorization` / `Bearer …` | how reports authenticate to Django |
| `WEBSOCKET_TOKEN` | a long random string | clients of the live feed must send it |

The full list is in [Configuration reference](#configuration-reference).

## 3a. Run with Docker (recommended)

```bash
docker compose up --build -d
```

1. **`migrate`** creates or upgrades the database (on the `data` volume) and exits.
2. **`api`** then listens on `http://127.0.0.1:5000` and **`worker`** on `ws://127.0.0.1:8765/ws`.

Check it:

```bash
docker compose ps          # api and worker: "healthy"
curl http://127.0.0.1:5000/health
docker compose logs -f     # follow both processes
docker compose down        # stop; the database volume is kept (down -v deletes it)
```

The ports are published on the host's **127.0.0.1 only**. To let other machines (Django on
another host) connect:
- change `127.0.0.1:` in the `ports:` lines of `docker-compose.yml`;
- set `INBOUND_API_TOKEN` and `WEBSOCKET_TOKEN` first.

If Django runs on the same machine outside Docker, the containers reach it at
`http://host.docker.internal:<port>`.

## 3b. Run in a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows   (Linux/macOS: source .venv/bin/activate)
pip install -e ".[dev]"
alembic upgrade head              # create or upgrade the database (once, and after updates)
```

Then start both processes, each in its own terminal:

```bash
python -m binance_connector.server     # the API, on API_HOST:API_PORT (127.0.0.1:5000)
python -m binance_connector.worker     # the worker, WebSocket on WEBSOCKET_HOST:WEBSOCKET_PORT
```

Ctrl+C stops either one cleanly: the worker lets running requests finish first.
(`flask --app binance_connector run` also serves the API, but for development only.)

## 4. Try it

With `INBOUND_API_TOKEN` empty, an OPEN looks like this:

```bash
curl -X POST http://127.0.0.1:5000/api/v1/requests -H "Content-Type: application/json" -d @docs/samples/open-request.json
```

`docs/samples/open-request.json` holds placeholder credentials. Use a copy with real Demo
keys and a new `request_id`. The answer is `202` with a `position_id`. Then:

```bash
curl http://127.0.0.1:5000/api/v1/positions/<position_id>
```

shows it move from `OPENING` to `OPEN`. The contract is in [api-contract.md](api-contract.md).

## 5. Run the tests

```bash
pytest                    # unit and functional tests: no network, no keys, about 30 s
pytest -m e2e -v          # end-to-end on Binance Demo (see below)
```

**The E2E tests place real orders on a Demo account.**
- They need `BINANCE_API_KEY` and `BINANCE_API_SECRET` (Demo) in `.env`.
- They refuse to run against any non-Demo URL.
- They skip a symbol the account already holds.
- They clean up after themselves.
- Most finish in about a minute. The TP/SL test waits for the market to move
  `E2E_TRIGGER_PERCENT` (0.05 %) and can take up to 45 minutes.

To produce the test-results report:

```bash
pytest --junitxml=reports/unit-functional.xml
pytest -m e2e --junitxml=reports/e2e.xml
python scripts/test_report.py --output docs/test-results.md reports/unit-functional.xml reports/e2e.xml
```

## Configuration reference

| Variable | Default | Meaning |
|---|---|---|
| `CREDENTIALS_ENCRYPTION_KEY` | **required** | Fernet key encrypting stored credentials |
| `DATABASE_URL` | `sqlite:///binance_connector.db` | SQLAlchemy URL; relative SQLite paths are relative to the working folder |
| `BINANCE_BASE_URL` | `https://demo-fapi.binance.com` | Binance REST |
| `BINANCE_STREAM_URL` | `wss://demo-fstream.binance.com/ws` | user-data streams (one per account) |
| `BINANCE_MARKET_STREAM_URL` | `wss://demo-fstream.binance.com/stream` | the mark-price feed |
| `BINANCE_RECV_WINDOW_MS` | `5000` | how long Binance accepts a signed request; use 20000 on a slow link |
| `BINANCE_TIMEOUT_SECONDS` | `10` | HTTP timeout per Binance call; use 30 on a slow link |
| `BINANCE_TRIGGER_PRICE_TYPE` | `MARK_PRICE` | what triggers TP/SL: `MARK_PRICE` or `CONTRACT_PRICE` |
| `LIMIT_ENTRY_TIMEOUT_SECONDS` | `300` | an unfilled LIMIT entry is cancelled after this |
| `PRICE_GUARD_MAX_DEVIATION_PERCENT` | empty (off) | reject a MARKET OPEN whose `entry_price` is further than this from the mark |
| `EXECUTION_WORKERS` | `4` | accounts executed in parallel |
| `RECONCILE_INTERVAL_SECONDS` | `60` | how often every active position is checked against Binance |
| `API_HOST`, `API_PORT`, `API_THREADS` | `127.0.0.1`, `5000`, `8` | the API server |
| `INBOUND_API_TOKEN` | empty (off) | bearer token Django must send |
| `DJANGO_REPORT_URL` | empty (reports wait) | where reports go |
| `DJANGO_REPORT_AUTH_HEADER` | `Authorization` | header name for report auth |
| `DJANGO_REPORT_AUTH_VALUE` | empty | its value |
| `DJANGO_REPORT_TIMEOUT_SECONDS` | `10` | HTTP timeout per report |
| `DJANGO_REPORT_MAX_ATTEMPTS` | `10` | then the report is marked FAILED |
| `WEBSOCKET_HOST`, `WEBSOCKET_PORT` | `127.0.0.1`, `8765` | the live feed |
| `WEBSOCKET_TOKEN` | empty (off) | token live-feed clients must send |
| `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `E2E_*` | — | **E2E tests only**; the service never reads them |

## Troubleshooting

| Symptom | Cause, and what to do |
|---|---|
| `The database schema is at …, expected …` | Run `alembic upgrade head` (Docker does it in `migrate`) |
| `CREDENTIALS_ENCRYPTION_KEY …` at start-up | Missing or invalid. Generate one (step 2) |
| Positions FAILED with `code -1021` | The request reached Binance outside `recvWindow`: a slow network or a skewed clock. The service retries these automatically; if they persist, raise `BINANCE_RECV_WINDOW_MS` to 20000 and `BINANCE_TIMEOUT_SECONDS` to 30, and sync the clock |
| FAILED with `-2015` / `-2014` | Wrong or revoked API key, or not a Demo key |
| FAILED with `-2019` | Not enough margin on the Demo account: top it up in the Demo UI |
| FAILED with `hedge mode` | Switch the Demo account to one-way position mode |
| `already holds … not managed by this service` | The account holds that symbol outside the service. Close it on Binance first |
| Reports pile up as PENDING | `DJANGO_REPORT_URL` is empty or Django is not answering 2xx. Check the worker log; they are delivered once it answers |
| WebSocket 401 | Send the `WEBSOCKET_TOKEN`, as a Bearer header or `?token=` |
