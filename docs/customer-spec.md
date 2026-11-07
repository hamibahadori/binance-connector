# Customer specification: independent Strategy execution service

The customer's detailed specification for this project, as received on 2026-11-06. This is
an English summary of the original, which was written in Persian; it keeps every
requirement. Section numbers (§1–§14) are the ones the rest of the documentation cites.

How each requirement was met is described in [api-contract.md](api-contract.md) and
[database.md](database.md). [test-results.md](test-results.md) maps the acceptance
criteria (§14) to the tests that prove them.

## 1. Goal

- An independent Python service that receives trade requests **from Django**, processes
  them, manages their state, and sends complete, continuous reports back to Django.
- The developer owns everything end to end: source, database, APIs, documentation, tests
  and technical write-ups.

## 2. Architecture

- `Django → Python Execution Service → Simulation Engine`; reports go `Python → Django`.
- **Django** receives Strategy signals and converts them into a standard request.
  **Python** receives that request and owns the operation and its state.
- **The AI path lives in Django.** The Python service does not receive or process AI data.
  AI and Strategy must not interfere with each other.

## 3. Request fields (sent by Django)

| Field | Note |
|---|---|
| Request ID | unique per request |
| User ID | unique per user |
| Account ID | the user's trading account |
| API Key | the account's access key; test data in the test environment |
| Secret Key | the account's secret; test data in the test environment |
| Symbol | e.g. BTCUSDT |
| Action | e.g. OPEN or CLOSE |
| Direction | LONG or SHORT |
| Quantity | position size or amount |
| Leverage | the leverage wanted "in the simulation" |
| Entry Price | if Django sets one |
| Stop Loss | stop-loss information |
| Take Profit | take-profit information |
| Timestamp | when the request was created |
| others | as agreed in the Django–Python contract |

The service must validate every field and return the result of receiving the request to
Django.

## 4. Several users at once

- Example: one BTC request for 10 different users, each with their own account, fully
  isolated.
- Each User ID and Account ID is identified independently.
- Each user's state is kept separately.
- Every position is linked to its user and request.
- No interference between simultaneous requests, and no mixing of one user's data with
  another's.
- Several simultaneous requests on the same symbol must be possible.
- Every result is recorded with its user and request ids.
- **Version 1 covers one exchange only.**

## 5. Suggested states

`RECEIVED, VALIDATING, ACCEPTED, REJECTED, OPENING, OPEN, CLOSING, CLOSED, ERROR`. Every
change must be recorded, and the latest state must be recoverable.

## 6. Reporting to Django

Reports go out after receiving a request and at every important step. Each report carries:

- Request ID, User ID, Account ID, Position ID;
- symbol, direction, current state;
- time the request was received; operation start and end times;
- entry, current and exit prices; computed profit or loss;
- TP/SL status, close reason, any errors.

**Django must be able to get the latest state of a request or position at any time.**

## 7. Live reporting over WebSocket

- Content: the asset's current price, position state, computed P&L, distance from the
  current price to TP and SL, last update time, and the service's connection status.
- **After a disconnect, the current state is re-sent on reconnect** so the two sides resync.

## 8. CLOSE commands from Django

Django can send CLOSE for any user and position. The service identifies the position,
performs the **simulated close**, and reports the final result. **A repeated request must
not execute twice.**

## 9. TP/SL

"In the simulation environment, **the service must check the price**, and close the
position when TP or SL is reached." Afterwards:

- the position becomes CLOSED;
- the close reason is recorded;
- the final result goes to Django;
- the related orders are updated.

## 10. Database

Its own structured database, with at least these entities:

- Users / Accounts
- Requests
- Positions
- Position Events
- Execution Records
- Reports
- Error Logs

Data must survive a restart and state must be recoverable. **Secrets must never appear in
logs or error reports.**

## 11. Suggested modules

`api`, `request_validator`, `account_manager`, `execution_manager`, `position_manager`,
`price_feed`, `websocket_manager`, `reporting_service`, `database`, `event_logger`.

## 12. Out of scope for version 1

- multiple exchanges;
- a dashboard for users;
- managing positions by hand from an outside environment;
- failover or several instances running at once;
- very complex or unusual scenarios;
- advanced risk and capital management;
- developing or changing the AI path in Python;
- any extension not described for version 1.

## 13. Deliverables

1. complete, runnable source;
2. every module and file needed;
3. the database and its structure;
4. **API documentation and the Django–Python contract**;
5. **database structure documentation**;
6. sufficient technical comments in the code;
7. configuration and dependency files;
8. **sample requests and responses**;
9. unit and integration tests;
10. **a test-results report**;
11. **an install, setup and run guide**.

Running the project **must not depend on the developer's private access or personal
environment**.

## 14. Acceptance criteria (each tested in the simulation environment)

1. Receiving requests from Django.
2. Identifying the correct user and account.
3. Handling several users at once, with their data fully separated.
4. Creating and managing a simulated position.
5. Receiving and showing the live price.
6. Live reports over WebSocket.
7. Receiving close commands from Django.
8. Simulated close on TP/SL.
9. Recording every event and state.
10. Preventing duplicate execution.
11. Recovery after a restart.
12. Handling errors and disconnects/reconnects.
13. The final report to Django.

Architecture principle: Django receives and converts requests; Python independently owns
the Strategy path, the operation's state and reporting.
