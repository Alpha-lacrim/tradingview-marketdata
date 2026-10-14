# Standalone FastAPI backend

For a self-contained directory that can be copied outside this repository, use
[distributions/marketdata-fastapi](../distributions/marketdata-fastapi/README.md). It includes
its own source, dependencies, migrations, documentation, tests and examples, verified from
an isolated copy. The guide below describes the equivalent original-repository packaging.

The company backend is independently installable and runnable now. It reuses FastAPI
Batches 1–4 and schema **0006**. `marketdata.api:create_app` opens a read-only database
pool and a committed-event reader. It does not import ingestion, construct providers,
load a registry file, acquire the collector lock, run migrations or start TradingView.
REST, OpenAPI, authentication, metrics, historical queries, subscriptions and replay retain
their existing contracts. PostgreSQL is its only required external service.

## Disposable Docker quick start (PowerShell)

Run in this checkout with Docker Compose and unused ports. This setup uses no `.env` file,
existing volume, production database or collector. The database lives in tmpfs and its
contents disappear when its container stops. Passwords and tokens below are public local
fixtures, unsuitable for deployment. The default ports are API **8005**, database **55914**.

```powershell
# Optional: change these if occupied; retain the same values in every terminal.
$env:STANDALONE_API_PORT='8005'
$env:STANDALONE_DB_PORT='55914'
docker compose -p marketdata-api-demo -f compose.api.yaml --profile tools build
docker compose -p marketdata-api-demo -f compose.api.yaml up -d --wait db
docker compose -p marketdata-api-demo -f compose.api.yaml run --rm migrate
docker compose -p marketdata-api-demo -f compose.api.yaml run --rm seed
docker compose -p marketdata-api-demo -f compose.api.yaml up -d --wait api
```

Check each command succeeds before proceeding. Migrations and sample provisioning are
explicit administrative operations; starting/restarting `api` never runs them. An unprepared
database fails startup. The API uses `sample_api`, granted SELECT on explicit relations.
The separate `sample_owner` is used only by migration/sample commands. The seed command
creates the reader and revokes PUBLIC CREATE/TEMP only in this disposable database.

Open `http://localhost:8005/docs` or `/redoc`; `/openapi.json` and `/health` are public.
Authorize with `sample-development-token-32-characters`. `/livez` tests process liveness;
`/health` tests DB readiness; `/api/v1/health` reports data/collector/streaming state.
Authenticated `/metrics` retains the existing API worker metrics.

`seed` inserts 120 completed one-minute candles per sample source and two quotes using
the existing journal-aware writer. Identities `DEMO:BTCUSD` (simulated crypto spot) and
`DEMO:EURUSD` (simulated forex OTC) use provider `sample`, venue `DEMO`. All observations
are `simulated`, volume is unknown/null, and no real instrument/provider availability is
implied. This is a standalone database fixture, not a scraper or mock collector process.
Re-running is safe for these demo identities and can emit candle corrections with newer
receipt times. It never truncates tables. Use `--minutes 1440` to include previous UTC-day
data for daily charts; the default two-hour history cannot supply a completed current day.

For bounded live WS quote events, run this **in a second terminal** while the client below
is subscribed:

```powershell
docker compose -p marketdata-api-demo -f compose.api.yaml run --rm seed .venv/bin/python -m marketdata.sample_data --events 30 --interval-seconds 2
```

The writer exits after 30 batches. The API continues serving history/snapshots/replay when
it exits. Freshness ages naturally; `realtime_quote_available` stays false for simulated
quotes. Versioned price freshness is based on observation timestamps; legacy price routes
also conservatively consider provider snapshots. There is no collector heartbeat here, so
collector/provider status stays unknown/absent rather than fabricated healthy.

Cleanup only this explicitly named demo:

```powershell
docker compose -p marketdata-api-demo -f compose.api.yaml down
```

No `down -v` is needed. After a DB stop, repeat migrations and seed before starting the API.

## REST and WebSocket examples

```powershell
$base='http://localhost:8005'
$headers=@{Authorization='Bearer sample-development-token-32-characters'}
Invoke-RestMethod "$base/api/v1/instruments" -Headers $headers
Invoke-RestMethod "$base/api/v1/prices/latest?symbols=DEMO%3ABTCUSD" -Headers $headers
Invoke-RestMethod "$base/api/v1/candles?symbol=DEMO%3ABTCUSD&provider=sample&provider_symbol=BTCUSD&interval=1m&limit=100" -Headers $headers
Invoke-RestMethod "$base/api/v1/candles?symbol=DEMO%3ABTCUSD&provider=sample&interval=5m" -Headers $headers
Invoke-RestMethod "$base/api/v1/coverage?symbols=DEMO%3ABTCUSD" -Headers $headers
```

Native history supports 1m; 5m/15m/1h/1d aggregate the same persisted minutes. Bounds align
to the requested UTC interval, defaulting to the preceding interval-aligned day. Missing
minutes remain explicit incomplete rollups; decimal fields serialize as strings. Follow
`next_cursor` with the same source/window. Keep exact provider symbols and instrument IDs.

Save this Python example locally and run with the normal development installation
`uv sync --frozen` (includes httpx for clients/tests). Start the bounded writer above after
the subscription acknowledgement appears. With default ports/token:

```python
import asyncio
import json

import httpx
from websockets.asyncio.client import connect


async def main():
    headers = {"Authorization": "Bearer sample-development-token-32-characters"}
    async with httpx.AsyncClient(base_url="http://localhost:8005", headers=headers) as client:
        checkpoint = (
            (await client.get("/api/v1/marketdata/changes")).raise_for_status().json()["cursor"]
        )
        async with connect(
            "ws://localhost:8005/api/v1/ws/marketdata",
            origin="http://localhost:8005",
            additional_headers=headers,
        ) as ws:
            await ws.send(json.dumps({"action": "subscribe", "symbols": ["DEMO:BTCUSD"]}))
            while True:
                message = json.loads(await asyncio.wait_for(ws.recv(), 90))
                print(message, flush=True)
                if message["type"] == "price_update":
                    break
        replay = (
            (await client.get("/api/v1/marketdata/changes", params={"after": checkpoint}))
            .raise_for_status()
            .json()
        )
        print(replay)


asyncio.run(main())
```

This demonstrates subscriptions, committed snapshots/updates and REST replay. Full reconnect
handling must restore subscriptions, reconcile to the new `snapshot_cursor` barrier, page
changes until `has_more=false`, then apply buffered live events. Deduplicate by cursor/event
identity, upsert candles by exact source/interval/bucket, preserve newer receipt versions.
A 409 requires REST refresh of the loaded chart window, prices and coverage. See the
[existing browser recovery example](../examples/browser-marketdata.js) and
[complete WS contracts](api.md#websocket-market-data-v1). The legacy `/ws/prices` remains
combined-process-only and closes with 1013 in API-only mode; use `/api/v1/ws/marketdata`.

## Independent install, start and deploy

The wheel's base dependencies are the API runtime: FastAPI/Uvicorn, asyncpg, Pydantic/settings,
Prometheus, AnyIO and websockets. HTTP provider tooling and migrations are optional extras;
no Databento SDK, httpx, Alembic, SQLAlchemy or psycopg is required by API startup. Shared
domain/persistence modules remain in one package; shipping dormant collector source is not
a dependency on a collector process. No duplicated API or persistence implementation exists.

In a dedicated API checkout/environment:

```powershell
uv sync --frozen --no-dev
# Supply a separately provisioned SELECT-only DATABASE_URL, API_KEY and allowed origins.
$env:APPLICATION_ROLE='api'
uv run --no-sync uvicorn marketdata.api:create_app --factory --host 127.0.0.1 --port 8005 --workers 2 --ws websockets --ws-max-size 65536 --ws-max-queue 16
```

Alternatively install `pip install .` (or the built wheel) and run the same `uvicorn`
command without `uv run`. `uv build` creates the wheel. A runtime-only install omits the
testing client/lint tools. Development/tests use `uv sync --frozen`; collector-only runtime
installs use `uv sync --frozen --no-dev --extra collector` (add `--extra databento` if needed).
Migration tooling uses `--extra migrations` in its separate administrative environment and
the repository's Alembic files. The existing full `Dockerfile` selects all historical runtime
extras explicitly, preserving collector and production migration behavior.

Build the independent API artifact with `docker build -f Dockerfile.api -t company-market-api .`.
Its default/final target is `runtime`, without migrations/config registries or optional
dependencies. The `tools` target is only for explicit migration operations. The same API
image can use the existing production file-secret entrypoint:

```text
.venv/bin/python -m marketdata.production api
```

Mount separate `DATABASE_URL_FILE`/`API_KEY_FILE`, set `API_ENVIRONMENT=production`, explicit
HTTPS CORS/WS Origins and trusted proxy addresses. See [.env.api.example](../.env.api.example)
and [deployment security](production-deployment.md). Provision TLS/private DB networking
through company infrastructure; the disposable Compose file is not a production deployment.
Production startup rejects write-capable API credentials. The API remains available with an
offline producer whenever its migrated database is readable; latest observations become stale,
provider status expires and only heartbeats/snapshots/replay continue until new commits arrive.

## Database and external-producer integration contract

The migration files are the authoritative schema; **0006** is currently required. Keep one
schema owner/admin, a separate writer, and an independent SELECT-only API principal.
[Explicit grants and inherited privilege checks](api.md#dedicated-read-only-database-role)
apply to both PostgreSQL and TimescaleDB. Review grants after schema changes. The API neither
owns relations nor needs a collector advisory lock or upstream credentials.

The supported integration boundary is the existing Python package:

Use one authoritative writer per database, preserving the existing single-writer assumption.
An external producer replaces the collector; do not run both owners for the same sources.
The sample utility is restricted to disposable `_test` databases, never a production writer.

1. Create canonical `Instrument` / `ProviderInstrument` mappings in `RegistryEntry` and call
   `Store.register(Registry(entries))` from the external writer. Identity is deterministic;
   mappings cannot silently change instrument/source identity. Concrete futures remain separate.
2. Normalize quotes as `Tick` with `Decimal` prices, original source event time when available,
   timezone-aware UTC receipt time, exact provider identity and honest `DataStatus`; call
   `Store.write(ticks)` in bounded batches.
3. Persist **completed, UTC-aligned native 1m** `MarketCandle` objects through
   `Store.write_candles(candles, coverage_plans)`. Supply coverage windows/session classification
   only when supported by the producer. Unknown/non-additive volume stays null with metadata.
   Newer receipts can correct existing bars; forming bars are rejected and older corrections
   are ignored.
4. Those methods atomically update history, latest state, coverage/revisions as applicable and
   append to the event journal using its transactionally locked clock. Only committed entries
   are served. API workers poll the DB; no Redis, broker, NOTIFY or provider connection is required.
5. `Store.publish_collector_status` is optional telemetry. Publish only genuine producer health;
   omitting it leaves unknown/expired health without breaking history or API readiness.

See [sample_data.py](../src/marketdata/sample_data.py) for an executable independent writer.
Never write history alone and expect streaming: old collector binaries/direct SQL do **not**
append replay events or maintain all derived state. A non-Python writer must implement and
validate the exact transactional logic in [persistence.py](../src/marketdata/persistence.py),
[events.py](../src/marketdata/events.py) and [migration 0006](../migrations/versions/0006_marketdata_events.py).
A SERIAL/IDENTITY sequence is not a commit-order cursor. Reuse Store where possible rather
than copying SQL into a second implementation. API contract/schema changes require coordinated
versioning; changing producers does not change REST/WS identities or data semantics.

## Repository recommendation and handover limits

**Keep one repository with independently built API/collector artifacts.** Separation is already
enforced at startup and dependencies. Shared schema, exact decimal models and transactional
writer/replay semantics make a second repository add release coordination without a current
technical benefit. Developers can own the API image/entrypoint and release it independently
against compatible schema. If separate repositories later become an organizational requirement,
first publish/version the shared domain/persistence package and migration contract; do not copy
the API and writer modules or fork the schema.

**Ready for independent frontend/backend developer handover.** No scraper/provider credential
or collector process is required for this setup. Company browser session authorization remains
external: native browser WS cannot set the permanent bearer; keep it server-side and connect
the [provisional BFF](../examples/company_bff.py) to real company sessions. Production needs
its reviewed roles/secrets, TLS/Origins, schema rollout and target-host qualification. Genuine
live prices require a separately qualified producer. TradingView simultaneous quotes/native
candles and source completeness remain collector limitations, not API extraction blockers.

Verification on disposable databases:

| Check | Result |
|---|---|
| Full PostgreSQL suite | 444 passed; 4 skipped (three Timescale tests, optional prior image fixture) |
| Full TimescaleDB suite | 447 passed; 1 skipped (optional prior image fixture) |
| Ruff lint/format; mypy source and BFF | Passed |
| Fresh migration; downgrade/re-upgrade regressions | Passed on both engines |
| Fresh minimal two-worker Linux image | REST/all intervals, auth, WS events/reconnect/replay, restart passed |
| API dependency boundary | Full lifespan with forbidden provider/ingestion imports; optional tools absent |
| Wheel/sdist build | Passed |
| Clean wheel install/start | Passed without collector/migration dependencies, including production role checks |

The skipped older container fixture is supplemented by the newly built standalone image
smoke above. No live upstream smoke, load benchmark or target production deployment was run.
Existing dependency deprecation warnings remain. Exact commands/state and prior evidence
are recorded in [HANDOFF.md](../HANDOFF.md).
Reproduce full suites using [the Batch 4 isolated commands](api-batch4-verification.md), with
`BATCH4_TEST_IMAGE` unset for the counts above. Run `uv run pytest -q tests/test_standalone_api.py`
with `TEST_DATABASE_URL` targeting a migrated disposable `_test` database for the new tests.
The locked Docker/development environment is the full-suite target; the clean wheel smoke
also exercised newer compatible resolver versions, without claiming a full-suite run on them.
No migrations or API contract changes were introduced. No production changes, commits, pushes
or deployments are part of this task.
