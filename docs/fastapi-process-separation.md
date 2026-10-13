# FastAPI Batch 1: independent processes

Batch 2 builds on this verified lifecycle separation. The company REST contracts,
corrected default latest-price selection, production bearer configuration, CORS and
tested dedicated SELECT-role instructions are now in [docs/api.md](api.md).
This document retains the legacy Batch 1 route/health contracts; production credentials
and deployment remain unprovisioned. Cross-process streaming remains Batch 3.

Both development streams are integrated locally and remain undeployed. The running
`tradingview` project still uses its legacy 100-source combined collector. Do not run this
branch's Compose commands in that project. See [integration verification](collector-fastapi-integration.md)
for exact commits, isolated test resources, the conflict inventory and replacement/rollback.

## Previous and new architecture

Previously `create_app` loaded the selected JSON registry, built/subscribed providers,
connected a write pool, registered instruments and coverage, and started `IngestionService`
in the FastAPI lifespan. Shutdown stopped provider tasks, drained the writer, and closed
the pool. In-memory fanout delivered tick observations before persistence and native
candles after commit. Every Uvicorn worker would repeat the entire collector startup.

Now `marketdata.api:create_app` owns only a read-only database pool, HTTP authentication,
REST queries and API metrics. It requires `APPLICATION_ROLE=api` (the default) and never
loads the JSON registry, constructs providers, registers instruments, acquires the
collector lock, or starts ingestion. Multiple API workers have independent read pools.
Database startup validates migration head; it never runs migrations.

`python -m marketdata.collector` requires `APPLICATION_ROLE=collector`. It owns provider
construction, selected registry registration, existing tick/candle ingestion, native
recovery, validation, persistence, ingestion metrics and shutdown. Existing ingestion and
transport algorithms are unchanged. A dedicated PostgreSQL connection holds a
database-scoped advisory lock before registration/provider construction. A second collector
fails before those operations. Loss of that connection stops ingestion; restart is required.
Collector pool maximum must be at least two because one connection holds the lock.
This lock coordinates owners using this branch's lock protocol. The preceding combined
collector does not acquire it; integration must stop that legacy owner before activating
the new collector. A successful lock acquisition cannot prove a legacy collector is absent.

The collector opens a write-capable pool (`Store(..., read_only=False)`, the default) and
requires a database principal permitted to register instruments and write history, latest
state, coverage and collector status. The default mock Compose principal owns these tables;
it must remain write-capable. API pools explicitly use `read_only=True` and have been tested
with a separate role granted only schema usage and SELECT. Native processes can already
receive independent DATABASE_URL values. The mock Compose example shares development
credentials; production credential separation and role provisioning remain Batch 2 work.
No production credentials or grants were changed for Batch 1.

```text
Collector -> providers -> existing ingestion -> PostgreSQL history/latest/coverage
                                            -> collector_status snapshot

API workers -> read-only PostgreSQL connections -> REST clients
```

The collector publishes sanitized existing `ProviderHealth` models every five seconds and
at shutdown. Revision `0005` adds only `collector_status`, a singleton row with run ID,
server-clock expiry, writer database state, queue depth and provider snapshots. It does not
alter or rewrite instruments, source mappings, historical data or coverage. Revisions
0001–0004 remain unchanged. PostgreSQL and TimescaleDB both use the same new table.
Snapshots expire after 20 seconds by default. Expired snapshots report ingestion as
`unknown`, provider state/transport as `disconnected`, and symbols as stale; they do not
prove a stopped process. Graceful shutdown reports `stopped`. This is periodic health,
not a cross-process market-data stream. API workers read native capability from persisted
coverage/latest candles and futures roll schedules from persisted instrument payloads;
they need no provider credentials or matching local registry file.

## Health, REST and WebSocket contracts

All existing REST paths, history bounds, pagination, Decimal serialization and response
models remain available. Default candle basis is derived from persisted native capability
rather than constructing a provider. Existing source and canonical identities are retained.
Latest-price age still comes from persisted receipt/bucket timestamps, independently of
health snapshots. Without a current source health snapshot, latest data is conservatively
stale. Delayed/simulated/unknown classifications remain intact.

API `/health` returns 200 when the database is readable even if the collector/feed is
stopped, disconnected or degraded. Database failures return 503. The existing fields remain;
additions are `ingestion_state`, `ingestion_database_state`, and `streaming_mode`. Ingestion
state reflects the collector process heartbeat; provider states report feed health separately.
`database_state` describes the API's database access, not the collector writer's last result.
`/providers` returns stored collector snapshots, or `[]` before any collector publication.
`/metrics` belongs to that API worker; `marketdata_api_requests_total` and
`marketdata_api_database_healthy` are API metrics. Existing ingestion metric families there
remain local and have no cross-process collector values. Scrape collector metrics separately
on its configured host/port (default `127.0.0.1:9000`; port 0 disables the listener).

API-only `/ws/prices` applies authentication, accepts the handshake, then immediately closes
with code 1013 and reason
`cross-process streaming unavailable; query REST history`. `/health` explicitly reports
`streaming_mode=unavailable`. No fabricated observations, polling pseudo-stream or claim of
cross-process delivery is made. Full UI streaming belongs to Batch 3.

The explicit `marketdata.api:create_combined_app` factory requires
`APPLICATION_ROLE=combined`. It retains the original local subscribe/unsubscribe, tick,
committed-candle, slow-consumer, auth and cleanup behavior for development/testing. Use one
worker only; the collector advisory lock rejects competing combined owners. This factory
is not an API scaling option. Its `/health` preserves the previous combined readiness rule.

## Independent startup and shutdown

Run from this worktree with `uv sync --frozen --extra databento`. In each terminal set
`DATABASE_URL` explicitly to the intended **isolated development database**. Do not copy
production credentials or use port 5432 while this parallel task is being reviewed.

Collector only, using the existing mock registry by default:

```powershell
$env:DATABASE_URL='postgresql://fastapi_test:disposable_test@127.0.0.1:55482/fastapi_batch1_test'
$env:APPLICATION_ROLE='collector'
$env:COLLECTOR_METRICS_PORT='9001'
uv run python -m marketdata.collector
```

API only (works with the collector stopped and persisted history already present):

```powershell
$env:DATABASE_URL='postgresql://fastapi_test:disposable_test@127.0.0.1:55482/fastapi_batch1_test'
$env:APPLICATION_ROLE='api'
uv run uvicorn marketdata.api:create_app --factory --workers 2 --port 8001 --ws-max-size 16384
```

Run both commands in separate terminals to operate both services. Ctrl+C stops each owner
independently. The collector handles SIGINT/SIGTERM, stops providers, drains its queue within
`SHUTDOWN_TIMEOUT_SECONDS`, writes final stopped status, releases the lock and closes its pool
and metrics listener. API shutdown closes only its read pool and clients. Never start a live
provider for the validation commands here.

Combined development mode (alternative to the two commands, one worker):

```powershell
$env:APPLICATION_ROLE='combined'
uv run uvicorn marketdata.api:create_combined_app --factory --workers 1 --port 8001
uv run python scripts/smoke.py --base-url http://127.0.0.1:8001 --combined
```

For a new, separate mock Compose deployment only:

```powershell
$env:POSTGRES_PORT='55483'
$env:API_PORT='8001'
$env:COLLECTOR_METRICS_PUBLISHED_PORT='9001'
docker compose -p fastapi-dev up -d --build
docker compose -p fastapi-dev stop collector
docker compose -p fastapi-dev start collector
docker compose -p fastapi-dev restart app
docker compose -p fastapi-dev stop app collector
```

The default Compose `app` is now API-only; `collector` is a separate service. Each depends
on the explicit migration service, never on the other process. Guest/pilot overrides now
target `collector`; they do not change registry contents or collection targets. Earlier
pilot/guest deployment commands describe the old deployment and are not this branch's
launch procedure. The integrated scale override targets collector and requires a reviewed
lock-aware image; the preserved legacy scale override/helper are schema-0004 only.
No new Compose application deployment was performed during integration.

## Disposable validation and preservation

```powershell
docker compose -p fastapi-batch1-tests -f compose.fastapi-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://fastapi_test:disposable_test@127.0.0.1:55482/fastapi_batch1_test'
uv run alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
```

Repeat with port 55481 and `uv run alembic upgrade head` for ordinary PostgreSQL. Both
services have tmpfs data directories, their own Compose network/project and loopback ports;
neither mounts the production volume. Fixtures intentionally clear only the `_test` database.
Process tests launch mock-only collector and two independent HTTP API subprocesses, prove
both restart directions and forbid provider construction/registration inside API workers.
Other tests cover native persisted capability/coverage, read-only transactions, registration
ownership, graceful task/pool cleanup, expired health, database connection loss/recovery,
legacy REST/auth and development WebSocket envelopes.

Validation results and the initial/final production container observations are in
[the FastAPI handoff](../HANDOFF.md). Production SQL, migrations, source connections,
Docker restarts and data modifications were not used for this task. Since the parallel
collector legitimately continues writing, an unchanged production row count is neither
expected nor claimed. Preservation evidence is isolation and absence of production access,
plus unchanged identities/coverage in deterministic test snapshots.

## Rollback and next batches

This branch is undeployed, so **production needs no rollback**. Stop only this task's
native processes with Ctrl+C and remove only its disposable services:

```powershell
docker compose -p fastapi-batch1-tests -f compose.fastapi-test.yaml down
```

Do not use `down -v` or run commands in the `tradingview` project. Leave this worktree intact
for review; no main checkout, merge or deployment is necessary.

If a future reviewed deployment adopts this branch and later rolls back: stop its API and
collector owners first; set `DATABASE_URL` to that explicitly authorized database; run
`uv run alembic downgrade 0004` using this branch's migration files; launch the preserved
pre-Batch-1 code/image as a single combined worker with its previous configuration. Downgrade
0005 drops only the collector-status snapshot; historical candles/ticks/coverage/identities
remain. Baseline code requires schema 0004, so code-only rollback with schema 0005 will fail
startup. Test this sequence on the disposable database before approving any deployment.
No production rollback command was executed.

Batch 2 documents scoped SELECT-only grants and independently verifies them on isolated
PostgreSQL/TimescaleDB, including denial of writes with session read-only mode disabled.
Production role/password provisioning and independent API/collector secrets are still
operator/DBA work. Session read-only mode is not a substitute for role permissions.
Batch 2 uses the exact verified integration commit as its base. The collector's
status/migration/Compose overlaps remain reconciled and the deployment's 100-source target
is unchanged. See the [API verification report](api-batch2-verification.md).
Batch 3 must implement/test
cross-process streaming and collector-independent downstream subscriptions.
