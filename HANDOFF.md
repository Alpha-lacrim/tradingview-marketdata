# FastAPI Batch 1 handoff — 2026-10-13

## Scope and Git boundary

Worktree: tradingview-fastapi-batch1, the dedicated sibling Git worktree.
Branch: fastapi-batch1-isolation.
Base: 2c5d0e6e51f51d0ae7298501df79e38859f3a464 (origin/main).
Verified against git ls-remote origin refs/heads/main before work; the new worktree was clean.
Batch 1 is finalized as a local commit on this branch for deliberate integration review.
Retrieve its exact commit with git rev-parse fastapi-batch1-isolation. No merge, push or deployment.

The existing home-rooted deployment checkout, its unrelated local changes and the other
worktrees remain independent. Inspection found local provider, activation, diagnostic,
Compose and ingestion changes that differ from the public baseline. None were copied from
unfinished work. The collector session owns artifacts/ordinary-scale and the reported
approximately 100-instrument target. No registries, provider transports, ingestion algorithms
or migrations 0001–0004 changed in this branch. Older pilot/availability evidence remains
in docs; it describes historical measurements, not this branch's startup or deployed universe.

## Architecture

Previously create_app loaded the JSON registry, constructed providers, registered instruments
and coverage, then started IngestionService in the FastAPI lifespan. Shutdown stopped the
collector and closed the pool. Every API worker could create another collector.

Now create_app requires role api (default) and owns a read-only database pool, REST,
authentication and API health/metrics. It never constructs providers, loads the registry
file, registers instruments, acquires a collector lock or starts ingestion. Native capability
and futures identities/roll schedules come from persistent state. History works with the
collector stopped or external source access unavailable. Query failures return sanitized 503s.
API /health depends on database readability; ingestion and feed health are separate fields.

python -m marketdata.collector requires role collector and owns existing provider lifecycle,
registration, ingestion, recovery, persistence and collector metrics. It holds a dedicated
PostgreSQL advisory lock before construction/registration, rejecting a second owner. Pool
maximum must be >=2. Ownership connection loss stops ingestion and requires restart.
Shutdown drains the existing writer and closes provider tasks, pool, status task and metrics
listener. Existing ingestion/transport algorithms are unchanged.

Collector Store uses read_only=False and requires a write-capable database principal. The
default mock Compose principal owns its tables; standalone tests proved collector writes.
API Store uses read_only=True and was also tested with a genuinely SELECT-only principal.
Each native process accepts its own DATABASE_URL. Production grants, credentials and separate
Compose DSNs remain Batch 2 work; none were changed during this finalization.

Migration 0005 adds only collector_status, a singleton snapshot with server-clock expiry,
run ID, writer database state, queue depth and sanitized ProviderHealth models. Default
publication is five seconds with a 20-second TTL. Expired status is unknown/disconnected,
never evidence of a live feed. Graceful shutdown reports stopped. No history, identities,
source mappings or coverage are rewritten. This branch requires head 0005; production was
not migrated. An actual SELECT-only role was verified in isolated tests; deployment role
provisioning belongs to Batch 2.

API-only /ws/prices authenticates, accepts the handshake, then immediately closes 1013 and
reports cross-process streaming
unavailable. No cross-process observations are implemented or claimed. The explicit
create_combined_app, role combined, preserves original local tick/committed-candle fanout
for one-worker development. Existing REST/auth/WebSocket tests now select that factory.
Full independent UI streaming remains Batch 3.

Commands, contracts, configuration and rollback:
[Independent processes](docs/fastapi-process-separation.md).
Use explicit isolated DATABASE_URL in each terminal. Collector: APPLICATION_ROLE=collector
with uv run python -m marketdata.collector. API: APPLICATION_ROLE=api with
uv run uvicorn marketdata.api:create_app --factory --workers 2 --port 8001.
Run both in separate terminals; Ctrl+C stops each owner independently.

## Files changed and reasons

| Files | Reason |
|---|---|
| src/marketdata/api.py | Read-only API factory, explicit combined compatibility, persistent health/metadata, separate readiness, honest WebSocket refusal |
| src/marketdata/collector.py (new) | Collector owner, lock, health publication, signals, metrics and shutdown |
| src/marketdata/persistence.py | Read-only pool, persistent registry/native capability, status methods and schema head check |
| src/marketdata/config.py, .env.example | Roles, health TTL/interval, metrics and API ports |
| src/marketdata/observability.py | API-local HTTP response counter and database health gauge |
| migrations/versions/0005_collector_status.py (new) | Additive process health table; downgrade removes only it |
| compose.yaml | Independent API/collector services and ports |
| compose.tradingview.yaml, compose.tradingview-pilot.yaml | Existing provider settings target collector; registries unchanged |
| compose.fastapi-test.yaml (new) | Separate PostgreSQL/TimescaleDB test project, tmpfs data, ports 55481/55482 |
| tests/test_process_separation.py (new) | Zero provider startup, read-only role/history, identity/coverage, duplicate ownership, downtime, expiry and cleanup |
| tests/test_process_restart.py, tests/process_isolation_worker.py (new) | Real mock collector/two HTTP API subprocesses; both restart directions |
| tests/test_api.py, tests/test_health_api.py, tests/test_native_candles.py | Explicit combined legacy compatibility and separate writer health |
| tests/conftest.py | Clear isolated collector status between integration tests |
| scripts/smoke.py | Independent mock API smoke with opt-in combined WebSocket verification |
| README.md, CODEX.md, docs/platform-guide.md, docs/tradingview.md, docs/fastapi-process-separation.md (new), HANDOFF.md | Architecture, commands, evidence, limits, rollback and review boundary |

## Verification actually executed

All database operations used the new fastapi-batch1-tests Compose project, tmpfs data,
fastapi_batch1_test database, fastapi_test user, and loopback ports 55481/55482. No production
connection or live market-data source was used. Both empty databases were migrated to 0005
using the appropriate PostgreSQL/Timescale Alembic mode.

Implementation verification before finalization:

- TimescaleDB 2.25.1/PG17: 310 passed, 62.07 seconds.
- PostgreSQL 17: 307 passed, 3 expected Timescale-only skips, 57.66 seconds.
- uv run ruff check .: passed.
- uv run ruff format --check .: passed, 87 files.
- uv run mypy: strict, passed, 30 source files.
- git diff --check: passed.
- Compose config validation for base + guest + pilot: passed; app role api and collector
  role collector/pilot registry, neither depends on the other service.

Tests cover standalone tick/native collector persistence, API-only persisted history with
forbidden construction/registration and absent registry file, two API lifespans and two
real HTTP workers, independent collector/API restarts, actual SELECT-only role, unchanged
identity/coverage snapshots, duplicate collector rejection, stopped/expired health, real
backend termination/recovery, task/pool/metrics cleanup and legacy REST/auth/WebSocket
contracts. API subprocesses forbid upstream constructors and Store.register; collectors
in subprocess tests explicitly use mock providers only.

Additional local smoke paused/unpaused only fastapi-batch1-tests-postgres-1 after verifying
its project label and mounts. REST and health returned 503 during DB unavailability. After
unpause, historical native candles returned 200 with unchanged Decimal close 101, and health
returned 200. No production outage was injected.

In both modes, seeded native/tick history, captured row counts and sorted JSON content hashes
for all eight market tables, downgraded only 0005 to 0004, and re-upgraded to 0005. All
hashes/counts matched: instruments 43, mappings 47, ticks 22, latest prices 17, candles 1,
latest candles 1, coverage 17, revisions 0. This proves preservation on disposable fixtures;
no unchanged production row-count claim is made while its collector continues writing.

Initial fixture failures were corrected before final validation. The remaining warning is
the pre-existing Starlette/httpx TestClient deprecation. Docker app image build/full Compose
application deployment and live provider tests were not executed. Actual subprocess HTTP
and isolated database outage tests supplied local execution coverage. Disposable services
were removed after verification; no task-owned collector/API test processes remain.

## Local finalization review

Reviewed all 25 Batch 1 files, including the seven formerly untracked source/test/documentation
files. All belong to this architectural isolation task. Credential review found only explicit
development placeholders and disposable test credentials. All files are UTF-8 text; no secrets,
production credentials, generated databases, binary/machine-generated artifacts, environment
files, dependency environments or unrelated edits are included in the local commit.
The handoff's container identifiers are historical operational evidence, not generated artifacts.

Fresh finalization checks: pytest -q -m 'not integration' passed 253 tests, with 57 integration
cases deselected (35.05 seconds); Ruff lint/format and strict mypy passed. Alembic heads/history
confirmed a single 0005 head following 0004; these commands did not connect to a database.
Review found that the pre-accept WebSocket close was represented as 1013 by TestClient but
would reject a real HTTP upgrade. Finalization added acceptance before the immediate close,
a database-independent regression, and an actual network close-code assertion in the existing
two-worker test. Full suites were rerun on new disposable services in the separate
fastapi-batch1-finalization-tests project. Final committed-code results:

- TimescaleDB 2.25.1/PG17: 311 passed, 71.41 seconds.
- PostgreSQL 17: 308 passed, 3 expected Timescale-only skips, 67.17 seconds.
- Ruff lint/format and strict mypy passed; git diff --check passed.
- The real HTTP-worker network test received WebSocket close 1013 with the documented reason.
- Empty disposable databases were upgraded to 0005 in their respective migration modes.

The migration-preservation evidence above remains from the completed implementation verification;
revision 0005 itself is unchanged. Documentation also clarifies the README migration range,
collector write permissions and SELECT-only API boundary. No existing containers were restarted,
production migrations executed or Batch 2 provisioning performed.

## Production preservation evidence

Read-only Docker metadata inspection identified project tradingview, image
tradingview-app:ordinary-100, database container tradingview-db-1, production port 5432,
database marketdata (Compose/HANDOFF), and volume tradingview_marketdata_pg. Before/after
tests, IDs/start times and mounts matched:

- App: 5d580ce8ea9275f8683cbc8ec04e8dd9ffabfd7577358e7be88d9ff7d4e38433;
  start 2026-10-13T06:55:34.126022742Z.
- DB: 62caad90d2e9367a5d823cc3775102a00a0e07a79046b1aba3cd30074c4b1238;
  start 2026-10-13T06:10:45.245870893Z; same named data volume/mount.

No production SQL, migrations, restart, volume operation, new TradingView connection,
instrument change or collector deployment occurred. All edits are in this separate worktree.
No registry/provider/ingestion/prior-migration diff exists against the base.

## Rollback and remaining integration work

This branch is undeployed, so production requires no rollback. Stop only task-owned native
processes with Ctrl+C. Disposable cleanup command:
docker compose -p fastapi-batch1-tests -f compose.fastapi-test.yaml down
Do not use down -v or the tradingview project. Leave this worktree intact for review.

For a future reviewed deployment rollback: stop that deployment's API/collector owners,
set DATABASE_URL to its explicitly authorized database, run uv run alembic downgrade 0004
using this branch's migrations, then launch preserved pre-Batch-1 code/image with one
combined worker and previous configuration. The old code requires schema 0004, so a code-only
rollback with schema 0005 fails startup. Downgrade removes only collector_status; preservation
was tested above. Exact conditions/commands are in the process-separation guide. No production
rollback was attempted.

Expected integration overlaps: api.py startup; persistence.py pool/schema/metadata methods;
config.py; observability.py alongside parallel loop-lag metrics; Compose service keys; factory
startup ownership; deployment overrides; README/CODEX/HANDOFF. The parallel scale override
currently targets app under the old combined architecture and must be reviewed for collector.
The legacy combined collector does not acquire this branch's advisory lock: a successful new
lock acquisition cannot prove it is absent. Integration must stop the legacy owner before
activating the new collector and reconcile any lock protocol changes on the collector branch.
Do not merge/copy it blindly. Factory/pool, activation and recovery changes remain owned by
the collector session. Reconcile any competing migration head and preserve identities,
activation/coverage and the 100-source target. Rerun the integrated full suite before deployment.

Batch 2 must provision separate SELECT-only API credentials/grants (including alembic_version,
collector_status, instruments/mappings, history/latest/coverage and aggregates), separate
Compose secrets/DSNs, and validate the integrated deployment. Production provisioning and
deployment were intentionally unexecuted because this task forbids shared runtime changes.
Cross-process streaming remains Batch 3. No unresolved blocker remains for isolated Batch 1.
