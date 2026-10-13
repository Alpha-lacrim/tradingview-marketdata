# Verified collector / FastAPI Batch 1 integration

## Git scope and strategy

Development-only branch: `integration/collector-fastapi-batch1`, in a new dedicated sibling
worktree. Source worktrees were clean and their exact HEADs matched both supplied commits:

| Reference | Exact commit |
| --- | --- |
| FastAPI Batch 1 | `6901bfadd44cb83ccdca49aac559e6f97a93a91d` |
| Collector/business universe | `368bce1293db59c7623bcb6331232dc525ff943e` |
| Shared merge base and local origin/main | `2c5d0e6e51f51d0ae7298501df79e38859f3a464` |
| Existing dirty main checkout | `324b1d83e0ccb3e897cb28bf09db5153ef2e0ec3` |

The branch starts at the completed FastAPI commit, then merges the exact collector commit
with `--no-ff --no-commit`. The final local merge commit includes reviewed resolutions and
integration tests. Both parents retain their histories; original branches/worktrees and
unrelated main/paused Batch 3 changes remain untouched. No remote fetch/push/main merge.
Run `git rev-parse integration/collector-fastapi-batch1` for the final integration hash.

Read-only Docker metadata identified the deployed application image as
`sha256:ca498afb02f6232440e3386936a4097e865cf9fefdbd0481adc0cc7ce623023f`
(`tradingview-app:ordinary-100`). It has no revision label, so its exact Git commit is unknown;
neither main nor a source commit is assumed to match it. No container filesystem or production
SQL was read. Its app/DB container IDs, start times and database mount were captured before
and after validation without accessing market data.

## Conflict inventory and resolutions

| Area/file | Collector changes | FastAPI changes | Integrated resolution and checks |
| --- | --- | --- | --- |
| `src/marketdata/observability.py` (text conflict) | Loop-lag histogram/max gauge | API response counter/DB gauge | Keep all four under each Metrics registry; existing ingestion and API metrics tests, lint/type checks. |
| `README.md` (text conflict) | Current 100 baseline, 99 proposal, evidence links | Independent process defaults | Document both, retain historical evidence and explicit undeployed/provisional status. |
| `docs/tradingview.md` (text conflict) | FXCM identity/raw volume evidence | Role/streaming limitations | Preserve both explanations, link current integration procedures. |
| `HANDOFF.md` (text conflict) | Business/current deployment evidence | Process/lock/migration evidence | Replace competing session narratives with concise integrated state; detailed source evidence stays linked. |
| `.env.example`, `CODEX.md`, `compose.yaml`, platform guide (automatic overlaps) | Scale settings, DB restart policy, qualification rules | Roles, status TTL, API/collector services, 0005 | Audit merged values; retain `restart: unless-stopped`, one head, mock default, independent services; validate Compose output. |
| Scale override/helper (semantic conflict) | Legacy combined app/image switching | app now API-only, collector separate | Scale override targets collector and demands an explicit integration image. Preserve legacy scale override separately; helper rejects split-process configuration and schema other than 0004. Never execute helper in tests. |
| Provider, recovery, Store/API (compatible) | Scoped FX/FXCM gate, raw-volume provenance, set membership | Read-only Store, persisted registry, ownership/status | Preserve source implementation without rewriting transport; broker, 38,400-candle recovery, lifecycle/REST/network tests. |

`compose.tradingview-rollback-image.yaml` remains **legacy-only**. It must never be combined
with the integrated base to roll back split services. Legacy combined images cannot satisfy
the standalone collector command. The unchanged original branch retains every legacy file.
The guarded switch helper remains an operator tool for the existing 0004 deployment, not
the deployer for this branch, and accepts no 99 preset.

## Integrated behavior and schema

The default `marketdata.api:create_app` requires role `api`, opens only a read-only pool,
and constructs/registers/ingests nothing. Multiple API workers own independent read pools.
`python -m marketdata.collector` requires role `collector`; the collector owns providers,
registration, native recovery, writes, health publication and graceful shutdown. Its session
advisory lock is acquired before registration/provider construction. Ownership loss stops
ingestion. The explicit one-worker `create_combined_app` retains development fanout.

Revision 0005 follows 0004 and creates only `collector_status`; revisions 0001–0004 are
byte-identical to both sources. Its singleton stores run ID, server-clock timestamps/expiry,
running/writer state, queue depth and sanitized provider health. Only the collector writes it.
Expired snapshots become unknown/disconnected/stale; stopped collectors leave historical
REST available. API DB failures return 503. Separate SELECT-only credentials are supported
and tested; production grants/separate Compose DSNs remain Batch 2 work.

Both PostgreSQL and TimescaleDB tests seed the original baseline and 99 registry with exact
broker Decimal candles, quote/latest rows, correction audit and disjoint gaps/activation
starts. All eight market tables are hashed in sorted row order before/after the 0005→0004→0005
round trip. No market row may change. API startup on 0004 fails safely and closes its pool;
it never runs a migration. Fixture recovery reduces only OANDA's gap, preserving FXCM's
gap and both activation starts. Concurrent API reads and collector writes remain independent.

API-only `/ws/prices` authenticates, accepts the network handshake, then closes 1013 with
`cross-process streaming unavailable; query REST history`. There is no cross-process event
transport or polling stream. Combined development retains local tick events (precommit,
`persisted: false`) and committed native candle events. Batch 3 owns independent streaming.

The 99-source registry stays PROVISIONAL: 50 Binance spot, 21 OANDA forex, 21 FXCM forex,
four OANDA OTC metals, two TVC oil references and one OANDA gas CFD. FXCM chart `FX:EURUSD`
and OANDA `OANDA:EURUSD` retain distinct canonical IDs, mappings, candles, quotes and gaps.
Typed unclassified FXCM/TVC volume stays NULL; finite raw source values remain metadata.
Latest native closes remain labeled `candle_close`; separately stored quotes remain accessible
through tick history. The prior 5,882/5,940 qualification and all 58 unresolved gaps are
preserved evidence, not new integration/live-capacity results. No registry is promoted.

## Isolated validation

The only databases used are in project `collector-fastapi-integration-tests`, defined by
`compose.integration-test.yaml`: standalone PostgreSQL 17 on loopback 55591 and TimescaleDB
2.25.1/PG17 on loopback 55592, database `collector_fastapi_test`, test-only principal.
Both have tmpfs storage, no mounted volumes/env files and a separate network. No production
credentials, SQL, migrations, pause/restart, volume operation or TradingView connection.

```powershell
uv sync --frozen --extra databento
docker compose -p collector-fastapi-integration-tests -f compose.integration-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://integration_test:disposable_test@127.0.0.1:55592/collector_fastapi_test'
uv run alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q
# Repeat on 55591, with ordinary `uv run alembic upgrade head`.
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run alembic heads
git diff --check
docker compose -p collector-fastapi-integration-tests -f compose.integration-test.yaml down
```

Validation actually executed:

| Check | Result |
| --- | --- |
| Full TimescaleDB 2.25.1/PG17 suite | VERIFIED: 335 passed in 83.42s |
| Full standalone PostgreSQL 17 suite | VERIFIED: 332 passed, 3 expected Timescale aggregate skips in 68.84s |
| Ruff lint / format | VERIFIED: passed / 108 files formatted |
| Strict mypy | VERIFIED: passed, 30 source files |
| Alembic heads and fresh upgrades | VERIFIED: one 0005 head; empty isolated PostgreSQL/Timescale upgrades passed |
| Populated 0004→0005 upgrade / 0005→0004 rollback | VERIFIED: all eight market-table counts/hashes unchanged, including source payloads, activation, gaps and corrections |
| Process/fault/network checks | VERIFIED: both restart directions, two real API workers, ownership contention/loss, DB connection recovery/503, SELECT-only role, listener cleanup, network 1013 |
| Existing `scripts/smoke.py` | VERIFIED: real mock collector/API subprocesses; 30 instruments, historical ticks/candles, healthy DB, unavailable streaming |
| Compose and PowerShell | VERIFIED: base/guest/pilot/scale config, explicit 100 registry/4 sockets/global8; helper parsed without execution |
| Core source comparisons | VERIFIED: collector registry/provider/recovery files identical to collector parent; API/collector/Store/config/0005 identical to FastAPI parent; prior migrations unchanged |
| Production preservation | VERIFIED: same app/DB IDs, start times, images and mount metadata; no production SQL or service/volume operations |

No final test failures. The first new fixture run had two fixture/expectation failures:
correction audit requires a later receipt timestamp, and latest prefers a native candle
when both candle/quote rows exist. Those fixtures were corrected without changing or
weakening production behavior. Both full suites subsequently passed. One inherited
Starlette/httpx TestClient deprecation warning remains. A Docker template parse error was
corrected by selecting metadata from parsed inspect JSON; isolation was then verified.
Git whitespace checks and PowerShell syntax parsing passed. Full command outputs/JUnit,
metadata snapshots and synthetic results remain ignored under `artifacts/`; they are not
committed logs, databases or credentials.

Bounded synthetic performance is **PARTIALLY VERIFIED**, not production qualification:

| Measurement | Observed result |
| --- | --- |
| 128-source × 300-minute fixture (38,400 acknowledged candles) | 1.295s whole in-memory recovery/fanout cycle; no per-candle list membership; exact events and no overflow |
| 5s mock run, 300 instruments, requested 5,000/s | 3,650 generated/persisted, 0 dropped, 661/s including drain; mean DB batch 1.040s |
| Collector loop monitor / runtime+drain overhead | Max sampled loop lag 0.219s / 0.521s |
| API startup / shutdown | 0.121s / 0.0036s with Python allocation tracing |
| 50 ASGI latest queries, 100 records each | p50 20.37ms, max 65.98ms; two idle API pool connections after requests |
| API traced allocation peak | 4,576,190 bytes; Python allocations only, not process RSS |
| Same-host original collector vs integration (2 × 5s each) | Original 532/684 rows/s; integrated 535/597 rows/s, 0 drops/all rows persisted; overlapping short-run ranges, integrated average about 7% lower |

The comparison extracts only the exact collector commit's Python source into ignored
artifacts, uses the **same disposable Timescale server**, restores schema 0004 for its old
Store and 0005 for integration, and clears only test fixtures between runs. Original source
worktrees are untouched. The requested rate is a generator setting, not achieved throughput;
Windows scheduling/local DB batches constrain these samples. The runs do not demonstrate
a material integration regression, nor do they prove 5,000/s capacity or rule out smaller
regressions. Whole recovery timing includes fixture construction and is not comparable to
the prior live 53s postcommit stall; the algorithmic regression explicitly forbids the old
linear membership path. UTC scheduling and bounded pool/retry/cleanup assertions pass in
the deterministic suite. Network REST/process behavior is verified separately by full tests;
ASGI timings do not include network latency. No live capacity or full-process RSS claim.

Reproduce the recovery check with
`uv run pytest -q tests/test_recovery_fanout.py --durations=4`, and the bounded DB benchmark
with the isolated DSN using
`uv run python -m marketdata.benchmark --seconds 5 --rate 5000 --instruments 300`.
The migration regression invokes Alembic against the fixture's explicitly passed test DSN,
so both migration modes exercise a real 0004 populated schema. Process tests launch only
mock collectors and instrument API workers to fail on provider construction/registration.
They check two HTTP workers, both restart directions, historical service after collector stop,
real network 1013, duplicate ownership, ownership backend termination and listener cleanup.

## Future authorized stop-and-replace procedure (not executed)

1. Review an image built from the verified integration commit, retain the approved **current
   100-source** registry/config, archive exact old images/configuration, and arrange a verified
   backup under the operator's procedure. Do not select the 99 proposal by default.
2. Review the resolved service commands, ports, network, existing database/volume identity,
   separate write/SELECT-only credentials and explicit DSNs. Keep the existing database
   service/volume; do not run generic `compose up`, `down -v` or recreate the project.
3. Stop the sole legacy combined app using its preserved deployment configuration. Verify
   its process/container has exited and that no other legacy owner/provider connections
   remain. **The legacy code has no new advisory lock. Successful lock acquisition alone
   does not establish exclusive ownership.** Never overlap old and new collectors.
4. With all relevant writers stopped and operator authorization, apply only 0005 to that
   explicitly identified database. Check the single head and retained mappings/history/
   activation/coverage against approved preflight evidence. No startup runs migrations.
5. Start exactly one lock-aware collector with the current approved registry and reviewed
   image/command. Confirm ownership, heartbeat, bounded recovery, durable native readback
   and visible unresolved gaps. Start read-only API workers independently and verify REST/
   health; expect WebSocket 1013 until Batch 3. A collector failure must not restart API/DB.
6. Keep the old deployment stopped; monitor recovery and identity/source-specific health.
   Production readiness also requires approved networking/source rights and simultaneous
   capacity qualification. These were not supplied by synthetic integration tests.

## Rollback (not executed on production)

For code rollback to a prior legacy image: stop **all** 0005 collector/combined/API owners,
verify no writer remains, then use this integration's migration files with the authorized
DSN to `alembic downgrade 0004`. This removes only the health table; isolated tests prove
all market rows/identities/coverage/revisions unchanged. Restart exactly one preserved
legacy combined image with its original approved configuration. Never start unchanged
legacy Store on schema 0005: its exact-0004 startup check refuses it. Old images may retain
the original long-recovery fanout defect. A schema-0005-compatible rollback image is an
alternative only after separate review/testing; none is claimed here. No downgrade below
0004, database recreation or data deletion is part of this procedure.

## Remaining boundaries / next work

INTEGRATED / VERIFIED: no unresolved merge or integration architecture blocker remains.
The branch is ready as the Batch 2 development base, subject to review/approval.
Batch 2 must start from this verified integration commit after review and
approval, then address its specified API work and production role/config provisioning.
Batch 2 is not implemented here. Batch 3 streaming, real image deployment validation,
simultaneous 99-feed/sustained reliability qualification, deployment-host networking and
employer instrument/volume/UI approval remain separate blockers to deployment/promotion.
