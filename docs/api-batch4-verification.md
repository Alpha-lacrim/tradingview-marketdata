# Batch 4 employer qualification report — 2026-10-14

The database-backed company API is **VERIFIED locally for a 100-instrument synthetic UI
workload**, including real TCP reads/streaming, independent processes, dedicated SELECT
credentials, Linux image startup/restart and controlled database failures. It is **not yet
qualified on the company's production host**. Source authorization, continuous genuine
TradingView quotes and sustained ingestion are separate outstanding requirements.

Work is isolated in `C:/Users/Pouyan/PycharmProjects/tradingview-fastapi-batch4`, branch
`fastapi/batch4-qualification`. It starts from Batch 2 `b9c08df51655f4767dd0669838f2d221c4854933`
plus a byte-preserved copy of the verified uncommitted Batch 3 worktree. Prior worktrees
remain untouched. Qualification performed no commit, push, merge or production deployment.
The user subsequently authorized publication to `fastapi/batch4-qualification` on GitHub;
the release revision is available through that branch's Git history. Publication does not deploy.
The user
authorized starting Docker Desktop, including automatic resumption of existing containers.
No production SQL, grants, migrations, volume replacement, collector configuration or source
universe change was performed. Only task-owned disposable resources were faulted/removed.

## 1. Exact supported endpoints — VERIFIED

All 16 documented GET operations remain available:

| Company endpoint | Purpose |
| --- | --- |
| `/api/v1/instruments` | Source discovery and pagination |
| `/api/v1/prices/latest` | Bulk quote/candle-close/unavailable selection |
| `/api/v1/candles` | Completed native/derived history with cursor |
| `/api/v1/candles/latest` | Latest completed native 1m per source |
| `/api/v1/coverage` | Tri-state coverage and explicit gap ranges |
| `/api/v1/health` | Separate API/database/collector/data/coverage/WS status |
| `/api/v1/marketdata/changes` | Persisted event reconciliation/replay |

Legacy GETs: `/health`, `/providers`, `/instruments`, `/instruments/{symbol}`,
`/prices/latest`, `/prices/latest/{symbol}`, `/ticks/{symbol}`, `/candles/{symbol}`,
`/futures/{exchange}/{root}/{alias}` (`front`/`next`). Legacy latest routes are deprecated.
Additional GETs: public `/livez`, protected `/metrics`, public `/openapi.json`, `/docs`,
`/docs/oauth2-redirect`, `/redoc` (documentation also accepts HEAD).
WebSockets: `/api/v1/ws/marketdata` is the company persisted stream; `/ws/prices` is legacy
and closes 1013 in API-only mode. Full schemas/parameters: [API contract](api.md).

## 2. Authentication — VERIFIED locally / production integration PARTIALLY VERIFIED

Production requires a bearer token of at least 32 characters; missing/malformed credentials
return 401, wrong token 403. Comparison is constant-time. Explicit HTTPS CORS origins are
validated; cookie CORS is disabled. WebSocket bearer and exact Origin checks are independent.
Mounted secret files keep credentials out of image source, Compose environment/inspection,
URL arguments and application fault messages. Unexpected REST errors return sanitized 500.
Uvicorn URL access logs are disabled by the production entrypoint; proxy log policy is documented.
Company browser session/BFF adapter remains provisional, with an injected real validator.
The real company identity system, production token provisioning and TLS are not exercised.

## 3. Historical data — VERIFIED

Native completed 1m and UTC-derived 5m/15m/1h/1d; bounded aligned half-open UTC windows,
1–1000 rows and source/window-bound cursors. Forming bars are excluded from v1. Incomplete
derived buckets remain explicitly incomplete. Prices/volume are decimal strings; unknown
volume stays null. Independent source identity, corrections and receipt time are preserved.
Pagination traversed 1439 stored bars across three pages without duplicates, retaining the
deliberate missing minute. REST history works without a collector or provider connection.

## 4. Current prices — PARTIALLY VERIFIED

Persisted genuine quotes, completed candle closes and unavailable states have separate
bases/types. Event and receipt age, skew, stale threshold, source status and quote availability
are explicit. Quote freshness can expire without another update; stale data never becomes
realtime merely because the collector reconnects. Quote behavior is verified with simulated
persisted observations. TradingView native candle mode does **not** supply a continuous
genuine quote stream. Live quote availability/access remains BLOCKED on an authorized source.

## 5. WebSocket — VERIFIED locally

Subscribe/unsubscribe/snapshot/ping/disconnect; batched snapshots, persisted price updates,
native completed/corrected bars, source status and heartbeat. Journal writes commit atomically
with market writes; API workers own independent bounded readers/queues. Retained replay
includes old corrections/recovered minutes. Expired/foreign epochs return 409 and require
refreshing the whole loaded chart. Queue/send limits protect healthy clients; oversized/binary
commands are rejected. Two actual workers receive the same commit, without provider creation
or ingestion. Reconnect, restart, rollback, retention expiry and slow clients are tested.
This is recoverable delivery, not exactly once; old binaries/direct SQL do not emit journal events.

## 6. Database security — VERIFIED locally / production provisioning PARTIALLY VERIFIED

Frontend clients receive no DB credentials. Separate owner/migration, collector writer and
API SELECT roles are documented. Production startup rejects administrative, schema-CREATE,
database-TEMP or market-write privileges. Real SELECT-only roles serve REST and WS; turning
off transaction read-only still cannot write protected tables. Queries are parameterized.
Query/row/window limits and pool/server/request timeouts are enforced. Production Compose
has no published PostgreSQL port, uses an internal DB network, explicit external volume and
separate mounted DSNs. Configuration assertions passed; real remote firewall reachability
and installation-specific function grants remain operator work.

## 7. Verified performance — VERIFIED local samples / sustained capacity NOT TESTED

[Measured tables](api-batch4-performance.md) contain p50/p95/p99/RPS, all concurrency levels,
API CPU/RSS, DB query duration/pool use, WS latency/queue pressure, event-loop lag and injected
degradation. [Machine evidence](api-batch4-evidence.json) retains reports, actual EXPLAIN
plans, metrics and SHA-256 of raw artifacts/JUnit. The [harness](../scripts/qualify_api.py)
refuses any DSN outside this task's two databases; it builds no provider.

Each engine ran five workloads × 120 requests × 1/8/24 concurrent clients, plus 120 concurrent
history requests during each 5/20/50-client streaming stage and 24 recovery requests:
**2184 normal measured requests per engine, zero failures**. Each engine delivered
**22,500/22,500** expected WS price events across the three stages. Simulated writer commits
updated 100 instruments while REST and WS remained active. Separately 16 requests per engine
returned expected 503 during the database pause; these are recorded as 100% failure for that
fault phase, never mixed into the normal zero-failure claim.

Representative p95 values at 8 concurrent clients (PostgreSQL/Timescale): latest 100
106.354/78.964 ms; history 500 210.894/152.524 ms; history 1000 359.653/238.059 ms;
coverage 100 248.847/149.706 ms. At 24 clients, history 1000 p95 rises to 943.713/891.272 ms;
the full p99/RPS table records that cost. At 50 WS clients, ACK-to-delivery p95 is
1366.275/1687.670 ms with the default 1s poll interval. No low-latency streaming SLA is implied.
Actual history SQL executes 3.204/3.515 ms using existing primary/chunk indexes with no spill.
No measured query-plan problem justified new indexes, caching or infrastructure.

Scope: Windows CPython 3.12.7 API process, Linux Docker databases PG 17.11 and
Timescale 2.25.1/PG 17.7, Intel Family 6 Model 183, 20 logical CPUs. The generator is a separate
process on the same shared development host; existing containers continue running. API
workers were active 72.092/59.744s, not an hour/day soak. CPU averages refer to one core;
RSS/pool/loop lag are sampled; per-client queue high-water is captured on enqueue. DB query
duration includes client decoding and injected timeouts; Windows clocks are coarse. REST
timings include response parsing; WS latency starts at local transaction acknowledgement,
not an exchange timestamp. One fixture day is populated, not years of history. Loopback
excludes TLS/WAN/browser/company-host effects. No target-host SLA or multiworker throughput
extrapolation is made. Database resource profiles are recorded separately; long-duration
resource drift is NOT TESTED.

Separate resource-profile reruns used 8 REST clients, 40 requests per workload and 20 WS
clients on the same 100-source fixture. Docker stats captured 10 PostgreSQL / 9 Timescale
samples: peak container CPU 632.51% / 121.54% (100% denotes one logical CPU), and maximum
reported memory 1.163 GiB / 1.251 GiB. These include the container's DB/background workers
and cache, not summed per-backend RSS. PostgreSQL coverage p95 in this later run rose to
898.019 ms; the earlier workload samples are not a latency ceiling. Repeated fixture
insert/delete runs and background maintenance affect this shared host. Both profile runs
still had zero normal failures and all 6000 WS events delivered. Full samples are retained
in the evidence JSON; budget DB CPU/memory and qualify the target server before rollout.

## 8. 100-instrument behavior — VERIFIED synthetic / live business universe BLOCKED

100 configured synthetic source identities: 50 crypto, 40 forex, 5 metals, 5 energy; exact
Decimal prices; 1440 nominal minutes each, with three deliberately missing minutes:
143,997 persisted bars. Discovery/latest/bulk coverage return 100 items. History 500/1000
returns exact prices; pagination and missing counts match the fixture. All statuses remain
simulated. These are not invented TradingView symbols or live availability claims. The
provisional 99-source employer selection/old 100-source collector qualification remain separate
preserved evidence and were not activated or requalified here.

## 9. Coverage and health — VERIFIED

EXPECTED missing ranges remain visible; NOT_EXPECTED requires stored session evidence;
unclassified/outside tracking stays UNKNOWN. Counts remain complete when detail is truncated.
Initial-loss count stays null. `/livez` stays 200 during DB unavailability. Readiness fails
only for DB serving failure; authenticated health reports collector unknown/stopped separately
from fresh/stale data and coverage gaps. Idle, operational and failed journal-reader states
are distinguished without failing otherwise readable history.

## 10. Failure/recovery — VERIFIED deterministic scope

| Scenario | Classification and actual exercise |
| --- | --- |
| Collector stopped / sources unused | VERIFIED: stored reads with provider construction/registration forbidden; actual mock collector restart on the same API |
| TradingView unreachable | PARTIALLY VERIFIED: API has no provider path and serves without it; actual upstream/network outage NOT TESTED |
| PostgreSQL unavailable/restored | VERIFIED: allow-listed disposable container pause/unpause,503/errors/WS heartbeat, recovery; backend termination and server timeout also exercised |
| API/collector restart independence | VERIFIED: native process tests plus actual Linux API container restart; collector persisted data retained |
| Multiple API workers | VERIFIED: actual two-worker supervisor and Linux image; no provider construction/registration/collector heartbeat duplication |
| Reconnect/missed events | VERIFIED: snapshot barriers, bounded replay, retained old corrections, epoch expiry and full resync |
| Stale prices/gaps/corrections | VERIFIED: clock-controlled staleness, explicit gaps, persisted historical correction readback |
| Slow clients | VERIFIED: bounded queue/send fault tests; healthy delivery unaffected |
| Backup/restore/rollback | VERIFIED: actual pg_dump/pg_restore into unique recovery DB on both engines; market-data-preserving 0004/0005/0006 migration round trips |

During load faults all 16 requests returned 503 in about 5s per query budget. After unpause,
first readiness succeeded in 9.482/10.850 ms; 24 subsequent history queries each succeeded.
These recovery samples do not predict production outage duration or reconnect time over WAN.

## 11–13. Deployment, rollback and frontend handoff — PARTIALLY VERIFIED

[Exact deployment/backup/restore/rollback steps](production-deployment.md) and
[production Compose](../compose.production.yaml) are provided. Its rendered configuration
passed isolation/secret assertions. The final image
`sha256:430ded32c6f8eee44c10959c1e840569f24f9c6e7a393cc9ba6bc621be9df141`
was built and tested with two workers, file secrets, non-root/read-only execution, authenticated
REST and WS, and restart preserving history on both DB engines. It is a local qualification
image, not a pushed release. Production secrets/HTTPS/DBA grants/network/session integration
still require operator execution. Existing volumes are never removed or replaced.

[Frontend guide](frontend-integration.md) supplies every requested connection/request/error/
freshness/gap/UTC/Decimal/reconnect detail with illustrative JSON. The standards-based
[browser example](../examples/browser-marketdata.js) includes prices/history/coverage and
reconciliation. Company developers do not need SQL or PostgreSQL credentials. The final
framework/session architecture is unknown; real company browser integration is NOT TESTED.

## 14. Executed validation — VERIFIED

Final full PostgreSQL suite: **436 passed, 3 expected Timescale skips, 109.60s**.
Final full Timescale suite: **439 passed, 115.40s**. This includes database integration,
plain/Timescale modes, REST/WS smoke, process independence, faults, SELECT roles, migrations,
Linux production container smoke and backup/restore. 23 inherited warnings concern
Starlette/httpx and the explicitly bounded legacy websocket backend. Future dependency
upgrades require requalification before changing that backend.

Ruff lint/format, strict mypy (37 source files, 38 including BFF), JS syntax, migration single
head 0006, production Compose validation and diff whitespace passed. Fresh Alembic upgrades
ran on both task databases. Load reports are generated from executed TCP tests, not ASGI-only
unit timing. Initial query-logger cleanup after backend termination and Timescale restore
extension setup failures were fixed; final suites reran successfully. No unperformed soak,
production outage or live source test is reported as passed.

The final metrics guard normalizes arbitrary HTTP method labels to OTHER, preventing
unauthenticated counter-cardinality growth. A Windows shared-listener scheduling regression
was corrected in the test harness: the first worker stops accepting new connections while
retaining its established socket, so both real workers must be observed and receive the
same commit. Targeted checks passed on both engines (3.24/3.49s). The failed routing run is
retained under ignored artifacts/batch4-timescale-routing-failure.xml; it was a connection
assignment assumption, not a dropped journal event. The Timescale full suite was rerun.

Reproduce on the two tmpfs databases (sequential tests/load per database):

```powershell
uv sync --frozen --extra databento
docker compose -p marketdata-batch4-tests -f compose.batch4-test.yaml up -d --wait
docker build -t marketdata-batch4:qualification .
$env:DATABASE_URL='postgresql://batch4_test:disposable_test@127.0.0.1:55891/marketdata_batch4_test'
uv run alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
$env:BATCH4_TEST_CONTAINER='marketdata-batch4-tests-postgres-1'
$env:BATCH4_TEST_IMAGE='marketdata-batch4:qualification'
uv run pytest -q --junitxml=artifacts/batch4-pg-final.xml
uv run python scripts/qualify_api.py --output artifacts/batch4-pg-performance.json
# Repeat 55892 / timescale container / alembic -x timescale=true; retain matching output names.
# Optional per-engine container resource profile (retain separate resources output names):
uv run python scripts/qualify_api.py --output artifacts/batch4-pg-resources.json --concurrency 8 --requests 40 --ws-clients 20 --db-resources
uv run python scripts/summarize_api_qualification.py
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run mypy src examples/company_bff.py
node --check examples/browser-marketdata.js
docker compose -p marketdata-batch4-tests -f compose.batch4-test.yaml down
```

## 15. Remaining blockers and audit decisions

[Source manifest](api-batch4-source-manifest.json) identifies exact Batch 4 file hashes and
the delta from the preserved Batch 3 snapshot. No prior pending changes were overwritten.

**BLOCKED:** TradingView access/redistribution rights and an authorized continuous quote
source; final employer instrument/source selection and live ingestion reliability. Production
guest ingestion defaults to blocked and must not be enabled from benchmark evidence.

**PARTIALLY VERIFIED:** production rollout, target-host resource budget, real credentials/DBA
grants, remote firewall, trusted HTTPS proxy, monitoring scrape topology, backup scheduling
and actual company session integration. The runbook and local tests make these reviewable,
not executed production changes.

**NOT TESTED:** sustained/multiday load, WAN/TLS/browser latency, actual TradingView outage,
database host failover/wire-level lost commit acknowledgement and production disaster recovery.
Old binaries/direct SQL do not publish journal
events; derived aggregates require REST refresh. Retention is count-based, not a guaranteed
reconnect duration. Limits/metrics are per worker; global gateway enforcement remains external.

Audit changes were limited to demonstrated security/operations gaps: production role
enforcement, secret-file deployment, sanitized unexpected/startup errors, whole-route query
budgets for legacy routes too, counted auth failures, process liveness, reader health and
pool/query/queue/lag metrics. No ingestion algorithm, registry promotion, schema revision,
OAuth/Kubernetes/Kafka/Redis or unrelated scaling change was introduced.
