# HANDOFF.md

## Current state — 2026-10-14

Current task: consolidate all completed collector/API development on
`integration/completed-main` in C:/Users/Pouyan/PycharmProjects/tradingview-main-consolidation.
Exact source cb4a06332adda936421a00b1b87155e7834cd24f; target/main and merge base
2c5d0e6e51f51d0ae7298501df79e38859f3a464. Main is an ancestor; the proposed merge tree equals
the integration head tree. Only consolidation documentation changes over the published API
source. Read docs/main-consolidation.md and the PR for exact merge SHA/tree and current tests/
CI. Final merge requires explicit user approval. No deployment, production SQL, registry
activation, volume change, branch deletion or history rewrite is authorized/performed.
Original worktrees and paused database-efficiency work remain untouched. Completed collector,
Batch 1/integration/Batch 2 and the Batch 3/4 snapshot are all present by ancestry/content.
The detailed Batch 4 qualification below remains historical evidence, not a new load run.

Batch 4 company API qualification is implemented, unmerged and
undeployed in C:/Users/Pouyan/PycharmProjects/tradingview-fastapi-batch4, branch
fastapi/batch4-qualification. Base is verified Batch 2 b9c08df51655f4767dd0669838f2d221c4854933
plus the verified uncommitted Batch 3 snapshot copied from tradingview-fastapi-batch3.
Original Batch 1/2/3/integration/ordinary-scale worktrees and the dirty deployment checkout
remain preserved. No paused candle-database optimization was imported.

Read docs/api-batch4-verification.md (15-point employer report),
docs/api-batch4-performance.md / api-batch4-evidence.json (actual tables/plans/metrics/hashes),
docs/frontend-integration.md and docs/production-deployment.md. Earlier API/process/journal
contracts and source/business evidence remain linked from the report and docs/api.md.

## Implemented changes over Batch 3

- Production API startup refuses administrative/schema-CREATE/database-TEMP/market-write
  DB roles; actual SELECT-only credentials serve two-worker REST/WS without ingestion.
- New file-secret production entrypoint and compose.production.yaml: independent migration,
  API and opt-in collector, private DB network, no DB publication, explicit external volume,
  non-root/read-only API, log rotation, bounded transport/admission, restart policies.
- Production guest ingestion is blocked by default until established access rights. API
  history has no provider path. No provider/registry/scheduling/parsing algorithm changed.
- Public /livez distinguishes process liveness from DB /health readiness. Versioned health
  adds actual idle/operational/degraded/unknown WS reader status. Source/collector/coverage
  degradation never creates a blanket historical-query failure.
- Sanitized unexpected/startup exceptions, legacy whole-route budgets, auth failure counts,
  no-store responses, route/query/pool/WS queue/reader/event-loop metrics. Reader failures
  close affected clients with resync; query instrumentation cleans up terminated proxies.
- Browser example adds explicit coverage callbacks; standards-based frontend guide and
  operations/backup/restore/rollback documentation require no frontend SQL credentials.
- scripts/qualify_api.py performs real TCP load with a separate API process, 100 synthetic
  sources and deterministic stored candles. Exact test DSN/project checks protect faults
  and cleanup. summarize_api_qualification.py publishes measured reports with raw hashes.

Schema remains 0006, inherited from Batch 3 (transactional commit-ordered replay journal).
Batch 4 adds no migration. APIs never register instruments or acquire collector ownership.
Latest quotes, candle closes, stale/unavailable observations and missing candles stay distinct;
UTC and exact Decimal strings, independent broker histories and corrections are preserved.
Only supported Store writes create replay events; direct SQL/old binaries do not. Retention
is count-based; expired cursors/restored epochs require full loaded-chart REST resync.

## Actual final verification

Task-owned marketdata-batch4-tests only: loopback 55891 PostgreSQL17.11, 55892
Timescale2.25.1/PG17.7, marketdata_batch4_test, separate network/tmpfs, no production mounts.
Final full suites: PostgreSQL 436 passed + 3 expected aggregate skips, 109.60s;
Timescale 439 passed, 115.40s. Include actual container DB pause/unpause, backend termination,
server/query deadlines, both restore engines, market-preserving migration round trips,
correction/replay/retention/slow-client faults, mock collector/API restart independence,
two real workers and Linux production container restart with mounted SELECT-only secrets.
23 inherited Starlette/httpx/legacy WS deprecations remain; upgrades require requalification.

Ruff check/format (141 Python files), strict mypy 37 source / 38 including BFF, Node syntax,
single head 0006, fresh upgrades, production Compose isolation assertions and whitespace pass.
Final local Linux image: sha256:430ded32c6f8eee44c10959c1e840569f24f9c6e7a393cc9ba6bc621be9df141.
Not pushed/deployed. Raw JUnit and benchmark checkpoints remain ignored under artifacts.

Per engine: 100 synthetic sources (50 crypto/40 forex/5 metals/5 energy), 143,997 stored native
minutes, three explicit gaps. 120 requests per workload at 1/8/24 REST clients; 5/20/50 WS
clients each subscribe 100 symbols. 2184 normal measured requests and 22,500 expected events
per engine: zero normal failures/all delivered. At 24 clients history 1000 p95 PG 943.713 /
TS 891.272ms; at 50 WS clients ACK-to-delivery p95 PG 1366.275 / TS 1687.670ms (1s poll).
16 expected 503 fault responses per engine took about 5s; historical reads recovered after
DB unpause. SQL plan used existing indexes, no spill, execution 3.204/3.515ms.
Separate 8-client/20-WS resource reruns retain all raw samples: PG peak Docker CPU 632.51%,
container memory 1.163GiB; TS 121.54%, 1.251GiB. Later PG coverage p95 was 898.019ms; values are
shared-host samples, not a ceiling. No WAN/TLS, target-host or sustained-capacity claim.

Final metrics labels cap unknown HTTP methods as OTHER. A Windows shared-listener test
routing assumption failed once; its XML is retained. The test now pauses new admission on
the first worker while retaining established sockets, proving both PIDs deterministically.
Targeted checks passed on both engines; final Timescale full rerun passed. See the report.

## Preservation and remaining gates

Docker Desktop was initially stopped. The user explicitly authorized starting it and
possible automatic resumption of existing containers. The existing ordinary-100 collector
and DB resumed; no production SQL/grants/migrations/recreation/source activation or collector
configuration change was performed. Existing 100/34/17 images/history/volume remain outside
Batch 4 operations. Legacy combined collector lacks the new advisory lock and must exit
before any separately approved lock-aware replacement. Never run legacy switch helpers here.

Remaining: source rights/redistribution and a genuine continuous quote source; final employer
selection; production DBA/secrets/HTTPS/firewall/real company sessions/monitoring/backup schedule;
target-host CPU/memory and sustained/wide-history capacity. TradingView outage/wire-level
commit-ACK loss/host failover/production DR are not tested. API readiness and ingestion/source
reliability must remain separate. The browser/BFF adapter is provisional until supplied with
actual company authorization. Do not deploy/promote from synthetic evidence alone.

Reproduction: build marketdata-batch4:qualification, use compose.batch4-test.yaml and matching
TEST_DATABASE_URL/BATCH4_TEST_CONTAINER/BATCH4_TEST_IMAGE. Never run pytest and the harness
on the same database concurrently. Full commands are in the report. Only task-owned
containers/network are removed after qualification; no production volume is removed.

The user authorized committing and pushing the complete Batch 3/4 API snapshot on 2026-10-14
to origin/fastapi/batch4-qualification (Alpha-lacrim/tradingview-marketdata). Get the exact
published revision with git rev-parse HEAD; confirm the remote ref before further publication.
This publication does not merge main or deploy services. Next: review the Batch 4 delta,
establish target-host/session/source permissions and perform the operator rollout after approval.
Task-owned test containers/network were
removed after final verification; no task-owned native worker remains. The reviewed delta
over Batch 3 and file hashes are in docs/api-batch4-source-manifest.json.
