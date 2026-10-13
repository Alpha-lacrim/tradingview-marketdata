# FastAPI Batch 2 — completion report

Verified on 2026-10-13, branch `fastapi/batch2-readonly`, isolated worktree
`C:\Users\Pouyan\PycharmProjects\tradingview-fastapi-batch2`.
Mandatory base `35ab3c03662ad00e8114fb313f8d2d15459a78d9` was verified locally on clean
`integration/collector-fastapi-batch1` before creating this worktree. No GitHub/main base,
push, merge to main or deployment. Get the reviewed Batch 2 hash with `git rev-parse HEAD`.

## 1. Existing functionality reused

Batch 1 database-only lifespan/read pool, parameterized Store queries, persisted registry,
dedicated latest-state tables, native candle rollups, UTC/Decimal models, tri-state coverage,
collector heartbeat expiry and legacy routes. Optional source/cutoff/cursor parameters
extend `native_candles`; no duplicate historical implementation, cache or infrastructure.
Collector, provider transports, recovery, registries and migrations are unchanged.

## 2. New routes implemented

Six GET operations under `/api/v1`: instruments, prices/latest, candles, candles/latest,
coverage and health. Stable schemas provide exact identity, truthful availability/freshness,
native/derived provenance, null observations/volume and coverage state. History supports
1m/5m/15m/1h/1d; latest candles explicitly returns native 1m. No symbol activation.

## 3. Backward compatibility

All nine original REST routes/envelopes remain. Legacy latest routes are deprecated with
disclosed corrected default selection; quotes gain `price_type=quote`. Legacy freshness
still uses receipt age and collector health. The integration regression now expects the
newer stale quote when it is the later observation, while retaining separate native history,
broker/gap/migration assertions. Explicit collector candle-provider selection is retained.
Incorrect bearer tokens now return 403; missing/malformed headers return 401. Documentation
routes are public for Swagger authorization. Native 1m tick volume retains recorded units;
non-additive derived volume stays null. See the compatibility section in [api.md](api.md).

## 4. Exact OpenAPI inventory

15 GET operations, checked against the generated schema:

```text
/health
/providers
/instruments
/instruments/{symbol}
/prices/latest
/prices/latest/{symbol}
/ticks/{symbol}
/candles/{symbol}
/futures/{exchange}/{root}/{alias}
/api/v1/instruments
/api/v1/prices/latest
/api/v1/candles
/api/v1/candles/latest
/api/v1/coverage
/api/v1/health
```

Authenticated internal `/metrics` is excluded from OpenAPI. `/ws/prices` remains 1013 in
API-only mode. `/docs`, `/docs/oauth2-redirect`, `/redoc`, `/openapi.json` are documentation
GET/HEAD routes. Classification of every starting route is in api.md. OpenAPI declares
CompanyBearer, structured errors and both typed health/error 503 bodies. Generated schema
is retained locally in ignored `artifacts/batch2-openapi.json`.

## 5. Authentication design

Constant-time bearer comparison; case-insensitive scheme; production requires a nonempty
secret token of at least 32 characters. Development/test may explicitly omit it. CORS is
an exact origin allow-list, HTTPS in production, without wildcard or cookie credentials.
Company BFF adds the secret server-side after existing company session/user authorization.
No permanent service key belongs in public JavaScript. No production credentials supplied.

## 6. Read-only database configuration

API factory starts zero providers/registration/ingestion, opens read-only transactions and
bounded pool/command/server timeouts, and runs no migrations or role provisioning. Scoped
DBA instructions grant SELECT on ten required relations plus twelve existing Timescale
views where applicable. Both database engines independently serve REST with explicit
SELECT-only grants; with transaction read-only off, UPDATE/DELETE/TRUNCATE/CREATE/ALTER
remain denied. Collector keeps its write role. Production role/password/PUBLIC-grant
hardening is unexecuted DBA work; PostgreSQL need not be publicly exposed.

## 7. Pagination

Exclusive canonical/provider keysets for bulk lists; maximum 100 requested symbols and
1000 response rows (configurable lower cap). Default maximum history window 31 days.
History cursor binds source/interval/window/as_of and `(bucket,status)` position.
Mixed-status page traversal verifies no duplicate or missing fixture rows. Gap detail is
bounded to 100 ranges without truncating counts. Parameterized input prevents SQL injection.
Concurrent late inserts/corrections do not have an MVCC snapshot; refresh bounded overlaps.

## 8. Freshness semantics

Fresh persisted quotes beat candle closes. V1 considers event and receipt age independently
of collector heartbeat; future clocks are stale. Otherwise use the later observation,
candle winning ties. Candle_close is never a live quote; missing observations remain null.
`realtime_quote_available` requires a fresh explicitly realtime quote. Delayed/indicative/
unknown/simulated sources keep their status. Exact OANDA and FXCM mappings never switch.

## 9. Coverage semantics

Clip stored EXPECTED/NOT_EXPECTED/UNKNOWN ranges; untracked time stays UNKNOWN. Preserve all
unresolved expected gaps, count stored completed rows and backfilled provenance independently.
Initial-missing is null because schema 0005 has no complete original-loss ledger. No completion
percentage conceals gaps. Elapsed partial rollups remain incomplete, forming buckets excluded,
Decimal OHLC/typed null volume preserved. Daily rollups use UTC, not native source sessions.
Historical 99-source qualification ledgers and all 58 unresolved gaps are untouched.

## 10. Exact test results

| Check | Executed result |
| --- | --- |
| Final full TimescaleDB 2.25.1 / PostgreSQL 17.7 | **381 passed**, 1 inherited warning, **85.83s** |
| Final full standalone PostgreSQL 17.11 | **378 passed, 3 expected aggregate skips**, 1 inherited warning, **85.70s** |
| New contract/operations tests | 46 cases, included in both final full runs |
| Ruff lint/format | Passed; includes Python documentation examples |
| Strict mypy | Passed, 33 source files |
| Alembic | One 0005 head, both fresh upgrades, existing populated status-only downgrade/re-upgrade snapshot regression passed |
| Real HTTP/fault/permissions | Two authenticated API processes, no collector; stored broker OHLC/NULL read-back; actual connection termination/recovery/server timeout; SELECT-only role with read-only off |
| Preservation | Eight market-table snapshots unchanged around reads/migration round trips; provider/collector/registry/migration files unchanged |
| Git whitespace | Passed |

Initial development failures were corrected: HTTP security dependency affecting WebSockets,
fixture status/normalization, FX-vs-FXCM venue expectations and an attempted change to the
legacy receipt-age contract caught by its regression. No final behavioral failure. One
inherited Starlette/httpx TestClient deprecation warning remains; no new dependency needed.

Final review corrected a keyset edge case: composite string comparison could skip canonical
symbols with a shared prefix followed by period/hyphen. The predicate now compares the same
two columns as ORDER BY, with a dedicated regression. Both full suites above were rerun
after this fix and the final health 503 OpenAPI metadata adjustment. Earlier 45-case targeted
runs passed on Timescale (15.11s) and PostgreSQL (11.30s); final full results supersede them.

Only `marketdata-batch2-tests`, loopback 55691/55692, `marketdata_batch2_test`, separate
network and tmpfs used. No production SQL/credentials/grants/provider connection/service
restart/volume operation. Metadata-only snapshots during finalization retain production
app/DB IDs, images, start times and mounts. This is isolation evidence, not frozen row-count
proof while the production collector continues writing. Test-owned processes/services exit.

## 11. Performance measurements

`scripts/benchmark_api_rest.py`: 100 unique simulated sources × 1440 candles (144,000 total),
one populated fixture day per source, authenticated sequential in-process ASGI requests.
Both runs compare returned values to stored NUMERIC rows and remove only their fixture IDs.

| Request | Samples | Timescale p50 / p95 / max ms | PostgreSQL p50 / p95 / max ms | SELECTs/request |
| --- | --- | --- | --- | --- |
| Latest prices, 100 sources | 50 | 13.997 / 19.867 / 49.519 | 11.560 / 12.919 / 44.336 | 2 |
| Latest candles, 100 sources | 20 | 13.389 / 16.798 / 17.936 | 11.613 / 12.755 / 13.029 | 2 |
| 1m history, 31-day window, 1000 rows | 20 | 41.071 / 62.007 / 63.630 | 26.696 / 31.651 / 44.722 | 3 |
| Coverage, 100 sources, 31-day window | 10 | 61.536 / 66.527 / 92.354 | 80.066 / 90.555 / 129.997 | 3 |

Actual historical `EXPLAIN (ANALYZE, BUFFERS)`: Timescale execution **4.572ms**, planning
0.685ms, existing chunk PK index/bitmap scans; PostgreSQL execution **3.507ms**, planning
0.179ms, existing `market_candles_pkey` Index Scan. Each reads 1440 fixture rows, returns
1001 for limit-plus-one. Latest-request Python allocation peaks: **1,146,683 / 1,145,208
bytes**, respectively, not RSS. No additional index/cache was justified.

Short local warm samples include query-logging overhead and exclude network/TLS/UI latency.
The 31-day window contains only one populated fixture day. Background host workload may
affect comparisons. No concurrency/sustained/full-history/live-feed/99-feed capacity claim.
Sanitized raw measurement/plan JSON remains ignored under artifacts.

## 12. Exact local commands

Complete API-only two-worker startup, scoped DBA grants, Swagger, curl/Python/JavaScript
requests and validation commands are in [api.md](api.md#isolated-local-use-and-validation).
The executed validation command set:

```powershell
uv sync --frozen --extra databento
docker compose -p marketdata-batch2-tests -f compose.batch2-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://batch2_test:disposable_test@127.0.0.1:55692/marketdata_batch2_test'
uv run alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q --tb=short --junitxml=artifacts/batch2-timescale-final.xml
uv run python scripts/benchmark_api_rest.py --output artifacts/api-timescale.json
$env:DATABASE_URL='postgresql://batch2_test:disposable_test@127.0.0.1:55691/marketdata_batch2_test'
uv run alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q --tb=short --junitxml=artifacts/batch2-postgres-final.xml
uv run python scripts/benchmark_api_rest.py --output artifacts/api-postgres.json
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run alembic heads
git diff --check
docker compose -p marketdata-batch2-tests -f compose.batch2-test.yaml down
```

Last command removes only this task's disposable services. No production teardown/deployment.

## 13. Remaining frontend integration

Company HTTPS hostname/BFF, existing user login/authorization/session integration, server
secret rotation, approved origins and DBA-provisioned API DSN. Render decimal strings,
price basis/source/broker, freshness, null volume and incomplete/unknown coverage faithfully;
follow pagination and handle documented errors. Establish gateway rate limits/monitoring and
load-test the actual UI workload on its host. Fetch examples are provided; no frontend app
was requested or built. Cross-process streaming remains Batch 3.

## 14. Authorization/deployment issues

TradingView guest remains experimental/unsupported; observations do not establish employer
redistribution rights or exchange entitlements. Provisional 99-source proposal is preserved,
unactivated/unqualified. Production grants/DSN/TLS/network/image qualification and source
rights remain external work. The running legacy 100-source combined collector/database are
untouched. Future replacement must follow the integration guide: the legacy owner has no
advisory lock, must exit first, and legacy rollback requires the tested 0005→0004 status-only
downgrade. No push, merge, deployment or overstated realtime capability.
