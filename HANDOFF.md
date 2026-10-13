# HANDOFF.md

## Current state — 2026-10-13

FastAPI Batch 2 is implemented locally on `fastapi/batch2-readonly` in
`C:\Users\Pouyan\PycharmProjects\tradingview-fastapi-batch2`. Mandatory integration base
`35ab3c03662ad00e8114fb313f8d2d15459a78d9` was verified locally before this isolated
worktree/branch was created. Completed integration and original worktrees are untouched.
Get the reviewed Batch 2 hash with `git rev-parse HEAD`. No push, main merge, deployment,
live TradingView access, production SQL/grants or collector restart.

Read [API contracts/examples](docs/api.md), [14-point completion report](docs/api-batch2-verification.md),
[integration/rollback evidence](docs/collector-fastapi-integration.md),
[process guide](docs/fastapi-process-separation.md) and [business evidence](docs/business-universe-qualification.md).

## Implemented behavior

- Six authenticated GET routes under `/api/v1`: instruments, prices/latest, candles,
  candles/latest, coverage, health. All nine legacy REST paths/envelopes remain; latest
  routes are deprecated with disclosed corrected default selection. OpenAPI has 15 GETs.
- `api_models.py` owns public schemas; `api_v1.py` routes; `price_selection.py` observation
  selection. Native SQL reused with optional source/cutoff/cursor arguments. Bulk lookup
  joins mappings/latest-state tables without tick-history scans or N+1 network queries.
- Fresh quotes beat candle closes; stale candidates use the later observation. V1 freshness
  considers event/receipt age independently of collector heartbeat. Future clocks are stale;
  missing prices stay unavailable. Candle_close is never a live quote. Realtime quote flag
  requires a fresh explicitly realtime quote; delayed/unknown/simulated statuses preserved.
- OANDA/FXCM stay distinct. FXCM source `FX:EURUSD`, registry venue `FX`, configured broker
  namespace `FXCM`; OANDA `OANDA:EURUSD`. No registry/source activation changes.
- Completed native 1m plus UTC-derived 5m/15m/1h/1d; forming buckets excluded. Elapsed rollups
  with missing base minutes stay incomplete. Latest candle endpoint is native 1m only.
  Decimal strings/UTC/typed NULL and identified tick volume retained. Non-additive rollup
  volume stays null; UTC 1d is not native exchange-session daily data.
- Coverage clips stored EXPECTED/NOT_EXPECTED/UNKNOWN without reclassification. Outside
  tracking is UNKNOWN; every expected gap remains visible. Recovered counts stored backfilled
  provenance; initially_missing is null (no complete initial-loss ledger). Gap detail capped
  at 100 ranges while counts remain complete. No fabricated completion percentage.
- Keyset bulk pages, at most 100 requested symbols/1000 response rows/default 31-day window.
  History cursor binds source/interval/window/as_of and bucket/status position. No MVCC
  snapshot during late recovery; refresh bounded overlaps. All queries parameterized.
- Production bearer configuration rejects missing/short token. OpenAPI auth/401/403,
  explicit HTTPS production CORS allow-list, no wildcard/credentialed cookies. Company BFF
  holds service token; browser uses existing company session. No production secret supplied.
- API factory remains zero-provider/registration/ingestion with read-only DB pool/timeouts.
  Explicit SELECT-only role independently tested with read-only mode disabled and writes/
  schema changes denied. Scoped DBA setup documented; production provisioning unexecuted.
- API/database readiness is independent of collector/TradingView connectivity. Absent/expired
  heartbeat is unknown. Historical reads work offline. API-only WebSocket remains 1013;
  cross-process delivery is Batch 3. No schema migration; single 0005 head.

## Validation

Only `marketdata-batch2-tests`, loopback 55691 (PG17.11)/55692 (Timescale2.25.1/PG17.7),
`marketdata_batch2_test`, own network and tmpfs. Test-owned native processes shut down;
disposable services removed at session end. No production DB connection.

- Final full TimescaleDB suite: **381 passed**, 85.83s.
- Final full PostgreSQL suite: **378 passed, 3 expected aggregate skips**, 85.70s.
- 46 new deterministic contract/operations cases: all five intervals and stored OHLC
  read-back, Decimal/volume/UTC/broker identity, pages without missing/duplicates, coverage,
  authentication/CORS/injection, read-only grants, actual server timeout/connection loss/
  recovery, two authenticated HTTP workers without a collector, unchanged market snapshots.
- Final health 503 OpenAPI metadata adjustment and keyset shared-prefix period/hyphen
  regression are included in both final full reruns. Earlier 45-case targeted suites passed
  on Timescale in 15.11s and PostgreSQL in 11.30s; final full results supersede them.
- Ruff lint/format passed (Python documentation included); strict mypy passed, 33 source
  files; git whitespace/single 0005 head/fresh migrations passed. Existing populated
  status-only downgrade/re-upgrade preserves eight market-table snapshots. One inherited
  Starlette/httpx deprecation warning remains. No final behavioral failures.

Benchmark: 100 simulated sources × 1440 candles, authenticated ASGI/direct read-back, no
provider/collector. Latest-100 p50/p95: Timescale **13.997/19.867ms**, PostgreSQL
**11.560/12.919ms**, two SELECTs. 1000-row 31-day history p50 41.071/26.696ms;
coverage-100 p50 61.536/80.066ms. Existing-index plans execute 4.572/3.507ms. Latest Python
allocation peak about 1.15MB, not RSS. Only one fixture day is populated in the 31-day window;
local sequential samples exclude network/TLS and are not capacity/live qualification.
Reproduce `scripts/benchmark_api_rest.py` with explicit `_test` DSN/nonproduction port;
it cleans only its unique fixture IDs. Sanitized measurements/JUnit/OpenAPI ignored in artifacts.

## Preserved production/business boundaries

Legacy `tradingview` app image remains
sha256:ca498afb02f6232440e3386936a4097e865cf9fefdbd0481adc0cc7ce623023f,
DB volume `tradingview_marketdata_pg`, loopback 5432, approved current 100-source universe.
Exact deployed Git hash unknown. Metadata-only finalization observations retain same app/DB
IDs/images/start times/mounts. No production SQL or service/volume commands. Its collector
continues writing; no unchanged production row-count claim.

**Legacy collector has no advisory lock.** A future approved replacement must stop it and
verify exit before one new owner starts. Legacy code requires 0004; rollback after 0005 needs
status-table-only downgrade with all new owners stopped. Do not run old scale switch helpers
or generic production Compose commands here. See integration guide.

99 business proposal remains provisional/unactivated: 50 Binance spot, both OANDA/FXCM for
21 forex pairs, four OTC metals, two TVC oil references and OANDA gas CFD. Earlier
5,882/5,940 native qualification (99.0236%) and all 58 unresolved gaps remain in unchanged
ledgers, not new capacity evidence. Guest rights/entitlements/employer redistribution remain
unresolved. Provider/collector/recovery/migration/registry files unchanged from integration.

## Next actions

Review local Batch 2 commit and integrate frontend/BFF using docs/api.md. DBA/operator must
provision production SELECT-only DSN, secret rotation, existing company session authorization,
HTTPS/network/origins/gateway limits and deployment-host load qualification. No deployment
is authorized here. Preserve existing history/activation/gaps. Batch 3 owns cross-process
streaming; simultaneous 99-feed qualification and business promotion remain separate.
