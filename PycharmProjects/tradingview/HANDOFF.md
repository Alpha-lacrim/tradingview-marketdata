# HANDOFF.md

## Current state - 2026-10-07

Experimental tradingview_guest defaults to completed native **1m candles**, all 17 existing
instruments unchanged. Hardening is implemented and deployed. Alembic head **0004**; 0001-0003,
market_candles constraints/hypertable, historical ticks and tick CAGGs are preserved. No new
dependencies/infrastructure. ONE API/ingestion worker. Main Compose app localhost:8000;
DB localhost:5432, retained volume tradingview_marketdata_pg. Use compose.yaml plus
compose.tradingview.yaml for guest; bare Compose selects mock. Current health deliberately
**degraded** because two older XAUT minutes are absent; no fabricated bars.

Parent Git root C:/Users/Pouyan, branch main. Baseline commit **6f3aa7a0**. Hardening is included
in the commit containing this handoff. Scope Git to this project; preserve unrelated untracked BATCHES_V1.md.
Secrets/.env/artifacts/research/exports ignored. AGENTS.md unchanged this task.

## Decisions / implementation map

- Existing guest-only transport, no accounts/cookies/browsers/new endpoints or instruments.
  Protocol evidence, all-17 identities and native volume/price semantics:
  docs/native-candle-verification.md and docs/tradingview.md. Native basis, exact Decimal/UTC;
  no forming bars. Crypto base volume and OANDA tick count separate; TVC oil volume NULL.
  TVC oil is a reference, OANDA metals OTC, XAUT a token, never exchange-futures substitutes.
- tradingview_candles.py measures request/first-message/native-bar-availability/parser timings.
  Original serial waits proved avoidable. At most **two outstanding chart sessions on ONE
  socket**, one receiver routes existing session IDs. No source protocol expansion.
- candle_ingestion.py immediately reconciles durable startup state per instrument, then
  **ONE normal UTC-minute cycle + grace**; wall-clock deadline recomputed, duplicate-attempt
  guard. Defaults: **2s grace / 3 completed minutes correction overlap / 300 completed
  wall-clock minutes recovery horizon**, plus forming-bar allowance. No per-second upstream
  polling or raw-tick writing. Overlap/backfill occurs within the same minute cycle.
- sessions.py interprets supported source schedules with local IANA/DST, overnight/day/history
  syntax and holidays. Crypto weekend EXPECTED; FX weekend NOT_EXPECTED. Unsupported calendars,
  correction-date syntax and uncertain OANDA maintenance vicinity UNKNOWN. Actual native bars
  positively establish EXPECTED. Compressed range classification uses day/session boundaries.
- 0004 adds candle_coverage not_expected/unknown multiranges and session_info; candle_revisions
  audit holds changed fields and exact old/new values. Previous blanket missing migrates to
  UNKNOWN pending source reconciliation. No main historical data deletion or migration rewrite.
- persistence.py COPY/stages then atomically UPSERTs changed values from newer receipts, latest,
  coverage and audit. Unchanged overlap leaves history/updated_at alone; older receipts cannot
  overwrite. Backfill provenance survives corrections. WebSocket emits actual changed committed
  payloads only. Latest closes retain bucket age; older receipt cannot freshen the price.
- EXPECTED minutes = persisted bars + unresolved expected gaps; NOT_EXPECTED/UNKNOWN separate.
  Coverage health exposes latest expected/persisted, expected lag, unknown/gap counts, oldest
  gap, recoverable/outside-horizon counts and fetch/persist/correction timestamps. Historical
  expected gaps OR UNKNOWN prevent healthy. Idle disconnected transport is intentional.
- DB read outage skips source fetch; write outage retains ONE bounded batch with jitter retries,
  no new upstream collection/false acknowledgement. Actual durable state drives later recovery.
  Network failures retry on minute cycles behind a backoff gate. Permanent rejection requires
  operator intervention. Old unavailable expected minutes remain explicit/degraded. No disk
  spool/distributed bus/crash-safe queue; cold start cannot promise pre-anchor history.
- 5m/15m/1h/1d query SQL over native 1m: corrections propagate immediately, verified additive
  volume only. Larger-bucket complete requires all wall-clock minutes, conservative at closures.
- Full cycle traces: /providers and structured logs. Prometheus separates scheduler/fetch/
  symbol/parser/write/close-to-commit timings, corrections, backfills, gaps and expectations.
  Tick counters are not primary candle health. Unrelated tick providers remain unchanged.

## Verified evidence

- **180 passed Timescale; 177 passed +3 Timescale-only skips plain PostgreSQL**. Ruff, formatting
  (56 files), strict mypy (25 source files), diff whitespace checks pass. Normal tests offline;
  final targeted native-candle/reliability rerun: **86 passed**.
  existing Starlette/httpx deprecation remains. Failure injection covers network/transport,
  independent DB read/write, restart/different watermarks, horizons, sessions, integrity, rollups.
- Both modes: fresh base->head, base downgrade/re-upgrade, 0004->0003->0004 on disposable *_test
  DBs. Candle open=100.1234567890123456789 survived latter; hypertable only in Timescale mode.
  No downgrade/deletion of main data. Isolated mock app passed REST/latest/history/candles/
  health/actual WebSocket smoke then stopped.
- Longest observation **900.007s, 15 normal cycles, 255 new rows**: 17 per bucket 11:36-11:50 UTC.
  Four native corrections audited (including USOIL close, volume NULL); zero duplicates,
  malformed/forming rows. Raw ticks **26,216->26,216**. Detailed diagnosis/baseline comparison:
  docs/candle-hardening-verification.md.
- Normal p50/p95/max seconds: first **3.443/7.322/7.322**; last **6.754/11.716/11.716**;
  commit acknowledgement **7.122/12.142/12.142**; first-last **3.228/4.771/4.771**;
  DB write call **0.036/0.117/0.117**. First-receive tail did not improve; differing network
  windows/small sample limit comparison. Upstream internals unproven; DB not the main delay.
- Live app restart **75.733s offline**: all 17 recovered buckets11:52/11:53 UTC. Live DB restart
  **83.817s offline**: /health503 database=false, /providers200, all 17 recovered 11:54/11:55.
  Raw unchanged, older gaps kept health degraded. Artifacts/live-candle-outages.json ignored.
- Final deployed-build observer **180.009s**, startup +3 normal cycles, **51 new rows**,
  17 each at 12:04/12:05/12:06 UTC, raw 26,216, no observer/DB failure or duplicate/forming rows.
  Normal-only first3.792/4.409/4.409; last7.132/9.534/9.534;
  commit7.648/9.925/9.925 seconds. Seven metric-series resets reflect ONE deployment.
- XAUT 11:07 and 11:34 UTC absent from native history, one before deployment. Keep both scheduled
  expected gaps; sparse trade-based charts may omit no-trade minutes, not proven transport loss.
  Reconciliation [11:20,11:40) proved all 17 invariants: XAUT 19/20, other instruments 20/20.
- Final WebSocket smoke initially failed because the test stopped draining overlap/startup
  messages while sleeping; its full receive queue delayed ping control frames. Harness now
  drains while waiting for the boundary. **Rerun passed 12:09/12:10 UTC**: all 17 independent
  native OHLC -> DB -> REST comparisons and all 17 committed WebSocket candles each minute,
  raw 26,216 unchanged. Evidence: artifacts/candle-hardening-final-smoke.json.
- Ignored evidence: artifacts/candle-hardening-soak.json, live-candle-outages.json,
  hardening-reconciliation.json, candle-final-soak.json, candle-final-normal-summary.json,
  serial-candle-timings.json, pipelined-candle-timings.json. Earlier native verification
  remains in artifacts/native-candle-final-smoke.json (09:57/09:58 all 17 source/DB/REST/WS).
- Original row-rate comparison preserved: 556.9 raw ticks/min vs 17 candles/min, 96.947% fewer
  history rows across different windows; not a disk/compression claim. No tick data deleted.

## Continuation / limitations

Read-only scripts/reconcile_candles.py proves bounded completeness partitions;
scripts/soak_candles.py supports 1h/6h/24h/72h commands in docs. It polls local API/DB only,
counts startup separately and excludes startup from normal latency distributions. Live smoke
adds diagnostic source requests, without changing collector cadence. Test fixtures clear only
migrated disposable *_test databases; never use main/user DB.

Next: longer soak through session/weekend transitions; evidence-based sparse-bar and exceptional
calendar policy; backup/retention policy. Actual Internet disconnection **NOT TESTED** (transport
failure simulated); no multi-hour/day soak, complete/versioned exception calendar, upstream
revision IDs, unlimited backfill, durable queue or crash-safe loss guarantee. Guest protocol,
availability and source licensing remain experimental/unverified. Direct Binance ConnectError
was linked to local network; authenticated OANDA/Databento await credentials/entitlements.
Keep single worker and preserved historical ticks.
