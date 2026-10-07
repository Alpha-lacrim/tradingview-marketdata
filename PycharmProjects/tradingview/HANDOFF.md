# HANDOFF.md

## Current state - 2026-10-07

Experimental tradingview_guest remains native completed **1m candles**, same 17 instruments,
one normal UTC-minute cycle plus 2s grace, overlap **3**, recovery horizon **300** minutes.
No tick-mode reversion, new instruments/dependencies/infrastructure or migration changes.
Alembic head **0004**; native candle constraints/hypertable, latest/coverage/revisions,
raw historical ticks and tick CAGGs preserved. Larger intervals query corrected 1m rows.

Main Compose app localhost:8000; DB localhost:5432, retained volume tradingview_marketdata_pg.
Use compose.yaml + compose.tradingview.yaml, ONE worker/collector. Bare Compose selects mock.
App/DB are running. Health intentionally remains degraded for two older missing XAUT minutes
(11:07 / 11:34 UTC); do not erase coverage or fabricate bars to make health green.

Parent Git root C:/Users/Pouyan, main. Committed hardening baseline **5fdfe972**.
Retrieval scalability work is included in the commit containing this handoff. Scope Git to
this project. Preserve unrelated
untracked BATCHES_V1.md. Secrets/.env/artifacts/research remain ignored.

## Retrieval decisions / implementation map

- providers/tradingview_candles.py retains exact native parser/metadata validation and socket
  lifecycle. providers/tradingview_pipeline.py owns a SINGLE independent receiver and bounded
  sliding dispatcher. One series per temporary chart session on ONE socket; sessions removed
  per job, socket closed per cycle. No receiver waits for dispatcher cleanup/response waits.
- Unique IDs across batches/provider connection generations, socket identity checks, explicit
  symbol/series/turnaround validation. Separate per-request 15s deadlines preserve successful
  siblings. Shutdown joins reader/cancels all futures; no detached tasks or socket pool.
- TRADINGVIEW_CANDLE_CONCURRENCY defaults **2**, validated 1..12. Timeout/disconnect/protocol/
  HTTP429 symptoms decrease effective next-cycle window (floor1), never automatically raise it.
  Existing minute-cycle jitter/backoff and permanent access-rejection halt remain.
  **Current deployed process fell back to window1 after live timeouts.** Operator restart/config
  is required to increase again; no evidence currently justifies a higher production ceiling.
- Shared-session two-series controlled probe explicitly rejected with critical_error:
  **exceed limit of series in the session**. STOPPED, no workaround. Multiple chart sessions
  per socket work; that does not imply multiple series per chart session are allowed.
- CandleIngestion still one owner/minute. Default safety warning budget **48s = 60 - 2 - 10**;
  CANDLE_CYCLE_SAFETY_MARGIN_SECONDS configurable. Owned timer counts overrun once even during
  DB retry, then finish bounded work and schedule next FUTURE UTC target; no overlapping cycles.
  Historical recovery handles skipped targets. A request returns overlap/history in ONE series
  request per symbol (up to301 bars including forming allowance), not one request per bar.
- /providers adds candle_retrieval stats and complete_current_minute cycle marker. Request
  durations end at parser completion, excluding dispatcher cleanup. Metrics cover inflight,
  window, timeouts, ignored frames, socket failures, possible-throttle symptoms, overruns,
  first/median/p95/last native receipt and DB duration. Symptoms are not proof of throttling.
- Session tri-state EXPECTED/NOT_EXPECTED/UNKNOWN, correction audit and durable DB/network/
  restart reconciliation are unchanged. Any expected historical gap or UNKNOWN prevents
  healthy. DB read outage skips source; write outage retains ONE bounded batch, never false
  acknowledgement. Unsupported calendars/sparse native minutes remain explicit uncertainty.

## Evidence and tools

- docs/candle-retrieval-scalability.md and candle-retrieval-benchmarks.json contain the new
  measured matrix/constraints. docs/candle-hardening-verification.md preserves prior baseline
  and previous live DB/app restart evidence. Native semantics/all17 identities remain in
  docs/native-candle-verification.md and docs/tradingview.md (oil volume NULL, crypto base
  volume/OANDA tick count, OTC/reference products distinct from exchange futures).
- Full regression: **225 passed Timescale; 222 passed +3 Timescale-only skips plain PG**.
  Ruff, formatting (61 files), strict mypy (27 source files), diff whitespace pass. Offline
  tests exercise windows1/2/4/6/8/12 with17/34/64/100, interleaving, 99 successful siblings after
  one timeout, stale generations, reconnect/cancellation/future cleanup, owned overrun timer,
  429/auth-send disconnect, partial persistence and recovery, idempotent17/100/500-row batches.
  Existing recovery/correction/session/rollup/API suites pass. Starlette/httpx warning remains.
- **72 synthetic cycles**, three per size/window cell: zero missing/errors/disconnects.
  At window2,100 symbols: p50 **18.128s**, max **18.161s** (3 completed bars each).
  12 more synthetic full-horizon cycles:100 x 300 completed bars, p50 **21.745s**, max **22.499s**,
  peak additional traced Python allocation ~72.3MB. These do NOT model upstream limits/tails.
  100-instrument verdict **VERIFIED SYNTHETICALLY ONLY**; production local cap remains100.
- Persistence-only3 rounds each17/100/500: plain PG p50 **.055/.272/1.093s**;
  Timescale p50 **.058/.324/1.451s**, all inserted expected rows. Includes COPY/latest/coverage
  plus one transaction, excludes registration. Disposable benchmark DBs retained, simulated
  records only. Fresh Timescale benchmark DB migrated base->0004. Main migration/head/hypertable
  rechecked. No main downgrade/deletion, no new migration requiring rollback validation.
- Initial live serial-window1 probe13/17,4 timeouts,83.565s; independent-reader window2
  probe15/17,2 timeouts,35.040s. Escalation stopped; windows4/6/8/12 and pools NOT TESTED live.
  Window2 failed NZD/XAU first frames arrived .579/.720s after request, no completed target
  at15s deadlines. Waiting for native replies dominates; network/vendor cause is unproven.
- Final deployed-build **900.013s** observation: **15 normal cycles**,13 complete on first
  collection; window2 seven cycles (six complete), fallback1 eight (seven complete). Three
  isolated timeouts in two cycles; all three recent gaps subsequently backfilled. **255 new
  rows**, one NZD/USD close/volume correction; zero DB/observer failures, duplicate minute
  attempts, malformed/forming rows or overruns. Raw **26,216->26,216**, no deletes.
- Complete-cycle p50/p95/max seconds: first **2.846/3.789/3.789**; last
  **6.015/19.313/19.313**; commit **6.187/19.633/19.633**; DB write **.043/.074/.074**.
  Six successful window2 cycles alone: body **3.691/5.337/5.337**, first **2.789/3.749/3.749**,
  last **5.530/6.990/6.990**, commit **5.679/7.327/7.327**. ALL15 attempt body
  **4.425/24.306/24.306**. Conditional fast samples do not prove improved reliability or an
  optimum; no justification for raising ceiling2. Docs/JSON preserve window-separated data.
- Reconciliation [12:58,13:13) UTC: all17, **255 EXPECTED=255 persisted**,0 missing/UNKNOWN,
  every invariant holds. Global older XAUT gaps remain2. Corrected NZD5m/15m/1h/1d OHLC and
  additive volume rechecked via REST against stored1m; day is partial. Artifacts ignored:
  retrieval-final-soak.json, retrieval-reconciliation.json, retrieval-corrected-rollups.json.
- Stored-only smoke first FAILED on missing XAU for13:05 UTC. Subsequent retry PASSED all17
  WebSocket OHLC->DB->REST identities/values at **13:08/13:09 UTC**, raw unchanged. No additional
  guest requests: scripts/smoke_tradingview_candles.py --stored-only --minutes 2.
  Artifacts: retrieval-stored-smoke-failure.json and retrieval-stored-smoke-retry.json.
  Independent fresh native comparison NOT repeated.

## Continuation / limitations

scripts/benchmark_candle_retrieval.py: offline synthetic transport matrix/history; guarded
*_test-only simulated persistence with a unique run namespace; opt-in live diagnostics stop
on any incomplete cycle and require preceding concurrency candidates. Stop normal app before
standalone live diagnostics; preserve volume and resume for native historical recovery.

scripts/soak_candles.py polls LOCAL API/DB only; complete_cycle_latency excludes failed/partial/
coverage-only cycles, while all failures/cadence remain visible. Use docs commands for
1h/6h/24h/72h. scripts/reconcile_candles.py proves bounded EXPECTED partitions, not just counts.
Integration fixtures truncate disposable *_test data; never use main/user DB.

Next priority: establish sustained source/network stability at the current low window, then
measure window4 on the single normal collector only after stable predecessor cycles. Do not
infer live100 capacity from the synthetic matrix or try socket pools to evade source limits.
No statistically defensible new live concurrency optimum, live100/pool experiment, multi-hour
soak, Internet-disconnection injection, complete/versioned exceptional calendar, unlimited
history, durable spool or crash-safe guarantee. Current-task DB/restart/network failures are
verified deterministically; prior live DB/app restart tests are documented separately.
Unsupported guest protocol/availability/data-use rights remain experimental. Preserve raw
history and the single-worker boundary. Legitimate credentialed providers remain isolated.
