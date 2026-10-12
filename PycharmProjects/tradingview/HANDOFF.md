# HANDOFF.md

## Current state - 2026-10-12

Experimental tradingview_guest stores native completed 1m candles for the permanent 17
instruments. One UTC-minute cycle plus 2s grace, correction overlap 3, recovery horizon 300
minutes. One guest socket per cycle, one temporary chart session/series per symbol,
independent receiver, bounded sliding concurrency ceiling 2; decrease-only overload fallback
floor 1. No authenticated access, browser automation, socket pool or production universe change.

Compose app localhost:8000 and DB localhost:5432 are RUNNING after the live capacity test. Compose marks the app unhealthy because durable
coverage gaps keep /health degraded; the API and collector are operational.
Use compose.yaml + compose.tradingview.yaml, ONE worker/collector. Bare Compose selects mock.
Volume tradingview_marketdata_pg preserved. Native market_candles remains a Timescale
hypertable, head 0004; constraints/latest/coverage/revisions/tick aggregates unchanged.
Raw tradingview_guest ticks remain 26,216. No benchmark mappings entered the main DB.

Five-day environment/process outage leaves roughly 74k older expected unresolved minutes
outside recovery horizon. Global health correctly remains degraded; do not erase coverage
or fabricate bars. Recent pause reconciliation [06:47,07:47) UTC:1,009 persisted of1,020
expected,11 absent XAUT minutes, all coverage invariants hold. Production is collecting again.

Git root C:/Users/Pouyan, main. Retrieval baseline commit 34dfa53a (hardening 5fdfe972).
Private GitHub review repository created: https://github.com/Alpha-lacrim/tradingview-marketdata.
Source publication is pending the reviewed commit/push. Existing four commits and nested
PycharmProjects/tradingview layout are retained. Root README/CI expose the project entry point;
root ignore rules prevent unrelated home-directory files from being staged. Scope source
commands to this project. BATCHES_V1.md remains local and unchanged; credentials, database
history/volumes, dumps, logs/caches and raw artifacts/research are excluded. Sanitized fixtures
and benchmark summaries are included. Gitleaks 8.30.1 checked all four historical commits and
the publication snapshot with no leaks; DSNs contain only development defaults/env placeholders.

## Current live capacity result

100-instrument LIVE verdict: **NOT TESTED**. Largest live request universe 64; largest clean
10-cycle immediate-complete universe 34. One socket/ceiling 2 was sufficient for all 64 chart
responses, but source completeness gate prevented escalation. No higher window/pool tested.

October12 UTC windows:
-17 controls:06:48-06:50 and07:03-07:05,3 cycles each, all51/51 candles per window.
-34 initial:06:53-06:59,6 complete of7 attempts; final opening-handshake timeout BEFORE
 chart requests. Controlled retry preserved failure and recovered all 34 missing minutes.
-34 retry:07:06-07:15,10/10 immediately complete,340/340; duration p50/p95/max
 12.510/17.014/17.014s. Both34 windows:544/578 immediate94.12%,578/578 eventual100%.
-64:07:18-07:27,10 cycles,640/640 valid native-series requests;631/640 target bars,
 only1/10 all-complete cycles. Nine omitted buckets remained absent on history retries:
 RUNE4,TRUMP2,VET2,GRT1. Initial three-bar verification had passed. Source sparsity/publication
 cause unproven; NOT a demonstrated transport capacity failure. Duration22.371/35.562/35.562s,
 p95 margin12.438s to48s. 100 escalation stopped, no unsupported reliability claim.
-All stages:0 individual request timeouts,0 established disconnect/protocol/throttle symptoms,
 no overruns/skipped targets; effective window 2. One handshake timeout retained separately.
-Client sessions created/removed match; pending futures/receiver tasks 0 after cycles, owner 1.
 Counts are client sends, not server acknowledgements. Memory includes retained checkpoint
 evidence; OS RSS/long-term memory stability not measured.
-Isolated native 34 persistence07:36:34 rows, retrieval9.478s, DBwrite0.336s,
 close-to-commit12.002s; marketdata_benchmark_ts_test unique provider namespace, no main writes.
-Production restored/rebuilt. Stored-only smoke FAILED on XAUT at 07:49 (16bars); adjacent
 07:47/07:48/07:50 DB buckets17. No duplicate/malformed/forming candles. REST/providers200.

Exact windows, verified identities/status/session evidence, timing/resource distributions
and omissions: docs/candle-live-capacity-benchmarks.json and docs/candle-retrieval-scalability.md.
Raw ignored artifacts:live-capacity-oct12-staged.json,live-capacity-oct12-retry.json (retains
prior_window),live-capacity-native-persistence.json,live-capacity-restored-reconciliation.json.
Earlier Oct7 interrupted/production samples remain separate, not pooled as October12 evidence.

## Implementation and tools

providers/tradingview_candles.py owns native parsing/metadata/socket lifecycle;
tradingview_pipeline.py owns dispatch/demux/deadlines. New additive request diagnostics:
socket generation/session/send/deadline/first metadata/last frame/status/source info and
client lifecycle counts. No protocol, persistence schema or concurrency-policy change.
Shared-session multiple series were previously rejected: "exceed limit of series in the
session". No workaround. Correction/history is ONE bounded series request per symbol,
up to 301 bars including forming allowance. 48s body warning budget; cycles never overlap.

scripts/benchmark_live_capacity.py: retrieval-only staged 17/34/64/100, one futureminute
per verification/capacity cycle. STOP normal app first; leave DB/volume. Gate requires>=90%
complete cycles, all primary targets eventually present,p95<38s,no disconnect/protocol
failure/skipped targets. Resume permits ONE recent pre-request handshake-timeout retry,
retains prior evidence and missing targets; does not retry access/per-symbol errors.
config/benchmarks/tradingview_candidates.json has84 UNVERIFIED candidates, not production
feeds.48 additions verified live; expanded set excludes known sparse XAUT, production retains.

scripts/benchmark_native_persistence.py uses largest passed stage from a fresh checkpoint,
TEST_DATABASE_URL ending_test only, unique namespace; never main DB. Stop normal collector
before use. Always restore with:
docker compose -f compose.yaml -f compose.tradingview.yaml up -d --build app

scripts/soak_candles.py observes LOCAL API/DB, no extra source requests.
scripts/smoke_tradingview_candles.py --stored-only observes committed WS/DB/REST.
scripts/reconcile_candles.py performs read-only bounded historical tri-state reconciliation.
Earlier synthetic17/34/64/100 and history/persistence matrices are in scalability docs;
100 synthetic atwindow 2~18.1s is NOT live evidence. Prior actual DB/restart failure tests
and native all 17 semantics remain in docs/candle-hardening-verification.md,
docs/native-candle-verification.md and docs/tradingview.md.

## Validation and next actions

Final full suites:233 passed Timescale;230 passed+3 Timescale-only skips plain PostgreSQL.
Ruff, format 64files, strict mypy 27modules pass. Existing Starlette/httpx warning remains.
Tests cover demux/interleaving/generation/timeouts/cleanup, session/correction/recovery/API,
100+ batched persistence, benchmark escalation/resume guards and test DB isolation.
No new migration; mainhead 0004/hypertable rechecked. No user data deletion/downgrade.

Next priority: a benchmark-only dense universe excluding newly sparse RUNE/TRUMP/VET/GRT,
independently reverify sources, repeat 64 gate, then 100 only if stable. Preserve current 17
and ceiling 2. No claim of reliable 100 live, long soak, socket-pool optimum, complete exceptional
calendar, unlimited history or crash-safe spool. Unsupported guest protocol/source availability
and data-use rights remain experimental; legitimate credentialed providers stay isolated.
