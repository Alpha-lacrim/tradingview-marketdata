# HANDOFF.md

## Current state - 2026-10-12

The original deployed experimental tradingview_guest application still collects native
completed 1m candles for the permanent 17 instruments. UTC-minute owner + 2s grace,
correction overlap 3, recovery horizon 300, one socket/window 2. No production registry,
schema, credentials, deployment or benchmark history changes were made in this session.
Compose app localhost:8000 and DB localhost:5432 remain running. Old roughly 74k expected
coverage gaps from a five-day outage lie outside recovery and keep health degraded; do
not erase or fabricate them. Timescale native candle hypertable/head 0004, guest raw ticks
26,216, no benchmark mappings, no duplicate native logical candles at verification.

## Retained connection-pool implementation

New providers/candle_pool.py is a CandleProvider capability behind the SAME CandleIngestion
scheduler/persistence transaction. Sorted canonical round robin fixes unique ownership;
workers have independent socket, decoder, receiver, generation, sessions, futures and
symbol health. Shared request budget bounds unfinished requests; local windows separately
bound sessions. Completed responses release shared permits before session cleanup, avoiding
capacity deadlocks when global concurrency is smaller than summed worker windows.
Worker tasks publish partial results incrementally; pool checks owner/source/window identities.

PoolLimits declares integration-verified connection/local/global ceilings, never infers
permissions from observed access. Defaults: connections1, worker_capacity100, max_instruments100,
global_concurrency2, deadline48s, close3s; existing worker window2 and request timeout15s.
Application validation maxima are32 workers/4096 instruments/128 global requests.
These fields apply when a permitted integration constructs the pool; the existing app factory
still constructs the one-socket provider. Guest multisocket activation/job-limit expansion is
rejected before network: tradingview_live_pool_permissions_unverified. Both live local100
safeguards remain. No documented numerical guest socket permission was established; official
TradingView policy/API references and the BLOCKED live boundary are in the pool document.

One connection attempt per nonempty worker/cycle; ordinary failures back off independently
across later minutes. Successful peers continue and retain candles. HTTP429 gates the whole
provider and decreases its window, never grows connections. Permanent access rejection or
uncertain close cleanup blocks replacements. Cancellation joins worker/receiver/acquisition
and emitter tasks. Cooperative deadline includes handshake/retrieval, then bounded I/O cleanup;
CPU/JSON/OS stalls prevent a hard real-time return guarantee. Pipeline now yields every32
parsed historical candles. UTC alignment, native Decimal OHLC/source/completion, EXPECTED /
NOT_EXPECTED / UNKNOWN, bounded recovery, idempotent writes and rollups are unchanged.

New low-cardinality connected-worker gauge; bounded health diagnostics report ownership,
worker phases/backoff/errors/resources, request timings, pool throughput and slowest worker.
Worker subset gauges cannot overwrite pool-wide coverage. No distributed infrastructure.

## Measured evidence

See docs/candle-connection-pool.md and docs/candle-pool-benchmarks.json for full configuration,
timing/resource/availability summaries and reproducible commands. Raw artifacts/pool-*.json
remain ignored. New synthetic workers have no network connector, require simulated mappings
and label all native-shaped bars SIMULATED. No production benchmark writes.

SYNTHETIC VERIFIED final independent-delay matrix:64/128/256/512,54 cycles,
13,824/13,824 actual targets, all resources cleaned. Three cycles/configuration, p95=max,
not sustained production percentiles. At512, p50/max seconds:
- original single/window2:27.652/27.786;
- single/window8:7.359/7.370;
- four/window2/global8:8.112/8.361;
- eight/window2/global16:5.457/5.465;
- 31/window2/global32 (~17 instruments/worker):5.891/6.083.
Equal-concurrency single beats four workers.31 workers cost5.250s CPU p95 vs2.953s for8.
No support for fixing partitions at17 or changing the production default.
Shared-source-capacity-two model at256: single/four/eight p50 16.777/17.438/18.447s;
extra sockets do not overcome modeled provider capacity. No inference about real provider.
Slower0.35s profile:256 single/window2 p95 46.418s leaves only1.582s retrieval margin,
insufficient proof of full minute collection/commit. These short-response profiles preceded
history cancellation fix; final principal/history matrices were rerun afterward.

Maximum-history256x300 completed bars/cycle: initial PARTIALLY VERIFIED cycles up to63.049s
exposed CPU cancellation starvation, retained in pool-history-before-cancellation-fix.json.
After cooperative parsing + per-bar simulated labeling, six cycles all256 targets:
single/window8 p50/max30.547/37.673s; eight/window2/global16 20.735/22.702s.
CPU p95 35.281/22.172s, extra traced peak182.5/184.8MiB; memory/CPU remain bottlenecks.
512 maximum-history, integrated full-history persistence and long memory soak NOT TESTED.

At128, fixed source omissions yielded128 valid responses but126 targets;378/384 immediate
AND eventual after bounded history per architecture. PARTIALLY VERIFIED coverage; transport
valid, no fabricated recovery. Transient slow request with2s deadline retained127 targets,
then recovered all128 on next simulated cycle (381/384 immediate,384/384 eventual).
Integration test failed one16-instrument handshake worker, stored112 healthy results with
UNKNOWN failed coverage, next minute recovered all256 two-minute candles; replay writes0.
Isolated SIMULATED persistence17/100/500 on marketdata_benchmark_ts_test,3 rounds,
p50/max0.059/0.109,0.243/0.284,1.242/1.620s, unique namespace only.

## Live boundary and original 17 observation

New live requests: NONE. Existing deployed original image observed via local stored-only
WS/REST/DB:08:53 UTC17/17 matching candles;08:54 UTC16/17, XAUT missing despite streaming
successful native series and zero transport failures. Smoke exited1; failure retained, not
called complete. Subsequent snapshot17 current. Source changes were NOT deployed/live-tested.

Prior LIVE evidence remains in docs/candle-live-capacity-benchmarks.json and scalability doc:
34 instruments10/10 complete cycles, all578 eventual across windows including one handshake
failure;64 instruments640/640 responses but631/640 native targets, nine persistent omissions
RUNE/TRUMP/VET/GRT. Largest live request universe64; clean ten-cycle universe34.
The earlier source-completeness gate stopped100. No new permission follows from prior tests.

## Validation and repository state

Final full suites:270 passed TimescaleDB (50.01s);267 passed+3 expected skips ordinary
PostgreSQL (49.97s). Ruff lint/format pass69 Python files; strict mypy pass29 source files;
git diff --check pass. Existing Starlette/httpx deprecation warning remains. Tests cover
ownership/parallelism/foreign routing/generations/failures/recovery/429/cancellation/global
capacity/deadlines/stalled cleanup,512 routing,128 persistence and single-socket compatibility.
No migration added; no production tables cleared/downgraded. Original17 registry preserved.

Git root C:/Users/Pouyan, main; scope commands to PycharmProjects/tradingview. Initial project
state was clean. This snapshot includes the pool implementation, regression tests and sanitized
synthetic benchmark evidence. Publication destination is the private repository
https://github.com/Alpha-lacrim/tradingview-marketdata, origin/main. Verify local/remote SHA
for publication state. Prior baseline34dfa53a (hardening5fdfe972) remains in history.
BATCHES_V1.md, raw ignored benchmark artifacts and unrelated home files are excluded.
Staged Gitleaks scan found no secrets. Publication does not deploy the application or enable
live pooling.

## Next recommended actions

Keep original17/single/window2. Obtain a documented entitled provider integration and its
account-specific limits before wiring pool activation into production or live benchmarking.
Use same verified dense identities for fair permitted live comparisons. Prefer a small bounded
worker count; do not assume17 is optimal or multiply connections after throttling. Run longer
controlled fault/memory soaks and integrated full-history persistence before rollout. Existing
old coverage gaps and exceptional-calendar limitations remain unresolved; preserve tri-state
source availability. One application worker/collector only; use compose.yaml +
compose.tradingview.yaml for the original guest app, bare Compose chooses mock. The current
app image is unchanged; do not describe offline source changes as deployed.
