# Native candle retrieval scalability

This experiment preserves the existing 17 instruments, native completed 1m semantics,
three-minute correction overlap, 300-minute recovery horizon and one UTC-minute cycle.
No new source commands, production instruments, credentials or sockets were introduced.
The benchmark-only live universe and October 12 results are documented below.

## Protocol evidence and architecture

Before this change, one WebSocket carried at most two independent temporary chart sessions.
Each job sent `chart_create_session`, `resolve_symbol` (`symbol_1`) and `create_series`
(`s1`, turnaround `s1`, resolution `1`, bounded bar count). The receive loop routed by
chart session, but also awaited deletion and replacement request sends. A blocked send
therefore prevented the loop from reading already-available replies. There were no deliberate
per-symbol sleeps. Chart sessions were removed after each job and the socket closed per cycle.

The site's already-captured serializers and per-series handler call sites support client
series identifiers, but syntax alone does not establish guest capability. A controlled
two-series experiment on one chart session received `critical_error` with
**`exceed limit of series in the session`**. The experiment stopped immediately. Shared-session
multi-series is **BLOCKED BY UPSTREAM**; no workaround or repeated limit probe was attempted.
Captured evidence remains in ignored `artifacts/shared-session-probe.json`.

The implemented structure is:

```text
one minute scheduler / one collection owner
  → bounded sliding dispatcher (configured upper bound, default 2)
  → ONE guest socket / one series per temporary chart session
  → independently owned single receiver
  → route by unique chart-session + symbol + series/turnaround identity
  → complete individual futures / release capacity
  → one atomic COPY + UPSERT + latest/coverage/revisions transaction
```

Request IDs never repeat across batches or socket generations. A receiver checks socket
identity and generation before and after `recv`. Unmatched/stale and duplicate messages
cannot satisfy a new request. The receiver keeps reading while dispatch/session cleanup
awaits sends. Request latency ends at parse completion, excluding dispatcher cleanup waits.
Failures remain per-symbol; valid siblings survive a timeout or disconnect. A provider-wide
protocol/access rejection still halts the provider. Cancellation joins the receiver and
cancels outstanding futures; cleanup itself is bounded.

There is no socket pool, automatic increase algorithm or priority scheduler. None has been
justified by live evidence. Multiple **chart sessions** can coexist on one socket; multiple
**series in one session** were rejected. These are different capabilities.

## Initial live probes: escalation stopped

These were diagnostic cycles with the ordinary collector stopped, not extra normal polls.
Each is only one sample, and failed cycles are not used as successful p50/p95 measurements.

| Kind | Instruments | Strategy | Window | Sockets | Cycle seconds | Current bars | Timeouts | Result |
|---|---:|---|---:|---:|---:|---:|---:|---|
| LIVE | 17 | previous loop, configurable window | 1 | 1 | 83.565 | 13/17 | 4 | FAILED; escalation stopped |
| LIVE | 17 | independent receiver | 2 | 1 | 35.040 | 15/17 | 2 | FAILED; escalation stopped |
| LIVE | 2 | shared chart session | 2 series | 1 | not a cycle benchmark | — | — | explicit session-series limit |

First / median-instrument / last receipt after close were 8.064 / 30.142 / 84.863 seconds
for the first failed probe, and 4.802 / 10.344 / 36.363 for the second. CPU time was
0.172 / 0.094 seconds; additional traced Python peak 1,407,808 / 1,513,117 bytes. Both
returned source metadata quickly for some timed-out jobs but no complete chart response
within the existing 15-second request deadline. Network versus upstream internals were not
established. This is **not proof of throttling**, nor evidence that higher concurrency is safe.
For example, window-two NZD/USD and XAU/USD received their first relevant frames after
0.579 / 0.720 seconds, but still had no completed target bar at their 15.004 / 15.019-second
deadlines. The single receiver was active; these tails were waiting for native responses,
not PostgreSQL transactions. The fake blocked-cleanup test separately proves the old
application reading/dispatch coupling is removed; it does not prove it dominated live latency.
No per-instrument connection fanout was attempted. Raw artifacts:
`retrieval-initial-probes.json`, `retrieval-window2-probes.json` under ignored `artifacts/`.

## Previous task: normal-collector live observation

**LIVE: 900.013 seconds, 15 normal cycles, zero startup cycles or duplicate minute attempts.**
Seven cycles used window two (six complete, one partial); eight used the protective window
one (seven complete, one partial). There were three individual timeouts in two cycles,
three expected gaps opened and subsequently backfilled, one audited NZD/USD close/volume
correction, no DB/observer failures and no cycle overruns. These were ordinary collector
cycles, with no additional upstream benchmark traffic. No higher window was tested after
the low-window failures; the configured ceiling remained **two**, that process effective
window **one**. This is conservative retention, not proof of a new global optimum.

| Seconds, p50 / p95 / max | Supplied window-two baseline (15 complete) | New window two (6 complete of 7 attempts) | Whole observation (13 complete of 15, windows 2→1) |
|---|---|---|---|
| Cycle body through reconciliation | not supplied | 3.691 / 5.337 / 5.337 | 4.200 / 17.652 / 17.652 |
| Close → first native receipt | 3.443 / 7.322 / 7.322 | 2.789 / 3.749 / 3.749 | 2.846 / 3.789 / 3.789 |
| Close → median instrument receipt | not supplied | 4.459 / 5.393 / 5.393 | 4.492 / 7.924 / 7.924 |
| Close → last native receipt | 6.754 / 11.716 / 11.716 | 5.530 / 6.990 / 6.990 | 6.015 / 19.313 / 19.313 |
| Close → DB commit acknowledgement | 7.122 / 12.142 / 12.142 | 5.679 / 7.327 / 7.327 | 6.187 / 19.633 / 19.633 |
| DB write call | 0.036 / 0.117 / 0.117 | 0.052 / 0.074 / 0.074 | 0.043 / 0.074 / 0.074 |

For **all 15 attempts including partial failures**, cycle body p50/p95/max was
**4.425 / 24.306 / 24.306 seconds**. Successful-only quantiles must not conceal those failures.
Seven complete fallback-window-one cycles alone had cycle p50/p95/max
7.142 / 17.652 / 17.652 seconds. Small, sequential Internet samples and fallback selection
prevent a statistically convincing concurrency comparison. The successful window-two
median is lower than the supplied baseline; reliability did not improve enough to justify
raising concurrency, and the mixed-window last/commit tail is worse. PostgreSQL remains small.
There were no recorded protocol errors or unexpected socket disconnects during this final
observation; request timeouts caused two recovery/backoff transitions. This does not establish
the cause of earlier network/source instability or show higher windows are harmless.

Database evidence: **255 new rows**, raw ticks **26,216→26,216**, zero logical duplicates,
zero malformed/forming candles. Read-only reconciliation of [12:58,13:13) UTC verifies
17×15 = **255 EXPECTED / 255 persisted / 0 missing**, all 17 coverage invariants hold.
The two older XAUT gaps remain outside that range and still degrade global health. One
correction updated the existing row, not an additional logical candle. Its 5m/15m/1h/1d
REST OHLC and additive tick-count volume were compared with corrected stored 1m rows;
all passed (the current day is explicitly partial).

The stored-only smoke initially **FAILED** for missing XAU/USD at 13:05 UTC during the
source timeouts. A later **PASSED** run verified all 17 WebSocket OHLC values/identities
against DB and REST at consecutive buckets **13:08 / 13:09 UTC**, raw unchanged. This
observed the normal collector; it did not make independent fresh native requests. Failure
and retry artifacts are retained separately. No live DB shutdown or forced Internet outage
was repeated in this task; deterministic recovery regressions passed and earlier live
DB/app restart evidence remains in the hardening report.

Raw ignored artifacts: `retrieval-final-soak.json`, `retrieval-reconciliation.json`,
`retrieval-stored-smoke-failure.json`, `retrieval-stored-smoke-retry.json`,
`retrieval-corrected-rollups.json`. Compact final distributions and reconciliation totals
are checked into [measured results](candle-retrieval-benchmarks.json).

## Synthetic retrieval matrix

**SYNTHETIC ONLY.** Real dispatcher, receiver, parser and lifecycle; a bounded fake socket
produces independent replies after `0.35 × (0.7 + (index % 5) × 0.15)` seconds. This does
not model vendor scheduling, network stalls, rate limits or server connection limits.
Synthetic symbols exist only in the benchmark; production registry is unchanged.
Each cell is three cycles; nearest-rank p95 equals max. Timings include trace-allocation
overhead, routing/parsing and socket cleanup, but no database or two-second grace.

| Instruments | Strategy | Window | Sockets | p50 cycle s | p95/max s | Missing | Disconnects |
|---:|---|---:|---:|---:|---:|---:|---:|
| 17 | per-request sessions | 1 | 1 | 6.024 | 6.043 | 0 | 0 |
| 17 | per-request sessions | 2 | 1 | 3.089 | 3.102 | 0 | 0 |
| 17 | per-request sessions | 4 | 1 | 1.645 | 1.656 | 0 | 0 |
| 17 | per-request sessions | 6 | 1 | 1.121 | 1.139 | 0 | 0 |
| 17 | per-request sessions | 8 | 1 | 0.885 | 0.886 | 0 | 0 |
| 17 | per-request sessions | 12 | 1 | 0.729 | 0.739 | 0 | 0 |
| 34 | per-request sessions | 1 | 1 | 12.236 | 12.289 | 0 | 0 |
| 34 | per-request sessions | 2 | 1 | 6.240 | 6.274 | 0 | 0 |
| 34 | per-request sessions | 4 | 1 | 3.144 | 3.154 | 0 | 0 |
| 34 | per-request sessions | 6 | 1 | 2.194 | 2.200 | 0 | 0 |
| 34 | per-request sessions | 8 | 1 | 1.696 | 1.723 | 0 | 0 |
| 34 | per-request sessions | 12 | 1 | 1.262 | 1.268 | 0 | 0 |
| 64 | per-request sessions | 1 | 1 | 22.965 | 23.006 | 0 | 0 |
| 64 | per-request sessions | 2 | 1 | 11.649 | 11.681 | 0 | 0 |
| 64 | per-request sessions | 4 | 1 | 5.865 | 5.880 | 0 | 0 |
| 64 | per-request sessions | 6 | 1 | 4.005 | 4.058 | 0 | 0 |
| 64 | per-request sessions | 8 | 1 | 3.014 | 3.031 | 0 | 0 |
| 64 | per-request sessions | 12 | 1 | 2.171 | 2.187 | 0 | 0 |
| 100 | per-request sessions | 1 | 1 | 36.057 | 36.136 | 0 | 0 |
| 100 | per-request sessions | 2 | 1 | 18.128 | 18.161 | 0 | 0 |
| 100 | per-request sessions | 4 | 1 | 9.246 | 9.248 | 0 | 0 |
| 100 | per-request sessions | 6 | 1 | 6.248 | 6.252 | 0 | 0 |
| 100 | per-request sessions | 8 | 1 | 4.773 | 4.829 | 0 | 0 |
| 100 | per-request sessions | 12 | 1 | 3.259 | 3.261 | 0 | 0 |

All 72 cycles had zero request failures, missing current bars, protocol errors or disconnects.
First, median-instrument, p95-instrument, last receipt, CPU and additional Python allocation
quantiles for **every** cell are in [compact measured results](candle-retrieval-benchmarks.json).
Resource figures measure Python process time/tracemalloc, **not OS CPU utilization or RSS**.
Fixtures include a forming bar and preserve exact synthetic Decimal values; retrieval outputs
are never written as real observations.

## Startup/backfill scaling

One `create_series` per symbol returns the requested recent history, including all correction
overlap. It is not three requests for three overlapping minutes or 300 requests for backfill.
At the default horizon the request allows 301 bars, accepting at most 300 completed minutes.
Missing minutes outside the horizon remain explicit. Startup reads each instrument's own
watermark and uses the same bounded pipeline; no additional collector is spawned.

| SYNTHETIC instruments | Window | Completed bars returned | p50 cycle s | p95/max s |
|---:|---:|---:|---:|---:|
| 17 | 2 | 5,100 | 3.589 | 3.660 |
| 34 | 2 | 10,200 | 7.221 | 7.231 |
| 64 | 2 | 19,200 | 13.636 | 13.891 |
| 100 | 2 | 30,000 | 21.745 | 22.499 |

Three cycles each, zero missing responses. At 100, maximum additional Python traced peak
was approximately 72.3 MB, including all returned objects. This is a bounded worst-horizon
result, not a prolonged-process memory/leak measurement or a live backfill guarantee.

## Persistence benchmark

Windows Python to local Docker database; three newly inserted batches per size, no main
database writes/deletions. Each call includes COPY staging, candle/latest UPSERT, coverage
classification and a single transaction; registration is excluded. Records are explicitly
SIMULATED in disposable `_test` databases. This measures more than a bare insert.

| Mode | Rows | p50 write s | p95/max s |
|---|---:|---:|---:|
| Plain PostgreSQL | 17 | 0.055 | 0.097 |
| Plain PostgreSQL | 100 | 0.272 | 0.290 |
| Plain PostgreSQL | 500 | 1.093 | 1.337 |
| TimescaleDB | 17 | 0.058 | 0.111 |
| TimescaleDB | 100 | 0.324 | 0.343 |
| TimescaleDB | 500 | 1.451 | 1.871 |

All reported changed-row counts equal batch size. No one-transaction-per-candle path was added.
There is no evidence to justify database redesign for this task.

## Capacity, deadlines and overload policy

Default collection-body warning budget is **48 seconds = 60 − 2 grace − 10 margin**.
100 synthetic instruments at window two fit with approximately 29.8 seconds of retrieval-only
margin (18.161-second maximum). Full-horizon retrieval leaves 25.5 seconds (22.499-second max).
Database, progress reads and production network tails still consume budget. These timings
cannot establish a reliable live maximum or support extrapolation beyond the tested 100.
Local guest subscription/job cap remains 100; raising it requires further validated capacity.

The earlier scalability task established **100 instruments synthetically only**. The staged
October 12 experiment below extends live evidence through 64 instruments. **100 live:
NOT TESTED** after the 64-symbol completeness gate failed. No production symbols or socket
pool were added. Synthetic timings do not establish live capacity.

Per-job default deadline is 15 seconds, measured independently. Timeout frees its window
slot, retains successful siblings, opens/retains appropriate coverage gaps and reduces the
next-cycle window. Timeout/disconnect/protocol/HTTP429 symptoms are observable; possible
throttle metrics deliberately do not assert causation. Existing jitter/backoff gates later
minute cycles. Access rejection stops the provider. No new sockets or rapid retry loop are
used to circumvent limits. Decrease-only concurrency requires operator intervention to rise.

The 48-second budget warns rather than throwing away valid data. Crossing the next UTC
target increments `marketdata_candle_cycle_overrun_total` once, even during a stalled write.
The single owner finishes bounded work; intermediate targets are skipped, not overlapped.
It then recomputes the next future UTC target; historical reconciliation recovers skipped
minutes when available. A DB outage can intentionally delay collection while one bounded
batch retries; it cannot grow an unbounded queue or report an uncommitted bar persisted.

## Reproducible commands

Offline transport/parse/routing model (no credentials or Internet):

```powershell
uv run python scripts/benchmark_candle_retrieval.py --synthetic --windows 1 2 4 6 8 12 --counts 17 34 64 100 --rounds 3 --latency 0.35 --output artifacts/retrieval-synthetic-matrix.json
uv run python scripts/benchmark_candle_retrieval.py --synthetic --windows 2 --counts 17 34 64 100 --bars 301 --rounds 3 --latency 0.35 --output artifacts/retrieval-synthetic-backfill.json
# Set TEST_DATABASE_URL to a separately migrated disposable database ending _test.
uv run python scripts/benchmark_candle_retrieval.py --persistence --rounds 3 --output artifacts/candle-persistence.json
```

Persistence runs use a unique simulated namespace; repeating a benchmark measures inserts,
not accidentally unchanged old rows. Data is retained in the disposable benchmark database.
Never point integration fixtures at a user database: fixtures truncate their test data.

Read-only observation of the normal collector, with no extra upstream requests:

```powershell
uv run python scripts/soak_candles.py --seconds 900 --output artifacts/retrieval-soak.json
uv run python scripts/smoke_tradingview_candles.py --stored-only --minutes 2 --output artifacts/retrieval-stored-smoke.json
```

Use `complete_cycle_latency` and `complete_normal_cycles` for all-17 successful-cycle
comparisons, alongside failure/coverage-only cycle counts. An empty backoff cycle is not a
two-second success. Full-cycle traces are in `/providers`; Prometheus stage histograms and
bounded per-symbol request timings separate dispatch/receive/parser/DB durations. Soak
cadence counters count normal/startup separately and detect duplicate normal minute attempts.
1h/6h/24h/72h commands remain in [operations](tradingview.md#timing-metrics-and-soak-observation).

Optional diagnostic live benchmark, only after source/network stability is established:

```powershell
# Stop only app, preserve DB/volume; avoid running two collectors against the same source.
docker compose stop app
uv run python scripts/benchmark_candle_retrieval.py --live --windows 2 4 --rounds 3 --output artifacts/retrieval-live.json
docker compose -f compose.yaml -f compose.tradingview.yaml up -d app
```

This issues at most one diagnostic cycle per future UTC minute plus grace, requests only the
current 17 symbols, and **stops immediately** on any timeout/missing/error. Higher candidates
require their predecessor in the command. It does not persist; the resumed app reconciles
the short observation outage using native history. It is not a production soak. A production
window change should instead observe the single normal collector, retain raw counts/coverage
and restore the previous setting on instability. No automatic concurrent source experiments.

## Staged live capacity harness

`scripts/benchmark_live_capacity.py` measures live retrieval without a database writer.
It never registers candidates or writes production market history. The permanent registry
remains `config/instruments.tradingview.json`. The separate
`config/benchmarks/tradingview_candidates.json` contains **unverified candidates**, not
enabled feeds: public symbol-page evidence is recorded per entry; real-time status remains
unknown until the existing native parser verifies identity, semantics and completion mode.

Stop the ordinary app first; leave PostgreSQL and its volume intact. Run exactly one harness:

```powershell
docker compose -f compose.yaml -f compose.tradingview.yaml stop app
uv run python scripts/benchmark_live_capacity.py --collector-stopped --candidates config/benchmarks/tradingview_candidates.json --counts 17 34 64 100 --baseline-cycles 3 --cycles 10 --output artifacts/live-capacity.json
# Always restore the ordinary collector, including after interruption or a failed gate.
docker compose -f compose.yaml -f compose.tradingview.yaml up -d app
```

Stages must follow a prefix of 17/34/64/100. Every verification batch and capacity cycle
waits for a future UTC minute plus grace; no concurrent collector or extra intraminute retry.
The same provider retains its decrease-only effective window across cycles and stages,
ceiling two, one socket. Three completed contiguous native bars must independently verify
each addition before admission. Verification batches contain at most 17 candidates and
are reported separately from capacity cycles. Invalid, delayed, sparse or incomplete
verification stops escalation; no flat bars or substituted products are manufactured.

The production baseline includes XAUT. Expanded sets omit **only** OKX:XAUTUSDT because
its sparse native minutes are already known, and replace it with another verified symbol.
Baseline absence is considered interpretable only when its request explicitly completes
without the target bar; an XAUT timeout is still a failed request. Sparse absences remain
in immediate/eventual counts and unresolved records. They are never called recovered.

The conservative escalation gate requires at least 90% immediately complete cycles,
all expected primary minutes eventually returned, p95 duration below **38s**, no disconnect,
protocol/connection failure or skipped target. The 48s budget and extra 10s gate margin are
reported independently. Multiple overload symptoms in a cycle stop the experiment. A
failed gate alone does not establish an architectural capacity failure: inspect source
stability, source-completed missing bars and request diagnostics first. Summaries include
failed attempts, not just the successful tail. Cycles are sequential; an overrun realigns
to a future UTC target and the next historical-series request covers skipped buckets.

One request per symbol retrieves overlap and recoverable missing history together, bounded
by the existing horizon. The in-memory reconciliation set proves which attempted/skipped
minutes returned eventually; unresolved minutes remain explicit at termination. It does
not claim source output for every wall-clock minute or unlimited historical recovery.

Per-request diagnostics include socket generation, chart session, request-send timestamp,
deadline, first metadata, last socket frame at completion, native availability, parser
completion, verified source metadata and completion status. Lifecycle statistics count
client session creation/removal sends and pending futures; deletion sends have no observed
server acknowledgement. Closing the owned socket bounds any uncertain server cleanup.
CPU is process CPU time; memory is traced Python allocation, **not total process RSS**.
The checkpoint itself deliberately retains bounded prior-cycle evidence, so rising traced
allocation alone is not proof of a transport leak. Receiver/task/future/session counts are
checked separately. No socket pool or higher concurrency is introduced by this harness.

A pre-request handshake timeout can have **one** controlled retry window:

```powershell
uv run python scripts/benchmark_live_capacity.py --collector-stopped --candidates config/benchmarks/tradingview_candidates.json --counts 17 34 64 100 --baseline-cycles 3 --cycles 10 --resume artifacts/live-capacity.json --output artifacts/live-capacity-retry.json
```

This preserves the original failed window inside the new result, retains its effective
window, rechecks the production baseline, reuses only independently verified identities
and carries unresolved targeted minutes into historical requests. It requires a checkpoint
within one hour whose last failure is a timeout **before any request timing exists**.
An access/protocol rejection, per-symbol timeout, stale checkpoint or second retry is
rejected. A successful retry does not erase the original failure or establish its cause.
Public symbol-page placeholders are not used as evidence of market-open/realtime status.

The full offline/integration suites pass **233 tests Timescale / 230 plain PostgreSQL plus
three Timescale-only skips**. They cover windows 1/2/4/6/8/12, 100-symbol routing, out-of-order
responses, a slow symbol, isolated missing responses with 99 successes, stale generations,
duplicate/extra frames, wrong series, reconnect/shutdown, no waiter/receiver leaks, owned
overrun timer cancellation, 429/auth-send disconnect backoff, partial persistence and next-cycle
recovery, correction overlap and idempotent batches of 17/100/500. Existing candle/session/
outage/rollup/API tests remain in the full suite. Existing Starlette/httpx deprecation remains.

No schema/migration or native candle integrity constraint changed. Existing raw ticks and
historical native data remain. The protocol is unsupported and its available history/session
semantics and data-use rights remain experimental. Longer soaks, adverse-network sustained
capacity, licensed high-count upstream coverage and a validated larger universe would be
required before claiming reliable 100+ live capacity. Socket pools are not an assumed fix.

## Staged LIVE results: October 12, 2026

All times below are UTC. One socket, concurrency ceiling **2**, effective window **2**
throughout every established connection. Retrieval only; no production DB writes. Duration
includes connection setup, dispatch, receipt/parsing and cleanup, excluding the grace wait.
Close-to-parsed timings include the two-second grace. Quantiles use nearest rank; with these
small samples p95 equals max. Failed attempts remain in duration/completeness denominators.
[Compact evidence and exact verified identities](candle-live-capacity-benchmarks.json) retain
all stage summaries, resource ranges, source metadata and unresolved buckets.

| Live instruments/window | UTC collection window | Cycles | Immediately complete cycles | p50 duration s | p95/max s | Request timeouts | Eventual candles | Overruns |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 17 initial | 06:48:02–06:50:11 | 3 | 3/3 | 9.448 | 9.599 | 0 | 51/51 | 0 |
| 34 initial | 06:53:02–06:59:12 | 7 | 6/7 | 21.874 | 29.861 | 0 | 238/238 after retry | 0 |
| 17 retry control | 07:03:02–07:05:11 | 3 | 3/3 | 6.887 | 8.874 | 0 | 51/51 | 0 |
| 34 controlled retry | 07:06:02–07:15:10 | 10 | 10/10 | 12.510 | 17.014 | 0 | 340/340 | 0 |
| 64 | 07:18:02–07:27:25 | 10 | 1/10 | 22.371 | 35.562 | 0 | 631/640 | 0 |

Every attempt finished below 48 seconds, with no skipped collection targets. The initial
34 window stopped on **one opening-handshake TimeoutError**, before chart requests were sent;
its failed attempt returned zero candles. There were no established-socket disconnects,
server close codes, protocol errors or recorded possible-throttle symptoms. This does not
establish whether the handshake failure was network- or server-originated. One controlled
retry retained that failure, rechecked 17, then recovered all 34 missing target candles.
Across **both** 34 windows: 16/17 complete cycles (94.12%), 544/578 immediate candles (94.12%),
578/578 eventual candles (100%). The clean ten-cycle retry is **VERIFIED LIVE**, not a claim
that the entire 17-attempt record was flawless.

Additional symbols were selected from public TradingView symbol pages, then each independently
returned **three contiguous completed native 1m bars**, exact source identity and accepted
streaming/realtime completion metadata before admission. Page placeholders were not delivery
status evidence. Forty-eight additions were verified: 41 Binance spot pairs and seven OANDA
OTC FX pairs. The 84-entry candidate file itself remains unverified configuration; its remaining
36 candidates were not native-tested. Expanded universes exclude known-sparse OKX XAUT;
production 17 and both baseline controls retain it. OTC metals/FX and TVC oil remain distinct
from exchange-traded futures; oil volume stays null.

At 64, **all 640 requests completed successfully** with valid native series, correct symbol
routing, Decimal OHLC, native basis and closed timestamps. Immediate/eventual target-minute
completeness was **631/640 = 98.59%**, but only one cycle had all 64 target bars. Nine bars
remained absent on repeated historical retrieval: RUNEUSDT four, TRUMPUSDT two, VETUSDT two,
GRTUSDT one. Their initial three-bar verification had passed; longer observation exposed
source availability/sparsity. Whether the exchange produced no trade or the vendor omitted
a bar is unproven. These are **source-completed omissions**, not timeouts or proof that an
existing bar could not be transported. No flat candles were fabricated.

Thus 64 is **PARTIALLY VERIFIED for coverage / availability-confounded**, with successful
transport and a **12.438s p95 safety margin**. The harness's stricter completeness gate returned
UNSTABLE / INCONCLUSIVE, so escalation stopped. **100-instrument verdict: NOT TESTED.**
No 100-symbol or two-socket live test was attempted. Extra sockets would not resolve bars
absent from the source series; no evidence justified increasing concurrency or socket count.

| Close → parsed seconds, p50 / p95 / max | 17 initial | 34 retry | 64 |
|---|---|---|---|
| First target candle | 4.529 / 5.895 / 5.895 | 3.971 / 5.013 / 5.013 | 4.243 / 7.554 / 7.554 |
| Median instrument | 7.489 / 8.188 / 8.188 | 8.680 / 11.659 / 11.659 | 13.686 / 24.288 / 24.288 |
| p95 instrument | 10.851 / 11.057 / 11.057 | 13.595 / 18.145 / 18.145 | 23.514 / 36.268 / 36.268 |
| Last target candle | 10.851 / 11.057 / 11.057 | 14.079 / 18.458 / 18.458 | 23.902 / 36.993 / 36.993 |

These parsed-target timings are not DB commit measurements or proof of exchange-audited
latency. One request per symbol fetched the three-minute overlap and any pending recovery
history together. Recovery of the failed 34 cycle did not prevent the following clean stage.
The nine 64-source omissions remained explicit rather than being counted as recovered.
Largest live universe tested: **64**. Largest clean ten-cycle immediate-complete universe:
**34**. Largest universe with 100% eventual target coverage: **34**. Largest tested request
universe below 48s at p95: **64**; this timing result does not imply complete minute coverage.
No measured live capacity above 64 or sustainable long-soak maximum is claimed.

### Lifecycle, persistence and restoration

Every completed capacity cycle ended with zero pending futures, zero receiver tasks and
one remaining owner task. Successful client session creation/removal sends matched the
instrument count, with zero current sessions; sockets were closed. This verifies client
ownership, not unobserved server deletion acknowledgements. At 64, process CPU was
0.375–0.734s per cycle, current traced Python allocation 5.08–8.32MB and additional peak
1.35–1.47MB. The checkpoint retains bounded per-cycle evidence, so memory grew with stored
diagnostics. OS CPU utilization/RSS and long-term memory stability were **NOT TESTED**.

A subsequent isolated native persistence test at **07:36:02–07:36:11 UTC** used the largest
fully successful stage, 34. All 34 target candles committed to `marketdata_benchmark_ts_test`
under a unique benchmark provider namespace: retrieval **9.478s**, batched DB write **0.336s**,
close→commit **12.002s**. One sample, not persistence percentiles. Production history was
untouched. To reproduce before restoring the ordinary app:

```powershell
# TEST_DATABASE_URL must name a migrated disposable database ending _test.
uv run python scripts/benchmark_native_persistence.py --collector-stopped --capacity-results artifacts/live-capacity-retry.json --output artifacts/native-persistence.json
```

The normal Compose collector was rebuilt/restored with the original 17 and ceiling two.
Raw ticks remained **26,216**, zero benchmark mappings in the main DB, zero duplicate or
malformed/forming rows; migration head 0004 and candle hypertable remain intact. Restart
reconciliation of **[06:47,07:47) UTC** returned 1,009/1,020 expected candles, all 11 missing
minutes XAUT, all coverage invariants holding. No data was deleted. Older five-day outage
gaps exceed the 300-minute horizon and correctly keep global health degraded.

Stored-only REST/WebSocket smoke **FAILED** on missing XAUT at 07:49 UTC; DB had 16 candles
then and 17 at 07:48, 07:50 and 07:47. This is retained as a failure, not an all-17 smoke pass.
REST/provider APIs remained operational. Full regressions: **233 Timescale passed; 230 plain
PostgreSQL passed, three Timescale-only skips**. Ruff, format (64 files), strict mypy (27
modules) passed. No migration/schema or concurrency behavior changed; additions are benchmark
tools, source identity evidence, request/lifecycle diagnostics and tests. Existing warning:
Starlette/httpx TestClient deprecation.

Next useful experiment: build a separately reverified dense universe excluding the four
newly observed sparse sources, repeat the 64 gate, then consider 100 only if coverage and
timing remain stable. Do not replace permanent instruments, fabricate omitted minutes,
increase polling frequency or infer live feasibility from synthetic results.
