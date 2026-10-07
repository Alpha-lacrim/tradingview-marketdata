# Native candle retrieval scalability — 2026-10-07

This experiment preserves the existing 17 instruments, native completed 1m semantics,
three-minute correction overlap, 300-minute recovery horizon and one UTC-minute cycle.
No new source commands, instruments, credentials or sockets were introduced.

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

## Final normal-collector live observation

**LIVE: 900.013 seconds, 15 normal cycles, zero startup cycles or duplicate minute attempts.**
Seven cycles used window two (six complete, one partial); eight used the protective window
one (seven complete, one partial). There were three individual timeouts in two cycles,
three expected gaps opened and subsequently backfilled, one audited NZD/USD close/volume
correction, no DB/observer failures and no cycle overruns. These were ordinary collector
cycles, with no additional upstream benchmark traffic. No higher window was tested after
the low-window failures; the configured ceiling remains **two**, current process effective
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

**100-instrument feasibility: VERIFIED SYNTHETICALLY ONLY.** No 34/64/100-instrument live
experiment or socket pool was run, and no unsupported production instruments were added.
Upstream/network variability is the unresolved live limiting factor. The synthetic maximum
tested is 100; the real maximum tested is 17, with intermittent failed low-window probes.

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

## Validation and remaining limits

The full offline/integration suites pass **225 tests Timescale / 222 plain PostgreSQL plus
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
