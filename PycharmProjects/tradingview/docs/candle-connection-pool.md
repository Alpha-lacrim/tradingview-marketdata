# Configurable candle connection pool

Implementation status: **SYNTHETIC VERIFIED**.
Separate operator-authorized live experiments now use the same pool and actual persistence;
see [live end-to-end evidence](candle-live-end-to-end.md). Numerical upstream allowances remain
unverified; experimental bounds are local safeguards.
The production registry remains the original 17 and the application factory continues
to select `TradingViewCandleProvider` with one socket and a request window of two.

## Architecture and ownership

`providers/candle_pool.py` implements `CandleProvider`, so the existing **single**
`CandleIngestion` owns UTC-minute scheduling, correction overlap, bounded historical
recovery and the shared atomic persistence transaction. Workers never run schedulers
or write to the database. FastAPI and migrations are unchanged.

```text
one CandleIngestion minute owner
    -> CandleConnectionPool: ownership, deadline, shared request budget
       -> worker w00: socket / decoder / generation / receiver / sessions / futures
       -> worker w01: socket / decoder / generation / receiver / sessions / futures
       -> ... fixed, bounded worker count
    -> validated per-instrument merged results
    -> original COPY/UPSERT + coverage/latest/revision transaction
```

Canonical symbols are sorted then assigned round robin. Sizes differ by at most one.
Assignment is stable under reordered input and remains fixed throughout a cycle and
reconnection. Subscription replacement during a collection is rejected. Duplicate
canonical identities, instrument/provider identities and upstream source identities are
rejected; there is no failover reassignment or duplicate ownership.

The factory must create distinct workers with the same logical provider name. Workers
must explicitly support the shared request-budget contract. Each owns independent mutable
connection state and symbol-health objects. Only result/diagnostic merging, instrument
ownership and the provider-wide request limiter are coordinated. Chart session IDs include
worker ID, generation and never-reused worker request serial; the receiver still checks
socket identity/generation before and after `recv`, metadata identity and series/turnaround.
Pool merging additionally checks each candle's owner, instrument, provider/source and
requested time window. Foreign worker keys cannot overwrite their actual owner's result.

Static partitioning deliberately trades load balancing for simple ownership and failure
reasoning. Uneven response times can leave the slowest worker determining cycle duration;
the benchmark reports each worker's elapsed time. No adaptive repartitioning was added.

## Limits and configuration

`PoolLimits` is an immutable **integration-supplied** declaration of permitted connections,
instruments per connection, total instruments, total concurrent requests and requests per
connection. This is not proof of permission. Supported production integrations verify their
provider/account agreement; explicitly authorized experimental harnesses declare local
safeguards without claiming upstream allowances. Synthetic limits are explicitly simulated.
Configuration exceeding the declaration is rejected before collection.

| Setting | Default | Application validation ceiling | Meaning |
|---|---:|---:|---|
| `CANDLE_POOL_CONNECTIONS` | 1 | 32 | Fixed maximum connection workers; idle workers do not connect |
| `CANDLE_POOL_WORKER_CAPACITY` | 100 | 4096 | Maximum owned instruments per worker |
| `CANDLE_POOL_MAX_INSTRUMENTS` | 100 | 4096 | Whole-pool assignment ceiling |
| `CANDLE_POOL_CONCURRENCY` | 2 | 128 | Shared dispatched unfinished request limit |
| `CANDLE_POOL_DEADLINE_SECONDS` | 48 | 60 | Retrieval/handshake deadline for the whole pool |
| `CANDLE_POOL_CLEANUP_SECONDS` | 3 | 10 | Each worker's additional close budget |
| `CANDLE_POOL_CONNECT_INTERVAL_SECONDS` | 0 | 10 | Fixed delay between worker connection starts |
| `TRADINGVIEW_CANDLE_CONCURRENCY` | 2 | 12 | Existing chart worker's local sliding window |
| `CANDLE_REQUEST_TIMEOUT_SECONDS` | 15 | 60 | Individual chart request and bounded capacity acquisition |

The pool fields apply when an integration constructs `CandleConnectionPool`; they do not
silently replace an application's existing single provider. The guest adapter's default
rejects `CANDLE_POOL_CONNECTIONS != 1`. The new live harness explicitly constructs workers
with `experimental_pool=True`, retaining each worker's 100-subscription/job safeguard.
No environment flag asserts upstream permission or activates production guest pooling.

The old 100-subscription and 100-job limits are **application safeguards**, not verified
upstream capabilities. They remain intact for the live adapter. Only `SyntheticCandleProvider`,
which has no network connector and requires simulated instrument mappings, expands the
bounded job limit for larger offline single-worker comparisons. Its candles are explicitly
`simulated`; benchmark history never enters production tables. The old staged live harness's
17/34/64/100 progression and conservative source-verification gate remain intact; the new
offline harness accepts arbitrary bounded counts independently of that live policy.

The observed one-series-per-chart-session restriction is an upstream rejection, documented
in [earlier scalability evidence](candle-retrieval-scalability.md). Multiple sessions on one
socket are supported by prior observations; this does not establish permission for more
sockets. TradingView's [published policies](https://www.tradingview.com/policies/) restrict
automated/non-display reuse, and its [API help](https://www.tradingview.com/support/solutions/43000474413-i-need-access-to-your-api-in-order-to-get-data-or-indicator-values/)
does not offer a public market-data download API. Checked October 12, 2026. No numerical
guest connection allowance was established. The operator's 2026-10-12 instruction authorizes
bounded technical investigation with local experimental ceilings. It does not establish
upstream allowances. The new harness records throttling/access rejection and stops escalation;
see the measured evidence, including failed runs, before drawing a capacity conclusion.

## Resource lifecycle, deadlines and failures

There is at most one collection task per nonempty worker, one receiver and one pending
capacity-acquisition task per worker. Each worker bounds active sessions by its local
window. A shared `BoundedSemaphore` limits unfinished requests across the pool, including
when the global limit is lower than the sum of local windows. The receiver releases a
request permit upon completion independently of dispatcher cleanup; dispatch waiting for
capacity also watches receiver failure. Completed-but-not-yet-deleted sessions remain
bounded by the local window. Cancellation releases permits exactly once and joins all
owned tasks. There are no tasks per instrument waiting for pool slots and no detached retries.

Each worker establishes at most one connection per cycle. A handshake failure/disconnect
does not cancel healthy workers. Incrementally retained completions survive a worker's
global deadline, including completions received before dispatch notices cancellation.
Remaining jobs receive explicit errors. A successful native series lacking the target bar
is a successful request with a source omission, never a transport timeout.

Network failures use jittered exponential worker backoff across later scheduled cycles;
healthy workers continue on the next minute. Ingestion recognizes pool-owned transport
backoff and does not hold the entire pool behind one worker's ordinary failure. An explicit
HTTP 429 instead gates the whole provider and decreases its global request ceiling;
worker overload retains the existing decrease-only local window. Connection counts never
increase automatically. Permanent access/protocol rejection stops future pool cycles.
Valid results from other workers already in progress are retained. Uncertain close cleanup
is reported and stops replacement connections until operator intervention.

The cooperative deadline starts in `pool.fetch_many` and includes establishment and requests.
The existing pipeline can then spend at most one second deleting sessions and the worker
can spend its configured close budget. The intended additional I/O cleanup allowance is
roughly four seconds with defaults. Native history parsing yields every 32 completed bars
so cancellation can interrupt large responses. JSON decoding, individual model construction
and OS scheduling remain synchronous: this is not a hard real-time 52-second upper bound.
DB progress reads precede this deadline;
persistence/reconciliation follow it. The 48-second **whole collection-body warning**,
nonoverlapping scheduler and next-minute overrun tracking remain separate and unchanged.
Database outage retry can still delay later collections intentionally. No promise is made
that a retrieval-only result automatically fits the full collection or commit deadline.

Native Decimal OHLC, source identity, event bucket and receive timestamps, forming-bar
exclusion, correction overlap and the 300-minute recovery bound are unchanged. Successful
results share the existing idempotent transaction and rollup path. Transport failures enter
the existing recovery coverage model: known calendars retain EXPECTED gaps; failed first
handshakes with no verified session retain UNKNOWN coverage. Source-completed omissions
remain explicit and may be found by later bounded native history; permanently absent bars
are never fabricated or forward-filled. NOT_EXPECTED semantics are unchanged.

## Diagnostics and benchmark method

Existing `health.candle_retrieval` contains pool configuration, ownership, aggregate results,
peak unfinished requests, next effective window and bounded worker diagnostics. Each worker
records phase (handshake/requests), health/transport, connection identity, elapsed time,
failure, deadline expiry, cleanup error, retry gate, session/future counts and its own request
diagnostics. Instrument timings contain worker/session/generation identity. Pool-owned
coverage and provider gauges are not overwritten by individual worker subscription subsets.
The only new Prometheus metric is `marketdata_candle_pool_connected_workers{provider}`;
ownership/session IDs stay in bounded health diagnostics, not new metric labels.

`scripts/benchmark_candle_pool.py` uses the real dispatcher, decoder, parser, routing,
deadline and cleanup with in-process synthetic sockets. It compares the original worker
path directly with the pool on the same identities. Connections have a modeled handshake
delay; per-symbol delay is `latency * (0.7 + (index % 5) * 0.15)`. All timing includes modeled
handshake, retrieval, parse and cleanup, excluding universe setup and the UTC grace wait.
Configurations alternate order by round. The optional shared semaphore inside synthetic
response emission models provider-wide service capacity independently of client windows.

Counts distinguish successful requests, actual native target candles and failed requests.
`--missing` omits fixed historical target buckets even on subsequent requests. `--recovery`
runs one additional bounded-history cycle and proves whether the original missing target
returned; no success is inferred from a response alone. Recovery explicitly advances the
synthetic scheduler past backoff; this is not a wall-clock recovery soak. CPU is process
CPU time, Python allocation is the extra traced peak above cycle-entry allocation (not
total process memory), RSS is measured on Windows, and cleanup checks
count sockets, sessions, emitters, receiver tasks and total remaining tasks. Tracemalloc is
enabled during measured cycles and contributes overhead. Short samples do not establish
sustained memory stability or trustworthy production p95; nearest-rank p95 equals max with
three samples. Close-to-first/last values are **modeled** two-second-grace latencies, not
exchange publication measurements. No persistence cost is included in transport matrices.

## Reproduce

No credentials, Internet, production registry changes or database writes:

```powershell
uv run python scripts/benchmark_candle_pool.py --synthetic --counts 64 128 256 512 --configs 1:2:2 1:8:8 4:2:8 8:2:16 --partition-size 17 --rounds 3 --latency 0.1 --output artifacts/pool-independent.json
uv run python scripts/benchmark_candle_pool.py --synthetic --counts 128 256 --configs 1:2:2 1:8:8 4:2:8 8:2:16 --rounds 3 --latency 0.35 --output artifacts/pool-slower.json
uv run python scripts/benchmark_candle_pool.py --synthetic --counts 128 256 --configs 1:8:8 4:2:8 8:2:16 --rounds 3 --latency 0.1 --service-capacity 2 --output artifacts/pool-shared-capacity.json
uv run python scripts/benchmark_candle_pool.py --synthetic --counts 256 --configs 1:8:8 8:2:16 --rounds 3 --latency 0.1 --bars 301 --output artifacts/pool-history.json
uv run python scripts/benchmark_candle_pool.py --synthetic --counts 128 --configs 1:8:8 8:2:16 --rounds 3 --latency 0.02 --missing 0 1 --recovery --output artifacts/pool-source-omissions.json
uv run python scripts/benchmark_candle_pool.py --synthetic --counts 128 --configs 8:2:16 --rounds 3 --latency 0.01 --slow-seconds 4 --deadline 2 --recovery --output artifacts/pool-transient-recovery.json
```

`configs` means `connections:local_window:global_window`. Application/provider safeguards
remain enforced separately; CLI counts are evaluation workloads, not production defaults.
`--live` rejects before any connection. For an authorized documented provider, implement a
worker satisfying the budget/partial-results contract and supply its verified `PoolLimits`;
the current guest adapter is not that permitted integration.

## Evidence

Measured October 12, 2026 on this Windows development host, three cycles per configuration.
The original application remained running; this was not a dedicated performance host.
Prior live one-socket evidence remains separate in
[live capacity evidence](candle-live-capacity-benchmarks.json).
The compact [pool benchmark evidence](candle-pool-benchmarks.json) preserves all profile
summaries, first/last retrieval distributions, resource observations, failure counts and
per-worker extrema. Raw cycle records are in ignored `artifacts/pool-*.json` locally.
Three-cycle p95 is the observed maximum, not a production percentile estimate.

### Interpretation and rejected conclusions

At equal total request concurrency of eight, the single worker beats four workers in the
independent-delay model. More client concurrency helps when modeled source work is
independent, but this does not establish that sockets themselves increase service capacity.
In the shared-capacity-two model at 256 instruments, single/eight-window p50/p95 is
16.777/16.949s, four/two is 17.438/17.442s and eight/two is 18.447/18.476s.
All 2,304 target bars are present across those nine cycles; extra sockets add cost without
overcoming the modeled provider-wide bottleneck. This is **SYNTHETIC VERIFIED**, not a
finding about TradingView's actual service capacity.

Approximately 17 instruments per worker is **INCONCLUSIVE as a production optimum**.
The final matrix includes 64/4=16, 128/8=16, 256/16=16 and 512/31=16-17 assignments.
At 512, eight workers with 64 instruments each beat 31 workers even though the latter has
twice the global request ceiling. Fixed partitioning, more handshakes and more worker
processing increase cost; no automatic connection-count growth or repartitioning is retained.
There is no evidence warranting a change to the production one-socket/window-two default.

### Minute budget, history and persistence

All complete normal-response configurations in the final 64-512 matrix fit the 48-second
retrieval budget with no timeout, handshake failure, disconnect or protocol error, and zero
retained sessions/emitters/receiver tasks. There is one remaining asyncio task: the benchmark
owner. These are **SYNTHETIC VERIFIED retrieval results**, not live minute-completeness claims.

The slower independent-response profile (base delay 0.35s) passed 128 and 256 instruments.
At 256, the original window-two single socket took p50/p95 46.365/46.418s, leaving just
1.582s to the retrieval budget. Window-eight single, four/two and eight/two took
11.953/12.531s, 12.093/14.736s and 6.585/10.623s respectively. The narrow baseline margin
is insufficient evidence of reliable full collection/commit within the minute.
These short-response slower/shared profiles were measured before the subsequent history
cancellation fix; their native histories have three completed bars and do not hit its
32-bar yield threshold. The principal matrix and full-history profile were rerun after it.

The 256-instrument maximum-history profile requests 301 bars, excludes forming bars and
parses 300 completed candles per instrument: 76,800 per cycle. An initial experiment
returned incomplete/late cycles (up to 63.049s): **PARTIALLY VERIFIED**, preserved as
`pool-history-before-cancellation-fix.json`. It exposed CPU starvation during large parsing
and benchmark-only simulated-status copying. That older raw harness used an unconditional
status label; the compact evidence corrects its verdict from the actual late/incomplete
cycle records. The current harness checks targets, deadline and cleanup before labeling
a profile complete. The retained changes yield every 32 bars and
label simulated observations per parsed bar instead of copying an entire worker result.
A regression test cancels mid-parse and proves the whole history is not processed first.

After the fix, all six cycles returned 256/256 target bars and cleaned up. Single/window-eight
p50/p95/max was 30.547/37.673/37.673s; eight/two/global-sixteen was
20.735/22.702/22.702s. Process CPU p95 was 35.281s versus 22.172s; additional traced peak
allocation reached 182.5 versus 184.8 MiB. This CPU-heavy recovery cost is a material
bottleneck. It does not validate 512 instruments at maximum history or establish a hard
deadline under arbitrary CPU/OS stalls; those cases are **NOT TESTED**.

Separate **SYNTHETIC VERIFIED** persistence probes used only migrated
`marketdata_benchmark_ts_test`, SIMULATED candles and a unique provider namespace.
Three rounds at 17/100/500 instruments measured p50/max transaction time
0.059/0.109s, 0.243/0.284s and 1.242/1.620s. These are isolated writes, not an integrated
512-instrument/full-history collection or outage soak. No production benchmark history,
instrument registry modification or schema migration was introduced.

### Availability, failure recovery and live boundary

Fixed native omissions at 128 instruments, single/eight-window and eight/two workers,
each returned 128 successful requests but only 126 actual target candles. Across three
cycles per configuration, both immediate and eventual primary completeness were 378/384;
the same absent source minutes remained absent after bounded history. This profile is
**PARTIALLY VERIFIED for data completeness**, with successful transport and cleanup.
The collector preserves that distinction and never fills nonexistent source candles.

The transient slow-response profile at 128 instruments, eight/two/global-sixteen and a
two-second deadline retained 127/128 targets in each initial cycle and recovered 128/128
on the next bounded-history cycle. Three cycles had 381/384 immediate and 384/384 eventual
targets; initial return p50/max was 2.112/2.132s including cleanup. The deadline failure is
retained as **PARTIALLY VERIFIED immediate coverage / SYNTHETIC VERIFIED recovery**.
No extra connections were created to retry it. Handshake failure/recovery is additionally
covered by the integrated persistence test below.

Deterministic tests cover failed handshakes, disconnects, wrong-worker/stale-generation
frames, partial-result retention, deadlines, cancellation while waiting for global capacity,
mid-parser cancellation, worker backoff/recovery, provider-wide 429 gating and stalled close.
An integrated 128-instrument cycle persists 112 healthy results while a 16-instrument
worker fails; the next minute recovers all 128 originals and all 128 new target candles.
Failed first-handshake observations remain UNKNOWN without verified session knowledge;
known EXPECTED gaps enter normal bounded recovery. Repeated writes change zero rows.

In the earlier synthetic implementation session, no new live chart requests were made.
That session blocked new guest multisocket/high-count tests. The subsequent operator-authorized
live experiments are in [end-to-end evidence](candle-live-end-to-end.md). The existing deployed
17-instrument application was observed via stored-only local API/DB/WS smoke: 08:53 UTC
had all 17 identical native targets; 08:54 had 16 because XAUT's successful streaming
series lacked its target bar (17 successful requests, zero transport failures). The smoke
therefore exited with failure and is retained as **PARTIALLY VERIFIED**, not quietly dropped.
The next observed snapshot had 17 current candles. This observation validates the original
deployed image; the new source changes were not deployed or claimed live-tested.
Production retained 17 mappings, 26,216 guest raw ticks, zero duplicate native logical
candles and no benchmark mappings. Existing old coverage gaps still degrade health.

Historical live 64-instrument evidence had 640/640 responses but 631/640 native targets;
its source-completeness gate stopped escalation. A 17-per-socket live optimum and sustained
pool operation remain **NOT TESTED**; subsequent >100/live pool experiments are separate.
A documented,
entitled integration and longer controlled measurements are prerequisites to rollout.

### Final independent-response comparison

Base delay 0.1s, handshake 0.03s, three completed history bars plus forming allowance.
Every row is **SYNTHETIC VERIFIED**: all requested targets present in all three cycles.
Times include handshake, parse and cleanup; persistence and grace wait are excluded.

| Instruments | Connections / local / global | p50 s | p95=max s | Minimum 48s margin | Targets / requested |
|---:|---|---:|---:|---:|---:|
| 64 | 1 / 2 / 2 | 3.504 | 3.526 | 44.474s | 192/192 |
| 64 | 1 / 8 / 8 | 1.036 | 1.059 | 46.941s | 192/192 |
| 64 | 4 / 2 / 8 | 1.080 | 1.082 | 46.918s | 192/192 |
| 64 | 8 / 2 / 16 | 0.754 | 0.823 | 47.177s | 192/192 |
| 128 | 1 / 2 / 2 | 6.925 | 7.013 | 40.987s | 384/384 |
| 128 | 1 / 8 / 8 | 1.931 | 1.932 | 46.068s | 384/384 |
| 128 | 4 / 2 / 8 | 2.188 | 2.237 | 45.763s | 384/384 |
| 128 | 8 / 2 / 16 | 1.510 | 1.545 | 46.455s | 384/384 |
| 256 | 1 / 2 / 2 | 13.835 | 13.918 | 34.082s | 768/768 |
| 256 | 1 / 8 / 8 | 3.717 | 3.779 | 44.221s | 768/768 |
| 256 | 4 / 2 / 8 | 4.267 | 4.388 | 43.612s | 768/768 |
| 256 | 8 / 2 / 16 | 2.673 | 2.847 | 45.153s | 768/768 |
| 256 | 16 / 2 / 32 | 2.311 | 2.457 | 45.543s | 768/768 |
| 512 | 1 / 2 / 2 | 27.652 | 27.786 | 20.214s | 1536/1536 |
| 512 | 1 / 8 / 8 | 7.359 | 7.370 | 40.630s | 1536/1536 |
| 512 | 4 / 2 / 8 | 8.112 | 8.361 | 39.639s | 1536/1536 |
| 512 | 8 / 2 / 16 | 5.457 | 5.465 | 42.535s | 1536/1536 |
| 512 | 31 / 2 / 32 | 5.891 | 6.083 | 41.917s | 1536/1536 |

At 512, eight workers use 2.953s process CPU p95 versus 5.250s for 31 workers.
Additional traced peak is 7.81 versus 8.18 MiB; the machine-readable evidence also records
absolute RSS samples. These short runs establish cleanup bounds, not sustained memory stability.

### Final validation and rollout

- Full TimescaleDB suite: **270 passed**, 50.01s.
- Full ordinary PostgreSQL suite: **267 passed, 3 expected Timescale-only skips**, 49.97s.
- Ruff lint and format: pass, 69 Python files formatted.
- Strict mypy: pass, 29 source files.
- `git diff --check`: pass. Schema head remains 0004; no migration needed.
- Stored-only original 17-instrument smoke: **PARTIALLY VERIFIED**, with the failed XAUT
  minute retained as described above. No source changes deployed.

The existing Starlette/httpx deprecation warning remains. Pool routing/ownership/resource
bounds are tested at up to 512 simulated instruments. Native persistence/coverage and API
tests ran on both database modes; no production data were cleared. Pool diagnostics remain
compatible with the existing health object. Longer memory/failure soaks, integrated maximum
history persistence remain outstanding before production rollout. Separate live experiments
and their failures are now documented in [end-to-end evidence](candle-live-end-to-end.md).
