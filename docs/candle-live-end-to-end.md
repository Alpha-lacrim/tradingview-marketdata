# Live guest candles through PostgreSQL

The 2026-10-12 operator-authorized experiment uses actual TradingView guest WebSockets,
native completed 1m bars, the production `CandleIngestion.cycle` reconciliation path and
the production `Store.write_candles` PostgreSQL transaction. Supported numerical guest
connection allowances remain unverified. No authentication, entitlement or access-control
workaround is used. At the time of this historical experiment the application kept its
original 17 instruments and one socket. It now has a separately verified
[34-instrument ordinary pilot](production-instrument-expansion.md).

## Reproduction

Create a dedicated database ending `_test` and migrate it with `uv run alembic upgrade head`
after setting `DATABASE_URL`. Do not share this database with pytest fixtures, which truncate
market-data tables. Use `TEST_DATABASE_URL` for the benchmark; the URL is never written to
evidence. Stop the ordinary collector first and restore the same image afterward:

```powershell
docker compose stop app
# Set TEST_DATABASE_URL to the dedicated migrated database.
uv run python scripts/benchmark_live_pool.py --collector-stopped --discover --counts 128 256 512 --cycles 3 --connections 6 --connect-interval 2 --output artifacts/live-pool-paced.json
docker compose start app
```

`--discover` saves current TradingView public scanner responses and candidate identities.
The observed scanner returned 909 BINANCE/OKX spot-USDT instruments in this session.
Reported volume * close ranks candidates; these floats never enter financial persistence.
The parser independently checks exact source identity, spot type, venue/currency, native
trade OHLC semantics, no gap filling/transformation and streaming/realtime delivery status.
Catalog inclusion is not native verification. A larger stage is a prefix of the same saved
ranked universe. Sparse or unavailable symbols are retained, not replaced after observation.

Six fixed workers own sorted round-robin partitions. Local windows are two and the shared
request window is twelve; at 512 each worker owns at most 86 instruments, below the unchanged
100 local safeguard. Starts are spaced two seconds apart; handshake/retrieval share the
48-second deadline. Historical recovery is bounded to 300 minutes with three-minute
correction overlap. Idle or rejected sources never trigger additional replacement sockets.
HTTP 429 imposes provider-wide backoff (at least one minute; honor longer Retry-After) and
decreases concurrency; the experiment stops escalation and preserves partial commits.

Every stage runs three baseline minute cycles, one labelled client-side socket disconnect
of w00 plus one database failure before the transaction, then two recovery cycles. The
database fault tests the ingestion retry path, not a PostgreSQL server outage or uncertain
commit. The socket fault closes a real connected client after 0.5 seconds; it is not an
upstream fault. Successful peers continue and native history is requested for missing minutes.

## Evidence and interpretation

Raw checkpoints in `artifacts/live-pool*.json` preserve per-request/source/worker evidence,
native candles, each candle's first acknowledged commit, immediate and eventual coverage,
failures, cleanup, and process measurements. Published measured summaries and per-candle
latencies belong in `candle-live-end-to-end.json`. Preserve failed runs separately.

An immediate candle must exist durably after that target's cycle transaction. Eventual
completeness counts only actual native candles durably read back in later cycles. Successful
network responses without the target bar remain observed native-target shortfalls. Missing
minute targets from scheduler overruns are included in eventual denominators. EXPECTED/UNKNOWN/NOT_EXPECTED
coverage semantics remain the production store's responsibility; no synthetic replacement
bars are created. Candles/latest/coverage/correction audit commit atomically.

Each batch's local UTC acknowledgement is sampled immediately after `write_candles` returns,
then a new pool acquisition reads durable OHLCV/source/status and checks exact Decimal values.
All candles in that transaction share its acknowledgement. This is close-to-local-commit-ack
latency, not PostgreSQL's internal commit timestamp or independently audited exchange latency.
Replay leaves the first commit measurement intact. Later corrections do not rewrite it.

The [Batch 2 availability audit](native-availability-investigation.md) separates request
success, native availability, commits and recoveries, and compares the common source cohort.
Historical raw response bounds were not captured; references below to source omissions mean
observed missing native targets, not verified permanent upstream omissions or no-trade minutes.

Resource measurements include process CPU seconds and one-core utilization, 250ms sampled
Windows working-set RSS, traced allocation peak, task count, event-loop lag and post-cycle
receivers/sockets. The benchmark retains growing evidence in memory and enables tracemalloc;
these figures include harness overhead. No long memory soak or server resource isolation is
claimed. Request windows, worker elapsed time, handshake outcomes, failures and cooldown
are retained. Percentiles over short runs describe those samples only.

## Initial unpaced result

At 10:45 UTC, eight simultaneous socket starts for 128 instruments yielded two HTTP 429
handshake failures. Six healthy workers completed 96 requests; 93 target candles committed
and were read back, leaving 32 transport failures plus three native source omissions.
Total cycle 8.036s, actual DB write 0.572s, target close-to-commit 9.682s. Process CPU 2.125s,
RSS sampled peak 105.895 MiB; all sockets closed and receiver tasks joined. The experiment
stopped before 256/512. This failure remains evidence, not a successful capacity result.

## Completed paced run

The six-socket run executed 18 cycles from 10:48 through 11:05 UTC, finishing at
11:05:43 UTC. All three requested sizes reached actual native PostgreSQL persistence.
Every stage's verdict is **LIVE INCOMPLETE**. Complete 128/256/512-minute coverage and
sustained capacity were not demonstrated. This universe contains only distinct BINANCE/OKX
spot-USDT sources, including tokenized/reference assets listed as spot; it does not establish
mixed FX/metals/energy or exchange futures capacity. Source omissions grow with the broader
ranked universe. There were no request failures in any of the nine baseline cycles.

| Instruments | Baseline immediate, 3 cycles | All-cycle immediate, 6 cycles | Eventual committed | Unresolved |
|---:|---:|---:|---:|---:|
| 128 | 373/384 | 602/768 | 743/768 | 25 |
| 256 | 709/768 | 1,386/1,536 | 1,419/1,536 | 117 |
| 512 | 1,271/1,536 | 2,588/3,072 | 2,663/3,072 | 409 |

Immediate means read-back verified in the original target cycle; eventual includes actual
bounded native-history recovery during subsequent cycles. All 5,376 expected target minutes
remain in denominators; all 4,825 supplied candles are actual committed rows, not successful
request counts. The separate failed unpaced run retains another 93 rows. No logical duplicate
candles were found. The database is ordinary PostgreSQL 17.7 inside the existing Timescale
container, migrated to head 0004; synchronous_commit, fsync and full_page_writes are all on.
The experiment never truncated production tables or changed its permanent registry.

| Instruments | Baseline wall max | All-cycle wall max | Immediate close-to-commit p95 | All first-commit p95 / max | DB write p95 |
|---:|---:|---:|---:|---:|---:|
| 128 | 17.399s | 18.157s | 19.653s | 79.073s / 139.073s | 0.871s |
| 256 | 22.363s | 47.765s | 49.158s | 49.158s / 109.158s | 1.338s |
| 512 | 45.578s | 46.055s | 46.798s | 46.798s / 104.292s | 2.669s |

The true DB-write column excludes read-back/progress checks; cycle wall includes them.
Commit latency is recorded per candle; recovered older candles retain their longer latency.
At 512 the baseline leaves only 2.422s against the 48s cycle budget. These are short-run
nearest-rank percentiles, often maxima, not service-level guarantees.

Controlled disconnect lost 19/39/84 target requests at 128/256/512. Recovery durably supplied
19/33/75 of those exact minutes, respectively, with close-to-first-commit 139.073/109.158/
104.292s. The remaining six and nine targets at 256/512 were absent from later valid native
history responses and remain unresolved. The 128 stage additionally lost the entire 10:52
cycle to DNS/handshake errors; the following cycle recovered most of those targets. Do not
attribute these local network errors to a confirmed TradingView service failure. Each injected
pretransaction database failure caused exactly one retry, then an actual successful commit.
No PostgreSQL server outage or uncertain-commit fault was tested.

| Instruments | Process CPU p95 | One-core CPU p95 | Sampled RSS peak | Additional traced peak | Loop lag max |
|---:|---:|---:|---:|---:|---:|
| 128 | 3.688s | 21.19% | 134.648 MiB | 5.904 MiB | 0.141s |
| 256 | 5.781s | 25.85% | 186.719 MiB | 12.363 MiB | 0.297s |
| 512 | 9.859s | 21.41% | 259.520 MiB | 20.991 MiB | 0.609s |

Maximum tasks were 16/21/24. Every cycle ended with sockets closed and no candle receiver
tasks. Worker elapsed p95 was 15.562/33.852/38.973s; client worker-start-to-ready p95 was
10.797/10.809/11.309s **including fixed pacing**. The recorded run used the initial timing
instrumentation; the retained implementation now reports pacing wait and handshake separately.
Do not interpret the older raw `handshake_seconds` field as network-only time.

The process retains growing evidence and tracing overhead, so rising RSS is not proof of
a collector memory leak. Other validation commands ran on the shared development host during
parts of the experiment; no isolated-host capacity comparison or server CPU/RSS measurement
is claimed. Retry-After/date handling, paced-worker throttle gating and improved deadline
classification added afterward passed deterministic tests; no second live throttle was induced.

The prior collector was restored with `docker compose start app`, preserving its image,
17 instruments and original single-socket/window-two settings. Production raw ticks remained
26,216 and there were zero benchmark mappings in its database. Historical five-day gaps remain
outside the 300-minute recovery horizon and correctly keep global health degraded.
The initial restoration stored-only smoke failed for the 11:06 bucket after a handshake
failure in the 11:07 collection and backoff in 11:08; those minutes later arrived through
native history. The failure is retained in `artifacts/live-pool-restoration-smoke.json`.
A warmed two-minute stored-only smoke **passed** for 11:15 and 11:16 buckets, with all 17
WebSocket candles matching PostgreSQL and REST. Its evidence is
`artifacts/live-pool-restoration-warm-smoke.json`; raw ticks were unchanged at 26,216.

Publish raw checkpoints reproducibly with:

```powershell
uv run python scripts/summarize_live_pool.py artifacts/live-pool.json artifacts/live-pool-paced.json
```

[`candle-live-end-to-end.json`](candle-live-end-to-end.json) includes both runs, exact source
identities, per-candle first-commit timestamps/latencies, unresolved minute identities,
request/fault/resource/cleanup summaries, native samples and raw checkpoint SHA-256 hashes.
Raw evidence and full native payloads remain in ignored artifacts. Next: independently
reverify a denser universe for a new fixed comparison, repeat on a quiet host, then run longer
memory/network/database-outage soaks. Preserve these incomplete results when choosing that
new universe; do not retrospectively drop instruments from this experiment's denominator.

Final validation: 281 tests passed on TimescaleDB; 278 passed plus three expected skips on
ordinary PostgreSQL. Ruff lint/format, strict mypy and `git diff --check` passed. No migration
was added. Existing Starlette/httpx TestClient deprecation warning remains.
