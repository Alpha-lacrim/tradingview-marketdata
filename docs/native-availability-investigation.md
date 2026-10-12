# Native availability investigation — 2026-10-12

Batch 2 preserved the running ordinary **34-instrument** pilot, its two-worker configuration,
existing TimescaleDB history and source mappings. No app restart, database write by a diagnostic,
registry change, migration or 64-instrument activation was performed. See the
[sanitized measurements and checkpoint hashes](native-availability-investigation.json).

## BINANCE:ATOMUSDT, bucket starting 12:14 UTC

This bar is **absent in two captured native chart-history snapshots**, not observed late or
recovered. Permanent upstream absence and its underlying cause remain unverified.

The ordinary 12:15 response completed with streaming status, exact BINANCE spot identity and
valid native semantics. `completed_candle_available_at=null` means the receiver did not see
the target timestamp before `series_completed`. Request start was 12:12, end 12:15, count four;
completion occurred at 12:15:09.023. The two-second grace cutoff therefore did not exclude a
seen target. Initial raw frames/history bounds were not saved, so this alone is insufficient
to prove full upstream history coverage or rule out a transient publication/revision.

The durable coverage start remains 12:10 UTC. Subsequent ordinary recovery requests repeatedly
start at 12:14, include the missing minute within the 300-minute horizon and succeed. The
12:13 and 12:15 neighbors are stored; 12:14 remains an EXPECTED gap in the database and REST.
The reconciliation path did not forget the target or reset activation.

Two independent, read-only single-symbol diagnostic requests ran while ordinary workers were
disconnected and had zero active requests/sessions/futures. They used the existing guest
adapter and validation, one connection/window one, no retries, login or access workaround:

| Capture | Raw history bounds, UTC | Raw bars | Accepted completed bars | ATOM 12:14 raw / parsed / committed |
|---|---|---:|---:|---|
| 12:49:25–29 | 07:47–12:49 | 301 | 298 | absent / absent / 0 |
| 12:52:25–27 | 07:50–12:52 | 301 | 298 | absent / absent / 0 |

Both responses contain the exact 12:13 and 12:15 timestamps and OHLCV vectors, with no 12:14
timestamp between them. History extends well before the target, excluding a short returned
tail as the explanation in these captures. Accepted counts exclude forming/out-of-window
bars; request success and raw bar counts are not completed-candle counts. Diagnostics wrote
no candles. Raw response vectors and semantic metadata were captured before parser filtering;
sanitized neighbor evidence and SHA-256 hashes are published in the adjacent JSON.

This rules out a current parser-filter/persistence miss in these captured responses. It does
not establish exchange trade activity, permanent provider omission, an outage cause, or what
the source might publish later. No substitute OHLC, zero-volume bar or verified upstream
omission label was written. Continue ordinary bounded recovery; beyond its horizon the gap
remains explicit until a separately authorized historical investigation supplies real data.

## Ordinary collector and recovery

The separate frozen 24-cycle interval covers buckets **[12:25,12:49) UTC**. It does not change
the earlier Batch 1 ten-cycle denominator or include its ATOM 12:14 target.

| Measure | Result |
|---|---:|
| Expected target candles | 816 |
| Successful retrieval requests | 782 |
| Available validated native current targets | 782 |
| Immediate durable target candles | 782/816 (95.833%) |
| Eventual durable target candles | 816/816 (100%) |
| Recovered after the target cycle | 34 |
| Retrieval errors / database failures | 34 / 0 |
| Successful transactions / transactions with current native targets | 24 / 23 |
| Current native close-to-commit min / p50 / p95 / max | 6.700 / 9.385 / 14.314 / 15.454s |
| Recovered target close-to-first-commit | 66.846s |

At 12:26 both worker handshakes hit the existing ten-second timeout: zero chart requests
succeeded and zero current candles were available. This was a transport failure, not a source
omission. Its coverage-only transaction is counted separately from the 23 transactions with
current native targets and excluded from candle-latency percentiles. Normal bounded recovery
committed all 34 missed 12:25 targets at **12:27:06.846071 UTC**. No intervention or rate-limit
bypass occurred; timeout root cause is unknown. Correction updates of earlier bars do not
increase the target-candle denominator. These timings use actual local acknowledgements after
Store transactions return; `created_at` is only supporting read-back evidence, not commit time.

REST read-back verified all 816 persisted target OHLCV values using Decimal and exact source
identities. A later stored-only observer verified **68/68** exact WebSocket → DB → REST targets
over consecutive buckets 12:53 and 12:54; it made no upstream requests. All measured cycles
ended with disconnected workers, zero active sessions/futures/requests and no cleanup error.

The original 17 coverage starts remain 2026-10-07 09:33 UTC and the additions remain
2026-10-12 12:10 UTC. Older missing/unknown partitions match the pre-pilot snapshot. Raw guest
ticks remain 26,216, new pre-activation candles zero and schema head 0004. Database and ingestion
are healthy; global health correctly remains degraded for unresolved historical coverage.
Five-day read-only reconciliation through 12:55 UTC proved all 34 partitions. The additions
evaluated only their 45 activated minutes: 764/765 native targets stored, solely ATOM missing.

## Historical 128/256/512 evidence

Only existing checkpoints were analyzed; no larger live benchmark was rerun.

| Stage | Expected | Successful / failed requests | Native current available | Immediate committed | Eventual committed | Recovered | Unresolved |
|---|---:|---:|---:|---:|---:|---:|---:|
| 128 | 768 | 621 / 147 | 602 | 602 | 743 | 141 | 25 |
| 256 | 1,536 | 1,497 / 39 | 1,386 | 1,386 | 1,419 | 33 | 117 |
| 512 | 3,072 | 2,988 / 84 | 2,588 | 2,588 | 2,663 | 75 | 409 |

Every baseline request succeeded: native/committed current targets were 373/384, 709/768 and
1,271/1,536. Controlled disconnects and the 128-stage DNS failure account for failed requests;
they must not be relabeled upstream candle omissions. The earlier unpaced 128 attempt had two
HTTP 429 handshakes, 96 successful requests and 93 commits; escalation stopped. Access safeguards
remain intact. Eventual counts/recoveries do not prove that bars themselves arrived late.

The common original 128-source cohort had eventual **743/768, 745/768 and 748/768** targets.
Broader-universe unresolved counts therefore do not establish a count-driven loss of that
cohort. Stages were non-simultaneous and time/count are confounded.

Recurring unresolved sources include BINANCE:XUSDUSDT (6/6 in each stage), BINANCE:RLUSDUSDT
(5/2/3 missing), BINANCE:SKHYBUSDT (3/2/1), OKX:BCHUSDT (1/1/1) and OKX:XAUTUSDT (3/5/2).
BINANCE:LITEBUSDT had 6/6 unresolved in 256 but only 1/6 in 512, demonstrating that a short
complete-absence sample is not permanent absence. The added 512 cohort also includes 6/6
unresolved BINANCE:ZRXUSDT and OKX:XMETAUSDT. Historical raw response bounds/vectors were not
retained, so sparse trading, source publication lag, revisions and retrieval coverage cannot
be distinguished conclusively. These are observed availability patterns, not verified causes.
Keep venues/products separate and scanner-only candidates unverified; do not replace the pilot.

## Changes and operations

The adapter now records pre-filter raw history bounds/count and target presence separately
from accepted completed counts/target presence. These are bounded summaries without OHLC data;
they distinguish a seen-but-filtered target from a target never seen before completion.
Regression tests cover both cases. Validation and persistence semantics are unchanged.
These diagnostics are committed source changes for the next planned deployment; the existing
Batch 1 image was deliberately left running and does not yet expose these additional fields.

`scripts/analyze_native_availability.py` reproduces the offline stage/cohort/source audit.
`scripts/probe_native_history.py` provides an explicit opt-in, bounded read-only capture. It
aborts for busy workers, cooldown/access rejection or a missed idle window; it never retries
a failed request. Do not run diagnostics continuously or concurrently with each other.

```powershell
uv run python scripts/analyze_native_availability.py artifacts/live-pool-paced.json --output artifacts/availability-audit.json
uv run python scripts/probe_native_history.py --live --source BINANCE:ATOMUSDT --target 2026-10-12T12:14:00+00:00 --output artifacts/atom-history.json
```

The probe rejects targets outside the configured history horizon, capped at 300 minutes.
Keep raw captures ignored. Use [Batch 1 operations](production-instrument-expansion.md) for
the tested 17-instrument rollback; neither rollback nor removing subscriptions deletes history.
64 remains deferred. Investigate future gaps with raw bounds/target evidence before assigning
an upstream cause; extend observation without weakening validation or clearing old gaps.

Final publication validation passed on disposable databases: TimescaleDB **296 passed**;
PostgreSQL **293 passed, three expected skips**. Alembic upgrade head passed for both modes;
Ruff lint/format and strict mypy passed. The existing TestClient deprecation warning remains.
