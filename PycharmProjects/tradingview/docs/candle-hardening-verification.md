# Candle hardening verification - 2026-10-07

This is measured experimental guest-feed behaviour, not an availability/latency guarantee.
The existing experimental unauthenticated transport, 17 identities and native OHLC semantics
are unchanged.
No new instruments, credentials, browsers, source endpoints or protocol commands were added.

## Latency diagnosis and measurement

Before optimization, three instrumented serial cycles returned all 17 in **11.617 / 13.465 /
13.157 seconds** (connection, retrieval and disconnect, no DB). The final cycle's individual
requests ranged **0.317-0.871 seconds**. Each symbol was sent only after the previous completion,
so those waits accumulated. There were no intentional per-symbol sleeps. This proves avoidable
application serialization, not the provider's internal scheduling or exchange-publication delay.

A single receiver now routes at most **two outstanding chart sessions on one connection**.
Three standalone pipelined probes returned all 17 in **4.359 / 5.763 / 7.387 seconds**. Separate
network windows mean this is evidence of improvement, not a controlled Internet benchmark.
Normal source cadence remains one UTC-minute cycle plus grace. No aggressive connection fanout.

The 900.007-second live observation recorded **15 normal cycles, 255 new candles**. Full latest
cycle milestones distinguish scheduler wake, request start, first relevant message, native-bar
message receipt, parser completion, fetch finish, write start and transaction return. Native-bar
availability below refers to receipt of the relevant chart vector before validation; it is not
an exchange event timestamp. Durations of stages use monotonic clocks; close latencies use UTC.

| Seconds | Supplied baseline p50 / p95 / max | Pipelined 15-cycle p50 / p95 / max |
|---|---|---|
| Close -> first native bar message | 3.45 / 4.70 / 5.11 | 3.443 / 7.322 / 7.322 |
| Close -> last native bar message | 10.01 / 13.11 / 14.35 | 6.754 / 11.716 / 11.716 |
| Close -> DB commit acknowledgement | 10.28 / 13.57 / 14.79 | 7.122 / 12.142 / 12.142 |
| First -> last receipt spread | ~6.46 median; other quantiles not supplied | 3.228 / 4.771 / 4.771 |
| Persistence call itself | Final receipt-to-insert ~0.28 median (different boundary) | 0.036 / 0.117 / 0.117 |
| Scheduler lateness | Not independently supplied | 0.000283 / 0.000678 / 0.000678 |
| Grace target -> cycle start | Not independently supplied | 0.000301 / 0.000697 / 0.000697 |
| Fetch-cycle duration | Not independently supplied | 5.084 / 10.105 / 10.105 |

Quantiles use nearest rank; p95=max in a sample of 15. Different time windows, a short sample,
network conditions, bar-availability vs completion timestamps and overlap/backfill volume limit
strict baseline comparability. The first-receive tail did **not** improve. Later receipts and
median commit improved; no sub-two-second or PostgreSQL-bottleneck claim is made. Persistence
call duration includes client transaction/pool round trips, not isolated server execution time.
The 15-minute run used the first instrumented pipelined hardening build; final follow-up validation
covers subsequent provenance/health/fanout refinements. Detailed local artifacts are ignored,
not committed: serial-candle-timings.json, pipelined-candle-timings.json and
candle-hardening-soak.json under artifacts/.

## Historical truth observed live

- Raw guest ticks stayed **26,216 -> 26,216**. No deletion or tick-mode activation.
- All 15 observed completed minute buckets had **17 logical native candles**.
- Zero duplicate logical candles, malformed OHLC or forming candles in the database inspection.
- Automatic overlap discovered **four** real changes: USDCHF close/tick volume at 11:39,
  USDJPY close/tick volume and NZDUSD tick volume at 11:48, USOIL close at 11:48. Oil volume
  remained NULL. Exact before/after values and discovery times are in candle_revisions.
- Earlier XAUT minutes **11:07 and 11:34 UTC** were absent from native history, one preceding
  deployment. They remain explicit scheduled gaps. Health correctly stayed degraded despite
  current complete buckets. Sparse trade-based native sources need not emit every open minute;
  this is not proof of an exchange transport loss, and no flat bar was invented.
- Read-only historical reconciliation proved partitions for all 17 over [11:20,11:40) UTC;
  XAUT had 19 bars/20 scheduled minutes; the remaining instruments had 20/20.

## Reliability verdicts

VERIFIED live process stop/restart: 75.733 seconds offline, then all 17 candles in both
11:52 and 11:53 UTC buckets. VERIFIED live PostgreSQL stop/restart: 83.817 seconds offline;
/health returned 503 with database=false while /providers returned 200; then all 17 candles
in both 11:54 and 11:55 UTC buckets. Raw count stayed 26,216 in both tests. The two older XAUT
gaps remained, preventing a false healthy result. Recovery evidence is in the ignored local
artifacts/live-candle-outages.json. The final code also preserves backfill provenance through
corrections and emits actual committed payloads in post-commit WebSocket messages.

VERIFIED automated checks: 180 passed in Timescale mode; 177 passed and 3 Timescale-only skips
in plain PostgreSQL; final targeted native-candle/reliability rerun: 86 passed. Ruff, formatting
and strict mypy pass. Fresh migrations, base downgrade/
re-upgrade and 0004->0003->0004 were tested in both modes on a disposable database; the latter
preserved a candle with open=100.1234567890123456789. The main native hypertable and all historical
raw rows remain. An isolated mock app passed REST/history/candles/latest/health/WebSocket smoke.
Final independent native -> DB -> REST comparison passed for all 17 instruments in both
12:09 and 12:10 UTC buckets, with all 17 committed WebSocket candles each minute and raw ticks
unchanged at 26,216. The first attempt failed due to the smoke client's undrained receive queue
while sleeping through startup/overlap traffic, delaying ping handling. Draining messages while
waiting fixed the harness; no server heartbeat settings or source cadence were changed.
Evidence: artifacts/candle-hardening-final-smoke.json (ignored).

A final deployed-build observer ran 180.009 seconds: one startup reconciliation and three
normal cycles, 51 new rows (17 per completed minute), zero duplicate/forming rows, DB or observer
failures. Seven metric-series resets reflect one deployment during observation. Startup is
excluded from normal latency: first p50/p95/max **3.792/4.409/4.409**, last **7.132/9.534/9.534**,
commit **7.648/9.925/9.925** seconds; write **0.039/0.042/0.042**. This small follow-up validates
the deployed refinements, not a replacement for the longer 15-cycle sample. The soak harness
now separates startup/normal cycle counts and excludes startup from normal-stage distributions.

Tests deterministically inject network/transport failures, independent DB read/write
failures, restart with different per-instrument watermarks, short/long outages and missing bars.
Actual Internet disconnection has not been performed. A multi-day source/session soak, exact
exceptional calendars and an independent exchange latency comparison remain unverified.
