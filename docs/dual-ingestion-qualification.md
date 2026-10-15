# Simultaneous TradingView collection — 2026-10-14

The collector now implements opt-in native-candle plus independent quote ingestion, compatible
with the exact delivered standalone FastAPI v1.0. **Company production readiness remains
PARTIALLY SATISFIED for all three goals.** This session has not qualified the new collector
against live TradingView or the company's server. Production has not been changed.

## Audit and implementation

Previously `candles` and `ticks` selected alternative provider objects. Candle mode stored
native completed 1m OHLC and exposed an explicitly labeled candle-close fallback. Tick mode
subscribed to genuine quote deltas, but did not maintain native historical candles. The quote
parser, native parser, coverage/backfill, collector ownership, transactional event journal,
and independent REST/WebSocket reader already existed.

Set **collector-only** `TRADINGVIEW_STORAGE_MODE=dual` to construct the existing candle
provider/pool plus independent quote workers. One collector holds the existing database
advisory lock; one native-candle scheduler owns all candle workers. Quote runners use their
own sockets, subscriptions, metadata validation, frame watchdog, reconnection and backoff.
A quote failure does not restart candle collection, and native fetch latency does not hold
the quote receive loop. No additional queue service, transport or database migration was added.

Quotes use the existing bounded receive queue and batch writer. `Store.write_quotes` commits
the latest observation per source and `price_update` journal events in the same transaction,
using the existing cursor lock. It does not insert `market_ticks`. Batch coalescing keeps the
newest actual observation per source; intermediate deltas within that batch need not become
historical ticks or frontend events. Stable UUID/order guards make uncertain-commit retries
idempotent. `TRADINGVIEW_QUOTE_HISTORY=true` opts into the existing historical-tick writer.
Explicit `ticks` mode retains its previous history behavior regardless of this new option.

Each quote socket has at most 100 configured sources. Counts determine the number of quote
workers; startup rejects configurations exceeding six total candle-plus-quote sockets. The
preserved commercial profile has four candle sockets plus one quote socket in dual mode.
These are local safeguards, not upstream entitlements or newly qualified capacity.
Existing candle pacing, request budgets, rejection and cooldown behavior remain. Quote HTTP
429 now honors the existing conservative Retry-After parser, with a 60-second minimum.

Operational quote names are `tradingview_guest_quotes_00`, etc. Normalized/persisted provider
identity remains `tradingview_guest`; source symbols, broker namespaces and database keys do
not change. Collector status puts quote health first because the released API selects the
first matching provider/prefix. Those snapshots include candle session expectation/state;
separate candle diagnostics remain available. A disconnected between-cycle candle socket
must not hide a connected quote transport. Combined-development legacy latest reads also
use persisted quote-versus-candle freshness selection in dual mode.

Modified implementation files: `src/marketdata/config.py`, `providers/factory.py`,
`providers/base.py`, `providers/tradingview.py`, `ingestion.py`, `persistence.py`, and
`collector.py`. New verification/configuration files: `tests/test_dual_ingestion.py`,
`scripts/observe_dual_ingestion.py`, `compose.tradingview-dual.yaml`, this report,
`docs/dual-registry-comparison.json`, `docs/dual-prior-live-sample.json`, and
`docs/dual-ingestion-verification.json`. Documentation
updates are in `.env.example`, `README.md`, `CODEX.md`, `HANDOFF.md`,
`docs/tradingview.md` and `docs/commercial-tradingview-test.md`.

## Semantics and v1.0 compatibility

- Native completed 1m OHLC remains authoritative. Quotes never create unfinished candles.
- Prices remain Decimal/NUMERIC; last, bid and ask remain distinct, including null fields.
- Only current delta fields are persisted. A bid-only update does not carry an old last,
  ask, volume or source timestamp forward. Equal prices with genuinely new observations
  remain eligible updates; missing observations are never generated.
- `lp_time` applies to a last present in that delta. Bid/ask source time remains null when
  unavailable. UTC source and receipt times remain distinct.
- Source identity and explicit streaming/realtime metadata are required. Delayed, forbidden,
  unavailable and conflicting identities are rejected; other sources continue. A prior
  stored quote retains the classification of that historical observation. Read provider
  health/source status as well as latest-price freshness after a rejection.
- Provider health retains its existing receipt/session semantics. Latest-price freshness
  independently uses both source age and receipt age, and detects clock skew. Heartbeats
  do not refresh observations. Restart/reconnect does not rewrite persisted receipts or
  restore per-session freshness from an old cache. An old source-time snapshot remains
  stale in the latest-price API even if just received.
- A candle-close fallback can be fresher than an old quote; check `price_type`,
  `price_basis`, `quote_freshness`, `realtime_quote_available` and source status.
- The released API's stale thresholds are its own configuration. Keep its settings aligned
  with the collector's chosen thresholds; quote activity is not a promise that a market is
  open, executable, or independently latency-audited.

Schema head remains **0006**. No migrations, existing API implementation, response/event
contracts, or ChartData fields changed. Count/Value placeholders and genuine typed volume
behavior remain as delivered. The ZIP and `distributions/marketdata-fastapi/` were preserved;
no exporter or packaging command was used. The ZIP SHA-256 remains
`3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a`.

The new `dual` setting belongs to the collector. **Do not pass the collector environment
to the released API:** its older Settings accepts `candles`/`ticks`, and does not need to
select ingestion capabilities. The opt-in Compose file changes only `collector`.
Compatible additions to shared collector modules intentionally create source-snapshot drift;
do not refresh the released v1.0 distribution to remove that drift.

## Deterministic verification

Disposable tmpfs databases use task label `marketdata.task=dual-ingestion`, loopback ports
55935/55936 and `marketdata_dual_test`. Exact server versions were queried:

| Database | Schema | Final full suite |
|---|---|---|
| PostgreSQL 16.1, without Timescale extension | 0006 | 480 passed, 6 skipped; 130.93 s |
| TimescaleDB 2.25.1 / PostgreSQL 17.7 | 0006 | 483 passed, 3 skipped; 155.95 s |

[Machine-readable evidence](dual-ingestion-verification.json) records the matrix, JUnit hashes,
release hash and goal verdicts. Ruff check and format (210 Python files), strict mypy (39
source/script files), Alembic head and merged Compose configuration passed. Existing dependency
deprecation warnings remain. Plain PostgreSQL skips three Timescale-only aggregate tests;
both engines skip three optional older container/image checks.

The new native fixture clock was fixed after its DB pause crossed a real minute boundary.
One later parallel PostgreSQL
run hit the existing metrics-listener test's one-second TCP timeout; its full standalone rerun
passed without changing that test or the implementation. Final tables reflect passing full runs.

`tests/test_dual_ingestion.py` adds deterministic factory, precision, atomic rollback,
retry/idempotency, concurrency/order, reconnect, source-status and independent-consumer checks.
Its representative fixture sources are BINANCE:BTCUSDT, OANDA:EURUSD and FX:EURUSD, with
distinct broker identities. These fixture prices/statuses are **not genuine live observations**.

The independent consumer is extracted from the exact released ZIP into a temporary directory
and launched in a separate process. Its module path is asserted to be under the extraction;
its database principal is SELECT-only and its API runs in production authentication mode.
This does not rely on the current root API implementation. Verified flows include:

- Quotes reach committed latest state, authenticated REST and real TCP WebSocket while the
  candle fetch is deliberately blocked and no candle rows exist.
- Quote disconnect/reconnect succeeds while candle work remains blocked.
- An uncertain quote commit retries without duplicate events; latest/event rollback is atomic.
- A verified task-owned PostgreSQL/Timescale container is actually paused and unpaused.
  Receive continues, the API reports database unavailability, and retained quote work recovers.
- Native bars then persist and reconcile with REST and unchanged ChartData.
- Continuous quote connection health and native session state reach the released source-status
  messages after native sockets disconnect. Delayed metadata degrades health and emits no quote.
- Reconnection snapshots and cursor replay reconcile committed identities and exact values.
- The API remains available after collector shutdown. Old quote timestamps/values stay unchanged
  across API process termination/restart and are visibly stale.

The full suites also exercise the existing correction/backfill, sparse/closed-session coverage,
outage bounds, migration rollback/re-upgrade, auth, reader permissions, restart and journal tests.
Optional older Batch 4 container/image faults are skipped unless their separate verified topology
is selected; the new dual test performs its own real task-owned database pause. No new Linux
production image or target-host/WAN capacity qualification is implied.

Reproduce separately per database; never run suites concurrently on the same database:

```powershell
$env:TEST_DATABASE_URL='postgresql://dual_test:disposable_test@127.0.0.1:55935/marketdata_dual_test'
$env:DUAL_TEST_CONTAINER='marketdata-dual-pg16-test'
uv run --no-sync pytest -q
# Timescale: use port 55936 and marketdata-dual-ts-test.
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy src scripts/observe_dual_ingestion.py
```

## Registries and genuine source evidence

[Full source comparison](dual-registry-comparison.json) preserves both registries:

| Registry | Crypto | Forex | Metals | Energy |
|---|---|---|---|---|
| Accuracy-selected 100 | 66 Binance + 22 OKX | 8 OANDA | 2 OANDA | 2 TVC |
| Proposed 99 | 50 Binance | 21 OANDA + 21 FXCM | 4 OANDA | 2 TVC + 1 OANDA |

There are 57 common identities, 43 accuracy-only identities and 42 proposal-only identities.
FXCM quote venue is FXCM while its exact native symbol prefix is FX:. No business proposal
promotion, registry replacement, source removal or activation occurred.

[Prior genuine sample](dual-prior-live-sample.json) is derived, with a source hash and original
timestamps, from the repository's **2026-10-13** TradingView captures. It is not a new run:

| Source | Accepted deltas | First-to-last receipt span | Last-source age range |
|---|---:|---:|---:|
| BINANCE:BTCUSDT | 7 | 7.571065 s | 1.197461–1.658596 s |
| OANDA:EURUSD | 6 | 6.056206 s | 0.822818–2.891644 s |
| FX:EURUSD | 5 | 8.451725 s | 0.297478–1.845753 s |
| OANDA:XAUUSD | 9 | 6.264610 s | 0.785425–3.729213 s |
| TVC:USOIL | 5 | 7.139761 s | 0.680549–0.936966 s |

All five samples reported streaming and no source rejection in those captures. Multiple
source/receipt timestamps fall inside individual minutes, demonstrating genuine quote-delta
publication independently of completed-candle polling. Some simultaneous delta receipts have
zero interarrival time. Bid/ask-only source timestamps were unavailable (4, 3, 2, 4 and 2
observations respectively). These spans are not full active-feed denominators or guaranteed
rates. They prove neither today's availability nor the new dual collector's live end-to-end
operation. Current availability, source-to-commit latency and live DB-to-company latency remain
**NOT TESTED** for these sources in this session.

Read-only inspection found the existing legacy app unhealthy, its TradingView health degraded
with `PermanentConnectionError`, disconnected transport and no qualifying documented idle
window. Its actual database migration version is **0004**, read using SELECT. No live provider
probe was started; no rejection/cooldown gate was bypassed. The exact permanent-error cause
still needs diagnosis under the deployment's operations procedure. Production services and
volumes were not stopped, migrated, replaced or modified.

## Safe final qualification and rollout

Quote queues are bounded and volatile. Exhaustion/shutdown drops are reported by the existing
counters; an abrupt collector crash can lose uncommitted observations. No durable quote spool
or upstream quote-replay capability was introduced. Committed latest state and events survive;
new receipt timestamps arise only from new upstream observations. The new dual collector has
not yet had a sustained live crash/restart qualification. Native recovery remains bounded by
the existing backfill horizon; older unresolved gaps remain visible rather than fabricated.

1. Diagnose the legacy permanent error from preserved diagnostics. Obtain an operator-approved
   live test window with no competing ordinary source scheduler. Commercial authorization is
   already reported established; actual source entitlements/status still need runtime validation.
2. Use a separate disposable live-test database ending `_test`, migrated to 0006, different from
   the databases pytest truncates. Provision a test collector owner and a SELECT-only reader.
   Start one dual collector on a frozen small subset: BTC, OANDA/FXCM EURUSD, gold and oil.
   Keep the candle pool/request limits and source rejection/cooldown guards unchanged.
3. Run the exact extracted v1.0 API with its separate environment against that test database.
   Use the read-only observer below and inspect collector observation counters/raw timestamps
   to distinguish upstream acceptance from batch-coalesced persisted delivery. Journal
   `recorded_at` is append time within the transaction, not actual commit time. For exact local
   commit acknowledgment latency, instrument the test writer's successful transaction return;
   never relabel append-time proxies as exchange/commit timestamps.
4. Extend to the approved 100 profile, then a sustained source/session soak with quote pauses,
   independent socket faults, test-only DB outages, collector crashes and cursor recovery.
   Do not drop sparse sources from the denominator. The 99 proposal needs separate activation
   approval and source qualification. Verify all required instruments rather than extrapolating
   from this small sample or old native-capacity results.
5. Company-server networking, laptop availability, firewall/TLS, secrets, SELECT role, genuine
   company-session/BFF authorization and monitoring remain external deployment checks. Verify
   the actual dashboard's 100-source subscriptions, source-specific freshness, charts and
   reconnect/replay over the company network.
6. Only after explicit production approval: back up and verify the existing volume/schema,
   stop the legacy lockless owner, apply the reviewed 0004→0006 procedure, deploy the reviewed
   collector image with the dual override, and retain the documented schema-compatible rollback.
   The existing commercial profile is unchanged until this reviewed cutover.

The observer performs no writes and opens no TradingView connections:

```powershell
uv run --no-sync python scripts/observe_dual_ingestion.py --base-url http://127.0.0.1:8002 --api-key-file C:/PRIVATE/test-api-key --origin https://YOUR-COMPANY-HOST --registry artifacts/frozen-live-subset.json --seconds 180 --output artifacts/dual-live-observation.json
```

It validates authentication, explicit source/status/price type, native REST reconciliation,
event replay and final quote freshness. It preserves per-source counts, intervals, missing
native minutes, source/receipt/journal/client times and sanitized failed checkpoints. It fails
on simulated status, fewer than two quote updates per selected source, no complete minute
window, missing minutes, stale latest quotes or reconciliation failures. Its 20,000-event
checkpoint bound fails visibly; rates describe persisted delivery and may coalesce upstream
updates. A passing short observer run is bounded evidence, not sustained production readiness.

## Employer goal verdicts

| Goal | Status | Working evidence | Remaining action |
|---|---|---|---|
| 1: historical native 1m data | PARTIALLY SATISFIED | Existing genuine native captures; deterministic persistence, coverage, corrections, recovery and released REST/ChartData | Qualify live dual collection and required-source completeness; diagnose legacy failure; perform approved schema/owner replacement |
| 2: genuine current prices | PARTIALLY SATISFIED | Dual orchestration, exact latest/journal writes and freshness/recovery fixtures; dated genuine intra-minute quote captures | New live dual → database → released API measurement per required source, including quiet/delayed/unavailable sources and sustained activity |
| 3: complete company delivery | PARTIALLY SATISFIED | Exact released ZIP independently consumes fixture native/quote data with auth, SELECT-only role, real TCP WS, replay and writer-unavailable operation | Genuine-source end-to-end qualification and company network/TLS/session/dashboard/capacity checks |

**Overall: all three goals are not yet end-to-end satisfied; the system is not yet qualified
for the company's actual production use.** Collector implementation and deterministic
compatibility are available for review; live qualification and the approved rollout remain.
