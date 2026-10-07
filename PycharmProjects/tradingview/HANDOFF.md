# HANDOFF.md

## Current state - 2026-10-07

The experimental guest source now defaults to **completed native 1-minute OHLC candles**
for all 17 existing instruments. Native retrieval was proved before schema changes. Tick
mode remains explicit (`TRADINGVIEW_STORAGE_MODE=ticks`); unrelated providers and all recorded
raw history were preserved. Alembic head is **0003**. No new dependencies/infrastructure.
AGENTS.md is unchanged; CODEX.md source map/execution notes, README, .env.example and guest
operations docs were updated. Pre-existing health work and unrelated BATCHES_V1.md remain.
Parent Git repository is C:/Users/Pouyan, main. Unrelated BATCHES_V1.md remains untracked.

## Source semantics / boundaries

- Guest-only WebSocket uses the existing public literal unauthorized_user_token, never login,
  cookies, browser automation, entitlement bypass or domestic providers.
- Cached/current served 32493.a85e902ab5ea671ba172.js verifies chart command serializers;
  served 48903.3be128434799eed23346.js verifies createSeries ordering and OHLC indexes.
- Verified commands: chart_create_session [cs, ""], resolve_symbol [cs, "symbol_1", source],
  create_series [cs, "s1", "s1", "symbol_1", "1", count, ""], chart_delete_session [cs].
  timescale_update/du series vectors are [epoch seconds,O,H,L,C,volume]. series_completed
  reports streaming (observed rt_update_period=5); it completes a request, not every bar.
- Source identity/type/currency/listed exchange, intraday, bar_source=trade,
  bar_transform=none, bar_fillgaps=false and streaming/realtime status are required.
  Forming bars never persist; a minute must end before the collection cutoff/grace.
- Basis native, provenance native_upstream_chart. OANDA/TVC price construction is not
  independently verified as executable last/bid/mid pricing. Preserve reported semantics.
  Crypto base volume and OANDA tick counts are distinct additive types. TVC oil volume is
  NULL because its sixth field lacks verified volume_type. No quote-volume reuse.
- All 17 verified: BTC/ETH/SOL/XRP (BINANCE spot), OKX XAUTUSDT token; eight OANDA FX pairs;
  OANDA XAUUSD/XAGUSD OTC metals; TVC USOIL/UKOIL reference commodities. No exchange futures,
  physical-oil spot, perpetual or index substitution. Streaming is upstream reported, not
  an independent latency audit or a data-use/redistribution license.

## Implementation map / decisions

- New candles.py validates Decimal OHLC, UTC minute bounds, completed state and scheduling.
  providers/candles.py adds optional native-candle capability without changing tick adapters.
  providers/tradingview_candles.py owns guest chart transport/parsing. Factory selects mode.
- candle_ingestion.py owns a separate bounded polling task, integrated by ingestion.py.
  Startup recovers the latest closed minute; later cycles calculate UTC boundary + grace
  each iteration, with bounded waits for clock changes. Slow cycles recover from coverage.
  Sequential chart requests share one socket per collection; sessions and socket close after
  use. Idle transport=disconnected can accompany healthy persisted candle coverage.
- Settings: storage mode candles default; CANDLE_INTERVAL=1m; grace=2 seconds (0..30);
  backfill window=300 completed minutes (3..1440), request timeout=15 seconds (1..60).
  A request allows an extra forming bar. No continuous quote subscription/raw tick writes.
- 0003 adds market_candles, latest_candles and candle_coverage. Timescale flag creates a
  seven-day-chunk market_candles hypertable; plain PostgreSQL works without the extension.
  Native history is independent of market_ticks. Existing 0001/0002 and 12 tick CAGGs remain.
- COPY staging, native UPSERT, latest state and compact tstzmultirange missing ranges commit
  atomically. Logical key: instrument/provider/interval/bucket_start/price_basis. Exact receipt
  retries do nothing. Explicit newer-receipt retrievals can correct the same minute; older
  receipts cannot overwrite it. No upstream revision ID or automatic late-correction scan.
- Routine recovery filters already-covered bars: normal collection inserts one row per
  available instrument/minute, without repeatedly updating a rolling history window.
  Coverage expects every minute since its initial anchor. Quiet/closed/source-absent minutes
  remain explicit gaps; no calendar is inferred. Recent history backfills within the bounded
  window after outage/restart. Older gaps remain unresolved/degraded, never fabricated.
- DB outage retains one bounded batch and retries; subsequent collection waits. Shutdown
  logs/counts unpersisted candles. No disk spool. Cold start without durable coverage cannot
  recover minutes preceding its first committed anchor. Network retry gates use bounded
  exponential backoff/jitter; provider-wide permanent rejection stops all its symbols.
- Candle health exposes expected_closed_minute plus per-symbol last_persisted_minute,
  minutes_behind and missing_candle_count. ALL symbols must be current and gap-free.
  Latest APIs use latest_candles, last=close, price_basis=candle_close, interval/bounds,
  age since bucket end and stale status. An old close cannot be freshened by a newer receipt.
- Native API basis defaults for active candle providers; explicit basis=native always works.
  UTC-aligned bounded pagination; 5m/15m/1h/1d SQL rollups from stored 1m, no extra upstream
  subscriptions. Larger buckets report complete=false if constituent minutes are missing.
  Volume sum requires one verified additive type and no null constituent volume. No native 1s.
- WebSocket emits type=candle,persisted=true after commit; backfill can arrive out of order.
  Tick fan-out retains type=tick,persisted=false. Slow clients remain bounded/close 1013.
- Candle counters: requested/received/persisted/upserted/duplicates/fetch_failures/gaps_detected/
  backfilled/shutdown_unpersisted; missing/age/lateness gauges and request/write histograms.
  Tick receive/persist counters are not incremented by candle ingestion.

## Existing tick behaviour to preserve

Owned adapters -> bounded queue -> COPY -> atomic history/latest. Overflow drops NEW ticks
with explicit metrics/logs; one retained batch retries DB outages, bounded shutdown reports
losses. Decimal/UTC event-vs-receipt time and source nanoseconds remain intact. Health separates
transport/feed/symbol freshness; tick mode needs all configured sources fresh in the current
connection. Guest tick frame timeout 45s, observation timeout 60s, STALE_SECONDS=30, exact-source
TRADINGVIEW_SYMBOL_STALE_SECONDS overrides. Reconnect revalidates identity/status. No tick
historical backfill or disk spool. Concrete futures retain independent identity and explicit
roll_at schedules. Mock is simulated; Binance live previously failed ConnectError; authenticated
OANDA/Databento sessions remain untested without credentials/entitlements. ONE API/ingestion worker.

## Verification / measurements

- Captured all 17 native series; portable fixture tests preserve OHLC/Decimal/time/identity/
  volume/status. Matrix: docs/native-candle-verification.md. Ignored research captures remain
  in artifacts/tradingview-research; no accounts or browser operations were used.
- Targeted candle suite: 43 passed. Full Timescale suite: 137 passed. Plain PostgreSQL mode:
  134 passed, 3 Timescale-only skips. Normal suites require no live market connection.
  One existing Starlette/httpx deprecation warning remains.
- Ruff, formatting (50 Python files), strict mypy (24 source files) and diff whitespace checks
  pass. Both modes tested fresh base->0003, downgrade base and re-upgrade. Additional plain
  0003->0002->0003 preserved all 147 pre-existing raw rows in marketdata_smoke_test.
- Compose guest app rebuilt, main database migrated to 0003. Initial live comparison matched
  independent native requests -> parser -> DB -> REST for ALL 17 sources over closed buckets
  09:39, 09:40, 09:41 UTC; WebSocket received all 17 committed candles per minute.
  artifacts/native-candle-smoke.json retains results. Raw guest count stayed 26,216; observed
  storage had exactly 17 rows/minute across consecutive buckets beginning 09:33 UTC.
  Final rebuilt-collector repeat passed for 09:57 and 09:58 UTC: all 17 native/DB/REST OHLC
  matched and all 17 WebSocket candles arrived each minute. artifacts/native-candle-final-smoke.json
  records 442 candles across 26 consecutive minutes, still 26,216 raw ticks and no gaps.
  Smoke scheduling now rechecks its wall-clock deadline (Windows sleeps can wake early);
  the collector already used a deadline-rechecking scheduler.
- Measured old receipt window [09:00,09:20) UTC: 11,138 raw ticks / 20 = 556.9 rows/minute.
  Compared with observed 17 candle rows/minute: 96.947% fewer history rows (32.76x reduction).
  Different measured windows; not a disk/compression claim. Full availability projects 24,480/day.
- Isolated mock app at localhost:8001 passed scripts/smoke.py (30 instruments, REST/history/
  candles/health/actual WebSocket); temporary process stopped. Existing tick mock path preserved.
- scripts/inspect_tradingview_ticks.py inspected live volume and exported one historical minute
  (17 derived last candles) without deleting raw data. Optional explicit --delete-raw requires
  unchanged verified export/fingerprint and full selected-basis coverage; corrupt exports are
  refused. Successful deletion tested ONLY on disposable data. No user history was deleted.
- Prior short tick benchmark remains 50,000/50,000 persisted over 300 instruments in 10.155s,
  zero drops; not rerun or reinterpreted as a candle/long-soak performance guarantee. CI not run remotely.

## Local operation / next work

Live Compose guest collector remains on localhost:8000; DB localhost:5432, retained volume
tradingview_marketdata_pg. Use both compose.yaml and compose.tradingview.yaml for guest mode;
bare Compose restores mock. Native smoke: scripts/smoke_tradingview_candles.py --minutes 3.
Old smoke_tradingview.py explicitly requires ticks mode. Disposable *_test DBs are cleared by
integration fixtures. Secrets/.env/exports/artifacts remain ignored. BATCHES_V1.md is unrelated.

Next: multi-hour source/network/DB-outage soak; validate session-aware missing-minute policy
before suppressing closed-session gaps; decide correction/replay/retention/backup policies.
Guest protocol and availability remain unsupported/experimental; no independent latency audit,
licensed exchange futures entitlement, unlimited archive backfill, durable queue, multi-worker
fan-out, market consolidation or crash-safe loss guarantee. See docs/tradingview.md for SQL,
maintenance constraints, provenance and operating procedures.
