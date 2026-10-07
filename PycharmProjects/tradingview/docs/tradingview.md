# Experimental TradingView guest market data

The operator explicitly requested investigation and use of TradingView extraction on
2026-10-07. This opt-in adapter implements the guest chart/quote protocol observed in the public
site's own JavaScript. It is **unsupported and experimental**. TradingView does not offer a
public market-data download API, and its published terms restrict automated/non-display
reuse. This implementation does not grant data-use or redistribution rights. Evaluate those
requirements before operating it beyond this explicitly requested experiment.

No account, paid subscription, browser installation or additional Python dependency is
needed by the adapter. It uses no user cookies, browser profiles, session extraction, login
credentials, permission-forcing flags, proxies or entitlement bypasses. Access rejections
stop the adapter. The literal `unauthorized_user_token` is the site's own guest token.

## Start locally

Keep the existing PostgreSQL/TimescaleDB architecture and apply the provider override:

```powershell
docker compose -f compose.yaml -f compose.tradingview.yaml up -d --build
uv run python scripts/smoke_tradingview_candles.py --minutes 3
```

The override enables the guest adapter for crypto, FX, metals and energy, and disables
futures. Existing history and other provider mappings remain in the database. Mock data
remains clearly simulated; filter queries by provider to see only guest observations:

```text
http://localhost:8000/prices/latest?provider=tradingview_guest
http://localhost:8000/ticks/CRYPTO:BTCUSDT?provider=tradingview_guest&limit=100
http://localhost:8000/candles/CRYPTO:BTCUSDT?provider=tradingview_guest&interval=1m
http://localhost:8000/providers
```

The smoke command explicitly uses live connectivity. The ordinary test suite uses captured
fixtures and requires neither Internet nor provider credentials. To restore the mock app:
`docker compose up -d app`. Do not delete the database volume to switch providers.

Native execution uses the commented provider/registry settings in `.env.example`, with
`TRADINGVIEW_WS_URL=wss://data.tradingview.com/socket.io/websocket`. Run only one collector.
The default is `TRADINGVIEW_STORAGE_MODE=candles`. Diagnostic `ticks` mode retains the
original quote collector and `scripts/smoke_tradingview.py`; explicitly set that mode before
running the old tick smoke. Recorded raw ticks are preserved in either mode.

## Verified initial instruments

All 17 exact source symbols were checked against the public symbol pages and returned
prices plus `update_mode: streaming` in a guest session on 2026-10-07:

| Category | Source symbols | Persisted identity / semantics |
|---|---|---|
| Crypto | BINANCE:BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT (all BINANCE) | CRYPTO:BTCUSDT etc.; Binance spot quotes delivered by TradingView |
| Tokenized gold | OKX:XAUTUSDT | CRYPTO:OKX:XAUTUSDT; token spot, distinct from XAU/USD |
| FX | OANDA:EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD, NZDUSD, EURGBP (all OANDA) | FX:OANDA:EURUSD etc.; broker OTC quotes |
| Metals | OANDA:XAUUSD, OANDA:XAGUSD | SPOT:OANDA:XAUUSD etc.; broker OTC metals, not COMEX contracts |
| Oil | TVC:USOIL, TVC:UKOIL | REFERENCE:TVC:USOIL and REFERENCE:TVC:UKOIL; TVC commodity reference series |

TVC is a TradingView series prefix, not an exchange venue. TradingView describes TVC
instruments as commonly CFDs/derived series. The exact oil construction methodology has
not been established here, so these are conservatively **reference** instruments. They are
not labeled physical WTI/Brent spot, NYMEX CL or ICE Brent futures. Their registry metadata
retains that limitation. Broker quote availability does not establish executable pricing
for the operator's account.

## Real-time gate and tick semantics

The following quote-delta rules apply to diagnostic **tick mode**. Native chart candles
have their own validated semantics and persistence path, described below.

- Registry mappings start `unknown`. Runtime metadata must match the configured source
  type, exchange/provider name and quote currency before observations can be emitted.
- Only explicit `streaming` or `realtime` modes with no reported nonzero delay pass.
  TradingView's own UI maps `streaming` to its Real-time indicator. Delayed, end-of-day,
  snapshot, forbidden and unrecognized modes are rejected for the connection. Other
  symbols continue; health, error logs and drop counters expose the rejection.
- This classification is **upstream-reported**, not an independent exchange latency audit
  or a promise of every trade. Guest delivery can be sampled/throttled. Source timestamps
  and local receipt times allow measuring observed age without inventing zero latency.
- Bid/ask messages can arrive before identity/status metadata. Those observations are
  discarded with the `source_metadata_unverified` metric and a warning per symbol per
  connection. They are not buffered and relabeled retroactively.
- Each current quote delta becomes its own tick. Old prices, last timestamps and volume
  are never filled into a newer bid/ask-only delta. `lp_time` is retained only when that
  same delta includes `lp`; it applies to `last`, not to bid/ask. Bid/ask-only upstream time
  stays null. Sizes and sequence numbers remain null because this probe did not supply them.
- Source numbers are decoded directly as Decimal. Equal-price observations are retained;
  no upstream event ID means no safe delivery deduplication or sequence-gap inference.
- Volume is preserved as reported; its measurement window has not been verified. Candles
  are aggregates of observed quote deltas, not a complete exchange trade tape.
- A source mode/identity rejection suppresses that symbol until reconnect. Fresh metadata
  is required after every reconnect. An old latest price remains historical; inspect its
  derived `age_ms`/`stale` and `/providers` freshness before treating it as current.

## Operational health and freshness

This section describes diagnostic **tick mode**. Candle mode uses persisted-minute coverage
and the separate health rules below; it does not use continuous quote stale thresholds.

The existing provider health model now separates `transport` from feed `state`:

- `disconnected`: no active transport; `connecting`: initial connection attempt.
- `connected`: socket handshake succeeded, before subscription requests are sent.
- `subscribing`: requests are being sent or current-session observations are still pending.
- `healthy`: every configured symbol has a valid, accepted, fresh observation in this session.
- `degraded`: stale symbols, rejected source metadata, malformed payloads, or no valid market
  observations within the configured timeout, even when frames/heartbeats continue.
- `reconnecting`: the owned ingestion runner is recovering/retrying the transport.

`transport` is independently `disconnected`, `connecting`, `connected` or `reconnecting`.
The old `stale` feed-state value remains accepted by the shared schema for compatibility;
this adapter uses `degraded` and explicit per-symbol `stale` flags. A socket handshake never
establishes feed health. Subscription restoration is confirmed by accepted observations for
all requested symbols, **not** by assuming an undocumented upstream acknowledgement.
This shared all-symbol health rule also applies to the existing adapters; their parsers,
transport protocols and normalized tick/persistence semantics are unchanged.

Configuration (also in `.env.example`):

| Variable | Default | Meaning |
|---|---|---|
| `STALE_SECONDS` | 30 | Default maximum receipt age per symbol |
| `TRADINGVIEW_SYMBOL_STALE_SECONDS` | `{}` | JSON overrides keyed by exact source symbol; 1..86400 seconds |
| `TRADINGVIEW_FRAME_TIMEOUT_SECONDS` | 45 | No application frames, including heartbeats, triggers transport recovery |
| `TRADINGVIEW_OBSERVATION_TIMEOUT_SECONDS` | 60 | No accepted market observation degrades the feed without forcing reconnect |

For example, `TRADINGVIEW_SYMBOL_STALE_SECONDS={"TVC:USOIL":120,"TVC:UKOIL":120}` gives
those sources longer receipt-age limits. This is an operator tuning example, not a verified
activity guarantee. Unknown override symbols fail startup instead of silently ignoring typos.
Calibrate thresholds per instrument using observed activity; quiet/closed markets may be
stale with a working transport. Freshness does not imply market-open status or independently
verified source latency. A provider-wide observation timeout can still degrade an otherwise
fresh set when symbol thresholds are longer than that timeout.

The existing one-second feed monitor refreshes health and closes a silent transport once
per session. Only the existing runner reconnects, with its existing backoff/jitter and full
metadata revalidation. A failed close cancels the blocked receive in that same runner so it
can recover; normal shutdown cancellation still propagates. Silence reports
`transport_frame_timeout` in health, metrics and logs. Continuing heartbeats with no accepted
observations reports `no_valid_market_observations`; no data-only reconnect loop is added.

`/providers` retains `last_message_at` (last application frame), `last_success_at` (last valid
observation), `instrument_last_success` and `stale_symbols`. It adds a local `connection_id`,
transport state, connection/disconnection times, close code and sanitized close-reason category,
consecutive reconnect failures, configured/requested/fresh/stale counts, restoration status,
degradation reasons and a `symbols` object keyed by canonical symbol. Each entry includes
source symbol, last receipt time, `age_ms` (null if never observed), observation count,
threshold, session observation flag, stale flag, bounded source update mode and last rejection
category. Counters persist across reconnects and reset on process restart. Consecutive reconnect
failures reset only after feed health is restored; `last_error` retains the last historical
error after recovery. Arbitrary upstream text, session details and frames are never exposed.
The current registry contains 17 symbols; the existing local subscription cap bounds this
adapter's health payload to 100 symbols.

`/health` adds `database_state` and `provider_transports` to its existing summary, and still
returns 503 for degraded database/writer/feed health. Both latest-price endpoints add
`age_ms`, `stale`, `stale_after_seconds` and `provider_state`. Age is calculated from the
**persisted row's receipt time**, so a healthy upstream feed cannot freshen a lagging database
row. A disabled, disconnected, pending or rejected symbol is stale even if its saved row is
recent. Historical tick/candle responses, raw timestamps and database schema are unchanged.

Prometheus uses the existing registry. Existing `marketdata_messages_total` counts application
frames, `marketdata_received_total` counts accepted observations, `marketdata_reconnects_total`
counts reconnect attempts, and `marketdata_parse_failures_total` counts malformed messages.
New families (all `marketdata_` prefixed): `connection_up`, `connection_age_seconds`,
`feed_healthy`, `symbol_observations_total`, `symbol_observation_age_seconds` (-1 before first
observation), `symbol_fresh`, `symbols{state="configured|fresh|stale"}`,
`rejected_observations_total{reason=...}`, `unknown_messages_total`, and
`transport_timeouts_total`. Symbol labels are configured canonical symbols, and rejection
labels are static categories; unknown methods are never metric labels. Metrics/health ages
refresh on monitor passes and reads. State/symbol transitions are logged, with malformed
payload and unknown-message warnings limited to once per 30 seconds per provider. Source
rejections are logged once per source per connection; no tick-by-tick logging is added.

Futures, indices, perpetuals and continuous aliases are intentionally unsupported by this
adapter. Use the existing documented Databento adapter with concrete definitions and the
required real-time entitlements for exchange futures. No delayed feed is substituted.

The transport has bounded WebSocket and frame buffers, heartbeat echo, TLS validation,
connection timeouts, and uses the existing reconnect, queue, batch writer and graceful
shutdown mechanisms. The local cap is 100 subscriptions per connection, not a verified
provider limit. No capacity or reliability claim is made for hundreds of guest symbols.
Internet gaps cannot be collected offline; committed local history remains accessible.
There is no automatic historical backfill or durable queue.

## Protocol evidence and maintenance

The public BTC page specified `window.WEBSOCKET_HOST = "data.tradingview.com"`; its inline
transport appended `socket.io/websocket`, used `~m~<UTF-16 length>~m~` framing and heartbeat
echo. The following site-served assets were inspected on 2026-10-07:

- [Quote/auth commands and qsd decoding](https://static.tradingview.com/static/bundles/32493.a85e902ab5ea671ba172.js):
  guest token; quote_create_session, quote_set_fields and quote_add_symbols; JSON m/p format.
- [Quote fields and delayed mode parsing](https://static.tradingview.com/static/bundles/70252.bb6077bde5aa656f2dfe.js).
- [Streaming-to-Real-time UI mapping](https://static.tradingview.com/static/bundles/46410.29b2c31c9df2fdfaaef0.js).
- [Public market-data API statement](https://www.tradingview.com/support/solutions/43000474413-i-need-access-to-your-api-in-order-to-get-data-or-indicator-values/),
  [usage terms](https://www.tradingview.com/policies/),
  [TVC series explanation](https://www.tradingview.com/support/solutions/43000709178/).

These are observed implementation details, not a contractual protocol specification.
Asset hashes/endpoints/schema may change. Captured real quotes are in
`tests/fixtures/tradingview_guest_quotes.json`; only the local session identifier was
replaced. Parser tests cover delays, identity changes, precision, partial/multiple frames,
heartbeats, malformed messages, reconnect revalidation and persistence without live access.
Reverify behavior against the site before changing any protocol assumption. Do not solve
access rejections by copying login cookies, forcing permissions or disabling TLS.


## Native one-minute candle storage

Genuine native 1m OHLC was verified for **all 17 configured instruments** before changing
storage. [The verification matrix](native-candle-verification.md) lists actual OHLC, volume
classification and UTC timestamps. A sampled price once per minute cannot reveal that
minute's high/low and is never substituted for a native candle.

Protocol evidence extends only the existing unauthenticated guest transport. Served bundle
`32493.a85e902ab5ea671ba172.js` defines chart_create_session, resolve_symbol, create_series,
chart_delete_session and timescale_update vector unpacking. Served chart-data bundle
[48903.3be128434799eed23346.js](https://static.tradingview.com/static/bundles/48903.3be128434799eed23346.js)
confirms createSeries argument ordering, num-bars requests and OHLC vector positions.
The current public chart HTML still references both bundles. Ignored research captures live
under `artifacts/tradingview-research/`; portable sanitized responses are checked in as
`tests/fixtures/tradingview_guest_candles.json`. No browser, login, cookie, DOM collection,
private account token or access-control bypass was introduced.

For each source the observed requests are:

```text
set_auth_token        ["unauthorized_user_token"]
chart_create_session  [chart_session, ""]
resolve_symbol        [chart_session, "symbol_1", source_symbol]
create_series         [chart_session, "s1", "s1", "symbol_1", "1", requested_bars, ""]
chart_delete_session  [chart_session]
```

`"1"` requests one-minute bars. `timescale_update.p[1].s1.s` contains rows whose `v` is
`[epoch_seconds, open, high, low, close, volume]`; `du` uses the same series-vector decoder.
`symbol_resolved` must match full source name, listed exchange, type and currency, report
intraday support, `bar_source:trade`, `bar_transform:none`, and `bar_fillgaps:false`.
`series_completed` must report `streaming` or `realtime`. Its observed `rt_update_period:5`
is a source update period, not a five-second data delay. Delayed/unverified/entitlement/error
responses are rejected. `series_completed` means **request completion**, not finalization of
every bar in the response. The last vector can still be forming and is excluded by time.

The stored basis is `native`, provenance `native_upstream_chart`. Native chart OHLC is kept
exactly as supplied. In particular, OANDA/TVC's reported `bar_source:trade` is retained as
metadata, not asserted to be exchange executions, executable prices or verified last/bid/mid
methodology. OTC metals, XAUT token and oil reference identities remain separate.
Crypto `volume_type:base` is per-bar base-asset volume; OANDA `volume_type:tick` is a per-bar
tick count, not physical/exchange-traded volume. Both types may be summed only within the
same verified type. TVC oil supplies a sixth numeric field but no verified volume_type:
**persisted oil volume is NULL**. No cumulative quote volume is reused.

### Collection and storage

Validated settings: `TRADINGVIEW_STORAGE_MODE=candles|ticks` (default candles),
`CANDLE_INTERVAL=1m`, `CANDLE_FETCH_GRACE_SECONDS=2` (0..30),
`CANDLE_BACKFILL_MINUTES=300` (3..1440), `CANDLE_REQUEST_TIMEOUT_SECONDS=15` (1..60).
Only the experimental guest adapter changes mode. Other providers retain their tick pipeline.

Startup fetches the latest minute whose end plus grace has passed. Thereafter each cycle
calculates the next UTC boundary plus grace from the current wall clock. Bounded waits
notice clock changes; collection never accumulates sleep(60) drift. Slow cycles/outages are
recovered from durable coverage on the next cycle. A regressed clock does not create an
invalid range or finalize future buckets. Each request asks for at least three recent native
bars (one may be forming), and up to recovery-window+1 during catch-up. Only newly missing
completed minutes enter persistence; already covered minutes are not rewritten every cycle.

One guest socket is used for sequential chart requests during a collection, with heartbeat
echo/ping-pong. Chart sessions are deleted after each response; the socket closes between
cycles. No continuous quote feed is started. Request/response sizes, request timeouts,
subscriptions (local cap 100), history window and pending batch are bounded. Network failures
use bounded exponential backoff/jitter gates and resume on minute scheduling; malformed or
rejected symbols cannot create candles. Permanent source rejections require restart/operator
intervention. A failed symbol does not terminate unrelated providers.

Migration `0003` adds:

- `market_candles`: Decimal/NUMERIC OHLC, source identity, UTC bucket bounds, native basis,
  source/status/volume semantics and receipt/provenance. Logical uniqueness is
  `(instrument_id,provider,interval,bucket_start,price_basis)`.
- `latest_candles`: efficient per-instrument/provider/basis state independent of tick history.
- `candle_coverage`: durable coverage start/end and compact PostgreSQL `tstzmultirange` missing
  ranges. Long outages do not require one operational row per missed minute.

Run `uv run alembic -x timescale=true upgrade head` for a seven-day-chunk candle hypertable.
Omit the flag for plain PostgreSQL mode; no Timescale extension is required there. Existing
`market_ticks`, `latest_prices` and tick-derived continuous aggregates remain untouched.
COPY staging, candle UPSERT, latest UPSERT and coverage updates commit in one transaction.
An identical receipt retry changes no row; an explicitly re-fetched/imported same-minute bar
with a newer receipt can correct OHLC/volume/provenance. Older receipts cannot overwrite it.
Routine collection does not continuously re-fetch previously covered minutes for corrections;
automatic late-correction discovery is not implemented.

A DB outage retains one bounded batch and retries with backoff. No further fetch occurs
until it commits; no unlimited queue or disk spool is introduced. Shutdown cancellation logs
and counts unpersisted bars. Restart recovers missing native history within the configured
window. A cold start with no durable coverage begins at its latest closed minute; it cannot
promise recovery before the first successful database commit.

### Gaps, health, latest prices and APIs

Every minute after coverage starts is expected until a source actually supplies it. Missing
minutes remain in durable ranges, including during quiet/closed markets; there is no invented
market-open calendar or fabricated flat bar. After reconnect/restart, recent native history
backfills gaps within the configured wall-clock window. Older/source-absent minutes remain
explicit unresolved gaps. There is no unbounded archive replay or unsupported-history fallback.

`/providers` identifies `storage_mode:candles`, `expected_closed_minute`, and each symbol's
`last_persisted_minute`, `minutes_behind`, `missing_candle_count`, age and stale flag.
Health is healthy only when every configured instrument has the expected closed minute and
no unresolved gaps/fetch failures. Therefore an old gap keeps the feed degraded even if its
latest minute is present. A polling transport is intentionally disconnected while idle;
`state:healthy, transport:disconnected` means complete persisted candle coverage, not an open
quote socket. `/health` also includes candle-writer database health.

`/prices/latest?provider=tradingview_guest` reads latest_candles. It exposes `last` as the
latest completed close, `price_basis:candle_close`, `interval:1m`, bucket_start/bucket_end,
`age_ms` since bucket end, stale/threshold and provider state. A newer receipt of an old bar
does not refresh its bucket age. It is not presented as a current live tick.

`/candles/{canonical}?provider=tradingview_guest&interval=1m` defaults to `basis=native` in
candle mode. Explicit `basis=native` always reads market_candles, even in diagnostic tick
mode. Start/end are aware UTC interval-aligned `[start,end)` bounds; max 31 days, limit 1..1000,
with next_start/next_after_status pagination. Native 1m candles never depend on market_ticks.
`5m`, `15m`, `1h`, `1d` are on-demand SQL rollups from stored 1m candles; no extra upstream
subscriptions. Open/close use first/last constituent bars, high/low use extrema. Volume is
summed only when all constituents have the same additive verified type and non-null volume.
A larger bucket reports `complete:false` if still forming or missing constituent minutes;
observations counts constituent minute bars. Days are UTC days, not session/settlement days.
`1s` native requests are rejected. Explicit `basis=last|mid` still queries old tick aggregates.

`/ws/prices` canonical subscriptions receive `type:candle`, normalized OHLC/bucket/provenance
and `persisted:true` after commit. Tick providers still send `type:tick,persisted:false`.
Candle recovery can emit older backfilled buckets; consumers should use the logical key.

Metrics include `marketdata_candles_total{provider,operation}` for requested, received,
persisted, upserted, duplicates, fetch_failures, gaps_detected, backfilled and
shutdown_unpersisted. Persisted/upserted count affected UPSERT rows, not exclusively inserts;
exact receipt replays count as duplicates. Received includes valid returned bars before
already-covered-minute filtering. Gauges report missing minutes, latest closed bucket age
and scheduler lateness. Histograms report request/write duration. No tick-received/persisted
counter is incremented by candle ingestion. Errors and losses are explicit structured logs.

### Inspect stored data

```sql
SELECT * FROM market_candles ORDER BY bucket_start DESC LIMIT 100;

SELECT instrument_id, COUNT(*), MIN(bucket_start), MAX(bucket_start)
FROM market_candles GROUP BY instrument_id;

SELECT bucket_start, count(*) FROM market_candles
WHERE provider='tradingview_guest' GROUP BY bucket_start ORDER BY bucket_start DESC LIMIT 10;
\watch 5

SELECT i.canonical_symbol, c.coverage_start, c.coverage_end, c.missing
FROM candle_coverage c JOIN instruments i ON i.id=c.instrument_id;

SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest';
```

In psql, run the minute count query followed by `\watch 5`. Do not send the psql meta-command
through a SQL-only API. Live verification compares native requests to DB/REST OHLC and checks
WebSocket updates plus unchanged raw-tick count across multiple boundaries:

```powershell
uv run python scripts/smoke_tradingview_candles.py --minutes 3 --output artifacts/native-candle-smoke.json
```

The first live three-minute comparison verified all 17 symbols for 09:39-09:41 UTC on
2026-10-07. Raw ticks stayed 26,216. Historical [09:00,09:20) receipt-window rate was
556.9 ticks/minute versus measured 17 candle rows/minute: 96.947% fewer history rows,
32.76x fewer. This is an observed row-rate comparison across different windows, not a
compression/disk-space benchmark. The theoretical full-availability total is 24,480 rows/day.

### Optional raw-history maintenance

Nothing deletes user history automatically. Inspect existing volume:

```powershell
uv run python scripts/inspect_tradingview_ticks.py
uv run python scripts/inspect_tradingview_ticks.py --basis last --start 2026-10-07T09:00:00Z --end 2026-10-07T09:01:00Z --output artifacts/derived-last.json
```

The exclusive JSON export uses every eligible recorded last observation, or both bid and
ask for `--basis mid`, with deterministic first/max/min/last ordering. It marks derived
provenance, incomplete observation coverage, no native equivalence, null volume, source row
count and source fingerprint. It never inserts derived values into native market_candles.
Inspect OHLC/counts, preserve the export and a database backup before considering deletion.

Only an explicit operator invocation can delete a bounded exported window:

```powershell
uv run python scripts/inspect_tradingview_ticks.py --basis last --start 2026-10-07T09:00:00Z --end 2026-10-07T09:01:00Z --output artifacts/derived-last.json --delete-raw
```

Stop diagnostic tick ingestion first. The guarded serializable transaction locks raw history,
recomputes the export/fingerprint, and refuses changed exports or any window whose selected
basis does not cover **every** raw observation. Mixed last/quote windows will usually be
refused; retain them. Windows cap at one day and one million observations. The export is
retained. Verify results and refresh affected old continuous aggregates manually after a
permitted deletion. Deletion was tested only on a disposable test database; no live user
history was deleted in this implementation.
