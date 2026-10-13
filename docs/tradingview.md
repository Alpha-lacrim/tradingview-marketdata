# Experimental TradingView guest market data

This FastAPI branch separates collection from serving; its Compose guest overrides target
`collector`, while `app` serves database reads only. See [process separation](fastapi-process-separation.md)
for current role commands, health snapshots and WebSocket limits. Deployment commands and
measurements below describe the preceding combined deployment. Do not apply them to the
running collector during parallel development; its owner controls that deployment.

For the separately authorized 128/256/512-instrument multi-WebSocket investigation with
actual native candle commits, completeness/latency/resource evidence and controlled recovery,
see [live end-to-end experiment](candle-live-end-to-end.md), which used an isolated test database.
The ordinary 34-instrument pilot, durable activation and original-17 rollback are documented
in [production expansion](production-instrument-expansion.md).

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
`CANDLE_BACKFILL_MINUTES=300` (3..1440), `CANDLE_CORRECTION_LOOKBACK_MINUTES=3` (1..30,
no larger than recovery horizon), `CANDLE_REQUEST_TIMEOUT_SECONDS=15` (1..60).
`TRADINGVIEW_CANDLE_CONCURRENCY=2` bounds outstanding requests (1..12), with one socket
and one series per temporary chart session. `CANDLE_CYCLE_SAFETY_MARGIN_SECONDS=10`
(1..30) gives a default 48-second cycle-body warning budget; grace plus margin must be
less than 60 seconds. Configured concurrency is an upper bound: timeout/disconnect/protocol
symptoms halve the effective next-cycle window, down to one. It never automatically rises;
operator reconfiguration/restart is required. Existing minute-cycle backoff also applies.
Only the experimental guest adapter changes mode. Other providers retain their tick pipeline.

Startup immediately reconciles each instrument's own durable state through the latest minute
whose **end plus grace** has passed. Subsequent normal cycles calculate the next UTC boundary
plus grace, with deadline rechecks and bounded sleeps for clock changes. A per-bucket attempt
guard prevents repeated normal cycles or clock regressions from producing extra source polls.
Two seconds is a **start grace**, not a promise that all candles commit two seconds after close.
No once-per-second upstream polling or continuous quote subscription is used.

A small rolling overlap retrieves the last three completed minutes in that same cycle. Recent
missing/UNKNOWN ranges extend the request backwards, bounded by 300 wall-clock minutes and
301 bars (allowing a forming bar). Only completed, identity-validated native bars are accepted.
One guest socket has a bounded sliding window of chart sessions (default **two**), routed by
session, symbol and series identifiers through an independently owned receiver task. Dispatch
and session cleanup cannot stop that receiver from consuming available frames. IDs are never
reused across batches/connections, and a connection-generation check rejects old readers.
No simultaneous recv calls, new protocol commands or per-symbol sockets.
Each chart session closes after completion; the socket closes between cycles. Individual
symbol errors/timeouts remain visible without discarding successful unrelated responses.
Each request has its own deadline, measured from dispatch, not a giant batch timeout. Work
is locally capped at 100 instruments and the configured history horizon. Shutdown cancels
and joins the reader and all waiters. If a cycle crosses the next UTC target, an owned timer
counts/logs the overrun; the collector finishes its bounded work and schedules the next future
UTC boundary. It never starts overlapping cycles. The following cycle recovers skipped history.

A controlled two-series/shared-session experiment was explicitly rejected upstream with
`exceed limit of series in the session`. The adapter retains one series per chart session
and does not work around that restriction. The existing connection pool is now available to
the ordinary factory via TRADINGVIEW_CANDLE_POOL_ENABLED; it retains one series per session,
bounded workers, global request limits, paced starts, shared 429 cooldown and access rejection.
See [retrieval scalability measurements](candle-retrieval-scalability.md) for live versus
synthetic results, conservative escalation commands and the limits of 100-instrument claims.

### Schema, corrections and recovery

Migration `0003` created market_candles (optional seven-day-chunk Timescale hypertable),
latest_candles and candle_coverage. Apply head `0004`, which adds not_expected/unknown
multiranges, source session metadata and a candle_revisions audit table. All existing candles,
raw ticks and tick-derived continuous aggregates remain. Previously blanket-calendar missing
ranges become UNKNOWN until reconciled using source metadata; they are not silently declared
complete. Ordinary PostgreSQL works without Timescale. Downgrade is only for disposable tests
or an explicitly chosen operator rollback; 0004 downgrade removes the new audit/classification.

COPY staging, candle UPSERT, latest state, revisions and coverage commit atomically. The logical
key remains `(instrument_id,provider,interval,bucket_start,price_basis)`. Newer receipts can
correct changed native OHLC, volume, source status or realtime classification. Older receipts
cannot overwrite rows. An unchanged overlap bar leaves history/updated_at unchanged. A changed
bar updates the same row and logs its changed fields; candle_revisions retains exact textual
old/new OHLC/volume and discovery time. Receipt time is local observation time, **not an upstream
revision identifier**. There is no protection against an upstream cache serving an older value
with a newer local receipt. Corrections older than the overlap require a missing-minute fetch
or explicit reconciliation; the collector does not scan an unlimited archive.

Only inserted/corrected rows fan out after successful commit; unchanged overlap bars do not
produce repeated historical WebSocket messages. Latest close still uses latest_candles and is
not freshened by a receipt of an older bucket. TVC oil volume remains NULL. 5m/15m/1h/1d are
on-demand SQL rollups of persisted 1m, so corrections are visible on the next query. Volume is
summed only for a single verified additive type (crypto base volume or OANDA tick count).
No materialization refresh lag is introduced for native candles.

DB failures retain at most one bounded batch and retry writes with jitter, without fetching
more upstream data or acknowledging persistence. The API reports DB/writer degradation.
When DB reads fail before retrieval, that minute attempts no upstream request. Recovery reads
actual durable state, then the next minute cycle backfills recent history. If a retained write
finally commits after several minutes, the following scheduled cycle reconciles those missed
minutes. Cancellation logs/counts the unpersisted batch. There is no disk spool or crash-safe
queue; recent native history is the recovery source. Cold start without a committed coverage
anchor starts at its latest closed minute; it cannot promise earlier pre-installation history.

Network failures preserve previous committed data, record fetch failures, and retry on minute
cycles behind a bounded backoff gate. Reconnect restores chart requests and validates metadata
again. Short recoverable outages reconcile expected gaps; longer outages recover only available
recent bars, retaining older expected gaps and UNKNOWN ranges. Missing historical bars are
never fabricated. A provider-wide access/protocol rejection disables subscriptions until
operator intervention/restart, without introducing authenticated access.

### Session expectations and coverage health

For completed minutes since the coverage anchor:

- **EXPECTED**: supported source session says trading should be active, or a native candle
  positively proves that minute exists. EXPECTED = persisted bars + unresolved expected gaps.
- **NOT_EXPECTED**: supported metadata establishes a closure/break/holiday; no gap is opened.
- **UNKNOWN**: absent/unsupported calendar, correction-date syntax or uncertain maintenance
  means absence cannot honestly be called data loss. Exposed separately and degrades health.

Interpretation follows TradingView's [documented chart session format](https://www.tradingview.com/charting-library-docs/latest/connecting_data/time-and-sessions/Trading-Sessions/):
local IANA time zones/DST, exclusive ends, default Monday-Friday trading days, overnight trading
*day* attribution, explicit days, multiple simple sessions, F previous-day starts and default
Monday-first historical schedule changes. Captured Binance 24x7 and OKX seven-day sessions
remain expected on weekends. OANDA 1700-1700 New York means Sunday evening through Friday
evening, **not** seven days. TVC's supported metadata describes its breaks/holidays; it is not
substituted with a futures exchange calendar. Date-specific correction days and unsupported
F-number/extended syntax remain UNKNOWN rather than using an invented precise schedule.

[OANDA's published operating hours](https://www.oanda.com/bvi-en/cfds/hours-of-operation/)
identify daily FX/metal maintenance around the New York close. Those account trading hours
have not been verified as exact guest-chart availability. Therefore the documented vicinity
16:59-17:05 FX / 16:59-18:05 metals is conservatively UNKNOWN for absent bars, while actual
native bars remain positive evidence. Neither this policy nor source metadata is a complete
venue-maintenance/exceptional-holiday calendar. A session being open does not guarantee a trade
every minute: sparse instruments such as XAUT may omit native bars. Such unresolved scheduled
minutes stay visible; no flat candle is fabricated or claimed to represent data loss proven
at the exchange. Calendar provenance is persisted; reconciliation uses the latest metadata,
with only supported historical-session syntax, not a versioned independent calendar archive.

`/providers` exposes expected_closed_minute, storage_mode and each symbol's session_expectation,
latest_expected_completed_bucket, last_persisted_minute, minutes_behind (expected missing
minutes after latest), missing_candle_count, unknown_minutes, not_expected_minutes,
oldest_unresolved_expected_gap, recoverable_missing_count, outside_horizon_missing_count,
backfill_horizon_minutes and fetch/persistence/correction timestamps. Known closed markets can
remain healthy using their last expected completed bucket; old closes still show their real
age. Any unresolved expected historical gap, UNKNOWN coverage or fetch failure prevents healthy
coverage. Idle polling transport=disconnected is intentional. `/health` remains 503 while
provider or DB health is degraded; `/providers` remains accessible during a DB outage.

Latest endpoints expose candle_close, 1m bucket bounds, actual age since bucket end and stale
status. They never present a completed close as a live tick. Native candle histories remain
bounded/aligned [start,end), max 31 days, limit 1..1000 with pagination. Larger buckets can be
partial at session boundaries; `complete` conservatively requires every wall-clock constituent
minute, not a claim about session-adjusted completeness. Explicit last/mid basis still queries
preserved historical ticks; native 1s is unsupported.

Measured results and limits: [hardening verification](candle-hardening-verification.md).

### Timing, metrics and soak observation

The collector records bucket_end, scheduler target/wake, cycle start/finish, request start,
first relevant message, completed-bar-message availability, parser completion, DB write start
and **post-transaction-return commit acknowledgement**. Source message availability precedes
validation; it does not claim the exchange's event-to-publication latency. Stage durations use
monotonic clocks; wall-clock close latencies are clamped at zero for clock changes. The full
latest cycle is in /providers and a structured cycle-completed log. No full upstream frames or
credentials are exposed. Parser and per-symbol request durations are independently measured.

Prometheus metric mapping (all marketdata_ prefixed):

- candle_fetch_cycle_total counts collection attempts, including failed DB reads.
- candle_timing_seconds{stage=...}: scheduler_lateness, grace_to_fetch_start,
  fetch_cycle_duration, close_to_first_receive, close_to_last_receive,
  first_to_last_receive, close_to_p50_receive, close_to_p95_receive, cycle_duration,
  parser, db_write, close_to_commit.
- candle_symbol_fetch_seconds{provider,symbol}: bounded configured-symbol request latency,
  including failed/timed-out requests.
- candles_total{operation=...}: requested, received, persisted/upserted (changed rows),
  duplicates (unchanged/older rows), corrections, backfilled, fetch_failures, gap_open,
  gap_close and shutdown_unpersisted. Replays after an uncertain commit are idempotent;
  counters are process-local and not a financial audit ledger.
- candle_missing_minutes, candle_minutes_behind, candle_expected_state{expectation=...},
  candle_age_seconds and candle_scheduler_lateness_seconds are coverage/lag gauges.
- database_errors_total and reconnects_total expose outage attempts independently.
- candle_request_concurrency, candle_requests_inflight, candle_cycle_instrument_count,
  candle_request_timeout_total, candle_cycle_overrun_total and candle_response_ignored_total
  expose bounded pipeline operation. tradingview_socket_count, tradingview_socket_disconnect_total,
  tradingview_request_error_total{category} and tradingview_possible_throttle_total{reason}
  distinguish connection errors and overload **symptoms**, not proof of upstream throttling.

Operational transitions log session expectation changes, gap opens/closes, outside-horizon gaps,
corrections, slow requests (>5s), cycle start/completion, DB unavailability/recovery and shutdown
losses. DB retry logs are limited to the first failure per retained batch; session/outside-gap
logs emit only on changes. Unchanged overlap bars are not individually logged. Tick counters
are not candle-mode primary health metrics.

Read-only soak observation polls the **local** API/DB, never the upstream source. It records
cycle summaries, per-symbol distributions, normal-cycle stage distributions (startup excluded),
and `complete_cycle_latency` restricted to all-instrument successful normal cycles. Always
report failed/partial and coverage-only cycles alongside successful latency quantiles; an
empty backoff cycle must not appear to be a fast complete collection. It also records
counters/resets, corrections/backfills, gaps,
closures/uncertainty, raw row counts, duplicates and malformed/forming bars. No fault injection
or memory measurement is claimed. Use the same single collector throughout each run:

```powershell
uv run python scripts/soak_candles.py --seconds 3600 --output artifacts/soak-1h.json
uv run python scripts/soak_candles.py --seconds 21600 --output artifacts/soak-6h.json
uv run python scripts/soak_candles.py --seconds 86400 --output artifacts/soak-24h.json
uv run python scripts/soak_candles.py --seconds 259200 --output artifacts/soak-72h.json
uv run python scripts/reconcile_candles.py --start 2026-10-07T10:00:00Z --end 2026-10-07T11:00:00Z --output artifacts/reconciliation.json
```

The reconciliation command is read-only and reports EXPECTED/NOT_EXPECTED/UNKNOWN, persisted,
missing expected, backfilled and corrected minutes per instrument/provider, plus the coverage
invariant. Use completed aligned UTC ranges up to 31 days. Reconciliation before the collector's
anchor describes missing scheduled history, not an ingestion guarantee before installation.
The tests deterministically inject transport failures and independent DB read/write failures,
including recovery beyond the horizon; no live access is needed. A manual live outage check
is: record raw count and coverage, stop/restart only the app (restart recovery), or briefly
stop/restart only DB; inspect /health and /providers during interruption, then reconcile all
completed outage minutes after the next collection cycle. Preserve the DB volume. Do not use
`down -v`. Deliberate Internet interruption is operator-controlled; never claim a simulated
transport failure was a live network outage.

### Inspect stored data

```sql
SELECT * FROM market_candles ORDER BY bucket_start DESC LIMIT 100;

SELECT instrument_id, COUNT(*), MIN(bucket_start), MAX(bucket_start)
FROM market_candles GROUP BY instrument_id;

SELECT bucket_start, count(*) FROM market_candles
WHERE provider='tradingview_guest' GROUP BY bucket_start ORDER BY bucket_start DESC LIMIT 10;
\watch 5

SELECT i.canonical_symbol, c.coverage_start, c.coverage_end,
       c.missing AS expected_gaps, c.not_expected, c.unknown
FROM candle_coverage c JOIN instruments i ON i.id=c.instrument_id;

SELECT i.canonical_symbol,r.bucket_start,r.changed_fields,r.discovered_at
FROM candle_revisions r JOIN instruments i ON i.id=r.instrument_id
ORDER BY r.discovered_at DESC LIMIT 100;

-- Bounded range partitions, using durable classifications (inside coverage anchor).
WITH minutes AS (
  SELECT generate_series('2026-10-07 11:20:00+00'::timestamptz,
                         '2026-10-07 11:39:00+00'::timestamptz,
                         interval '1 minute') AS bucket
)
SELECT i.canonical_symbol,
       count(*) FILTER (WHERE bucket <@ c.not_expected) AS not_expected,
       count(*) FILTER (WHERE bucket <@ c.unknown) AS unknown,
       count(*) FILTER (WHERE bucket <@ c.missing) AS expected_missing,
       count(*) FILTER (WHERE NOT bucket <@ (c.not_expected+c.unknown)) AS expected
FROM candle_coverage c JOIN instruments i ON i.id=c.instrument_id
CROSS JOIN minutes
WHERE bucket >= c.coverage_start AND bucket < c.coverage_end
GROUP BY i.canonical_symbol;

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
