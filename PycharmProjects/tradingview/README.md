# Multi-asset market data

Asynchronous market-data ingestion into PostgreSQL/TimescaleDB, with REST history, latest
prices, OHLCV, provider health and downstream WebSocket subscriptions. Documented vendor
adapters are supplemented by an explicitly requested, opt-in experimental TradingView guest
collector. Read [its limitations and activation steps](docs/tradingview.md) before enabling it.

The default is **simulated data only**: four crypto pairs, eight FX pairs, gold/silver spot
simulations, WTI/Brent CFD simulations, and two synthetic concrete contracts for each of
GC, SI, CL, Brent, ES, NQ and YM (30 instruments). Futures use the `SIM` venue and explicitly
synthetic schedules, not invented exchange dates.

## Local setup

Docker Compose v2.24+ and a running Linux Docker engine:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/prices/latest
```

The migration service runs before the app. Open [API docs](http://localhost:8000/docs).
`docker compose down` stops the services and retains the named database volume. Ports bind
to localhost; the application runs as an unprivileged user with one Uvicorn worker.

For native development with Python 3.12+ and uv:

```powershell
uv sync --frozen --extra databento
docker compose up -d db
uv run alembic -x timescale=true upgrade head
uv run uvicorn marketdata.api:create_app --factory --workers 1 --ws-max-size 16384
```

Run either the native app or the container app on port 8000. Databento's official SDK is
optional; omit its extra for mock/Binance/OANDA. Application startup requires schema revision
`0003` and never creates tables. `.env.example` contains safe local development defaults.

## Architecture and reliability

```text
Provider transport -> parser -> Decimal/UTC Tick -> bounded asyncio queue -> batch writer
                                             |                           |
                                             v                           v
                                    bounded client queues          COPY temporary stage
                                             |                           |
                                      WebSocket clients        atomic history + latest UPSERT
                                                                         |
                                                             REST / Timescale candles
```

Each provider has an owned task and independent reconnect/backoff/jitter. Subscription sets
are restored after reconnect. Authentication, entitlement and invalid subscription failures
stop that provider for operator intervention while other feeds continue. Connection timeouts,
ping/pong and provider heartbeats support stale detection. Health includes per-instrument
freshness; a quiet/closed market can be stale without a broken transport.
Transport connection alone never marks a feed healthy: all configured symbols must have
fresh accepted observations in the current connection. `/providers` separates transport,
subscription restoration and feed health, and reports per-symbol age/count/rejection state.
Guest frame/observation timeouts and source-specific freshness overrides are documented in
[TradingView operational health](docs/tradingview.md#operational-health-and-freshness).

The writer retains one batch and retries during database outages. Receive loops continue
until the bounded queue fills, then **new ticks are dropped** with exact metrics and explicit
error logs. Logs summarize drops at most once per provider/reason/second. Memory capacity is
the queue plus one pending batch, plus bounded downstream/SDK queues. Persistent schema/data
errors retain the batch for operator intervention. There is no disk spool or crash-safe
recovery of queued observations.

SIGTERM/SIGINT are handled by Uvicorn's lifespan. Shutdown stops providers, drains until
`SHUTDOWN_TIMEOUT_SECONDS`, reports remaining observations as lost and closes the pool.
WebSocket observations are emitted before persistence acknowledgement and include
`persisted: false`. Slow consumers receive close code 1013 when their queue overflows.

This version combines ingestion and serving in **one process**. Multiple Uvicorn workers or
replicas would create duplicate subscriptions and independent fan-out; do not enable them.
No Redis, Kafka, RabbitMQ or large observability stack is required.

## Provider coverage

| Adapter | Implemented coverage | Status and requirements |
|---|---|---|
| Mock | All initial categories | simulated, offline; aggregate tick frequency/seed configurable |
| Binance Spot | Configurable spot pairs, initially BTC/ETH/SOL/XRP against USDT | realtime individual trades; public API; exchangeInfo validation |
| OANDA v20 | Account-available FX, metals and explicit CFDs | practice simulated; live conservatively indicative; token/account required |
| Databento Live SDK | Concrete outright exchange futures, one session per dataset | unknown unless operator verifies realtime/delayed entitlement; paid account required |
| TradingView guest (experimental) | 17 verified native 1m chart series; diagnostic quote mode | unsupported observed guest protocol; runtime realtime/identity gate; usage-rights restrictions; no exchange futures |

Official protocols were checked on 2026-10-07. Implemented adapters do not imply activated
feeds: Binance connectivity failed with `ConnectError` in this environment; authenticated
OANDA/Databento sessions have not been tested because credentials were not supplied.
The operator reports Binance/OANDA network blocking. TradingView guest quotes were reached
from this environment; domestic exchange feeds are excluded by operator preference.

Binance uses individual trades (`last`, `last_size`); bid/ask remain null. Trade IDs are
retained, gaps logged, and exact trade replays safely deduplicated. Reconnect does not backfill
missed trades. The `websockets` library handles control-frame ping/pong and bounded buffering.

OANDA emits sampled, account-specific quotes, up to four updates per instrument per second.
`CURRENCY` maps to OTC FX, `METAL` to broker OTC metal pricing, and `CFD` stays a CFD. OANDA
metals are not physical bullion or COMEX futures. Instrument availability depends on the
account/region. Energy symbols must come from account discovery; none are guessed.

Databento uses `mbp-1` with concrete `raw_symbol` subscriptions. Definition replay validates
instrument IDs, outright classification, exchange, root, currency, delivery month, expiry,
constant tick size and contract size before observations are accepted. Undefined prices stay
null, negative futures prices are valid, and last/last_size are populated only on trades.
Original sequence values are retained; filtered venue sequences are not assumed contiguous.
`F_MAYBE_BAD_BOOK` observations are explicitly rejected/counted. SDK callbacks use a bounded
thread-safe bridge. Reconnect resumes live observations; automatic tick replay/backfill is
not implemented, and upstream errors require intervention.

`GLBX.MDP3` covers CME Globex products, including CME/COMEX/NYMEX futures; `IFEU.IMPACT`
supports ICE Europe futures. Requested GC/SI/CL/ES/NQ/YM and ICE Brent contracts require
current vendor definitions and the applicable subscriptions. **Genuine real-time CME,
COMEX, NYMEX and ICE data requires exchange agreements/entitlements.** Confirm account
pricing and redistribution rights with the provider; this service does not grant them.
Physical WTI/Brent spot/reference pricing is not implemented. CFD prices and exchange
futures are distinct instruments throughout the pipeline.

Official references:

- [Binance WebSocket streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), [exchangeInfo](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general)
- [OANDA environments](https://developer.oanda.com/rest-live-v20/development-guide/), [account instruments](https://developer.oanda.com/rest-live-v20/account-ep/), [pricing stream](https://developer.oanda.com/rest-live-v20/pricing-ep/), [instrument types](https://developer.oanda.com/rest-live-v20/primitives-df/)
- [Databento Live API](https://databento.com/docs/api-reference-live/client/live), [MBP-1](https://databento.com/docs/schemas-and-data-formats/mbp-1), [definitions](https://databento.com/docs/schemas-and-data-formats/instrument-definitions), [conventions](https://databento.com/docs/standards-and-conventions/common-fields-enums-types), [licensing](https://databento.com/pricing)
- [Timescale continuous aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/about-continuous-aggregates/)

## Configuration and live activation

See `.env.example` and typed validation in `src/marketdata/config.py`.
For the experimental guest source, use [the dedicated Compose override](docs/tradingview.md)
and `config/instruments.tradingview.json`; it rejects unverified/delayed delivery at runtime.

- `MARKET_DATA_PROVIDER_CRYPTO`, `_FOREX`, `_METALS`, `_ENERGY`, `_FUTURES`: select `mock`,
  `binance`, `oanda`, `databento`, `tradingview_guest` or `disabled`. All futures use `_FUTURES`,
  including metals/energy; the guest adapter does not support exchange futures.
- `REGISTRY_FILE`: JSON instrument/mapping list. Missing selected mappings fail configuration;
  live feeds never fall back to mock. Restart for upstream subscription changes.
- `ENABLED_ASSET_CLASSES`: JSON array. Queue/batch/pool/flush/stale/reconnect/mock/WebSocket
  limits and log level are validated. See the example for all variable names.
- `DATABASE_URL`: PostgreSQL DSN. Compose overrides it with internal `db` host and
  `POSTGRES_PASSWORD`; its database port is controlled by `POSTGRES_PORT`.
- Optional `API_KEY`: bearer authorization for HTTP/WebSocket requests except `/health`.
  Use TLS and deployment access controls consistent with redistribution licenses. Browser
  clients need a trusted backend to set the WebSocket authorization header; secrets are not
  URL parameters. Provider tokens live in environment/secret management, never registry files.

For crypto, set `REGISTRY_FILE=config/instruments.binance.json` and
`MARKET_DATA_PROVIDER_CRYPTO=binance`. Only providers with entries in the selected registry start.

For OANDA, set `OANDA_ENVIRONMENT=practice` or `live`, `OANDA_API_TOKEN`, `OANDA_ACCOUNT_ID`:

```powershell
uv run python -m marketdata.cli discover-oanda
uv run python -m marketdata.cli discover-oanda --symbols EUR_USD,GBP_USD --output config/oanda.local.json
```

Choose any further FX/metals/energy instruments **from the actual account output**.
`--cfd-asset-class energy` categorizes explicitly selected CFDs. Set relevant selectors to
`oanda` and point `REGISTRY_FILE` at the output. Discovery is read-only; this service never
submits orders. Generated files are created exclusively, never overwritten.

For futures, obtain an entitled official instrument-definition DBN export. Offline import
performs no billable historical API requests:

```text
uv run python -m marketdata.cli import-databento --file definitions.dbn --symbols <concrete-raw-symbols> --asset-class futures --source "vendor export provenance" --rolls rolls.json --status unknown --output config/futures.local.json
```

Use `metals`, `energy` or `futures` as appropriate. Delivery month comes from authoritative
`maturity_year`/`maturity_month`, never expiry. Options, spreads, missing constant tick/contract
sizes and duplicate canonical months are rejected. `rolls.json` maps raw symbols to explicit
aware `roll_at` timestamps. Without it ingestion works, but front/next resolution is refused.
Verify units and subscriptions before setting realtime status. Set `DATABENTO_API_KEY`,
`MARKET_DATA_PROVIDER_FUTURES=databento`, and the registry path.

Merge registry JSON arrays to combine categories. One canonical instrument may have multiple
provider mappings; duplicate identities and ambiguous mappings fail validation. Add pairs
through registry configuration, without source edits. Add adapters by implementing
`MarketDataProvider`, factory/config selection and official protocol fixtures/error tests.
Provider code does not import persistence. Subscribe/unsubscribe stage upstream subscriptions
for the next connection; downstream subscriptions only control application fan-out.

## Identity and futures rollover

Instrument IDs are UUIDv5 values from canonical identities. Binance spot uses
`CRYPTO:BTCUSDT`; OTC venues use `FX:OANDA:EURUSD`, `SPOT:OANDA:XAUUSD`; CFDs use
`CFD:<venue>:<product>`. Imported futures use `FUTURES:<exchange>:<root>:<YYYYMM>`, retain
the actual contract code/provider symbol, and reject same-root/month ambiguity. Existing
identity cannot be reassigned to another asset/type, exchange, currency or contract.

FRONT/NEXT are logical resolvers only. Choose the earliest active delivery month with
`roll_at > at`, or the following eligible contract for next. Operators provide product-aware
schedules before expiry, accounting for delivery/liquidity. No expiry-day or fixed-business-day
roll rule is assumed. All history stays under concrete contracts; there is no fabricated
continuous or back-adjusted series.

## Database, timestamps and candles

`0001` creates `instruments`, `provider_instruments`, `market_ticks`, `latest_prices`.
Prices/quantities use Decimal -> unconstrained PostgreSQL NUMERIC -> decimal strings in JSON.
Binary float inputs and nonfinite prices are rejected. Unavailable fields remain null.
Latest state is the latest **complete observation per instrument/provider**, not a consolidated
price; quote updates do not inherit old trade values with mismatched timestamps.

Event time and local receipt time remain distinct, UTC-aware values. Source nanoseconds are
retained for Binance/Databento/OANDA; TIMESTAMPTZ itself has microsecond precision. Missing
event time stays null. `storage_timestamp` uses source time when available, otherwise receipt
time, and is the history partition/query key. Source times are never rewritten to look ordered.

COPY to a temporary stage and history/latest UPSERT happen in one transaction. The key
`(storage_timestamp,tick_id)` makes uncertain-commit retries idempotent. Binance trade IDs
yield stable UUIDs. Other feeds keep repeated events unless safe upstream identity is known;
equal price values are never a deduplication rule. Latest ordering uses event nanoseconds,
receipt time and UUID. Indexes serve canonical/provider/time history and latest lookups.

`0002` optionally creates a daily-chunk hypertable and 12 continuous aggregates (six intervals,
each for last/mid). Enable on initial upgrade using `alembic -x timescale=true upgrade head`.
Plain PostgreSQL uses `alembic upgrade head` with on-demand candles. To enable Timescale after
a plain `0002`, stop ingestion, back up, downgrade to `0001` and upgrade with the flag.
Hypertable downgrade rebuilds the ordinary table and requires maintenance time/free space.

Intervals: `1s`, `1m`, `5m`, `15m`, `1h`, `1d`, UTC epoch aligned, not settlement/session days.
Last candles use observed trades and sum `last_size`, never vendor cumulative volume. Mid
requires bid+ask and has null volume. No silent basis fallback or empty-bucket fill. OHLC ties
are deterministic by source nanoseconds, receipt time, then UUID. Current buckets report
`complete: false`. Provider and data status remain separate aggregate dimensions.

Real-time aggregates include the unmaterialized tail. Policies refresh the preceding seven
days every minute, excluding the newest bucket. Older backfills/corrections need manual
`CALL refresh_continuous_aggregate(...)`. No retention/compression policy is imposed without
storage requirements. Pool/query/write timeouts are bounded.

## APIs

The experimental guest source defaults to **completed native 1-minute candle storage**.
`0003` adds `market_candles`, `latest_candles` and durable `candle_coverage`; it preserves all
raw history and tick continuous aggregates. Timescale upgrades create a seven-day-chunk candle
hypertable; ordinary PostgreSQL remains supported. One bounded collection runs at each UTC
minute boundary plus `CANDLE_FETCH_GRACE_SECONDS=2`, with immediate closed-minute recovery at
startup. `TRADINGVIEW_STORAGE_MODE=ticks` explicitly restores the old diagnostic quote path.
Other providers retain their existing ingestion behaviour.

`/candles/{symbol}?provider=tradingview_guest` reads persisted native candles directly; use
`basis=last` or `mid` explicitly for old tick-derived history. Native `1m`, `5m`, `15m`, `1h`
and `1d` use stored 1m bars; larger intervals are SQL rollups with incomplete coverage marked
`complete:false`. No 1-second native candles are offered. Latest guest prices expose
`price_basis:candle_close`, interval, bucket bounds, close as `last`, age and staleness.
Guest WebSocket updates have `type:candle`, native OHLC and `persisted:true` after commit.
A one-minute sampled price is never presented as OHLC.

See [candle operations and recovery](docs/tradingview.md#native-one-minute-candle-storage)
and the [17-instrument verification matrix and measured reduction](docs/native-candle-verification.md).
Run the opt-in live multi-minute check with:

```powershell
uv run python scripts/smoke_tradingview_candles.py --minutes 3 --output artifacts/native-candle-smoke.json
```

Missing minutes remain explicit, including quiet/closed sessions: no market calendar or
fabricated bars. Bounded native historical recovery defaults to 300 minutes. Existing tick
inspection, verified derived exports and optional guarded deletion are documented separately;
normal startup/migrations never delete raw data.

| Endpoint | Behaviour |
|---|---|
| `/health` | App/DB/writer/feed health; 503 when degraded |
| `/providers` | Sanitized state, errors, reconnects and instrument freshness |
| `/metrics` | Prometheus received/persisted/drop/reconnect/parse/gap counters, queue/latency |
| `/instruments?limit=100&after=...` | Ordered canonical page; use last symbol for next page |
| `/instruments/{symbol}` | Instrument and provider mappings |
| `/prices/latest?provider=mock&limit=100&after=...` | Latest page with `next_after` |
| `/prices/latest/{symbol}?provider=mock` | Latest observation(s), per provider |
| `/ticks/{symbol}?start=...&end=...&limit=100&cursor=...` | History with `next_cursor` |
| `/candles/{symbol}?provider=mock&interval=1m&basis=last` | Candles with `next_start` |
| `/futures/{exchange}/{root}/front` or `/next` | Concrete resolution; optional aware `at` |
| WebSocket `/ws/prices` | Canonical subscribe/unsubscribe; normalized ticks |

All table endpoints are GET. History ranges are `[start,end)`, default one hour. Tick windows
cap at one day; candle windows cap at 31 days (one day without aggregates). Limits: 1..1000.
Explicit candle boundaries must align to intervals; defaults include the forming bucket.
Keep the original end fixed when paging. Event-time pagination is not a snapshot: late
events behind a consumed cursor may require a later overlap query.
If a candle page splits statuses within one bucket, also pass `next_after_status` as
`after_status` with `next_start`; this preserves distinct simulated/live/delayed histories.

Both latest-price endpoints add `age_ms`, `stale`, `stale_after_seconds` and `provider_state`.
For tick providers, age uses each persisted observation's local receipt time; pending/disconnected/rejected or
inactive sources are stale. Feed recovery cannot make an old database row fresh. This is
receipt freshness, separate from upstream event age, data classification or market-open status.
Native candle-close age instead measures time since `bucket_end`; candle coverage determines
staleness. An intentionally idle polling transport can be disconnected while coverage is healthy.
`/health` includes `database_state` and `provider_transports`; degraded components return 503.

```json
{"action":"subscribe","symbols":["CRYPTO:BTCUSDT","FX:SIM:EURUSD","SPOT:SIM:XAUUSD"]}
```

Acknowledgements have `type: subscribed`; updates have `type: tick`, canonical identity,
symbol, provider, source/receipt times, available prices/sizes, status and `persisted: false`.
Unknown symbols/invalid commands receive an error envelope. Credentials are never exposed.

## Validation and load testing

```powershell
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/smoke.py
```

The offline suite needs no credentials or live markets. Integration tests skip unless
`TEST_DATABASE_URL` points to a migrated disposable database ending in `_test`; fixtures
clear its market-data tables. Create it separately:

```powershell
docker compose exec -T db createdb -U marketdata marketdata_test
$env:DATABASE_URL='postgresql://marketdata:marketdata@localhost:5432/marketdata_test'
uv run alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q
```

The smoke utility expects a running default mock app at localhost:8000. CI is configured
for both migration modes and downgrade/re-upgrade. It has not been run remotely.

```powershell
uv run python -m marketdata.benchmark --seconds 10 --rate 5000 --instruments 300 --output artifacts/benchmark.json
```

Use a separate migrated benchmark database: each run writes a new `BENCH:SIM:*` namespace.
A local Windows/Python 3.12 -> Docker TimescaleDB measurement persisted 50,000/50,000 ticks
across 300 instruments in 10.155 seconds including drain (~4,924 rows/sec), zero drops and
163 ms mean database batch duration. This short synthetic result is not a sustained capacity
guarantee or live-feed latency benchmark.

## Troubleshooting and next work

- Start Docker Desktop in Linux-container mode if its engine is unavailable.
- Schema startup error: migrate the same database used by the app.
- Degraded live feed: inspect `/providers` and sanitized logs; verify credentials, account
  symbols, definitions and entitlements. Do not bypass restrictions or disable TLS checks.
- Stale with heartbeats: instruments can be quiet/closed; no exchange-calendar service or
  invented market-open flag exists.
- DB outage: watch queue depth, database errors and dropped counters. Persisted latest state
  can lag WebSocket observations. Recover before bounded capacity is exhausted.
- Old backfill missing from candles: explicitly refresh affected aggregate views.

Priorities before live production deployment: activate/soak-test entitled feeds and concrete
definitions, choose product-specific rolls/calendars, measure longer load/outage recovery,
and configure backup/retention and deployment access controls. Durable backfill/replay,
crash-safe spooling, dynamic upstream subscriptions, per-user downstream entitlements and
multi-process fan-out remain outside this version.

### Native candle reliability

The experimental guest collector uses one UTC-minute cycle plus two seconds of start grace,
a three-minute correction overlap and a 300-minute bounded recovery horizon. Bounded outstanding
chart requests (configurable upper bound, default two) share one socket. An independent receiver
routes replies while the bounded dispatcher sends/cleans up sessions. Individual timeouts reduce
the next-cycle window; a cycle overrun is counted and recovered without overlapping collectors.
Migration 0004 adds session-aware tri-state coverage and an
atomic correction audit; it preserves all native candles and historical raw ticks. EXPECTED
gaps and UNKNOWN coverage keep health degraded; supported closures do not create false gaps.
Larger native timeframes query corrected 1m rows directly. See [guest operations](docs/tradingview.md)
for timestamp/latency semantics, outage recovery, Prometheus labels, read-only soak commands
and historical reconciliation. No multi-hour reliability claim is made without an executed soak.

See [retrieval scalability](docs/candle-retrieval-scalability.md) for the live/synthetic benchmark
matrix and capacity limits. The 100-instrument result is **synthetic only**; shared-session
multi-series was explicitly rejected upstream, and no socket pool was introduced. The offline
benchmark exercises 17/34/64/100 instruments at windows 1/2/4/6/8/12 and bounded 300-minute
history retrieval without modifying the production registry. To verify the normal collector's
WebSocket/DB/REST path without additional upstream requests:

```powershell
uv run python scripts/smoke_tradingview_candles.py --stored-only --minutes 3 --output artifacts/stored-candle-smoke.json
```
