# Market Data REST and WebSocket API v1

**Batch 4:** [employer qualification](api-batch4-verification.md),
[frontend handoff](frontend-integration.md), [production deployment](production-deployment.md).
`GET /livez` is a public, database-independent liveness probe, excluded from OpenAPI.
Versioned health adds `websocket_delivery` (idle/operational/degraded/unknown). Production
startup requires a dedicated SELECT-only DB principal, with no schema CREATE/database TEMP
or market-table write/administrative privileges. Unexpected errors return sanitized 500
responses; all market responses disable caching. Production commands disable access logs.

The company API reads persisted PostgreSQL/TimescaleDB records. It never contacts
TradingView during HTTP requests. Base URL for the isolated local example is
`http://127.0.0.1:8002/api/v1`; the deployed HTTPS hostname must be supplied by the operator.
This branch is locally verified and **undeployed**. The collector and its instrument
selection are independent. The 99-source business proposal remains provisional.

## Authentication and browser access

Use `Authorization: Bearer <token>` for all v1 requests. `API_KEY` comes from an environment
variable/secret manager. Set `API_ENVIRONMENT=production`: startup rejects a missing token
or one shorter than 32 characters. Generate a strong random token, rotate it through your
secret-management process, and serve the API over TLS. No production credential is provided
in this repository. All holders have the same market-data read permission; the API does
not implement individual user accounts or token scopes.

Missing/malformed authorization returns **401**, with `WWW-Authenticate: Bearer`.
An incorrect bearer token returns **403**. Comparison is constant-time; the scheme is
case-insensitive. Token values and database credentials do not appear in API errors.
`API_ENVIRONMENT=development` (the default) or `test` may omit `API_KEY`, deliberately
allowing local unauthenticated access. Mark production explicitly; do not reuse development
configuration there. `/health` is the public minimal legacy readiness probe. Versioned
health, metrics and all data reads require authentication. Documentation/schema routes
are public and contain contracts, not market records or credentials.

Do **not** place the permanent service bearer token in browser JavaScript. The supported
**PROVISIONAL** integration option is browser → company backend-for-frontend (BFF) → market-data API.
The frontend is unfinished and its final authentication architecture is unknown. This is
an optional adapter design, not a finalized company requirement; delivery depends only on
API bearer authentication and explicit WebSocket Origin validation.
The company backend authenticates/authorizes users using its existing identity/session
system and adds the service token on the server. Prefer same-origin browser requests and
Secure, HttpOnly, SameSite session cookies. The market-data service accepts bearer headers,
not browser session cookies. User-specific authorization, session expiry and rate limits
belong to the company backend/gateway. No OAuth service or new infrastructure is introduced.

If an approved client needs cross-origin access, set
`API_CORS_ORIGINS=["https://frontend.company.example"]`. Origins must be exact scheme/host/
port strings without trailing slash, path, wildcard or credentials. Production requires
HTTPS origins. Only GET and Authorization/Content-Type headers are allowed. Credentialed
cookie CORS is disabled. Allowed preflight succeeds without a token; disallowed preflight
returns 400 without an allow-origin header. Allowed-origin 401/403/503 responses retain
CORS headers. CORS is a browser policy, not authorization.

## Endpoint inventory and compatibility

The OpenAPI inventory is **16 GET operations** (the 15 Batch 2 routes plus REST reconciliation):

| Route | Starting-route classification | Contract |
| --- | --- | --- |
| `/api/v1/instruments` | NEW | Paginated configured source identities and verified persisted availability |
| `/api/v1/prices/latest` | NEW | Bulk persisted quote/candle-close/unavailable results |
| `/api/v1/candles` | NEW | Completed native 1m history and derived UTC intervals |
| `/api/v1/candles/latest` | NEW | Bulk latest completed native 1m candles |
| `/api/v1/coverage` | NEW | Bounded tri-state completeness and missing ranges |
| `/api/v1/health` | NEW | Authenticated API/database/collector/freshness/coverage summary |
| `/api/v1/marketdata/changes` | NEW in Batch 3 | Bounded commit-ordered replay; expired cursor returns 409 |
| `/health` | KEEP | Public legacy readiness, HTTP 200 when DB is readable in API-only mode |
| `/providers` | KEEP | Persisted collector health; expired/stopped snapshots are disconnected |
| `/instruments` | KEEP | Legacy canonical list; `limit`, `after` |
| `/instruments/{symbol}` | KEEP | Legacy instrument and source mappings |
| `/prices/latest` | DEPRECATE | Legacy response envelope retained; corrected default selection, see below |
| `/prices/latest/{symbol}` | DEPRECATE | Legacy per-symbol envelope retained; use v1 for truthful observation freshness |
| `/ticks/{symbol}` | KEEP | Bounded legacy tick history and timestamp/UUID cursor |
| `/candles/{symbol}` | KEEP | Legacy native or last/mid tick-derived history; retains its forming/complete flags |
| `/futures/{exchange}/{root}/{alias}` | KEEP | Persisted explicit roll-schedule resolution, never a root contract history |

No existing route is removed or replaced. No WRAP/REPLACE category was needed: the new
public contracts reuse Store query methods and the existing database pool. Additional
non-OpenAPI surfaces:

| Route | Classification | Behavior |
| --- | --- | --- |
| `/metrics` (GET) | INTERNAL ONLY | Authenticated per-worker Prometheus metrics, excluded from OpenAPI |
| `/ws/prices` (WebSocket) | KEEP | API-only accepts/authenticates then closes **1013**; combined legacy fanout remains distinct |
| `/api/v1/ws/marketdata` (WebSocket) | NEW in Batch 3 | Committed database events, snapshots and status; see contract below |
| `/openapi.json` (GET/HEAD) | KEEP | Generated OpenAPI schema |
| `/docs`, `/docs/oauth2-redirect`, `/redoc` (GET/HEAD) | KEEP | Swagger/ReDoc documentation |

Intentional compatibility changes: default database-only latest selection now prefers a
fresh persisted quote over a candle. If both candidates are stale, use the later source
timestamp (quote event time, or receipt if absent; candle bucket end); candle wins ties.
Future-dated quotes are stale and a completed candle is preferred when available. Legacy
quote results gain `price_type="quote"`. Legacy freshness retains its receipt-age plus
collector-health contract; v1 freshness is observation age, independent of transport health.
The explicitly configured collector/combined `candle_providers` selection remains compatible.
Authentication with an incorrect bearer token now returns 403 instead of the previous
generic 401. Documentation is accessible without a token so Swagger's Authorize button works.
Native 1m volume reads now retain recorded tick-volume units; non-additive rollups remain null.

## Common contracts and limits

Prices, OHLC and volume use **decimal JSON strings**. Preserve strings or use a decimal
library in clients; avoid `Number()` for exact financial arithmetic. Optional observations
are JSON `null`, never zero. All output timestamps are UTC ISO-8601 ending in `Z`.
Aware input offsets are normalized to UTC; naive dates/timestamps are rejected.

Identity is `(instrument_id, provider)` plus exact `provider_symbol`. Canonical symbols are
not aliases to another source. In particular:

| Canonical symbol | Exact TradingView symbol | Registry venue | Broker |
| --- | --- | --- | --- |
| `FX:OANDA:EURUSD` | `OANDA:EURUSD` | `OANDA` | `OANDA` |
| `FX:FXCM:EURUSD` | `FX:EURUSD` | `FX` | `FXCM` |

`venue` preserves the registry exchange value. `broker` is the configured
`FX:<broker>:<pair>` canonical namespace, or null when absent. It does not assert verified
upstream availability. Provider remains `tradingview_guest` for both. HTTP filters never
rewrite `FX:` to `OANDA:` or blend their candles/gaps. Instruments with multiple provider
mappings produce one item per source mapping. Inactive persisted instruments remain
queryable and explicitly return `active=false`; listing never activates a source.

| Parameter/configuration | Bound/default |
| --- | --- |
| `limit` | 1–1000, default 100; additionally capped by `API_MAX_ROWS` (default 1000) |
| repeated `symbols` | 1–100 canonical symbols; unknown symbols return 404 for the whole request |
| `after` | At most 220 characters; key returned by server |
| history/coverage window | Positive, at most `API_HISTORY_MAX_DAYS` (default 31, configurable 1–366) |
| candle/coverage start/end | Half-open `[start,end)`, UTC interval aligned; default preceding day |
| allowed history intervals | `1m`, `5m`, `15m`, `1h`, `1d` |
| query timeout | `DB_TIMEOUT_SECONDS` (default 10); pool acquisition, asyncpg commands and API server statement timeout |
| v1 request query budget | `DB_TIMEOUT_SECONDS` across route queries; timeout returns 503 |
| gap range detail | First 100 ranges per source; full counts retained and truncation indicated |
| health summary | At most 1000 sources; `summary_truncated=true` and unknown freshness above that |

Filters and values are parameterized. There are no SQL, arbitrary table, mutation, schema
or credential endpoints. SQL relation names never form the public contract. No query cache
is introduced. Bulk latest queries use the dedicated latest-state tables, not tick-history
sorting; 100 requested symbols take two SELECTs (one validation, one bulk lookup).

## Instruments

`GET /api/v1/instruments`: `asset_class` (`crypto`, `forex`, `metals`, `energy`, `futures`),
`provider`, `provider_symbol`, `limit`, `after`.

Returns `items` and `next_after`. Each item includes canonical/UUID/provider/source identity,
venue/broker, display name, asset class, instrument type, currency, active flag, configured
realtime status/delay, supported native-history intervals and availability.
`availability=persisted` means a matching persisted observation exists, **not** that the
source is currently reachable. Otherwise it is `configured_unverified`. Native-history
intervals are advertised only when native latest/coverage evidence is stored. A supported
query can still return an empty range. Display name/currency are null when absent.

## Latest prices

`GET /api/v1/prices/latest`: repeated `symbols`, `provider`, `provider_symbol`, `limit`, `after`.
Without `symbols`, list configured sources, including unavailable ones. Returns `items`,
`as_of` and `next_after`.

Selection is limited to each **exact source mapping**:

1. Prefer a quote whose receipt and supplied event timestamp are no older than its threshold
   and are not in the future. No collector heartbeat is needed for this persisted fact.
2. Otherwise use the newer available quote observation or completed native 1m candle close;
   ties favor the candle. A future quote cannot displace an available completed candle.
3. With neither candidate, return unavailable with null prices/timestamps. No source switching.

`price_type` is `quote`, `candle_close` or `unavailable`. A quote's `price_basis` is `last`,
`bid_ask`, `bid` or `ask`; its unavailable `last` stays null. No synthetic mid is supplied.
A candle's basis is **`candle_close`**, last is its recorded close, bid/ask are null and its
bucket start/end are supplied. It is never described as a live quote.

`source_timestamp` is upstream event time for a quote, or candle bucket end for a close
(a bucket boundary, not an exchange event). It is null if a quote has no event time.
`received_timestamp` is the original local observation receipt, never HTTP request time.
`age_ms` is the greater of source age and receipt age, clamped to zero;
`clock_skew=true` forces stale. `freshness_basis` states `source_and_receipt`, `receipt` or
`unavailable`. `freshness` is `fresh`, `stale` or `unavailable`.

Quote thresholds use `STALE_SECONDS` (default 30), with exact-source overrides in
`TRADINGVIEW_SYMBOL_STALE_SECONDS`; candle-close threshold is 60 plus
`CANDLE_FETCH_GRACE_SECONDS` (default 2). Freshness does not claim market-open status.
`quote_freshness` separately reports whether a persisted quote exists/is fresh, even when
a candle was selected. `realtime_quote_available=true` requires a fresh quote explicitly
persisted with `realtime` status. A fresh delayed/indicative/simulated/unknown observation
retains that classification and cannot satisfy this flag. A candle may retain its native
source's `realtime_status=realtime` while **realtime_quote_available is false**.

Abbreviated response shapes (illustrative contract examples, not live observations):

```json
{"price_type":"quote","price_basis":"bid_ask","last":null,"bid":"1.01","ask":"1.02","source_timestamp":null,"freshness_basis":"receipt","realtime_status":"unknown","realtime_quote_available":false}
```

```json
{"price_type":"candle_close","price_basis":"candle_close","last":"1.123456789012345678901","bid":null,"ask":null,"quote_freshness":"unavailable","realtime_quote_available":false}
```

## Completed historical candles

`GET /api/v1/candles`: required `symbol`, `provider`; optional exact `provider_symbol`,
`interval` (default `1m`), `start`, `end`, `limit`, `cursor`.
Returns `items`, interval, normalized start/end, fixed `as_of` and `next_cursor`.
Unknown canonical/source combinations return 404. A known source without matching candles
returns an empty items list, never fabricated bars. No tick-derived fallback is used in v1.

`origin=native` identifies stored TradingView/base 1m records. Higher intervals are
`origin=derived`, `base_interval=1m`, `price_basis=native`: UTC epoch-aligned aggregation of
those native minutes. Day means UTC midnight, **not a native exchange-session daily candle**.
First open, maximum high, minimum low and last close preserve Decimal precision.
Distinct realtime-status groups remain separate records within a bucket.

Only elapsed buckets and completed underlying records are returned, even when end is in
the future. An elapsed rollup with missing base minutes remains visible with `complete=false`,
actual `observations`, and `expected_observations` (1/5/15/60/1440). It must not be treated
as complete merely because the market may have been closed; use coverage classification.
No empty bucket is generated. `complete` describes base-minute presence, not just elapsed time.

`volume=null` means unknown/unavailable typed source volume. A native recorded tick volume
is identified as `volume_type=tick`, never traded quantity. For rollups, volume is summed
only when every row has known, identical additive source units and a non-null value;
otherwise volume and volume_type are null. Unknown/raw volume metadata does not become
traded quantity. `realtime_status`, `source_status` and maximum underlying receipt timestamp
retain source provenance; a status group does not prove the rollup is complete.

## Latest completed candles

`GET /api/v1/candles/latest`: repeated `symbols`, `provider`, `provider_symbol`, `limit`, `after`.
Returns `items`, `as_of`, `next_after`. Each item has source identity, `availability` and
`candle` (the latest persisted completed **native 1m** candle, or null). This endpoint does
not accept a higher interval; use historical rollups for those. It never substitutes a quote.

## Coverage

`GET /api/v1/coverage`: repeated `symbols`, `provider`, `provider_symbol`, `start`, `end`,
`limit`, `after`. Defaults to the preceding completed day ending at the current minute
boundary. Explicit end must exclude the forming minute. Returns `items`, start/end, `as_of`,
`next_after`.

Counts use the requested UTC window, intersected with stored classifications:

| Field | Meaning |
| --- | --- |
| `tracked_start`, `tracked_end` | Existing activation/coverage bounds; null without tracking |
| `expected_minutes` | Window minutes minus NOT_EXPECTED and UNKNOWN; does not remove missing EXPECTED minutes |
| `persisted_completed_candles` | Actual stored completed native minutes in the requested window |
| `initially_missing_candles` | **Null**: current schema has no complete initial-missing event ledger |
| `recovered_candles` | Stored minutes carrying `backfilled=true` provenance; not inferred from receipt latency |
| `unresolved_expected_gaps` | Full count of stored EXPECTED missing minutes within window |
| `not_expected_minutes` | Stored source-session NOT_EXPECTED classification |
| `unknown_minutes` | Stored UNKNOWN plus all requested time outside tracked bounds |
| `missing_ranges` | First 100 clipped EXPECTED gap ranges, ordered by start, half-open |
| `missing_ranges_truncated` | More ranges exist; summary counts still include all |
| `latest_completed_bucket` | Source's latest completed stored native bucket start, independently of requested range |
| `latest_persisted_bucket` | Source's latest stored bucket start, independently of requested range |
| `coverage_state` | `gaps` if unresolved expected gaps exist; otherwise `unknown` if uncertainty remains; else `complete` |

No inferred exchange calendar or synthetic gap repair is added. Outside tracking is UNKNOWN,
not loss or closure. Persisted rows can include time outside tracking; therefore observed
row count and EXPECTED denominator need not be identical populations. No completion percentage
is reported. Initial-missing plus recovered cannot be reconstructed reliably from current
ranges; null is deliberate. Broker recovery never reduces another broker's gaps.

## Health and offline behavior

`GET /api/v1/health` returns `api=available`, database availability, collector heartbeat
state, data freshness counts and all-history coverage classification. HTTP status is **200**
when database reads succeed, even if TradingView is disconnected, data is stale or the
collector is stopped. HTTP **503** indicates unavailable DB access. API and database health
are separate from collector/feed readiness.

Collector is `running`/`stopped` only with a current stored heartbeat; absent/expired means
`unknown`. Provider states come from that snapshot. Data freshness uses persisted observations;
stale sources make aggregate freshness stale, otherwise unavailable sources make it
unavailable, otherwise it is fresh. Counts include configured sources, not just those returning
prices. Over 1000 sources makes aggregate freshness unknown and signals truncation. Coverage
is gaps-first, otherwise unknown when tracking is missing/empty or stored UNKNOWN exists.
`streaming_mode=database` identifies Batch 3's database reconciliation delivery path; an
unavailable database makes versioned health report `streaming_mode=unavailable`. This is
separate from provider connectivity and does not imply a fresh upstream quote stream.

Historical reads work with the collector stopped or TradingView unavailable. Failed database
reads return 503 and do not reuse old HTTP values as if a query succeeded. Database startup
requires existing migration head 0006 and fails if unreachable/unmigrated; no schema or role
is provisioned by FastAPI. Existing connection failures recover through the asyncpg pool
once PostgreSQL is available again.

## Pagination and errors

Bulk/source-list pages use exclusive keyset `after=canonical_symbol/provider`. Keep the
same filters on the next request. `next_after=null` means exhausted. Values can change during
collection; bulk pages are not a frozen database snapshot and concurrent registry changes
may require restarting pagination.

Historical pages order by `(bucket_start,realtime_status)`. Follow opaque `next_cursor`
with the same symbol/provider/source/interval; keep the same start/end or omit them on
subsequent requests. The cursor carries the original normalized window and `as_of` cutoff,
and rejects mismatched filters/ranges, malformed/oversized data and future cutoffs. Changing
limit is supported. Mixed-status buckets do not cause skipped or duplicate rows. The cursor
is not an authorization credential and is not signed. Late historical inserts/corrections
can change pages between calls; it does not promise an MVCC snapshot. Re-query a bounded
overlap if consuming history while recovery is running.

All v1 application errors use:

```json
{"error":{"code":"invalid_request","message":"invalid query parameters"}}
```

| HTTP | Code | Use |
| --- | --- | --- |
| 401 | `authentication_required` | Missing/malformed bearer header |
| 403 | `access_denied` | Incorrect bearer token |
| 404 | `not_found` | Unknown canonical symbol/source mapping |
| 409 | `resync_required` | Journal cursor expired or belongs to another epoch; refresh loaded chart history |
| 422 | `invalid_request` | Invalid timestamps, ranges, interval, limit, symbol list or cursor |
| 503 | `database_unavailable` | Database error or query deadline |
| other HTTP errors | `request_failed` | For example an unsupported HTTP method |

Versioned health's database-error 503 retains its typed health body; a request deadline
returns the standard error envelope. OpenAPI documents both. CORS preflight uses
middleware's 400 response. Legacy endpoints retain the previous `detail` error envelope.

## Frontend and server examples

The BFF/browser paths in these examples are **PROVISIONAL**, not a known company frontend.

Browser through the company BFF (the BFF authenticates the session and supplies the server
bearer token; it must explicitly allow only approved market-data routes and query fields):

```javascript
const query = new URLSearchParams();
query.append("symbols", "FX:OANDA:EURUSD");
query.append("symbols", "FX:FXCM:EURUSD");
const response = await fetch(`/market-data/prices/latest?${query}`, {
  credentials: "same-origin"
});
if (!response.ok) throw new Error(`Market data failed: ${response.status}`);
const page = await response.json();
for (const item of page.items) {
  // Keep Decimal strings; show basis + freshness beside any displayed value.
  const display = item.last ?? "Unavailable";
  console.log(item.canonical_symbol, item.provider_symbol, display,
              item.price_type, item.freshness, item.realtime_status);
}
```

Server-side Node fetch after your existing session/authorization check:

```javascript
const url = new URL("/api/v1/prices/latest", process.env.MARKET_DATA_API_ORIGIN);
url.searchParams.append("symbols", "FX:OANDA:EURUSD");
const response = await fetch(url, {
  headers: { Authorization: `Bearer ${process.env.MARKET_DATA_API_TOKEN}` },
  signal: AbortSignal.timeout(12000)
});
// Return the API status/body to the authenticated browser; do not forward its secret header.
```

Python (server-side secret environment; example dates refer to requested history, not source availability):

```python
import os
from decimal import Decimal
import httpx

headers = {"Authorization": "Bearer " + os.environ["API_KEY"]}
with httpx.Client(base_url="http://127.0.0.1:8002", headers=headers, timeout=12) as client:
    params = {
        "symbol": "FX:FXCM:EURUSD",
        "provider": "tradingview_guest",
        "provider_symbol": "FX:EURUSD",
        "interval": "5m",
        "limit": 100,
        "start": "2026-10-12T00:00:00Z",
        "end": "2026-10-13T00:00:00Z",
    }
    while True:
        response = client.get("/api/v1/candles", params=params)
        response.raise_for_status()
        page = response.json()
        for candle in page["items"]:
            close = Decimal(candle["close"])
            print(candle["bucket_start"], close, candle["complete"])
        if page["next_cursor"] is None:
            break
        params["cursor"] = page["next_cursor"]
```

PowerShell with curl (token comes from the secret environment, never a literal committed key):

```powershell
curl.exe --fail-with-body -H "Authorization: Bearer $env:API_KEY" `
  "http://127.0.0.1:8002/api/v1/instruments?asset_class=forex&limit=100"
curl.exe --fail-with-body -H "Authorization: Bearer $env:API_KEY" `
  "http://127.0.0.1:8002/api/v1/prices/latest?symbols=FX%3AOANDA%3AEURUSD&symbols=FX%3AFXCM%3AEURUSD"
curl.exe --fail-with-body -H "Authorization: Bearer $env:API_KEY" `
  "http://127.0.0.1:8002/api/v1/candles/latest?provider=tradingview_guest"
curl.exe --fail-with-body -H "Authorization: Bearer $env:API_KEY" `
  "http://127.0.0.1:8002/api/v1/coverage?symbols=FX%3AFXCM%3AEURUSD&start=2026-10-12T00%3A00%3A00Z&end=2026-10-13T00%3A00%3A00Z"
curl.exe --fail-with-body -H "Authorization: Bearer $env:API_KEY" `
  "http://127.0.0.1:8002/api/v1/health"
```

Open `http://127.0.0.1:8002/docs`, select **Authorize**, enter the token (without `Bearer`),
and try v1 operations. The full required/nullable Pydantic field schema and error models
are at `/openapi.json`; `/redoc` is the alternative reader.

## Dedicated read-only database role

Only the API server receives its database DSN; clients never receive PostgreSQL credentials.
The collector retains its independent write role. Automatic role provisioning is deliberately
absent: creating principals and modifying inherited PUBLIC grants is DBA work, outside HTTP
and API startup. The following must be reviewed/run by a DBA against an explicitly authorized
database, after migrations, using the actual schema/database/collector-role names:

```sql
CREATE ROLE marketdata_api LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT CONNECT ON DATABASE marketdata TO marketdata_api;
GRANT USAGE ON SCHEMA public TO marketdata_api;
GRANT SELECT ON public.instruments, public.provider_instruments,
  public.market_ticks, public.latest_prices, public.market_candles, public.latest_candles,
  public.candle_coverage, public.candle_revisions, public.collector_status,
  public.marketdata_event_clock, public.marketdata_events,
  public.alembic_version TO marketdata_api;
ALTER ROLE marketdata_api SET default_transaction_read_only = on;
```

Use interactive psql `\password marketdata_api` or the approved secret manager; do not
paste passwords into SQL files, logs or command history. The API also sets read-only
transactions, UTC and statement_timeout per pool connection. Grants, not that setting,
prevent market-table writes even when transaction read-only mode is turned off.

For Timescale legacy tick-derived endpoints, grant SELECT on the existing
`public.candles_{1s,1m,5m,15m,1h,1d}_{last,mid}` views individually (12 views).
Do not grant all public/internal tables or future tables by default. No internal Timescale
schema grants were required by the tested version. Review additional grants after migrations.

Check inherited PUBLIC privileges and memberships. A strict installation should revoke
PUBLIC CREATE on the public schema and PUBLIC CREATE/TEMPORARY on the target database,
then retain explicit required privileges for existing authorized principals. In particular,
the collector needs TEMPORARY for its COPY staging tables. These shared-grant changes can
affect other applications and must not be applied automatically or without DBA review.
The API role should own no schemas/tables and have no write-role memberships. Prevent
access to SECURITY DEFINER write functions under the deployment's actual function grants.
Keep PostgreSQL on private networking; no public database port is needed.

Independent tests on both database engines serve the API with explicit relation SELECT
grants, turn session read-only **off**, and prove UPDATE/DELETE/TRUNCATE/CREATE/ALTER are
still denied. Production role, secret, grants, networking and redistribution permissions
remain **unprovisioned** in this local batch.

## Isolated local use and validation

Run from the Batch 2 worktree, never the production project or completed integration worktree:

```powershell
uv sync --frozen --extra databento
docker compose -p marketdata-batch2-tests -f compose.batch2-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://batch2_test:disposable_test@127.0.0.1:55692/marketdata_batch2_test'
uv run alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/benchmark_api_rest.py --output artifacts/api-timescale.json
# Standalone PostgreSQL:
$env:DATABASE_URL='postgresql://batch2_test:disposable_test@127.0.0.1:55691/marketdata_batch2_test'
uv run alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run pytest -q
uv run python scripts/benchmark_api_rest.py --output artifacts/api-postgres.json
```

Start API only in another terminal with the same explicitly selected disposable DB:

```powershell
$env:APPLICATION_ROLE='api'
$env:API_ENVIRONMENT='development'
# Optional local token: generate it in memory, without printing or committing it.
$env:API_KEY=uv run python -c "import secrets; print(secrets.token_urlsafe(32))"
$env:API_CORS_ORIGINS='["http://localhost:3000"]'
uv run uvicorn marketdata.api:create_app --factory --host 127.0.0.1 --port 8002 --workers 2
```

An empty database correctly returns configured/unavailable or empty results; the API
does not register instruments. Tests/benchmark seed visibly simulated records only in the
disposable database. To operate a collector in an independent mock development project,
follow [process separation](fastapi-process-separation.md) with an explicit separate write
DSN. No live TradingView collector is required for REST verification.

Stop task-owned API processes, then clean only test containers:

```powershell
docker compose -p marketdata-batch2-tests -f compose.batch2-test.yaml down
```

Both database services bind loopback 55691/55692, use tmpfs and have no production volumes.
The benchmark refuses a database not ending `_test` or using port 5432; it deletes only
its unique synthetic fixture IDs on exit. Tests clear `_test` tables, so never point them
at valued history. [Verification report](api-batch2-verification.md) contains measured results,
limitations and the remaining frontend/deployment work. No push, main merge or deployment
is part of this batch.

## WebSocket market data v1

Endpoint: **`/api/v1/ws/marketdata`**. It works in API-only workers and the explicit combined
factory, always from committed PostgreSQL state. REST remains the historical source of truth.
The old `/ws/prices` is unchanged: combined ticks are observations **before commit**; its
native candles are sent after Store returns successfully. API-only legacy sockets still
close 1013. Do not infer durable events from legacy tick messages.

### Delivery and cursor mechanics

Revision **0006** adds `marketdata_event_clock` and `marketdata_events`. Existing market,
latest, correction-audit and coverage tables are preserved. `Store.write` journals only
successful latest-price changes, at most one quote per instrument/provider in a tick batch;
intermediate persisted ticks remain queryable through REST. Identical numerical prices at
new observation timestamps are eligible. Duplicate UUID retries and older ticks that cannot
advance latest state create no quote event. `Store.write_candles` journals actual new native
1m inserts and corrections; unchanged overlap/retry bars create no candle event. Coverage-only
transactions create no candle event. Journal records and market records commit or roll back
**together**. Failed transactions never publish committed events.

A singleton counter UPDATE allocates a block of IDs **inside the data transaction**, at its
end, and keeps the row lock until commit/rollback. A second writer cannot allocate a later
cursor while the first is uncommitted. Readers see only the last committed head under MVCC;
SERIAL/IDENTITY ordering is not used as commit ordering. Records within a batch are ordered
by canonical instrument, provider and observation/bucket time. Across transactions ordering
is journal commit order, not upstream tick sequence or candle-time order. A recovered old
minute or correction can arrive after a newer minute: charts must upsert by logical identity.

Each API worker owns one bounded journal reader and its own downstream queues. It polls
only the indexed journal tail, default once per second when clients exist, and processes
250-event pages. It does not scan the complete candle database or depend on collector memory.
Catch-up yields between pages; no notification, Redis, broker, provider connection or new
upstream access is needed. A disconnected reader retains its cursor and retries bounded
queries. LISTEN/NOTIFY was considered but omitted: it would only be a wake-up hint, while
reconciliation is still required for missed notifications. See PostgreSQL's
[LISTEN race guidance](https://www.postgresql.org/docs/18/sql-listen.html) and
[transactional NOTIFY behavior](https://www.postgresql.org/docs/18/sql-notify.html).
Notification loss therefore cannot affect this implementation.

Retention keeps the last `MARKETDATA_EVENT_RETENTION_COUNT` journal records, default 100,000,
pruned transactionally on append. This is **an event count, not a promised time duration**;
100 sources of minute candles alone generate approximately 144,000 events/day. Quotes and
corrections shorten the retention window. Increasing retention never restores already-pruned
records. Market/candle/tick history, activation starts and coverage gaps are never pruned by
journal retention. PostgreSQL vacuum/disk monitoring remains ordinary DBA work.

Only writers using these Store transactions publish events. Arbitrary administrative SQL,
old collector binaries and aggregate refreshes do not publish journal events. The revised
collector and 0006 must be adopted together in a future reviewed deployment. The reader is
SELECT-only; it does not provision migrations or grants. Grant the existing API role SELECT
on the two new tables after migration, using the scoped DBA procedure above. The collector
role needs SELECT/UPDATE on the clock and SELECT/INSERT/DELETE on the journal. No API write
credential, ticket table or server-side browser-session assumption is added.

### Browser authentication (PROVISIONAL)

**The company frontend is unfinished and its final authentication architecture is unknown.**
The BFF is a provisional supported adapter option, not a required frontend implementation.
The market-data service independently requires its configured bearer header and an exact
allowed Origin. Browser WebSocket construction exposes URL/subprotocol parameters, not an
arbitrary Authorization-header option; see the [browser constructor](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/WebSocket).
Never embed the permanent API key in JavaScript, query strings or subprotocols.

If a BFF is adopted, the browser connects to its same-origin WS route with the company's
real Secure/HttpOnly/SameSite session cookie. The BFF validates the user/session/read
permission and exact browser Origin before upgrading, supplies the service bearer **only on
the server-to-server header**, and forwards the validated Origin. It must enforce session
expiry/revocation on open connections, sanitize logs, apply admission/rate limits, bound
transport queues and use HTTPS/WSS. It must not forward user-supplied Authorization headers.
Only approved browser Origins belong in `WEBSOCKET_ALLOWED_ORIGINS`, for example
`["https://frontend.company.example"]`; the default empty list disables the new socket.
Production origin validation requires HTTPS. REST CORS and WS Origins are separate settings;
CORS does not authorize a WebSocket.

[`examples/company_bff.py`](../examples/company_bff.py) is an optional bounded proxy adapter
with an injected real-session authorizer, periodic revocation/expiry checks, fixed route
allow-list and server-owned bearer header. It supplies **no login/session/user implementation**.
Install only into the eventual backend after deciding that architecture. An alternative
reviewed browser auth adapter can leave delivery/cursor/schema code unchanged. Native/test
clients can supply the bearer header and allowed Origin directly. Origin validation is a
browser defense, not a substitute for credentials, network access control or user permission.

Failed authentication or missing/unapproved Origin is rejected **before registration and
acceptance** (ASGI policy close 1008; real network handshake HTTP 403). Missing REST credentials
remain 401, invalid REST credentials 403. Browsers often expose a rejected handshake only as
1006; check the company's protected session endpoint first and use BFF/server diagnostics.
No replayable one-time ticket is issued. Session anti-replay/CSRF/logout policy belongs to
the eventual auth adapter; the API's permanent bearer is a server credential with the same
read permission for every holder. No production credential/origin/session integration is
supplied or activated in this worktree.

### Commands and subscription JSON

All commands are UTF-8 JSON **text**, with exactly the following fields: `action`, optional
`symbols`, optional `request_id` (string at most 64 characters). Extra fields, wrong types,
invalid symbols and malformed JSON are errors. Canonical symbols have 3-100 uppercase
letters/digits/underscore/colon/period/hyphen. At most 100 entries per command; duplicate
symbols are idempotent. Select identities from `/api/v1/instruments`, never invent them.
The command below uses a canonical symbol in the default explicitly simulated registry.

```json
{"action":"subscribe","symbols":["CRYPTO:BTCUSDT"],"request_id":"prices-1"}
```

| action | symbols | Effect |
| --- | --- | --- |
| `subscribe` | Required nonempty list | Atomically add canonical instruments, across their persisted source mappings; ack followed by persisted price snapshot batches |
| `unsubscribe` | Required nonempty list | Remove; repeats/absent instruments are safe no-ops; ack is a queue-order fence |
| `snapshot` | Optional list | Refresh requested subscribed instruments, or all subscriptions if omitted/empty; ack and snapshots; never adds subscriptions |
| `ping` | Omit/empty | Application heartbeat with matching request_id; transport ping/pong remains enabled independently |
| `disconnect` | Omit/empty | Safe close 1000; no automatic server subscription restoration |

Subscribe validates the entire request before changing state. Unknown symbols return error
`unknown_symbol`; source/symbol/total capacity violations return `subscription_limit`.
Malformed commands return `invalid_command`; DB failures return `database_unavailable`
without installing an empty/fabricated snapshot. Repeated subscribe does not multiply
subscriptions. Symbols are canonical identities, not provider aliases; every event preserves
the exact provider/source/venue/broker. Unsubscribe acknowledgments follow any already queued
events; events for removed instruments are not queued after that ack.

### Event JSON schemas

Every message has `version:1`, `type` and `server_timestamp` (UTC). The complete generated
[JSON Schema](ws-marketdata.schema.json) is authoritative, including required fields, nulls,
enums and nested price/candle definitions. Source identity and decimal strings are the same
as REST v1. `request_id` echoes the command where applicable, otherwise null.

| type | Additional fields and semantics |
| --- | --- |
| `subscription_ack` | `action` (subscribe/unsubscribe/snapshot), `request_id`, sorted full `symbols`, nullable `cursor`, nullable `snapshot_cursor` |
| `price_snapshot` | `request_id`, `snapshot_cursor`, zero-based `batch`, `final`, `items:PriceItem[]`; one repeatable-read DB snapshot across all batches; no synthetic prices |
| `price_update` | `cursor`, `event_id` (decimal **string**), `recorded_at` (UTC), `persisted:true`, `data:PriceItem`; actual persisted latest quote, never a candle masquerading as a quote |
| `candle_completed` | Same durable envelope fields, `data:CompletedCandle`; newly inserted native completed 1m candle, including recovered old minutes |
| `candle_corrected` | Same durable envelope fields, `data:CompletedCandle`; full replacement of the same native logical minute with newer receipt and changed OHLCV/status |
| `source_status` | `collector` (running/stopped/unknown), `database` (available/unavailable), `batch`, `final`, `items:SourceState[]`; periodic current status, no durable event cursor |
| `heartbeat` | nullable `cursor`, `database`, `request_id`; queue-ordered progress/checkpoint after eligible events have been enqueued, not proof the client applied them |
| `error` | `code`, `message`, `request_id`; safe error without input payload/credentials |
| `resync_required` | `code:cursor_expired`, `message`, `request_id`; best-effort advisory before close 1013 when worker falls beyond retention or epoch changes |

`PriceItem` carries source identity plus `last`, `bid`, `ask` (decimal strings/null),
`price_type`, `price_basis`, `source_timestamp`, `received_timestamp`, `freshness_basis`,
`age_ms`, `freshness`, `stale_after_seconds`, `clock_skew`, `realtime_status`, `quote_freshness`,
`realtime_quote_available`, `bucket_start`, `bucket_end`; see Latest prices above. Snapshots
use the same persisted selection as `/api/v1/prices/latest`. `price_update` describes the
journal's actual latest-quote observation at that commit, with freshness evaluated now;
REST's combined latest selector can prefer a newer candle when that quote is stale. It is
not a notification that a fabricated current market price exists.

`CompletedCandle` contains `(instrument_id,canonical_symbol,provider,provider_symbol,venue,
broker)`, `interval:1m`, `origin:native`, `base_interval:1m`, `price_basis:native`, UTC
`bucket_start`/`bucket_end`, decimal `open`/`high`/`low`/`close`, nullable decimal `volume`,
nullable `volume_type`, `realtime_status`, `source_status`, UTC `received_timestamp`,
`observations:1`, `expected_observations:1`, `complete:true`. No forming candle is finalized.
A corrected event replaces that minute; recompute/refetch any affected derived chart
bucket via REST. There are no derived 5m/15m/1h/1d WebSocket completion promises.
`recorded_at` is the DB journal row-write time **inside the transaction**, not an exchange
or exact commit timestamp. `server_timestamp` is serialization time, not observation time.

`SourceState` contains the same source identity and `freshness`, `quote_freshness`, `age_ms`,
`price_type`, `price_basis`, `realtime_quote_available`, `source_timestamp`,
`received_timestamp`, `provider_state`, `transport`, `session_expectation`, `candle_state`.
Freshness is observation age independent of collector heartbeat. Absent/expired collector
health stays unknown; stopped/expired provider transport is disconnected. Closure is only
forwarded from a current source health snapshot (`NOT_EXPECTED`/`closed`), never inferred
from silence or a stale price. Closed markets can legitimately have stale latest data.
Database-unavailable status is authoritative: do not reuse its items as successful reads.

**TradingView's current native-candle mode has no genuine persisted current-price quote
stream.** It emits completed/corrected candles, snapshots whose price_type/basis can be
`candle_close`, and status. Closing prices are never relabeled live quotes. Other configured
persisted tick adapters can emit genuine new quote observations with their original
simulated/delayed/indicative/unknown/realtime classifications. A changing number does not
establish realtime entitlement or permission to redistribute data.

### Initial snapshots and reconnect recovery

The subscribe snapshot and `snapshot_cursor` are read in one repeatable-read transaction:
all data visible at that fence is reflected in the snapshot, and subsequent eligible
commits are journaled after it. New instruments skip events already represented by their
snapshot. Existing instruments retain their own barrier when another is added, so adding
an instrument never permanently skips their intervening candles. For an established
connection, `snapshot_cursor` applies **only to the requested snapshot instruments**, not
all previous subscriptions. Do not promote that partial snapshot fence to the full-chart
checkpoint. `subscription_ack.cursor` is worker reconciliation progress, possibly older
than the snapshot; it can be null. Heartbeat cursor is withheld until the worker reaches
all subscription barriers.

Clients own subscriptions and restore the desired full set on a new connection. Recommended
procedure (implemented in [browser-marketdata.js](../examples/browser-marketdata.js)):

1. Authenticate through the eventual adapter and fetch instruments; retain exact identities.
2. Before initial historical reads, capture `/api/v1/marketdata/changes` without `after`;
   it returns no events and the current committed `cursor`/`until`.
3. Fetch and paginate REST candles for the visible chart window; fetch persisted latest prices.
   Or do these bounded reads during the socket's synchronization phase, as the example does.
4. Open a secure socket, restore all desired symbols and buffer incoming messages. On initial
   subscribe ack take its complete new-subscription `snapshot_cursor` as fence H.
5. Fetch changes with `after=<last applied checkpoint>&until=H&limit=250`, following the
   returned `cursor` until `has_more=false`, preserving the same `until`. Process recovered
   candles AND corrections anywhere in loaded history, not just buckets after the newest bar.
6. If replay returns 409, refresh the **entire loaded chart window** through paginated REST
   while continuing to buffer socket messages. This recovers changes older than a short
   overlap. REST retains explicit gaps/unknown coverage; never fabricate missing minutes.
7. Mark H applied, then process buffered/live messages in order. Upsert candles by
   `(instrument_id,provider,provider_symbol,interval,bucket_start)`; reject an older receipt
   version when REST has already shown a later correction. Decimal strings remain exact.
8. Advance checkpoints only after application of durable events or ordered available-DB
   heartbeats. Keep cached chart data and its checkpoint together; a cursor alone is not
   chart history. Event IDs are strings (use BigInt if comparison is needed), scoped by epoch.
9. After disconnect use exponential backoff with jitter, revalidate authentication, and repeat.
   Show disconnected/stale state immediately, including while replay fails with 503. A socket
   open event does not mean chart resynchronization has finished.

REST changes returns `{items,cursor,until,has_more}`. `after` is exclusive and `until` inclusive.
Omitting `until` captures the current head for that page; freeze the returned `until` for
subsequent pages. The default head-only call is not a request to replay all old records.
The cursor is a bounded version/epoch/position watermark, **not an auth credential**, wall
clock or candle timestamp. Malformed/future cursors return 422. Expired/foreign-epoch cursors
return 409, even mid-pagination if retention overtakes replay. Events are immutable within
a retained journal; REST history itself is not a frozen MVCC snapshot across pages.

Socket notifications are not permanent history and do not promise exactly-once delivery.
Retransmission/replay may duplicate logical events. Checkpoints and candle upserts make
recovery idempotent. API restart requires client reconnect; collector restart requires no
API/provider reconstruction. During PostgreSQL outages existing sockets report unavailable
status, keep their last cursor and recover once reads work. If retention was overtaken they
close 1013 and require REST refresh. REST history is available during source/collector outages
when PostgreSQL is available. Database backup restoration requires a fresh journal epoch
before serving clients; restoring an old counter with the same epoch could reuse cursors.
That administrative restore procedure is not performed here.

### Limits and slow consumers

Limits apply **per API worker**; a gateway's deployment-wide admission limit is still needed
when scaling worker/replica count. Shared collector state is unnecessary. Defaults:

| Setting / resource | Default and hard bound |
| --- | --- |
| `WEBSOCKET_MAX_CLIENTS` | 1000 clients/worker; configurable 1-10000 |
| instrument subscriptions | min(100, WEBSOCKET_MAX_SYMBOLS); default existing setting 500, v1 cap **100** |
| `WEBSOCKET_MAX_TOTAL_SUBSCRIPTIONS` | 10000 instrument/client pairs per worker; 1-100000 |
| `WEBSOCKET_QUEUE_CAPACITY` | 256 outbound messages/client; 1-10000 |
| `WEBSOCKET_MESSAGE_MAX_BYTES` | 65536 UTF-8 bytes incoming/outgoing; 1024-1048576 |
| `WEBSOCKET_SEND_TIMEOUT_SECONDS` | 5 seconds per send/close; >0 to 30 |
| `WEBSOCKET_SNAPSHOT_MAX_ROWS` | 1000 source rows/request, including all mappings; 1-1000 |
| `WEBSOCKET_SNAPSHOT_BATCH_SIZE` | 25 rows/message; 1-100, must fit max rows |
| `WEBSOCKET_HEARTBEAT_SECONDS` | 10 seconds; >0 to 60; queries/catch-up can delay it |
| `MARKETDATA_POLL_SECONDS` | 1 second; 0.01-30; commit latency also includes query/send time |
| `MARKETDATA_EVENT_BATCH_SIZE` | 250 events/reader page; 1-1000 |
| REST changes `limit` | 1-1000, default 250, additionally capped by API_MAX_ROWS |
| `MARKETDATA_EVENT_RETENTION_COUNT` | 100000 retained events; 1-1000000; not a time guarantee |

Unknown symbols/invalid subscriptions are atomic errors, not partial acknowledgments.
Oversized outgoing messages or queue overflow mark **only that client** failed and close
**1013**, with a REST-resynchronization reason. Overflow increments the existing worker
`marketdata_ws_dropped_total` metric. No silent coalescing/drop policy claims complete
delivery; a best-effort resync advisory may itself be lost, so close/disconnect is authoritative.
The database writer never awaits a socket. A slow send is canceled at its deadline and
closes 1013; other queues/readers/ingestion keep working. Incoming oversize closes 1009,
binary commands close 1003, client disconnect closes 1000. API shutdown cancels its owned
reader, closes queues/sockets and pool; no detached market task is left running.

Transport frame/reassembly queues must also be bounded **before** ASGI receives a command.
Run Uvicorn with its bounded `websockets` backend, `--ws-max-size 65536 --ws-max-queue 16`,
plus transport ping/pong (20-second defaults). The currently installed SansIO backend has
an unbounded internal ASGI receive queue; the application-level checks alone cannot bound
that transport. Set equivalent limits at the eventual BFF/gateway as well. The locked
legacy websockets backend emits a Uvicorn deprecation warning; requalify bounds when
changing server dependencies/backends. Do not assume a backend honors flags it ignores.

### JavaScript use and isolated validation

The browser module accepts endpoint paths and callbacks so it does not assume a completed
company auth/UI. With a future same-origin BFF and real session endpoint:

```javascript
import { startMarketData } from './browser-marketdata.js';
const stream = await startMarketData({
  symbols: selectedCanonicalSymbols, // Choose from the instrument list, 1-100 instruments.
  sessionUrl: '/session',             // Replace with the actual protected company endpoint.
  onPrice: p => renderPrice(p.last, p.price_basis, p.freshness),
  onCandle: c => upsertChartCandle(c), // Also invalidates derived buckets after corrections.
  onStatus: s => s.items.forEach(i => showStale(i.canonical_symbol,
    s.database !== 'available' || i.freshness !== 'fresh')),
  onConnection: state => showConnectionState(state),
  onError: error => showMarketDataError(error),
});
// Later: stream.stop();
```

Rendering callback functions are application integration points, not a UI supplied by this
repository. No mock observations are used by these examples. The provisional Python adapter
needs the company's real session-authorizer callback; its paths/expiry schema are not a
final frontend contract. Production TLS/session/credential/gateway setup remains unexecuted.

Use a separate project and only task-owned disposable databases:

```powershell
docker compose -p marketdata-batch3-tests -f compose.batch3-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://batch3_test:disposable_test@127.0.0.1:55791/marketdata_batch3_test'
uv run alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
$env:BATCH3_TEST_CONTAINER='marketdata-batch3-tests-postgres-1'
uv run pytest -q
# Repeat on 55792 with alembic -x timescale=true upgrade head and matching timescale container.
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run mypy src examples/company_bff.py
node --check examples/browser-marketdata.js
git diff --check
# Development API only, using this migrated test DB and an explicit approved Origin:
$env:APPLICATION_ROLE='api'
$env:WEBSOCKET_ALLOWED_ORIGINS='["https://frontend.company.example"]'
uv run uvicorn marketdata.api:create_app --factory --workers 2 --port 8003 --ws websockets --ws-max-size 65536 --ws-max-queue 16
# Remove only these task-owned disposable services after testing:
docker compose -p marketdata-batch3-tests -f compose.batch3-test.yaml down
```

The actual-outage test inspects the exact disposable project/loopback DSN before pausing and
resuming that container. It never targets production. Tests and publishers are explicitly
simulated, with genuine provider fixture tests unchanged. Exact evidence and remaining limits
are in [Batch 3 verification](api-batch3-verification.md).
