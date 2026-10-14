# Company frontend handoff

See the [developer handoff](../README.md) for isolated local setup, runnable requests,
the readiness verdict and the separate simultaneous TradingView quote/candle milestone.

The API serves persisted data without PostgreSQL credentials in the frontend. The operator
must supply the final HTTPS hostname and the company's existing authenticated backend routes.
Local service base: `http://127.0.0.1:8015/api/v1`. Production service base:
`https://<operator-supplied-host>/api/v1`. Swagger: `/docs`; OpenAPI: `/openapi.json`;
ReDoc: `/redoc`. The complete field and WebSocket contract is [api.md](api.md).

## Authentication

Server clients send `Authorization: Bearer <API_KEY>` over HTTPS. The service token stays in
the company backend. A browser cannot set a custom Authorization header on a native WebSocket;
do not put the token in its URL, local storage or bundled JavaScript. Use the company's
existing session-authenticated backend to relay REST and WebSocket traffic. The small
[BFF adapter](../examples/company_bff.py) accepts the real company session validator; it does
not supply a login system. This adapter is provisional until the company supplies that system.
It validates browser Origin and session expiry/revocation. Configure exact allowed HTTPS
origins on the API; the relay forwards the validated Origin and adds the bearer server-side.

## Requests and responses

Discover sources with `GET /api/v1/instruments?limit=100`. Follow `next_after` using `after`.
Keep `(instrument_id, provider, provider_symbol)` as source identity. OANDA and FXCM histories
are separate. Select the exact discovered canonical symbol; never infer an alias or provider
symbol. Inactive stored instruments remain queryable. `configured_unverified` availability
does not prove upstream access.

`GET /api/v1/prices/latest?symbols=SIM%3AEXAMPLE&limit=100` accepts up to 100 repeated `symbols`.
Without symbols it pages configured sources. Relevant fields in an illustrative simulated
result (see OpenAPI for the full envelope) are:

```json
{"price_type":"candle_close","price_basis":"candle_close","last":"123.123456789012345678901","bid":null,"ask":null,"freshness":"stale","quote_freshness":"unavailable","realtime_quote_available":false,"realtime_status":"simulated","bucket_start":"2026-10-10T00:00:00Z","bucket_end":"2026-10-10T00:01:00Z","source_timestamp":"2026-10-10T00:01:00Z","received_timestamp":"2026-10-10T00:01:02Z"}
```

A completed candle close is a historical observation, including when fresh. It is never a
current quote. `quote` observations may have last, bid and/or ask; null stays null. `unavailable`
means no usable persisted observation. Show price basis, freshness and market-data status
alongside values. A changing number does not prove realtime entitlement. TradingView native
candle mode currently supplies completed candles, not a genuine continuous current quote feed.

For history use `GET /api/v1/candles` with `symbol`, `provider`, optional `provider_symbol`,
`interval=1m`, `start`, `end`, `limit=500` or `1000`. The range is half-open `[start,end)`;
keep the same source/interval/window and feed back `next_cursor` as `cursor` until null.
Intervals: native completed `1m`, derived UTC `5m`, `15m`, `1h`, `1d`. UTC-derived 1d is not an
exchange-session daily bar. Forming minutes are excluded. An elapsed rollup with missing base
minutes has `complete=false`. Example complete synthetic bar:

```json
{"instrument_id":"00000000-0000-0000-0000-000000000001","canonical_symbol":"SIM:EXAMPLE","provider":"qualification","provider_symbol":"SIM_001","venue":"SIM","broker":null,"interval":"1m","origin":"native","bucket_start":"2026-10-10T00:00:00Z","bucket_end":"2026-10-10T00:01:00Z","open":"123.123456789012345678901","high":"123.123456789012345678901","low":"123.123456789012345678901","close":"123.123456789012345678901","volume":null,"volume_type":null,"realtime_status":"simulated","source_status":"streaming","received_timestamp":"2026-10-11T00:00:00Z","observations":1,"expected_observations":1,"complete":true}
```

All output timestamps are UTC ISO 8601 with `Z`; inputs require timezone offsets and interval
alignment. Preserve decimal strings. Use a decimal library for exact arithmetic; avoid
converting prices/volume to JavaScript `Number`. Unknown volume is null, not zero. Tick volume
and non-additive source volume are not traded asset quantities.

Call `/api/v1/coverage` for the same source/window, and follow `next_after`. A missing minute
has no fabricated candle. `unresolved_expected_gaps` and `missing_ranges` expose EXPECTED
absences; `not_expected_minutes` reflect stored session evidence; `unknown_minutes` are
unclassified/outside tracking. Never assume an empty range is a closed market. Range details
cap at 100 and explicitly report truncation; counts remain complete. `initially_missing_candles`
is null because the schema has no complete initial-loss ledger. Example fields:

```json
{"coverage_state":"gaps","expected_minutes":1440,"persisted_completed_candles":1439,"unresolved_expected_gaps":1,"unknown_minutes":0,"not_expected_minutes":0,"initially_missing_candles":null,"missing_ranges":[{"start":"2026-10-10T02:00:00Z","end":"2026-10-10T02:01:00Z"}],"missing_ranges_truncated":false}
```

## WebSocket and reconnection

Connect to `wss://<host>/api/v1/ws/marketdata` through the authenticated relay. Commands:

```json
{"action":"subscribe","symbols":["SIM:EXAMPLE"],"request_id":"selection-1"}
{"action":"snapshot","symbols":["SIM:EXAMPLE"],"request_id":"refresh-1"}
{"action":"unsubscribe","symbols":["SIM:EXAMPLE"]}
{"action":"ping","request_id":"ping-1"}
{"action":"disconnect"}
```

Process subscription acknowledgements and batched initial `price_snapshot` messages. Live
events are `price_update`, `candle_completed`, `candle_corrected`; `source_status` and `heartbeat`
report freshness/database/collector status. Events identify persisted observations and include
an opaque cursor. Upsert candles by exact source/interval/bucket; receipt versions prevent an
older replay overwriting a newer correction. Derived charts need recomputation/refetch after
a base-minute correction. Do not treat legacy `/ws/prices` as the company stream.

Use reconnect backoff with jitter and restore the desired subscriptions. Obtain the snapshot
barrier, buffer live events within a bound, and reconcile `/api/v1/marketdata/changes?after=<last>
&until=<barrier>` through all pages. Apply old recovered/corrected minutes too. A 409 means
retention/epoch expiry: refresh the entire loaded REST chart window plus prices/coverage.
Refresh bounded overlaps during ordinary history pagination; the cursor is not an MVCC
snapshot across late corrections. Delivery is recoverable, not exactly once.

Close codes: 1000 clean disconnect, 1003 binary input rejected, 1009 oversized input,
1013 slow client/capacity/restart/resync. Authentication/Origin denial rejects the handshake.
One slow client cannot block healthy clients. Heartbeat loss should trigger reconnect; data
can remain stale while transport is connected. PostgreSQL outage gives unavailable status;
collector outage alone leaves historical reads available.

## Small standards-based example

[browser-marketdata.js](../examples/browser-marketdata.js) supplies REST pagination, history,
latest prices, coverage callbacks, bounded streaming buffers, retry/watchdog and reconciliation.
It requires no frontend framework. Configure real company endpoints and render callbacks:

```javascript
import { startMarketData } from './browser-marketdata.js';
const connection = await startMarketData({
  symbols: selectedCanonicalSymbols,
  sessionUrl: '/company/session',
  apiBase: '/marketdata/api/v1', websocketPath: '/marketdata/ws',
  onPrice: p => renderPrice(p.last, p.price_basis, p.freshness),
  onCandle: c => upsertChart(c),
  onCoverage: c => renderGaps(c.missing_ranges, c.unknown_minutes),
  onStatus: s => renderSourceStatus(s),
  onConnection: s => renderConnection(s),
  onError: e => renderError(e),
});
// Dispose when the view is removed:
connection.stop();
```

Errors have `{"error":{"code":"database_unavailable","message":"database temporarily unavailable"}}`.
401 requires login/service configuration; 403 denies credentials; 404 unknown source; 422
invalid bounded/aligned query; 409 requires resync; 503 is retryable with backoff; 500 is a
sanitized internal error. Preserve the last displayed data with an explicit stale/error state.
Never retry 401/403/422 indefinitely. The frontend does not need table names or SQL.
