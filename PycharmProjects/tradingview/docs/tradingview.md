# Experimental TradingView guest quotes

The operator explicitly requested investigation and use of TradingView extraction on
2026-10-07. This opt-in adapter implements the guest quote protocol observed in the public
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
uv run python scripts/smoke_tradingview.py
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
  timestamp and `/providers` freshness before treating it as current.

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
