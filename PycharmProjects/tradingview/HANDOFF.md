# HANDOFF.md

## Current state ? 2026-10-07

The platform now runs a live experimental TradingView guest collector locally, alongside
the retained offline mock mode and documented Binance/OANDA/Databento adapters.
The repository initially contained only AGENTS.md, CODEX.md and this handoff template.
No previous application code was replaced. AGENTS.md remains unchanged; CODEX.md now
includes the implemented source map and execution commands. README.md is the operational guide.

## Implemented architecture

- Python 3.12+, asyncio, FastAPI/Pydantic v2, asyncpg, Alembic/SQLAlchemy migrations.
- Decimal/NUMERIC financial values; UTC event/receipt times; original source nanoseconds.
- UUIDv5 canonical instruments, provider mappings, concrete futures and explicit roll schedules.
- Owned provider tasks -> bounded queues -> COPY batches -> atomic history/latest UPSERT.
- Bounded pre-persistence WebSocket fan-out; REST registry/latest/history/candles/futures resolution.
- Provider and per-instrument freshness, sanitized structured logs, Prometheus metrics.
- Database retries with a retained batch, explicit overflow losses, bounded shutdown drain.
- Docker Compose app + TimescaleDB + one-shot migrations; app runs as an unprivileged user.
- Locked dependencies in uv.lock, Ruff, strict mypy and GitHub Actions for both database modes.

Important source files: src/marketdata/{domain,registry,config,ingestion,persistence,api}.py;
providers/{base,mock,binance,oanda,databento,tradingview,definitions,factory}.py;
cli.py (discovery/import), benchmark.py; scripts/{smoke,smoke_tradingview}.py; tests/.
Guest activation/limitations/evidence: docs/tradingview.md, compose.tradingview.yaml,
config/instruments.tradingview.json, tests/test_tradingview.py and its captured fixture.

## Provider reality

| Adapter | Current status |
|---|---|
| Mock | Fully exercised end to end; all observations visibly simulated. Default registry has 30 instruments across all requested categories. Energy non-futures examples are explicit CFDs; futures use synthetic SIM venue/dates. |
| Binance Spot | Official exchangeInfo + individual trade stream implemented, parser/error/gap fixtures tested. Live public probe failed with ConnectError. Operator now reports Binance and OANDA are blocked on their Iranian network; network cause was not independently diagnosed. No real-time connectivity claim. |
| OANDA v20 | Read-only account-instrument discovery, sampled pricing stream, heartbeat and classification implemented. Practice is simulated, live conservatively indicative. OTC FX/metals and CFDs remain distinct. No authenticated live session tested. |
| Databento | Official SDK MBP-1, dataset-isolated sessions, bounded callback bridge, concrete raw symbols, definition replay validation and offline DBN registry import. Real SDK records and retry/definition rejection paths tested offline. No authenticated live session tested. |
| TradingView guest | Explicitly authorized experimental public-site quote protocol. Live container -> Timescale -> REST/history/candles/WebSocket verified for 17 source series. Only upstream-reported streaming/realtime modes with verified identity and no nonzero delay are emitted. Unsupported protocol and usage-rights restrictions; no cookies, account tokens, permission flags or entitlement bypasses. |

Official protocol references and activation steps are in README.md. No provider credentials
or paid subscriptions were available. CME/COMEX/NYMEX/ICE real-time subscriptions and
redistribution agreements must be verified by the operator. Physical WTI/Brent spot/reference
pricing is not implemented; no CFD is labeled as an exchange future.

## Current access and budget constraints

- Latest operator instruction REVOKES acceptance of delayed data: real-time only, zero cost,
  no domestic exchange sources (including Wallex). Both XAUT and XAU/USD are required.
  Operator explicitly authorizes TradingView extraction/reverse engineering; retain the
  prohibition on access-control/entitlement bypasses and never infer realtime from motion.
- TradingView's official support states it has no public market-data API. Databento's
  quickstart specifies paid live licensing; historical trial credits are not a free live feed.
  Website reachability does not establish API access, eligibility or data entitlements.
- TradingView public HTML was reachable but had quote placeholders; Playwright/Edge trials
  timed out. Guest WebSocket worked. Protocol commands/framing/fields and streaming->Realtime
  UI mapping were verified against current site-served JS; references are in docs/tradingview.md.
  Source symbols were verified against public-page data-symbol attributes and live responses.
- Live registry: BINANCE BTC/ETH/SOL/XRP USDT; eight OANDA FX pairs; OANDA XAUUSD/XAGUSD;
  OKX XAUTUSDT; TVC USOIL/UKOIL. Gold token/OTC gold are distinct. TVC oil remains conservative
  REFERENCE identity with unverified construction methodology, not physical spot or futures.
  All 17 reported streaming on 2026-10-07. This is source-reported realtime, not an independent
  latency audit, complete exchange tape, or proof of perpetual free availability/use rights.
- Initial price deltas before source metadata are explicitly discarded/count logged. Metadata
  revalidates after every reconnect; source mode/identity rejection suppresses that symbol
  for the connection. Prices/timestamps are never filled from older deltas. Bid/ask-only
  event times stay null. No sequence IDs/trade sizes are invented or price-based dedup used.
- Existing persistence/API architecture and migrations are unchanged. Guest futures, indices,
  perpetuals and continuous aliases are unsupported; paid exchange futures remain unactivated.
- Already committed local history remains queryable during Internet loss. Incoming data
  stops during disconnection; missed observations require supported historical backfill.
  A future durable spool would protect received ticks during DB outages, not Internet gaps.

## Schema and decisions to preserve

- Alembic head: 0002. 0001 creates instruments/provider_instruments/market_ticks/latest_prices.
- 0002 opts into Timescale with `-x timescale=true`: daily chunks and 12 real-time CAGGs
  (last/mid for 1s/1m/5m/15m/1h/1d). Plain PostgreSQL mode uses on-demand aggregates.
- Source event time, otherwise receipt time, is the partition/history key. Latest ordering
  uses event nanoseconds, receipt time and UUID. History/latest commit together.
- Stable delivery UUIDs make uncertain-commit retry idempotent; Binance uses actual trade IDs.
  Repeated equal prices are preserved. Latest state is isolated by instrument/provider and
  represents one complete observation; it does not merge stale quote/trade fields.
- Futures are FUTURES:<exchange>:<root>:<YYYYMM>, with actual provider contract code retained.
  Same-month identity ambiguity is rejected. Front/next resolution requires explicit roll_at,
  never guesses expiry-day rollover, and never rewrites history.
- CAGG OHLC ordering uses a scalar fixed-width signed-offset key for nanoseconds/receipt/UUID
  ties. Named composite/array ordering types are unsupported by Timescale first/last and
  were not retained. Historical corrections behind the watermark require policy/manual
  refresh; tests force-refresh historical buckets for repeatability.
- Queue overflow drops NEW ticks, with exact metrics and rate-limited error summaries.
  SDK bridge observations discarded during reconnect/shutdown are also explicitly counted.
- WebSocket observations have persisted:false. Slow clients close with 1013.
- Run ONE ingestion/API process. No Redis/Kafka or distributed fan-out was introduced.

## Verified

- Full PostgreSQL/Timescale suite: `uv run pytest -q` with disposable marketdata_test URL:
  **82 passed**, one existing harmless Starlette/httpx deprecation warning.
- Guest fixture suite: **23 passed, 1 integration skip** without a database URL.
- `uv run ruff check .`, `uv run ruff format --check .` (39 files), `uv run mypy`
  (20 source files): passed. No new dependency or migration was needed.
- Base mock network smoke passed before switching the running app: 30 instruments,
  latest/history/candles/provider health and WebSocket.
- Compose override validation/build/start passed; one-shot migration exited successfully;
  application and TimescaleDB containers healthy.
- `uv run python scripts/smoke_tradingview.py`: passed over real localhost HTTP/WebSocket,
  17 persisted latest instruments, 5 historical BTC ticks, 8 1-second candles at observation,
  live client updates for SOL, gold and silver; provider healthy with no stale symbols.
- Paid OANDA/Databento sessions remain untested. No long-duration guest soak/load test or
  independent exchange timestamp/latency comparison has been completed.
- `uv sync --frozen --extra databento` (Windows Python 3.12.13; Linux image Python 3.12.15).
- `uv run pytest -q` with TEST_DATABASE_URL=.../marketdata_test: 58 passed.
- Same suite against plain mode .../marketdata_plain_test: 55 passed, 3 Timescale-only skips.
- `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`: passed.
- Fresh upgrade, downgrade through base, and re-upgrade in Timescale and plain modes: passed.
  Hypertable downgrade preserves history by rebuilding the ordinary table before base removal.
- `docker compose up -d --build`: passed; app/database healthy; migrations exit successfully.
- `uv run python scripts/smoke.py`: passed over real localhost HTTP/WebSocket connections:
  30 instruments, persistence/latest/history/candles, provider health and metrics.
- Real local database outage: stopped only this project's db container; app stayed alive,
  provider stayed healthy, /health returned 503, 3 DB errors observed, queue reached 430.
  Restarted db: health recovered to 200, queue drained to zero, no dropped ticks, latest age
  approximately 0.24 seconds. Follow-up network smoke passed.
- Latest load run: 300 instruments, 50,000 received/persisted, zero drops in 10.155 seconds
  including drain (~4,924 rows/sec), mean batch 163 ms. Native writer -> Docker TimescaleDB,
  while local mock app and Docker build were active. Short synthetic result, not a soak test.
  Repro: `uv run python -m marketdata.benchmark --seconds 10 --rate 5000 --instruments 300`.
  Ignored artifact: artifacts/benchmark.json (run 9B881F28).
- One harmless dependency warning: Starlette TestClient deprecates its httpx compatibility
  path. Tests still pass; no production workaround or broad dependency upgrade was added.
- CI workflow configured but not executed remotely.

## Local environment / working tree

Compose services are intentionally left running at http://localhost:8000 with the LIVE
TradingView guest override, PostgreSQL localhost:5432; volume tradingview_marketdata_pg.
Restart with `docker compose -f compose.yaml -f compose.tradingview.yaml up -d` to retain
the live configuration. Bare `docker compose up -d app` restores mock defaults. Query
`/prices/latest?provider=tradingview_guest` to exclude older simulated observations.
Existing historical data was preserved. Docker Desktop was started.
The local database additionally contains disposable marketdata_test, marketdata_plain_test
and marketdata_benchmark databases created during verification. Benchmark namespaces contain
only explicitly simulated data. Do not use integration fixtures on a production database:
TEST_DATABASE_URL must end in _test and its market-data tables are cleared.

Git resolves to the existing parent repository C:/Users/Pouyan on branch main. The initial
project snapshot is scoped to PycharmProjects/tradingview/ within that repository. Scope
future Git operations to this directory and preserve unrelated parent files/index entries.
No nested repository was initialized. Secrets/local .env/DBN exports/artifacts are ignored.

## Known limits / next priorities

1. Soak the experimental guest collector and monitor per-symbol freshness/rejections/drops,
   reconnects and protocol changes. Resolve data-use rights before relying on it in production.
   Exchange futures still need actual authorized realtime entitlements; do not replace them
   with TVC reference quotes or delayed/continuous series. Domestic feeds are out of scope.
2. Choose product-specific rollover schedules and operational calendars. Synthetic sample
   contracts are not exchange contract metadata. Definition imports require constant tick size
   and contract size, and reject options/spreads/ambiguous same-month contracts.
3. No durable spool, automatic tick backfill/replay, lossless long-outage guarantee, dynamic
   upstream subscriptions, cross-provider consolidation, or multi-process fan-out. SDK definition
   replay does not backfill missed market ticks. Consider durable recovery from measured needs.
4. Configure retention/compression, backups, deployment authentication and per-user downstream
   redistribution controls before exposing licensed data. No retention policy is imposed by default.
5. Provider symbols are unique per provider in this registry/schema; colliding raw symbols across
   datasets are rejected. Add a provider namespace/scope if future datasets require collisions.
6. Monitor data quality and longer-duration capacity. Health distinguishes receipt freshness
   from market status; there is no exchange-calendar market-open claim.

Continue by reading AGENTS.md, CODEX.md, this file and the relevant implementation/tests.
Preserve the working implementation and verify current behaviour before changing it.
