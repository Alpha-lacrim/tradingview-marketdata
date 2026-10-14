# External database and producer integration

The API reads the same PostgreSQL/TimescaleDB schema **0006** as the original collector.
The migration sources here are unchanged snapshots with LF line endings. The DBA owns the explicit
migration/backup/rollback plan. API startup only checks compatibility and opens read-only
connections. Both engines use the same native candle/event tables and contracts.

The development architecture keeps the Windows-laptop collector and its market-data database
separate from the company application database. Company-server FastAPI connects remotely
using configurable SELECT-only DATABASE_URL, with TLS/network access provisioned by the DBA.
No collector process or provider credentials are needed on the server. Deployment and actual
database migration are outside this task. The dashboard reads schema 0006 unchanged.
See [metric semantics](api.md#go-dashboard-candle-compatibility).

Use separate principals: schema owner/admin, authoritative external writer, SELECT-only API.
The [API reference](api.md#dedicated-read-only-database-role) includes explicit grants and
PUBLIC privilege review. The API owns no tables, needs no upstream credentials, and remains
ready while the producer is offline whenever the database is readable. Quotes age into stale,
health snapshots expire, and history/replay remain queryable. No new market events appear
without committed writes. Health and data freshness are separate from API readiness.

Use one authoritative writer per database/source, preserving the existing ownership
assumption. Do not start the old scraper and a replacement producer for the same sources.
The Python integration boundary is the included shared `marketdata` package:

1. Build `Instrument`, `ProviderInstrument` and `RegistryEntry`, then register
   `Registry(entries)` with `Store.register`. Canonical/source identities remain stable.
   Spot, OTC, CFDs, indices, perpetuals and concrete futures stay distinct.
2. Normalize quotes to `Tick` using `Decimal` prices and UTC-aware event/receipt timestamps.
   Preserve missing upstream time as null and use honest `DataStatus`. Batch `Store.write`
   maintains history, latest state and replay events in one transaction.
3. Supply completed UTC-aligned `MarketCandle` 1m bars to `Store.write_candles(candles,
   coverage_plans)`. It maintains history/latest/coverage/correction audit and events atomically.
   Newer receipts can correct bars; older receipts are ignored. Forming bars are rejected.
   Unknown/non-additive volume remains null with metadata.
4. Supply coverage/session expectations only when known. Missing minutes and incomplete
   derived intervals stay visible. Derived 5m/15m/1h/1d history uses persisted 1m bars.
5. Optional `Store.publish_collector_status` telemetry must describe actual producer health.
   Its absence does not prevent API reads. The sample writer publishes no fake heartbeat.

`src/marketdata/sample_data.py` is the executable independent producer example. It writes
only visibly simulated data to disposable `_test` databases. It never starts a provider,
loads a TradingView registry or calls upstream services.

**Direct SQL/legacy writers are not automatically streaming-compatible.** They must maintain
all expected state and append events using `events.py`'s transactionally locked clock in the
same data transaction. SERIAL/IDENTITY alone is not a commit-order cursor. API workers poll
committed journal rows; no broker, Redis or NOTIFY is required. Reuse the shared writer instead
of independently copying its SQL. Non-Python producers need explicit contract tests against
the supplied persistence/event implementation and migrations.

The public contracts are `docs/openapi.json`, `docs/ws-marketdata.schema.json` and
[api.md](api.md). On reconnect, restore subscriptions, replay to the acknowledgement's
`snapshot_cursor`, then drain bounded buffered live events. Upsert candles by exact
source/interval/bucket and preserve newer receipt versions. A 409 requires loaded-window
REST refresh; replay is bounded and delivery can duplicate. The supplied browser example
implements this recovery procedure.

Production uses an independent API DSN/token, TLS and explicit HTTPS CORS/WS Origins.
The runtime image supports mounted secrets through `.venv/bin/python -m marketdata.production api`
with `DATABASE_URL_FILE`, `API_KEY_FILE`, `API_WORKERS` (1..8) and explicit
`FORWARDED_ALLOW_IPS`. Do not put the permanent bearer in frontend code or WS URLs. Wire the
provisional BFF to genuine company session authorization; no login system is supplied.

The collector entrypoint and concrete transports are excluded. Only `marketdata.api:create_app`
is supported. Unchanged shared source retains lazy combined-mode branches and generic ingestion
type definitions for compatibility/provenance; `create_combined_app` and
`marketdata.production collector` are not distribution modes. They cannot enable a scraper here.
Tests assert that collector and concrete provider modules are absent.
