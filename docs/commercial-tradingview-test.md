# Authorized commercial TradingView preparation

On 2026-10-14 the operator confirmed full TradingView support permission and the company's
appropriate access arrangement for this use case, then explicitly confirmed that the existing
public-site connection is approved. No dedicated endpoint or account authentication was supplied.
This installation therefore keeps the observed public-site transport and `tradingview_guest`
provider identity, preserving all historical rows and mappings. Commercial permission does not
change the wire protocol, make it a documented vendor API, or establish realtime delivery for
every source. Upstream identity, delay, entitlement rejection and cooldown gates still apply.
TradingView's [public API guidance](https://www.tradingview.com/support/solutions/43000474413-i-need-access-to-your-api-in-order-to-get-data-or-indicator-values/)
remains distinct from this operator-reported arrangement. Keep the private support agreement in
the company's records, outside this repository.

## Prepared configuration

`Settings` and the production Compose profile enable the reported access approval. Set
`TRADINGVIEW_PRODUCTION_ACCESS_APPROVED=false` to revoke production ingestion; tests cover both
approved and revoked operation. `.env.example` now matches the approved installation.

Apply `compose.production.tradingview.yaml` after `compose.production.yaml`. It selects the
preserved accuracy-selected 100-source registry, native completed 1m candles, four connections,
two concurrent requests per worker/eight global, two-second connection pacing and five-second
collection grace. Futures remain disabled. These are existing local operational bounds, not a
claim about newly granted upstream limits. The unapproved business-proposal registry is not
selected. No schema change: head remains 0006.

An additional opt-in `compose.tradingview-dual.yaml` adds independent quote workers to this
same registry. It requires a reviewed dual-capable collector image and a separate approved
switch. It has not been applied to production. See [dual qualification](dual-ingestion-qualification.md)
for the released v1.0 consumer checks and bounded quote observer. The existing commercial
profile remains candle-only until that switch is approved.

## Target preparation and switch

Follow [production deployment](production-deployment.md) for verified volume identity, separate
DB principals/file secrets, reviewed images, HTTPS origins, backups and firewall. Follow
[collector integration](collector-fastapi-integration.md) for the legacy schema0004 collector's
stop-and-replace and tested rollback. The currently running legacy collector has no ownership
lock; it must be absent before starting the new collector. Do not run a second live scheduler.
This preparation has not switched or migrated that deployment.

On the prepared Linux target, use the same project/configuration for every operation:

```bash
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml -f compose.production.tradingview.yaml --profile ingestion config --quiet
# After the reviewed backup, legacy stop, schema0006 migration and principal setup:
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml -f compose.production.tradingview.yaml --profile ingestion up -d --no-deps collector app
```

These are rollout instructions, not evidence that target prerequisites are already provisioned.
Never attach new code to an unverified volume or restart an old schema0004 binary on schema0006.

## End-to-end test

From this repository with dependencies installed, point the test at the running split-process
API. The token is loaded from a private file; it is never a CLI argument. `--origin` must match
the actual configured HTTPS company origin. HTTP is allowed only on loopback:

```bash
uv run python scripts/smoke_production_tradingview.py --base-url http://127.0.0.1:8002 --api-key-file /etc/marketdata/secrets/api_key --origin https://YOUR-COMPANY-HOST --registry config/instruments.tradingview-100.json --minutes 3 --output artifacts/production-tradingview-smoke.json
```

The read-only observer opens no upstream connection and needs no SQL credentials. It checks
liveness/readiness, unauthenticated HTTP rejection, authenticated versioned WS subscription,
future completed native minutes, exact source/status/Decimal OHLCV reconciliation with persisted
REST history, latest candles/prices, coverage and running collector health. Events arrive from
the transactionally committed database journal, not combined-process legacy fanout. A checkpoint
retains source identities, events and missing symbol/minute pairs even on failure. Credentials
and arbitrary exception text are excluded. Runtime is up to the selected minutes plus 55 seconds
and REST reconciliation. The observer does not replace independent database integrity checks.

Run during a confirmed overlapping source session. The smoke is intentionally strict: closed,
sparse or missing native minutes fail; it never synthesizes bars or removes a source to obtain
a pass. Inspect coverage and health to classify failures. Corrections that race REST reconciliation
also fail visibly and require inspection. A passing three-minute run is connectivity evidence,
not sustained capacity or a continuous quote-feed guarantee. Candle-close fallback remains
distinct from a live quote.

Then perform the existing isolated API qualification and recovery tests in
[Batch 4 verification](api-batch4-verification.md), followed by company HTTPS/browser-session
testing and a sustained target-host soak. Destructive faults and restore tests belong only on
the disposable test topology, never the running production database.

## Preparation verification — 2026-10-14

Full isolated PostgreSQL: 439 passed, three expected Timescale-only skips (111.17s).
Full isolated TimescaleDB: 442 passed (122.88s), including Linux production-container
startup/restart with mounted SELECT-only secrets. Final targeted approval/smoke/checkpoint
regressions: nine passed. Ruff check/format, strict mypy source plus the new observer,
Alembic head 0006, merged Compose profile/revocation override and CLI help passed.
The task-owned test containers/network were removed. Existing production services/volume
were preserved. The live commercial target smoke has not run; the legacy app was unhealthy
on inspection and must follow the documented replacement procedure before this versioned test.
