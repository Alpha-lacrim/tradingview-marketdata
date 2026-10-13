# Collector and FastAPI integration handoff

Integration is now implemented locally on `integration/collector-fastapi-batch1` from exact
FastAPI `6901bfadd44cb83ccdca49aac559e6f97a93a91d` and collector
`368bce1293db59c7623bcb6331232dc525ff943e`. The completed resolutions and verification are
in [the integrated report](collector-fastapi-integration.md); the inventory below records
the source handoff requirements. No deployment or live test is authorized by this document.

The collector branch `ordinary-scale-256` is finalized as a local commit based on
`2c5d0e6e51f51d0ae7298501df79e38859f3a464`. Its name predates the business-universe steer.
FastAPI Batch 1 is clean at local commit `6901bfadd44cb83ccdca49aac559e6f97a93a91d` on
`fastapi-batch1-isolation`. That worktree was inspected read-only. No merge, push or deployment
was performed. Integrate the two commits in a separate third worktree, preserving both originals
and the paused Batch 3 worktree.

The employer accepted 5,882/5,940 (99.0236%) as a provisional development milestone, sufficient
to proceed with service integration. The 99-source selection remains unapproved for deployment;
its simultaneous scheduled capacity and sustained reliability have not been qualified.

## Integration points and conflicts

| Area | Required reconciliation |
| --- | --- |
| Startup and ownership | Preserve Batch 1's API-only default, `marketdata.collector` entry point, lock lifetime and shutdown. Only the collector builds providers, registers sources and starts ingestion. Legacy stored-only TestClient evidence does not validate the new lifecycle. |
| Provider factory | Both branches inherit the bounded pool factory. Preserve source-specific subscriptions, shared cooldown, pacing, request budgets and cleanup. API workers must never call the factory or open TradingView transports. No new sockets are justified by integration alone. |
| Identities and persistence | Retain the 99-source proposal and seven alternatives separately from the accuracy-selected baseline. `OANDA:EURUSD` and native `FX:EURUSD` (FXCM) have distinct IDs, canonical symbols, candles, ticks and coverage. Preserve Batch 1's read-only API Store and persisted-registry loader; never register an active proposal in API startup. |
| Recovery and fan-out | Keep the set conversion in `CandleIngestion.commit`: list membership across tens of thousands of recovered bars caused quadratic work. Preserve changed-key acknowledgements and source-specific recovery/activation starts. Run the large-recovery regression. API-only processes serve database-backed REST reads and do not consume collector-local fan-out; no cross-process polling stream is implemented. |
| Quotes | Port the scoped `FX:` chart namespace to `FXCM` quote-venue validation and unknown-volume evidence preservation without relaxing symbol/type/currency/delay gates. Run mismatch regressions. The 14 real stored quotes prove persistence and stored API readback only; implement continuous quotes and UI freshness separately. Never present candle_close as a live last price. |
| Metrics and health | Both branches add metrics beside `Metrics.registry`; combine API request/DB gauges with event-loop lag histogram/max, retaining names and avoiding duplicate registration. Preserve collector_status TTL, ownership-loss shutdown and provider/source degradation. API health and collector health must remain distinct. |
| Configuration and Compose | Reconcile `.env.example`, `compose.yaml`, README/CODEX/HANDOFF and source docs. Keep the narrow DB `restart: unless-stopped` correction. Scaling overrides currently target the legacy `app` service; they are incompatible with API-only `app` plus separate `collector` until explicitly adapted. Count=99 is not a supported switch preset. Pin exact reviewed image/configuration rather than running a worktree's default Compose project. |
| Migrations | This collector branch adds no migration and its Store requires 0004. Batch 1 adds 0005 collector_status and requires 0005. Preserve its head check, test fixtures and status schema; test upgrade on isolated TimescaleDB and plain PostgreSQL. Do not deploy a 0005 database with a legacy Store that requires exactly 0004. No production migration occurred here. |
| REST and WebSocket | Preserve Batch 1's read-only persisted identities and stored native candles. Verify separate broker query results, stale quote versus candle latest selection and timestamps. API-only workers accept then close 1013 because cross-process streaming is unavailable; the real network regression proves that refusal, while combined development separately tests overflow. Test API restart and collector restart independently. |
| Documents and tests | Resolve documentation around the resulting combined behavior, not by taking one branch wholesale. Keep all collector regressions, original benchmark denominators, the 58-target ledger and Batch 1 process/lock/API tests. No Batch 3 changes or test weakening. |

## Legacy collector and controlled deployment

**The currently deployed legacy combined collector does not acquire Batch 1's advisory lock.**
A successful lock acquisition by the new process does not exclude that legacy collector.
A future authorized switch must first stop the sole legacy collector and verify it exited,
then start exactly one lock-aware collector. API replicas must have role=api and read-only DB
sessions. Preserve the original project, database identity, volume and exact 100/34 rollback
images/configurations; never run `compose down -v` or recreate the database.

Migration 0005 and the legacy Store's exact 0004 check also make rollback a schema-compatibility
decision. Before any deployment, qualify an image for the old registry on schema 0005 or a
reviewed, narrowly scoped collector_status downgrade after stopping all 0005 processes.
Do not blindly restart the unchanged legacy image after migrating production. Test either
rollback route on isolated resources first; do not execute it during this integration task.

## Next steps in the third worktree

1. Merge the two local commits there and resolve the startup/Compose/metrics/docs overlaps.
2. Run both full isolated DB suites, lint, formatting, strict typing and 0004-to-0005 migration
   checks; verify immutable identities and activation starts with expanded historical data.
3. Exercise independent API and collector processes, ownership contention/loss, recovery fan-out,
   both broker histories/quotes and network WebSocket 1013. No live access is required for these
   deterministic checks; any later TradingView test needs permitted access and bounded budgets.
4. Request employer instrument/product/ranking approval before activation. Qualify simultaneous
   business collection and the Iranian deployment network separately. Missing source bars and
   unresolved no-trade/publication causes remain visible; 99.9% is a longer-term goal.

Production remains the accuracy-selected 100 profile, not the 99 proposal. The 106-source
historical qualification and two-source isolated cycle do not establish simultaneous 99 capacity.
