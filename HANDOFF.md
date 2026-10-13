# HANDOFF.md

## Current state — 2026-10-13

Collector and FastAPI Batch 1 are integrated in a NEW isolated worktree on
`integration/collector-fastapi-batch1`. Original source branches/worktrees remain clean and
unchanged. Source commits: FastAPI `6901bfadd44cb83ccdca49aac559e6f97a93a91d`, collector
`368bce1293db59c7623bcb6331232dc525ff943e`; common base/local origin/main
`2c5d0e6e51f51d0ae7298501df79e38859f3a464`. Start from FastAPI, merge collector with two
parents and reviewed integration fixes. Get final hash with `git rev-parse HEAD` here.
The unrelated dirty home-rooted main checkout at `324b1d83` and paused Batch 3 are untouched.
No push, merge into main, deployment, Batch 2 implementation or live TradingView access.

Read [integration report](docs/collector-fastapi-integration.md) for the conflict inventory,
test commands, schema preservation, isolated resources and future deployment/rollback.
[Process guide](docs/fastapi-process-separation.md) gives entry points and API contracts.
[Collector handoff](docs/fastapi-business-integration.md) retains the reconciled source risks.

## Integrated behavior

- Default API factory: role api, zero providers/registration/ingestion; read-only DB pool;
  multiple workers and historical REST remain available with collector stopped.
- Collector entry point: role collector, provider factory/registry/native recovery/writes,
  session advisory ownership acquired before registration/construction, status/metrics and
  shutdown. Combined one-worker development mode keeps local fanout.
- Single schema head 0005; 0001–0004 unchanged. New collector_status is additive; snapshots
  expire to unknown/disconnected/stale. API fails safely before migration and returns 503
  on DB outage. Collector writes health; SELECT-only API role tested separately.
- API-only authenticated WebSocket accepts then closes 1013; no cross-process delivery.
  Combined tick events remain precommit, candles postcommit. Streaming remains Batch 3.
- Collector FX:/FXCM quote identity correction, NULL/unclassified raw-volume provenance,
  independent OANDA/FXCM IDs/history/gaps and set-membership recovery optimization retained.
- Split scale override now targets collector and requires an explicit lock-aware image.
  Legacy scale override is preserved separately; legacy switch rejects split config/schema
  other than 0004 and accepts no 99 preset. Do not execute it for integration tests.

## Business evidence remains provisional

99 proposed sources: 50 Binance spot USDT, 21 OANDA forex, 21 FXCM forex, four OANDA OTC
metals, TVC USOIL/UKOIL references and OANDA gas CFD. Both broker histories stay independent.
Proposal/config, old 17/34/100/128/256 registries and frozen qualification ledgers are intact.
Prior historical result: 5,882/5,940 native candles (99.0236%); all 58 unresolved gaps preserved.
This does not qualify simultaneous 99-source collection or independent UI streaming.
No automatic activation; source/ranking/product/volume/UI decisions still require approval.
Read [business evidence](docs/business-universe-qualification.md) and the linked ledgers.

## Production boundary

Read-only metadata identified the same legacy 100-source combined app image
sha256:ca498afb02f6232440e3386936a4097e865cf9fefdbd0481adc0cc7ce623023f,
project tradingview, DB volume tradingview_marketdata_pg and port 5432. Exact deployed Git
commit is unknown (image has no revision label). No production SQL/credentials/data used.
App ID 5d580ce8ea9275f8683cbc8ec04e8dd9ffabfd7577358e7be88d9ff7d4e38433;
DB ID 62caad90d2e9367a5d823cc3775102a00a0e07a79046b1aba3cd30074c4b1238.
Start times/images/mounts matched before/after tests; detailed evidence in the integration report.

**Legacy combined collector never acquires the new advisory lock.** Before any future
approved replacement, stop it and verify exit before starting exactly one new owner.
An acquired advisory lock does not exclude it. Keep the current 100 registry unchanged.
Legacy code requires 0004: rollback after migration requires stopping all new owners and
the tested health-table-only downgrade, then one preserved legacy image/config. Never
blindly restart legacy code on 0005. These are documented procedures, not executed actions.

## Validation / next actions

Only isolated project collector-fastapi-integration-tests, loopback 55591/55592,
collector_fastapi_test, separate network and tmpfs data directories are used. No production
mounts, app deployments or provider connections. All test-owned native processes exited;
disposable DB services are removed after validation. Verified results:

- TimescaleDB 2.25.1/PG17: 335 passed (83.42s).
- Standalone PostgreSQL 17: 332 passed, 3 expected aggregate skips (68.84s).
- Ruff lint/format passed (108 files); strict mypy passed (30 source files); single 0005
  head, fresh upgrades, populated status-only downgrade/re-upgrade and whitespace passed.
- All eight market table snapshots preserved through 0004/0005; broker REST/tick/candle,
  NULL volume, gap recovery, API/collector restarts, two API workers, read-only role, ownership
  loss/contention, DB 503/recovery and real network 1013 verified. Existing real mock smoke passed.
- 38,400-candle fixture cycle: 1.295s and no linear acknowledgement search. Same-host short
  mock comparison averages differ about 7%, with overlapping ranges/all rows persisted/zero
  drops. Performance remains partial evidence; requested 5,000/s was not achieved. API traced
  startup 0.121s, latest ASGI p50 20.37ms, allocation peak 4.58MB; no live/RSS capacity claim.

Initial new fixture failures were corrected; no final failures. Existing Starlette/httpx
deprecation warning remains. Detailed commands/evidence/limits are in the integration report.

After verified local commit, Batch 2 must start from **this integration commit**, subject
to review/approval. Production SELECT-only credential/grant/DSN provisioning, application
image/deployment qualification and business promotion remain unexecuted. Batch 3 owns
cross-process streaming. No simultaneous 99-feed/live qualification is claimed.
