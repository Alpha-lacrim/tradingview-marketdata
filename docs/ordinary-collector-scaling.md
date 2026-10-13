# Ordinary TradingView collector: 100-source qualification

This document preserves the earlier ordinary scaling and **ACCURACY-SELECTED 100**
measurements. The current objective is the employer's business universe; see
[business qualification and decisions](business-universe-qualification.md). The 100-source
accuracy-selected run is complete and remains deployed as the preserved baseline. It is
not qualification of the provisional 99-source business registry, which has not been promoted.

The live path is `create_app` → registry → ordinary provider factory → existing
`CandleConnectionPool` → `CandleIngestion.run` → real `Store` transaction → PostgreSQL →
REST and persisted WebSocket events. No synthetic candles or benchmark namespace are active.
Schema remains 0004. Paused Batch 3 work is excluded.

## Fixed identities and activation

[Source validation](ordinary-source-validation.json) retains the original catalog/ranking
hashes and actual native 1-minute compatibility samples for all 256 earlier candidates.
All passed that bounded native preflight; compatibility does not promise future availability.
The 64/128/256 registries retain all original 34 entries unchanged in nested prefixes.

The [100-source selection](ordinary-100-selection.json) was frozen before its measurement:
33 original entries unchanged, plus 67 additions that each supplied all 60 completed native
minutes in the preceding 05:41–06:41 UTC selection window on 2026-10-13. The operator expressly
approved making `OKX:XAUTUSDT` inactive and replacing its active slot. Its canonical identity,
source mapping, 2026-10-07 09:33 UTC activation and 1,238 existing candles were independently
verified after the switch. The replacement is `OKX:PUMPUSDT`. No other original was removed.

Reproduce the exact frozen registry offline, including its captured CRLF byte hash:

```powershell
uv run python scripts/prepare_ordinary_100.py
```

It refuses a different selection/hash or overwriting another universe. New sources were
registered by ordinary ingestion with durable next-full-minute activation boundaries in
earlier stages. Switching to 100 does not reset them. All inactive expanded identities,
coverage rows, historical candles and unresolved gaps remain stored and queryable.
No measured universe changes in response to an omission within that run.

## Actual earlier ordinary measurements

These are actual scheduled cycles, separate from the prior isolated live benchmarks.
Native and immediate counts concern the current target; eventual includes later recovery.

| Universe / sockets / global | Cycles | Requests succeeded / expected | Native / immediate commits | Eventual commits | Current close-to-commit p50 / p95 / max, seconds | Slowest cycle / cycle-budget margin, seconds |
|---|---:|---:|---:|---:|---|---|
| 34 / 2 / 4 | 2 | 34 / 68 | 34 / 34 | 68 / 68 in later audit | 10.550 / 10.550 / 10.550 (one supplied cycle) | 12.113 / 35.887 |
| 64 / 2 / 4 | 10 | 640 / 640 | 639 / 639 | 639 / 640 (99.8438%) | 11.918 / 13.124 / 13.124 | 11.167 / 36.833 |
| 128 / 4 / 8 | 10 | 1,248 / 1,280 | 1,248 / 1,248 | 1,280 / 1,280 (100%) | 13.609 / 18.972 / 18.972 | 17.050 / 30.950 |
| 128 / 6 / 8, post-outage | 10 | 1,280 / 1,280 | 1,270 / 1,270 | 1,270 / 1,280 (99.2188%) | 19.795 / 22.502 / 22.502 | 20.612 / 27.388 |
| 256 / 4 / 8 | 10 | 2,560 / 2,560 | 2,394 / 2,394 | 2,394 / 2,560 (93.5156%) | 31.255 / 36.797 / 36.797 | 35.001 / 12.999 |
| Accuracy-selected 100 / 4 / 8, grace 5s | 30 + 3 settling | 2,975 / 3,000 | 2,972 / 2,972 | 2,997 / 3,000 (99.9%) | 22.984 / 33.052 / 39.161 | 34.253 / 10.747 |

34 had two opening-handshake timeouts in one minute. Later independent readback found all
68 targets and exact REST results. Only 34 stored WebSocket events were captured: 33 still
match current rows, and one later XAG correction matches retained revision old-values.
The initial prototype observer had a UTC key-format bug; its frozen raw 34 summary remains
unchanged. The later audit is explicitly separate evidence, not an invented first-commit time.
64 had one unresolved `BINANCE:MUBARAKUSDT` source target and no request errors. It passed the
original >=99.5% short escalation gate, but its 639/640 result is below the current 99.9% rule.
128 four/global-eight had one 32-source opening-handshake timeout; all 32 recovered in the
next cycle with first-commit latency 72.761 seconds. It failed the original zero-error gate.
128 six/global-eight met the operational collector gate, but its ten native omissions failed
both the original coverage gate and the current 99.9% rule. 256 had 166 unresolved native
targets; some sources supplied no current bar in the entire window. It failed qualification.
All supplied 64/128/256 targets exactly matched independent DB, REST and persisted WebSocket
readback. All these full windows had zero DB errors/retries and clean workers/sessions.

An earlier six/global-eight 128 run has only two completed cycles before Docker/host
interruption and no final downstream audit. Two post-restoration 128 recovery cycles supplied
249/256 expected targets. Both remain separately labelled partial/recovery observations.
No ordinary six/global-twelve 256 qualification was run; no new 512 activation occurred.
The prior six/socket global-twelve benchmark evidence remains in candle-live-end-to-end.md.

| Window | CPU seconds sampled | Peak app RSS, MiB | DB transaction acknowledgement p50 / max, seconds |
|---|---:|---:|---|
| 34 | 1.34 | 99.1 | 0.033 / 0.446 |
| 64 | 14.96 | 101.9 | 0.475 / 0.497 |
| 128 / 4 / 8 | 23.99 | 107.7 | 0.563 / 0.602 |
| 128 / 6 / 8, post-outage | 41.58 | 390.2 | 3.328 / 3.742 |
| 256 / 4 / 8 | 69.26 | 304.8 | 3.577 / 3.939 |

[Sanitized checkpoints](ordinary-collector-scaling.json) retain window timestamps, raw hashes,
first acknowledgement times, unresolved targets, worker error classes, CPU distributions,
loop-lag histogram/max, cleanup and history/activation integrity. CPU/RSS are sampled from
app PID 1, not total host or PostgreSQL usage. App loop metrics include startup. These windows
occurred at different times and database/recovery states: the four-versus-six comparison
holds global concurrency at eight, but does not isolate a causal socket-count improvement.

## Current 100 configuration and acceptance

Four connections, 25 sources per worker, local request windows two and global concurrency
eight are selected from the ordinary four/six socket evidence. Starts are paced two seconds
apart. Grace is five seconds; pool deadline 48 seconds, cleanup three seconds, correction
lookback three minutes, bounded history recovery 300 minutes. WebSocket queue is 1024.
The application cycle warning budget is `60 - grace - safety_margin`: 45 seconds at grace
five and margin ten. Report this separately from the 48-second pooled retrieval deadline.

The completed fixed baseline covers targets ending 06:56–07:25 UTC on 2026-10-13: 30
consecutive scheduled cycles, followed by three separate settling cycles through 07:28. These settling cycles can recover baseline omissions but do not increase its 3,000
expected-target denominator. Expected, request-success, actual native target, immediate
independent commit, eventual recovery and unresolved gap counts remain distinct.

The current numerical gate is >=99.9% eventual expected targets, exact DB/REST/stored
WebSocket consistency, clean sessions/pending requests, consecutive scheduled cycles and
at least five seconds of cycle budget left. A recovered request failure remains visible,
including its longer first-commit delay; it is not rewritten as request success.
Source omissions count against 99.9%. A successful request without a bar is neither a
fabricated candle nor a transport failure. HTTP 429/access rejection stops escalation,
checkpoints evidence and retains provider-wide cooldown (>=60s, respecting longer Retry-After)
and reduced request budgets. Repeated deadline/integrity failures prohibit promotion.
The earlier operational collector gate remains useful diagnosis, never a substitute for
this requested coverage criterion. Ten or thirty cycles do not prove sustained reliability.
Nor does a forward window establish 99.9% of the pre-existing historical database.

## Deployment identity and exact activation / rollback

Project **tradingview**, original working directory
`C:/Users/Pouyan/PycharmProjects/tradingview`. Sole app container at the 100 switch:
`5d580ce8ea9275f8683cbc8ec04e8dd9ffabfd7577358e7be88d9ff7d4e38433`.
Existing DB container:
`62caad90d2e9367a5d823cc3775102a00a0e07a79046b1aba3cd30074c4b1238`;
database `marketdata`, system identifier `7693817192740921379`, schema 0004,
volume `tradingview_marketdata_pg` at `/var/lib/postgresql/data`.
DB restart policy is now `unless-stopped`, changed in place without restart/recreation.
Never run `down -v`, create another production DB/project, or run truncating tests on it.

Reviewed source is in `artifacts/ordinary-scale`, branch `ordinary-scale-256`, based on
verified origin/main 2c5d0e6e. The name predates the 100-source objective. The original
home-rooted dirty checkout and paused `artifacts/batch3-database` are preserved; do not
pull the repository-root relocation into the deployed checkout.

100 switched at 2026-10-13 06:55:32 UTC, ready at 06:55:36. Image
`tradingview-app:ordinary-100` has manifest identity
`sha256:ca498afb02f6232440e3386936a4097e865cf9fefdbd0481adc0cc7ce623023f`.
The exact original 34 image is preserved as `tradingview-app:rollback-34-20261012`, image
config SHA `7936134ccf5aea4f4f8ca6e5641b4ea773cf7ddf56f36d7eeae3f748af22bc42`.
Inspect retained images; do not incidentally rebuild a known-good rollback tag.

From the clean worktree, activation and rollback commands are:

```powershell
# Activate the reviewed 100 image; preserve the original 34 fallback.
./scripts/Switch-OrdinaryCollector.ps1 -Count 100 -Connections 4 -Concurrency 8 -GraceSeconds 5 -Image tradingview-app:ordinary-100 -RollbackCount 34
uv run python scripts/observe_ordinary_collector.py --registry config/instruments.tradingview-100.json --minutes 30 --settle-minutes 3 --required-completeness 0.999 --output artifacts/ordinary-100-baseline.json

# Exact original 34 image and pilot configuration, preserving all expanded history.
./scripts/Switch-OrdinaryCollector.ps1 -Count 34
# Exact original 17 sources and single-socket configuration on that retained image.
./scripts/Switch-OrdinaryCollector.ps1 -Count 17
```

The helper verifies the sole existing app, project, volume, database system identifier,
required image and Compose configuration before stop. It waits for a bounded early-minute
idle window and replaces only app with `--no-deps --no-build`. It verifies registry readiness
and unchanged DB/container/volume, and attempts its explicit fallback if replacement fails.
For a tested expanded fallback specify its exact image, sockets, concurrency and
`-RollbackGraceSeconds`. Do not rely on defaults to preserve a differently tuned stage.
Global `/health` remains degraded for unresolved old coverage; registry readiness is
checked through `/providers`. Rollback never clears history or resets activation. Verify
missed-minute recovery by independent readback. The exact old image retains the older
large-recovery fan-out defect; a long outage should not be treated as risk-free on that image.

## Interruption / recovery and the scoped outage fix

Before preflight the original 34 image/configuration/volume were preserved. Read-only
native preflight ran while that sole app was stopped, with a bounded 48s pool deadline,
3s cleanup and a `finally` restart of the same container. Failure and restart traces are
retained separately. No registration or DB write occurred in native preflight.

A later real Docker/host interruption left the engine and DB stopped. Restarting the exact
existing DB restored all history on the same volume. Ordinary startup recovered **38,084**
real native candles; DB acknowledgement took 5.537s, close-to-commit 74.059s, whole cycle
74.074s. A post-commit linear list lookup per changed candle produced about 53s event-loop
lag and missed subsequent deadlines. Converting acknowledged keys to a set fixes that
specific quadratic fan-out defect. Regression tests verify exact large-batch fan-out and
suppression when no durable changed-key acknowledgement is supplied. This does not include
paused Batch 3 DB/persistence/retry changes or introduce a migration.

The following guarded interruption helper was prepared but **NOT RUN**: the employer
steered the task to business qualification and instructed that production remain unchanged.
It is an optional future fault test, not a measured result or next automatic action:

```powershell
uv run python scripts/qualify_ordinary_recovery.py --seconds 70 --cycles 3 --output artifacts/ordinary-100-controlled-recovery.json
```

It confirms an idle phase, arms an independent hidden watchdog, stops only the existing app,
and restarts the same container in `finally`, retaining its exact image/configuration and
DB volume. The watchdog also starts that same ID after 110s if the harness dies. No second
collector is created. It independently checks every missed target in PostgreSQL, REST and
captured persisted WebSocket events, with actual recovery acknowledgement times. The pause
and recovery cycles are fault measurements, excluded from the normal baseline.

Older gaps outside the 300-minute recovery horizon remain unresolved and visible. Source
status remains validated at runtime; observed native streaming is not a blanket entitlement
or promise of exchange-real-time delivery. No access controls were bypassed.

## Verification

TimescaleDB and ordinary PostgreSQL suites run only on migrated disposable databases ending
in `_test`; production schema is inspected without migration/reset. Final validation is
recorded in HANDOFF.md and the business report. Raw live checkpoints remain ignored;
sanitized measurements retain failed and partial denominators.

## Completed accuracy-selected 100 result

The 30 scheduled minutes expected 3,000 targets. 2,975 requests succeeded, 2,972 returned the
native target and were immediately durable. A worker opening timeout affected 25 requests;
all 25 recovered, with maximum eventual first acknowledgement 88.290s. Three native targets
remained unresolved after settling: BINANCE:EIGENUSDT ending 06:58 UTC, OKX:PUMPUSDT ending
07:14, and BINANCE:DOTUSDT ending 07:16. No source omission was removed from the denominator.
All 2,997 supplied targets exactly matched DB/REST/persisted WebSocket readback, cleanup
passed, and DB errors/retries were zero. The new 99.9% eventual gate passed exactly; the older
zero-request-error escalation rule did not. This short result is not sustained reliability.

CPU delta 116.25s; peak app RSS 250.1 MiB; sampled one-core CPU p95 16.840%; application loop
lag maximum 0.363s and p95 histogram bucket <=5ms. DB acknowledgement p50/max 2.557/2.999s.
Next-deadline margin minimum 25.747s, separate from cycle-warning-budget margin 10.747s.
The fixed old-history hash (27,526 rows before 01:55 UTC), all activation starts, database
identity/schema and 26,216 raw production ticks were preserved in before/after snapshots.
Its raw checkpoint is immutable historical evidence; never relabel it as a business gate.
