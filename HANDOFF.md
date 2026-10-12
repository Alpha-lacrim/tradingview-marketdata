# HANDOFF.md

## Current state — 2026-10-12

The upstream application is already at the Git root following PR #1 (533e568 → 5d5492b →
b94d1a2). The separate tradingview-marketdata clone and upstream root README/platform-guide,
CI and ignore changes remain intact. Do not pull relocation into the home-rooted deployment
checkout; see docs/repository-layout.md. This batch ports reviewed changes in an isolated
root-layout worktree without relocating the deployed directory or changing its Docker project.
Publication is not deployment. GitHub is public; never stage credentials or raw artifacts.

Batch 2 investigated BINANCE:ATOMUSDT's 12:14 UTC target without restarting or changing the
running 34-instrument deployment. Two bounded idle-window raw native-history captures at
12:49 and 12:52 both contain 12:13/12:15 neighbors and sufficient preceding history but no
12:14 timestamp. Original target-presence diagnostics were null before request completion;
ordinary recovery keeps requesting 12:14. The gap remains EXPECTED, not synthesized or
classified as a verified permanent upstream omission. Cause/permanent absence and possible
future publication remain unknown. Read docs/native-availability-investigation.md/json.

Separate 24 scheduled cycles over [12:25,12:49) UTC: 816 expected; 782 successful requests,
782 available native targets, 782 immediate and 816 eventual commits. Both socket handshakes
timed out at 12:26 (34 retrieval errors, zero DB failures); all 34 missed 12:25 bars recovered
at 12:27:06.846071, latency 66.846s. 24 successful transactions include one coverage-only
transaction; 23 had native current targets. Current close-to-commit min/p50/p95/max:
6.700/9.385/14.314/15.454s. All 816 REST/DB OHLCV values matched. Stored-only WebSocket/DB/REST
smoke passed 68/68 over buckets 12:53/12:54. Older gap partitions, activation and original
identities match pre-pilot evidence. No diagnostic DB writes or 64 activation.

New source diagnostics separate raw history bounds/count/target presence from accepted
completed count/target presence, with tests for missing and completion-filtered targets.
They are host/committed source changes, not yet in the preserved running Batch 1 image.
Offline analyze_native_availability.py reports separate request/native/commit/recovery counts
and recurring source/cohort patterns. Opt-in probe_native_history.py permits one bounded
read-only capture while workers are idle; aborts busy/cooldown/access-rejected states, no retries.
Raw frames remain ignored; sanitized evidence/hashes are published. Historical patterns are
observations, not proven no-trade/late-source causes. Common 128-source eventual counts were
743/768, 745/768, 748/768 across larger non-simultaneous stages; time and count are confounded.

Batch 1 deployed the ordinary application with a separate 34-instrument pilot and left it
running against the existing production marketdata TimescaleDB database. The normal path is
create_app → selected registry → provider factory → CandleConnectionPool → CandleIngestion
→ real Store transaction → PostgreSQL → REST/persisted WebSocket. No benchmark namespace or
synthetic source is used. The original 17 registry is unchanged and remains a tested rollback.
64 was neither tested nor activated; no new 256/512 experiment occurred in this session.

Read docs/production-instrument-expansion.md and its adjacent JSON for operations and measured
evidence. Raw checkpoints remain under ignored artifacts/batch1-*. The transport remains
unsupported and experimental, with no inferred upstream entitlement or redistribution right.
The pilot is distinct from the employer's eventual final selection.

## Implementation and decisions

TRADINGVIEW_CANDLE_POOL_ENABLED is an explicit validated ordinary-factory opt-in. Instrument
count comes from REGISTRY_FILE; capacity overflow fails before registration/upstream I/O.
The existing pool owns a single scheduler and sorted worker partitions. Pilot override uses
two connections, local windows two, global four, two-second start pacing, 48-second deadline
and three-second cleanup. Local guest safeguards are six sockets, 100 instruments/jobs per
worker, 600 total and 12 global requests, not provider-granted numerical allowances. Grace +
deadline + cleanup must fit one minute. Default/original override remains single socket and
explicitly disables pooling. Native identity/status/completion and Decimal validation,
provider-wide 429 cooldown/Retry-After, decreasing windows and rejection behavior remain.

config/instruments.tradingview-pilot.json copies the original 17 entries unchanged and adds
source-qualified CRYPTO:BINANCE:<BASE>USDT spot identities for BNB, DOGE, ADA, LINK, LTC, BCH,
AVAX, TRX, DOT, NEAR, SUI, UNI, FIL, AAVE, ATOM, ZEC and ICP. Each addition had all three native
baseline targets and all six eventual minutes in the previous 128 live stage, independent of
scanner discovery. Prior/current native samples and the prior raw evidence hash are retained.
Mapping status stays UNKNOWN until runtime chart validation proves streaming/realtime delivery.

Application registration atomically initializes a new native coverage row at the next full
UTC-minute boundary, with an empty start=end interval. New additions start at 2026-10-12
12:10 UTC; original 17 retain 2026-10-07 09:33 UTC. Registration never overwrites existing
coverage. Legacy native history without coverage retains its first actual stored bucket.
Restart before the first candle uses durable coverage_end. Current-minute diagnostics exclude
not-yet-activated buckets. Source reassignment under the same instrument/provider key fails
atomically; canonical IDs remain deterministic and original identities unchanged. Removing a
source from active registry deletes no candles/latest/coverage/mappings. Reactivation resumes
that history with bounded recovery. Read-only reconciliation excludes pre-activation time.
No schema migration was required; head is still 0004.

## Measured live evidence

Ten scheduled full-universe cycles cover buckets [12:10,12:16) and [12:21,12:25) UTC:
340 expected targets; 340 successful requests; 339 actual native targets; 339 immediate and
339 eventual durable commits (99.706%); ten successful transactions; zero retrieval/database
errors. ATOM 12:14 was not seen before initial request completion and remains an EXPECTED
gap. Batch 2 confirmed snapshot absence, not permanent absence/cause. No synthesis or source
replacement occurred. Nine of ten scheduled minutes were full.
Initial startup/transitional 17-eligible cycles, incidental uncaptured cycles and original-17
rollback interval are excluded from this denominator; resumed startup is reported separately.

Scheduled close-to-local-commit min/p50/p95=max: 6.537/6.925/11.798s. DB write:
0.391/0.405/0.463s. This measures actual real transaction-return acknowledgement, not internal
PostgreSQL commit time or request duration. Short samples are not sustained-capacity claims.
A later separately recorded final spot check of bucket 12:31 UTC persisted 34/34, zero
retrieval/database errors, with 15.454s close-to-commit; 11.798s is not a latency ceiling.
Development/tests/image builds overlapped parts of initial observation on the shared host.

First five-minute strict smoke failed on ATOM after four 34/34 WebSocket/DB/REST minutes;
retain artifacts/batch1-pilot-smoke.json and batch1-pilot.log. Live original-17 rollback then
passed two consecutive minutes (34/34 exact WebSocket/DB/REST). Restore startup recovered
51/51 newly suspended targets over [12:17,12:20), plus 17 current new startup targets. Startup
marked all 68 backfilled; only 51 were previously missed historical targets. Startup current
close-to-commit was 39.352s; recovered candles 81.352–201.352s. Old history was retained.
Warmed post-restart three-minute strict smoke passed 102/102 targets; preserve
batch1-resumed-smoke.json. An earlier local observer started before recreated-app HTTP
readiness and saw WebSocket EOF; warmed rerun passed. This was not a provider failure.
Additional REST/DB read-back verified exact Decimal OHLCV/source/status for all 339 scheduled
persisted targets, plus the one true absence. Published evidence preserves per-candle first
commit acknowledgements and checkpoint hashes. No provider server outage was injected.

Every measured pool cycle ended with workers disconnected, no cleanup errors and zero active
requests/sessions/pending futures. No HTTP 429 or access rejection. Actual rollback/restoration
kept one collector at a time and retained all activation timestamps. The new sources have
zero pre-activation candles. Original IDs/mappings and all old missing/unknown partitions
outside the 300-minute horizon match the before snapshot. Guest raw ticks remain 26,216;
zero duplicate logical candles. Five-day reconciliation proved all 34 coverage partitions
while evaluating new sources only from activation. Database and ingestion are healthy;
global health remains degraded due historical gaps and ATOM's missing minute. REST is available.

## Verification and operations

Batch 2 final root-layout publication checkout: TimescaleDB 296 passed (53.06s), PostgreSQL
293 passed plus three expected Timescale skips (52.21s). Both disposable test DBs ran
Alembic upgrade head; Ruff lint/format and strict mypy passed. Targeted diagnostics/audit:
47 passed, four integration cases deselected. Five-day read-only reconciliation through
12:55 UTC proved all 34 partitions: additions evaluated 45 minutes, 764/765 stored, only
ATOM 12:14 missing. Running app container remains the same instance started 12:21:33 UTC.
No migration change or production truncation/deletion. Existing TestClient warning remains.

Batch 1 TimescaleDB suite: 292 passed (44.76s). Ordinary PostgreSQL: 289 passed, three expected
Timescale-only skips (43.49s). Targeted activation/restart tests pass; Ruff lint/format and
strict mypy pass; both migrated disposable test databases ran Alembic upgrade head; scoped
diff whitespace check passes. Existing Starlette/httpx TestClient deprecation warning remains.
No production tables were truncated/dropped and no migration changed. Final application image
contains the source/registry changes; scripts are host-side read-only observers.

Activation: build app with compose.yaml + compose.tradingview.yaml +
compose.tradingview-pilot.yaml, stop app, then up -d --no-deps --no-build app with those files.
Rollback: stop app, then up -d --no-deps --no-build app using only compose.yaml +
compose.tradingview.yaml. Do not touch DB volumes or launch another worker/collector. Full
commands and stored-only smoke options are in docs/production-instrument-expansion.md.

The deployment checkout's Git boundary is intentionally C:/Users/Pouyan with an allowlist
and only three tracked wrapper paths outside PycharmProjects/tradingview. However, fetched
origin/main has already merged root relocation through PR #1 (533e568, 5d5492b, b94d1a2).
Do not pull that relocation into the home-rooted checkout. Publication uses an isolated
worktree artifacts/batch2-publication based on the current upstream root layout, preserving
upstream work and the deployed directory. Port only reviewed files; no blanket home-directory
staging, credentials, raw artifacts or database files. Repository is public according to the
upstream relocation handoff; older private claims were stale. Publication is not deployment.

## Previous experiments and next actions

Prior isolated 128/256/512 experiments and their failures remain documented in
candle-live-end-to-end.md/json: all were LIVE INCOMPLETE, and eight unpaced sockets previously
caused HTTP 429. They are separate from this production pilot. Preserve raw evidence and
existing larger-stage denominators; no new larger-stage claims are made here.

Continue the fixed 34 universe for longer quiet-host/native availability and memory soaks.
Continue investigating unresolved native targets without replacing symbols or pretending
transport success means native coverage. Test real DB/server and network outages and uncertain
commit recovery in a controlled setting. Keep 64 inactive until longer 34 evidence supports
it; then evaluate within the same granted scope without increasing request/connection pressure
after throttling. Original five-day gaps beyond recovery horizon remain unresolved and must
not be cleared. Confirm employer final instrument selection independently of this pilot.
