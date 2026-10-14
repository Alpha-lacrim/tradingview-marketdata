# Ordinary collector instrument activation

This preserves the original 34/17 pilot evidence. Current deployed accuracy-selected 100
results are in ordinary-collector-scaling.md; the unapproved employer business proposal,
source qualification and preserved-history activation conditions are in
[business-universe qualification](business-universe-qualification.md).

The separate `config/instruments.tradingview-pilot.json` contains the original 17 entries
unchanged plus 17 Binance USDT **spot** pairs: BNB, DOGE, ADA, LINK, LTC, BCH, AVAX, TRX,
DOT, NEAR, SUI, UNI, FIL, AAVE, ATOM, ZEC and ICP. This fixed pilot is not the employer's
eventual final selection. No futures, perpetuals or alternative venues are substituted.
New canonical names are `CRYPTO:BINANCE:<BASE>USDT`; source names are
`BINANCE:<BASE>USDT`. Original UUIDs and source mappings are unchanged.

Every addition supplied all three baseline target minutes and all six eventual target
minutes in the earlier 128-instrument live experiment. The sanitized native samples and
raw checkpoint SHA-256 are in [machine-readable evidence](production-instrument-expansion.json).
Scanner discovery alone is not verification. Mapping status remains UNKNOWN until each
current chart response passes the existing exact identity, type, venue/currency, intraday,
native trade OHLC, no transform/fill, aligned completion and streaming/realtime checks.
Previous access or samples do not guarantee future native-minute availability or grant
data-use entitlements. The guest transport remains experimental and unsupported.

## Configuration and ordinary execution

`REGISTRY_FILE` determines the universe; there are no count-specific branches.
`TRADINGVIEW_CANDLE_POOL_ENABLED=true` makes the ordinary provider factory construct the
existing `CandleConnectionPool`. Validated settings control connection count, per-worker
capacity/window, total instruments, global concurrent requests, connection-start pacing,
deadline and cleanup. Registry overflow fails before database registration or upstream I/O.
The explicit guest opt-in retains bounded local safeguards: six sockets, 100 instruments
and jobs per worker, 600 total, 12 global requests. These are not upstream allowances.
Deadline + grace + cleanup must fit the ordinary one-minute cadence. The pilot uses two
workers, two requests per worker, four globally, two-second connection-start spacing,
48-second deadline and three-second cleanup. No additional collector process is used.

The normal path is `create_app` → selected registry → provider factory → connection pool
→ `CandleIngestion.run/cycle` → `Store.write_candles` → existing TimescaleDB/PostgreSQL.
REST and persisted-candle WebSocket fan-out continue through the same application.
There is no benchmark namespace, RecordingStore, fixture transport or synthetic candle
in live pilot collection. Access rejection, decreasing request windows and provider-wide
HTTP 429 cooldown/Retry-After behavior remain intact. No in-cycle replacement sockets.

## Activation and history

The application registers the active native-candle providers in the same transaction as
instrument mappings. A newly activated source receives an empty coverage interval whose
start/end is the next full UTC-minute boundary. Its first eligible bucket starts there;
collection waits until that bucket has closed plus grace. No initial historical backfill
was requested for this pilot. The 17 additions activated at **2026-10-12 12:10:00 UTC**.

Existing `candle_coverage` rows are never reset by registration. Legacy native history
without a coverage row retains its first stored bucket as its beginning. Restart before
the first candle uses the durable coverage endpoint, rather than a rolling overlap that
could forget activation. `latest_candles` remains updated only by real accepted candles.
Source reassignment under an existing instrument/provider key fails atomically; canonical
UUIDs remain derived from the original canonical symbol and identity checks still apply.

Removing a source from the selected registry stops its subscriptions, without deleting
instruments, candles, latest state or coverage. REST history remains queryable. Downstream
WebSocket subscriptions follow the current active registry. Reactivation resumes the same
coverage history and bounded recovery, including real minutes missed during suspension.
Read-only reconciliation now evaluates only the requested range at/after coverage_start;
pre-activation wall-clock time is not reported as missing or unknown collection history.
Original five-day coverage and older unresolved gaps remain intact. No migration was needed.

## Safe activation and rollback

Run from the project directory. Ensure this is the only collector for this universe.
Build before replacing the app; do not remove database containers, volumes or history.
For this existing deployment, that directory remains the original nested checkout. The
upstream root-layout publication is separate; changing checkout names changes Compose project
and volume names. Continue operating the deployed directory until an intentional deployment
migration. Batch 2 does not deploy the new diagnostics or start an app from its publication checkout.

```powershell
docker compose -f compose.yaml -f compose.tradingview.yaml -f compose.tradingview-pilot.yaml build app
docker compose -f compose.yaml -f compose.tradingview.yaml stop app
docker compose -f compose.yaml -f compose.tradingview.yaml -f compose.tradingview-pilot.yaml up -d --no-deps --no-build app
uv run python scripts/smoke_tradingview_candles.py --stored-only --registry config/instruments.tradingview-pilot.json --minutes 3 --output artifacts/pilot-smoke.json
```

Rollback to the original 17 and single socket uses the same tested application image:

```powershell
docker compose -f compose.yaml -f compose.tradingview.yaml -f compose.tradingview-pilot.yaml stop app
docker compose -f compose.yaml -f compose.tradingview.yaml up -d --no-deps --no-build app
uv run python scripts/smoke_tradingview_candles.py --stored-only --minutes 2 --output artifacts/rollback-smoke.json
```

The original override explicitly disables pooling and selects one connection, preventing
a leftover `.env` flag from defeating rollback. Its registry file remains unchanged.
Restore the pilot with the activation commands. Registration preserves timestamps and
history. An intentionally inactive source is not promised continuous upstream collection.

## Measurement definitions

Successful requests are transport/parser responses, including valid responses without
the target bar. Available native targets are actual validated bars for the closed minute.
Committed targets are those durably stored, with exact Decimal OHLCV/source/status read
back independently and compared with REST and persisted WebSocket messages. Recovered
targets are previously absent minutes later supplied by native history and committed;
they do not rewrite immediate completeness. Unresolved native targets remain in the denominator;
missing stored bars alone do not verify an upstream omission.

Close-to-commit is measured from the actual UTC bucket end to the local UTC acknowledgement
immediately after the real Store transaction returns. It includes scheduler/grace, network,
parsing and transaction work; it is not a request-duration estimate or PostgreSQL's internal
commit timestamp. Transaction `created_at` is not substituted for commit acknowledgement.
Short-run percentiles are descriptive, not sustained-capacity guarantees. Development
validation and image builds overlapped parts of the initial observation on this shared host.

## Live result — 2026-10-12

34 instruments were deployed to the existing `marketdata` TimescaleDB database and left
running. Ten measured scheduled cycles cover buckets [12:10,12:16) and [12:21,12:25) UTC.
Activation/startup/transitional cycles, incidental uncaptured cycles and the original-17
rollback interval are excluded from this scheduled denominator and reported separately.

| Scheduled measure | Actual result |
|---|---:|
| Expected target candles | 340 |
| Successful retrieval requests | 340 |
| Available validated native targets | 339 |
| Immediate durable targets | 339/340 (99.706%) |
| Eventual durable targets at final read-back | 339/340 (99.706%) |
| Successful scheduled transactions | 10 |
| Retrieval errors / database failures | 0 / 0 |
| Close-to-local-commit acknowledgement min / p50 / p95=max | 6.537 / 6.925 / 11.798s |
| Database-write min / p50 / p95=max | 0.391 / 0.405 / 0.463s |

A later final spot check of bucket **12:31 UTC** also persisted 34/34 with zero retrieval
or database errors, at **15.454s** close-to-commit. It is recorded separately from the frozen
ten-cycle denominator above; the pilot does not establish an 11.798s latency ceiling.

`BINANCE:ATOMUSDT` lacked its 12:14 UTC bar despite a successful native chart response.
It remains an EXPECTED gap; subsequent history did not supply it. Nine of ten scheduled
minutes were immediately complete. The first strict five-minute smoke failed on that
minute after four full WebSocket/DB/REST comparisons; the failure checkpoint is retained.
After restart, the warmed three-minute smoke passed 102/102 exact downstream/DB/REST
OHLC comparisons. Additional historical REST read-back verified exact persisted Decimal
OHLCV/source/status for all 339 scheduled targets, and absence for the missing target.

Batch 2 captured raw native history twice and confirmed the target absent in those snapshots,
with both neighboring bars and sufficient history bounds. Permanent absence/cause remains
unverified; see [the investigation and extended recovery evidence](native-availability-investigation.md).

An actual rollback to the original single-socket 17 registry passed two consecutive
WebSocket/DB/REST minutes (34/34). Expanded history remained stored. On restoring the pilot,
the startup transaction recovered **51/51** newly missed historical candles over buckets
[12:17,12:20) UTC, plus 17 new current startup candles. All 68 were flagged backfilled by
startup reconciliation; distinguish the 51 previously missed targets from the 17 current
targets. That startup's 34 native targets had close-to-commit 39.352s because startup was
late in the minute. Recovery close-to-first-commit ranged 81.352–201.352s. No unresolved
old gaps were cleared. A local observer attempted its WebSocket before recreated-app HTTP
readiness and received EOF; its warmed retry passed, without extra upstream comparison.

Each measured pool returned zero active requests, sessions and pending futures, with
disconnected workers and no cleanup error. No HTTP 429/access rejection occurred.
Original 17 UUID/source mappings, coverage starts and pre-horizon missing/unknown ranges
match the before snapshot. Guest raw ticks remain **26,216**; zero logical candle duplicates,
zero new pre-activation candles, schema head **0004**. A five-day reconciliation excluded
pre-activation time for additions and proved all 34 EXPECTED/NOT_EXPECTED/UNKNOWN partitions.
The database and ingestion are healthy; global `/health` remains correctly degraded due
existing old gaps and the new ATOM omission. REST remains available.

Final full suites: **292 TimescaleDB passes**; **289 ordinary PostgreSQL passes and three
expected Timescale-only skips**. Ruff lint/format, strict mypy, both test-database Alembic
upgrade-head checks and scoped diff whitespace checks pass. Existing Starlette/httpx
TestClient deprecation warning remains. No migration or production-history deletion.

Measured cycles, native samples, per-candle first commit acknowledgements, recoveries,
integrity checks and raw checkpoint hashes are in the adjacent JSON. Do not change the
fixed selection or weaken validation to hide the omission.
64 instruments are not activated or tested in this session; a short pilot with a native
omission is insufficient evidence for escalation. Longer observation and recovery/outage
soaks remain the next step.

## Git boundary

The actual project is `C:/Users/Pouyan/PycharmProjects/tradingview`; Git root is
`C:/Users/Pouyan`. This layout is intentional in the root README and prior publication
history. Root ignore rules allow only this project and `.gitignore`, `README.md`, and
`.github/workflows/test.yaml`; these are the only three tracked paths outside the project.
The index was empty, and this session performs no staging, commit or push. Scope any future
publication to the reviewed project paths and preserve the allowlist; do not restructure
the checkout or use blanket home-directory staging.
