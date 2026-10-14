# Distribution verification

This distribution reuses the verified API/schema source snapshot and transport-free
regression fixtures. Parent evidence (prior task): full disposable PostgreSQL suite
444 passed/4 skipped; TimescaleDB 447 passed/1 skipped. These counts cover the original
repository and are not the distribution's standalone test results.

## Current ChartData and PostgreSQL 16.1 qualification — 2026-10-14

The Go contract is implemented by a separate /api/v1/dashboard/candles route. It reuses
the exact historical query and cursor logic; all existing REST/WS payloads are retained.
Count/Value have no schema mapping; approved zeros are placeholders. Stored volume is used
with its existing availability/rollup semantics. Float64 conversion occurs only at the new
response boundary. Persistence, schema 0006, collector and event journal were not changed.

| Engine actually queried | Independent copied project |
|---|---|
| Plain PostgreSQL 16.1 (Debian 16.1-1.pgdg120+1), no Timescale extension | 143 passed, 28.10s |
| Plain PostgreSQL 17.11 | 143 passed, 29.63s |
| TimescaleDB 2.25.1 / PostgreSQL 17.7 | 143 passed, 35.63s |

Commands ran in a new Windows temporary directory outside the repository, using only
this project’s uv.lock, uv sync --frozen, local fixtures and new virtual environment.
No test was skipped in those full suites. One initial PostgreSQL 17 fixture connection
timed out (132 passed, one setup error); the full rerun passed without changing connection
timeouts or weakening tests. Twenty existing dependency deprecation warnings remain.

New Docker tmpfs instances: marketdata-chart-pg161-check (55927),
marketdata-chart-pg-check (55925), marketdata-chart-ts-check (55926). All use
marketdata_chart_test; no deployed database, service, schema or existing volume was altered.
SHOW server_version and pg_extension queries verified the versions above; plain 16.1 had
zero Timescale extensions. Fresh migrations reached 0006 without a Timescale flag on plain
PostgreSQL. The full suites include 0006 downgrade/re-upgrade and preservation, production
SELECT-only role enforcement, read-only pooling, all candle intervals, persisted-event
WebSocket delivery/reconnect/replay, browser and provisional BFF regressions.

Ruff lint/format passed (49 Python files); mypy passed (24 source files / 25 including BFF).
Wheel/sdist build and a fresh minimal Docker runtime build passed from the isolated copy.
The 16 pre-existing OpenAPI paths and every pre-existing component schema were compared
against the earlier snapshot: no changes. Only ChartData and its new route were added.
The parent exporter --check also verified all 53 managed snapshot files.

Actual two-worker container smoke on PostgreSQL 16.1 used the SELECT-only sample_api role:
both simulated instruments, all five intervals, a–h mapping against original decimal REST,
row alignment, pagination, auth, latest/coverage, health/metrics/docs, committed quote WS
delivery, reconnect snapshot, REST replay and legacy /ws/prices close 1013 passed.
The test harness initially had Windows Origin JSON quoting and replay/snapshot assertion
errors; these were corrected in the harness without modifying API contracts or code.

The clean ZIP is built by scripts/package_distribution.py with deterministic metadata,
an explicit file/type allowlist and exclusions for environments, caches, credentials,
dumps, collected data and concrete provider transports. Public disposable demo credentials
and .env.example placeholders are intentional; no actual secrets or .env file are shipped.
Extraction verifies CRC, entry names and byte equality for all 66 delivery files.

ZIP-extracted candidate verification also passed with a fresh environment: PostgreSQL 16.1
**143 passed, 31.83s**; TimescaleDB **143 passed, 35.69s**; Ruff/format (49 files) and mypy
(25 including BFF) passed. All executable/config/test files are identical in the final ZIP;
documentation was finalized after those runs. The API restarted healthy and
served ChartData after the sample writer exited. Final extraction checks reverify every byte
and run the DB-independent suite from the extracted project.

Compatibility limits: these are actual results for the exact engine versions above, not
a target-host deployment claim. Other exact versions, company TLS/network/session setup
and sustained load are not newly qualified. TimescaleDB remains optional; do not run its
opt-in migration on plain PostgreSQL. Zero metrics are ambiguous by the approved contract;
float64 loses decimal precision. Future Count/Value require source/business definitions
and typed persistence/query mappings. Existing raw metadata is not promoted to metrics.

The earlier extraction’s 116-test-per-engine evidence is retained below as prior evidence;
the 143-test results above supersede it for this compatibility release.

## Prior extraction results — 2026-10-14

The entire directory was copied to a fresh directory under the Windows temporary folder,
outside the original repository, excluding generated environments/caches. All commands below
ran from that copy with its own lockfile and virtual environment. No parent package, source
path, registry dependency, symlink or Git submodule was used. Tests assert the package is
local and collector/concrete provider modules are absent. Parent API/schema/migration code
is unchanged (LF-normalized snapshots); fixture imports are explicitly local.

| Check | Result |
|---|---|
| `uv sync --frozen` from isolated copy | Passed |
| DB-independent suite | 64 passed; 52 integration cases deselected |
| Full PostgreSQL distribution suite | 116 passed, 27.22s |
| Full TimescaleDB distribution suite | 116 passed, 29.98s |
| Ruff lint and format | Passed, 48 Python files |
| Mypy source / source plus BFF | Passed, 24 / 25 source files |
| Fresh schema migration / 0006 downgrade and re-upgrade | Passed on both engines |
| OpenAPI / manifest parity | Passed against the verified source snapshot |
| Wheel and sdist build | Passed from isolated copy |
| Clean wheel runtime install | Passed with only locked API dependencies |
| Wheel/image lifespan without collector or optional tooling | Passed, including production SELECT-only checks |
| Fresh Docker runtime/tools builds from copied context | Passed |
| Two-worker Docker REST/WS and independent API restart | Passed |

Disposable projects were `marketdata-dist-pg-check` and `marketdata-dist-ts-check`, with
loopback DB ports 55915/55916 and API 8015. Both databases used tmpfs, without existing
volume mounts. One initial PostgreSQL concurrent run had a fixture connection setup error;
the full isolated rerun passed without changes or weakened checks. Twenty existing FastAPI/
Starlette/Uvicorn/websockets deprecation warnings occurred in each full successful suite.

The supplied REST/WS example served native 1m history and received a committed price snapshot;
its live mode also exited successfully after the requested two committed quote updates from
the bounded external sample writer.
Additional real-container smoke independently wrote 1,440 completed sample minutes per source,
served all five intervals, latest/coverage, docs/OpenAPI, health/metrics and authenticated
requests, then observed committed quote delivery, reconnect snapshots and REST replay.
An import blocker proved full DB lifespan without ingestion/concrete transport imports;
httpx, Databento, Alembic, SQLAlchemy and psycopg were absent in the runtime image/wheel.
The API restarted healthy after the sample writer exited, with no migration at startup.
Only task-owned disposable Compose services were removed after validation, without volume
operations. Generated environments/caches were removed from the handover directory.

Reproduce using [README](../README.md). The full suites include candle corrections, missing
minutes, pagination/error handling, freshness, authentication/CORS/WS Origin, slow consumers,
replay/commit ordering, SELECT-only permission denial, migration preservation, BFF session
rejection and Node browser recovery. Synthetic fixture success is not live-provider evidence.

Known limits: no production deployment, live upstream test, new load benchmark or company
HTTPS/session integration is claimed. Browser authorization remains a company responsibility;
the included BFF is provisional. All sample observations are simulated. Dormant combined-mode
source branches are not supported distribution modes; the collector/transports are absent.
