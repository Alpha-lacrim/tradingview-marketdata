# HANDOFF.md

## Current state - 2026-10-12

Repository organization now places the application directly at the Git root on `main`.
Restructuring commit `533e568` was reviewed on `chore/root-layout` and merged through PR #1
as `5d5492b`; all preceding implementation commits remain ancestors. Work is in the separate clone
`C:/Users/Pouyan/PycharmProjects/tradingview-marketdata`; the original home-rooted checkout
and `PycharmProjects/tradingview` working directory, parent PyCharm project, environment,
ignored artifacts and live containers remain intact. Do not pull the relocation into the
home-rooted checkout; follow `docs/repository-layout.md` to adopt the new clone safely.
GitHub visibility is PUBLIC, verified via GitHub on 2026-10-12; older private claims were stale.
Publication is not deployment. The separate clone is synchronized to the new `main` layout.

Root README now summarizes setup, capabilities and measured guest limitations. The original
detailed platform reference is in `docs/platform-guide.md`; guest operations and evidence
remain in their existing docs. Root CI no longer sets a nested working directory/cache path;
the duplicate nested workflow and wrapper README/allowlist are removed. Packaging, Docker,
scripts, registries and migration paths retain their application-relative layout. 89 relocated
files match their original Git blobs, including all source/tests/scripts/config/migrations,
AGENTS.md, dependency lock and Docker/Compose files. No application behavior changed.
Git/Docker ignore rules also exclude plain/compressed SQL dumps, dump/backup/volume directories
and local registry/IDE/operator files from the build context.

Operator-authorized TradingView guest multi-WebSocket investigation now reaches actual
native completed 1m candles through the production CandleIngestion cycle and PostgreSQL
Store transaction. It does not demonstrate complete minute coverage or sustained capacity.
The original deployed image/registry remains 17 instruments, one socket/window two. It was
paused during experiments and restored with docker compose start app, without rebuilding.
Old five-day coverage gaps outside the 300-minute horizon remain; preserve them and the
EXPECTED / NOT_EXPECTED / UNKNOWN distinctions. Main DB guest raw ticks remain 26,216;
no benchmark mappings or production table truncation. Schema head is still 0004.

## Implementation and decisions

TradingViewCandleProvider adds explicit experimental_pool=True construction for the live
harness. Default app construction still rejects multisocket settings. Both local worker
100-subscription/job safeguards remain, as do exact native identity/status validation,
Decimal OHLCV, UTC cutoff, bounded history, unique ownership and access rejection. Experimental
PoolLimits describe local safeguards, not documented upstream guest allowances/entitlements.
No login, cookies, proxies or entitlement workaround was introduced.

The existing connection pool keeps one scheduler, independent workers, shared request
budget, deadlines, cancellation/partial-result retention and independent network backoff.
New configurable connection-start pacing defaults to zero; the experiment used two seconds.
Worker timing now separates pacing wait and handshake. HTTP status is retained, HTTP 429
reduces global concurrency and imposes at least 60s provider cooldown, honoring longer
numeric or HTTP-date Retry-After. Waiting paced workers recheck the gate before connecting.
Deadline classification now uses asyncio timeout.expired(), avoiding Windows clock-boundary
misclassification. These post-run diagnostic/backoff refinements passed deterministic tests;
no second live throttling run was induced.

scripts/benchmark_live_pool.py is opt-in and requires collector-stopped plus a dedicated
migrated TEST_DATABASE_URL ending _test. Do not use its database with pytest fixtures that
truncate tables. It discovers exact current public scanner identities or accepts saved
benchmark-only candidates, then uses the SAME ranked universe at 128/256/512. Unique test
provider namespaces isolate writes. RecordingStore remaps persistence identity and reads
back Decimal OHLCV/status after each actual transaction; every candle retains its first
commit acknowledgement/latency across replay/correction. Evidence checkpoints are atomic.
Three baseline cycles, one real client socket disconnect plus pretransaction DB failure,
and two recovery cycles run per stage. Process CPU/RSS/traced allocation/task/event-loop
measurements and native omissions are recorded. No source candles are synthesized.

scripts/summarize_live_pool.py publishes sanitized summaries, all per-candle first commits,
missing-minute identities, native samples and SHA-256 of each raw checkpoint. Docs, README,
CODEX.md and .env.example describe this workflow. The synthetic harness remains offline;
its --live error directs operators to the new actual-persistence harness.

## Live evidence

See docs/candle-live-end-to-end.md and docs/candle-live-end-to-end.json. Raw checkpoints,
catalog response/candidates and full native payloads remain under ignored artifacts/.
Eight unpaced socket starts at 10:45 UTC: two HTTP 429 handshakes, 96 successful requests,
93 actual target commits out of 128. Stopped before escalation; failure retained.

Paced run 10:48..11:05 UTC: fixed SIX sockets/window two/global twelve, 18 total cycles,
909 catalog candidates, ranked BINANCE/OKX spot-USDT prefixes only. Not mixed FX/energy or
futures capacity. All baseline requests completed. No further HTTP 429/access rejection.
Instrument count: baseline immediate (three cycles); six-cycle immediate; eventual commits:
- 128: 373/384; 602/768; 743/768 (25 unresolved).
- 256: 709/768; 1,386/1,536; 1,419/1,536 (117 unresolved).
- 512: 1,271/1,536; 2,588/3,072; 2,663/3,072 (409 unresolved).
All are LIVE INCOMPLETE. Successful responses lacking targets remain source omissions.
Actual ordinary PostgreSQL 17.7 DB marketdata_live_pool_test, head 0004, synchronous_commit,
fsync and full_page_writes on: 4,825 paced rows plus 93 failed-run rows; all native/realtime/
completed/aligned, zero duplicate logical candles. No schema migration added.

Immediate close-to-commit p95: 19.653 / 49.158 / 46.798s. Includes local acknowledgement,
not server/exchange commit timestamp. All-cycle wall max: 18.157 / 47.765 / 46.055s.
DB-write p95: 0.871 / 1.338 / 2.669s. Sampled RSS peaks: 134.648 / 186.719 / 259.520 MiB.
512 baseline max45.578s leaves only2.422s against48s budget. Full metrics are in the JSON.
Controlled failed targets recovered:19/19,33/39,75/84; remaining native gaps stay unresolved.
128 also lost one whole recovery cycle to local DNS/handshake failure, then recovered most
history. Each injected DB failure caused one retry then commit. Every cycle closed sockets
and joined receivers. PostgreSQL server outage/uncertain-commit faults were NOT tested.
Harness retains growing diagnostics/tracing overhead and shared-host validation ran during
parts of the experiment; these measurements are not a memory soak or isolated capacity claim.
Earlier synthetic matrices and maximum-history findings remain in their existing documents.

## Repository restructuring verification

Fresh `uv sync --frozen --extra databento` and Docker build passed from the new root.
Ruff lint and format checks passed (75 Python files); strict mypy passed 29 source files.
CLI help/imports and Alembic head detection passed. Both Compose configurations validated,
and actionlint passed for the single root CI workflow. GitHub's Markdown API rendered the
README; 27 local documentation links/anchors and 21 sensitive artifact ignore examples passed.
Git tracked paths contain no nested application or ignored private artifacts.

Isolated Compose project `marketdata-root-review` used ports 55432/18000 and its own volume.
Timescale tests: 281 passed (72.23s). Plain PostgreSQL mode: 278 passed, 3 Timescale-only skips
(71.48s). Both disposable databases upgraded to 0004, downgraded to base and re-upgraded to
0004. The actual mock network smoke passed: 30 instruments, healthy DB/feed, latest state,
REST tick/candle history, metrics and WebSocket observations. Existing Starlette/httpx
TestClient deprecation warning remains. No new live provider requests were made.
Gitleaks 8.30.1 found no secrets in the staged diff or full exported tracked tree (4.43 MB).
`git diff --check` passed. Review containers/network were removed with `down` after smoke/tests;
the separate review volume was retained. Original main worktree remains clean and its original
app/DB containers remain running; pre-existing guest coverage degradation was not repaired by
this organization task. GitHub Actions passed both database modes for the branch push, PR #1
and merged main (`5d5492b`, run 38244983432). Remote README HTML checks passed: one title,
five sections, two tables, five code blocks and 25 links. No browser was available for a
visual screenshot; rendering was verified through GitHub's rendered README API.

## Prior implementation verification and restoration

Final Timescale suite:281 passed (40.83s). Final ordinary PostgreSQL suite:278 passed +3
Timescale-only skips (39.74s). Targeted pool/harness tests:45 passed +2 database skips.
Ruff lint/format pass73 Python files; strict mypy
passes29 source files. Fresh experiment DB migrations applied to head0004. git diff --check
passes. Existing Starlette/httpx TestClient deprecation warning remains.

Restoration stored-only smoke initially FAILED: bucket11:06 UTC had no WebSocket targets
because the11:07 collection handshake failed;11:08 remained in backoff. Subsequent native
history supplied those minutes.11:09 and11:14 cycles committed17/17;11:12 bucket had16/17.
Preserve artifacts/live-pool-restoration-smoke.json. Warmed stored-only smoke PASSED for
11:15 and11:16 buckets:17/17 each, WebSocket = PostgreSQL = REST, raw ticks unchanged26,216.
Evidence:artifacts/live-pool-restoration-warm-smoke.json. Existing historical gaps keep
health degraded even during currently complete cycles. REST remains available.

## Next work

PR #1 is merged; adopt the separate root checkout using
`docs/repository-layout.md`. Keep operating the existing collector from its original directory
until explicitly migrating deployment/configuration; a new Compose project name creates a
different volume. Preserve `tradingview_marketdata_pg` and never use `down -v` for migration.

Independently reverify a denser fixed universe for a new quiet-host comparison; preserve
these original denominators/results. Separate source liquidity/completion from transport
failure and timing. Compare bounded worker counts/windows without increasing connections
after throttling. Run longer memory/native maximum-history persistence and real DB/network
outage soaks before rollout. Complete minute coverage at128/256/512 remains unproven.
Keep the original single application worker/17 registry. Publication does not deploy code.
Remote is https://github.com/Alpha-lacrim/tradingview-marketdata. Main now contains the root
layout and latest application implementation. Raw artifacts and local configuration remain
ignored. Use dedicated branches for new work and recheck remote main before publication so
concurrent implementation work is preserved.
