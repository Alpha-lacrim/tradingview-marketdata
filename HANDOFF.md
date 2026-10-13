# HANDOFF.md

## Current state — 2026-10-13

The current priority is the employer's business universe, not maximizing candle availability
by selecting unrelated sources. TradingView is the only market-data/directory/ranking source.
User explicitly requires BOTH broker feeds for all 21 proposed forex pairs. The proposal is
99 source entries: 50 Binance spot USDT, 21 OANDA, 21 FXCM, four OANDA OTC metals, TVC USOIL/
UKOIL references and OANDA NATGASUSD CFD. Seven further same-underlying metal/energy source
alternatives were tested. Seven futures aliases remain comparison-only/untested; no alias
is persisted as a contract. All specific market/product/UI choices remain provisional.

**Production is unchanged during business qualification:** one accuracy-selected 100-source
collector, four paced sockets, worker windows two, global eight, grace five, image
tradingview-app:ordinary-100 (manifest ca498afb02f6232440e3386936a4097e865cf9fefdbd0481adc0cc7ce623023f).
App container 5d580ce8ea9275f8683cbc8ec04e8dd9ffabfd7577358e7be88d9ff7d4e38433.
No business promotion, no second ordinary scheduler/collector, no production test data.
The two new business provider fixes below are host-tested and NOT in that deployed image.

Read docs/business-universe-qualification.md for complete results, root cause, decisions,
network restrictions, source/product semantics and conditional activation plan.
Machine-readable primary 99 ledger: docs/business-active-qualification.json/.csv.
Full 106-source + futures comparison: docs/business-source-qualification.json/.csv.
Inputs: docs/business-ranking-snapshot.json, business-directory-snapshot.json.
Proposal: docs/business-universe-proposal.json; inventory: business-registry-inventory.json.
Timestamp-level unresolved ledger: docs/business-missing-targets.json/.csv.
FastAPI integration risks: docs/fastapi-business-integration.md.
Frozen registry/evidence paths have scoped -text attributes to preserve recorded byte hashes
across Windows/Linux checkouts. Manifest hashes describe staged Git blobs, excluding its self-hash.
Exact file groups/hashes: docs/business-change-manifest.json.
Frozen live evidence/hashes: docs/business-live-evidence.json. Raw originals are ignored
artifacts/business-*.json. Do not overwrite the previous accuracy-selected registry/results.

## Git and deployment safety

Clean main-based worktree: artifacts/ordinary-scale, branch ordinary-scale-256,
Base 2c5d0e6e51f51d0ae7298501df79e38859f3a464. Its name predates both priority changes.
The finalized work is committed locally on this branch; `git log -1` gives its commit.
No merge, push, deployment or publication. The old home-rooted
checkout remains dirty at 324b1d83; do not pull root relocation into it. Its unrelated changes
are preserved. Scaling changed only its DB restart-policy line and current-state handoff.
Paused artifacts/batch3-database / batch3-database-efficiency remains untouched.
No paused Batch 3 Store/configuration/retry rewrite was incorporated.

Project tradingview, deployed directory C:/Users/Pouyan/PycharmProjects/tradingview.
DB container 62caad90d2e9367a5d823cc3775102a00a0e07a79046b1aba3cd30074c4b1238.
Database marketdata; system identifier 7693817192740921379; schema 0004.
Volume tradingview_marketdata_pg -> /var/lib/postgresql/data. Reverify before any action.
DB restart policy unless-stopped was corrected in place after the earlier real outage,
without restart/recreation; both deployment and clean compose.yaml retain that narrow fix.
Business test resources use marketdata_business_qualification_test on the existing server;
pytest uses separate marketdata_test / marketdata_plain_test. No fixture truncates production
or the qualification database. No database was dropped/reset; no production migration ran.

## Actual business evidence

Binance list frozen 2026-10-13 07:41:10.599241 UTC from 506 TradingView-listed spot USDT
pairs, ordered by 24h_vol|5 USD turnover, exact-symbol tie break, positive/non-null volume.
This is explicitly NOT raw USDT quote volume; definition/eligibility still provisional.
All 50 exact symbols passed live native spot/quote-currency/source validation. No OKX/perp
substitution. Before the TradingView-only steer, Binance public directory transport failed
and its market-data host returned 451; requests stopped without proxy/geographic workaround.
No Binance endpoint data informed the proposal. The Iranian server has not been qualified.

106-source historical qualification ran 07:52:03.100–07:58:43.395 UTC (400.295 seconds),
two captures of fixed 06:49–07:49 UTC history. Confirmed production idle windows; two paced
sockets/global four/local two; batches <=16; retrieval deadline12/cleanup3/request timeout8.
212/212 successful native requests, 6,302 native/durable targets per capture, 212 exact
independent DB readbacks. Zero native transport/HTTP/DNS/handshake/DB errors/retries; cleanup
passed. Maximum per-source request1.757s, batch7.466s, DB acknowledgement0.153s.
This is historical source compatibility, NOT simultaneous 99-source ordinary capacity.
Historical acknowledgement ages include the lookback; never call them current-minute latency.

The employer accepts 99.0236% as a provisional development milestone; no further open-ended
tuning or extended live test is required before FastAPI development. 99.9% remains a longer-term
goal. All 5,940 test targets are EXPECTED; NOT_EXPECTED=0, UNKNOWN=0. Do not reinterpret unknown
source-absence causes as UNKNOWN session minutes.

Primary 99 expected5,940, first/eventual durable5,882 (99.023569%), recovered 0, unresolved58.
Binance 2,963/3,000; OANDAforex 1,259/1,260; FXCMforex 1,260/1,260;
primary metals 237/240; primary energy 163/180. All source ledgers remain visible.
Conservative healthy snapshots 91/99 (98/106 including alternatives); no source is fully
VERIFIED as production-ready. FDUSD had a stale source-timed last but fresh delivered
bid/ask deltas; bid/ask source time is unavailable and not borrowed from last.

RLUSD 34, palladium OANDA:XPDUSD 3 and OANDA:NATGASUSD 17 gaps remained on a later 104-bar
native recheck; responses spanned both sides of the fixed window and recovered 0. Other
required gaps: CTK 1, U 1, DOT 1 and OANDA CADJPY 1. All are source-absent in successful native
captures, not transport/DB loss; underlying no-trade versus publication cause remains UNKNOWN.
Do not claim permanent loss, infer closure, erase counters, replace markets or synthesize bars.

## FXCM identity, volume, storage and API proof

TradingView FXCM source symbols use FX:; native full_name/ listed_exchange are FX:EURUSD / FX.
Quote metadata reports exchange FXCM. The old quote gate compared FXCM with FX and rejected
valid streaming quotes. The scoped quote adapter now normalizes only that captured namespace/
venue relationship, retaining exact symbol, type, currency and delay validation. Initial
26 rejected quote captures are preserved separately; post-fix all 26 FXCM sources emitted
accepted quotes. Regression cases reject wrong source, exchange, currency, type and delay.

OANDA has validated tick volume; Binance has validated source-reported base traded volume.
FXCM/TVC native samples have numeric raw vectors but no validated type/units. Typed volume
stays NULL; new parser retains finite raw decimal strings as source_volume_raw metadata,
unknown and non-additive. Do not call them simply absent, traded contracts or physical units.
50 live native candles independently matched NULL volume plus raw metadata in the test DB.
Quote volume's measurement window remains unverified; never combine broker volumes.

A corrected isolated two-broker CandleIngestion.cycle covered target 08:29 UTC: requested 2,
current native 2, received 72, changed/backfilled 62, fetch 11.827s, DB 0.0746s, whole 11.911s,
close-to-commit 42.620s after deliberate 25.717s idle-phase scheduler deferral. Both independent
coverage starts 07:53 remained intact, ends advanced08:29, 36 minutes per broker, zero missing/
unknown; native API returned 36 each. DB regression makes disjoint broker gaps and proves
OANDA-only recovery leaves FXCM missing and both starts unchanged. Harness preflight/ownership
errors were fixed and retained separately, not counted as upstream failures.

14 actual OANDA/FXCM EURUSD quote deltas were persisted with exact payload readback and
queried via an existing in-process stored-only API client: OANDA 8, FXCM 6. Zero providers/
public port; latest correctly stale without an active source. This is isolated persistence
and historical API access, NOT continuous realtime UI streaming. Production candle-mode
latest close remains candle_close. Real simultaneous quote/candle UI behavior is future work.
Provisional default UI: OANDA 20 pairs for explicit tick-volume semantics on tied coverage,
FXCM CADJPY for better observed native coverage. Retain BOTH histories; no automatic failover.

## Preserved earlier ordinary evidence

Accuracy-selected 100 completed 30 scheduled cycles plus 3 settling, targets ending06:56–07:25
UTC, finished07:28:24. Expected3,000; requests 2,975; native/immediate 2,972; recovered 25;
eventual2,997 (exactly99.9%). Unresolved EIGEN end06:58, OKX PUMP07:14, Binance DOT07:16.
All 2,997 DB/REST/stored-WebSocket matches; worker cleanup passed, DB errors/retries 0.
Current close-to-commit p50/p95/max 22.984/33.052/39.161s; eventual maximum 88.290s.
Slowest cycle 34.253; warning budget45, remaining 10.747; next-deadline margin 25.747.
CPU 116.25s; peak RSS 250.1MiB; loop lag max 0.363s/p95bucket<=5ms. Old history hash/activation
starts, DBidentity/schema and 26,216 rawticks were preserved. Its numerical gate passed;
older zero-request-error gate failed. This short accuracy-selected result does not qualify
business coverage or sustained production reliability.

64 ten cycles 639/640 native/durable;128 four/global8 eventual1,280/1,280 after32 handshake
recoveries;128 six/global8 ten cycles1,270/1,280;256 four/global8 ten cycles2,394/2,560.
An earlier128 six/global8 checkpoint has only two cycles before host/Docker interruption.
Further256 tuning was canceled; no new512 activation. Keep failed/partial denominators.
Earlier real startup recovered 38,084 native candles; post-commit list membership blocked
loop about 53s. Set membership fixes that narrow defect; DB schema/Store unchanged. Older
gaps beyond300-minute recovery horizon remain unresolved. Prepared 70s interruption test
was NOT RUN after the business steer. Ordinary evidence is in ordinary-collector-scaling.*.

## Verification and next branch

Final suites: 316 TimescaleDB passed (46.88s); PostgreSQL mode 313 passed +3 expected aggregate
skips (45.56s). Both isolated DBs ran upgrade head (0004). PostgreSQL mode used a separate DB on the
Timescale host with no continuous aggregates; a standalone plain-PostgreSQL server was not
requalified during finalization. The initial stricter extension preflight stopped before tests;
a verified no-aggregate database then passed the full compatible suite. Ruff lint/format and strict mypy passed.
Existing Starlette/httpx deprecation warning remains. Native source, quote persistence/API,
namespace normalization, broker separation/coverage and unknown-volume proofs are recorded.

The separate FastAPI worktree is clean on fastapi-batch1-isolation at local commit
6901bfadd44cb83ccdca49aac559e6f97a93a91d. It was inspected read-only and left untouched.
It contains migration 0005, API-only/collector-only startup, advisory-lock ownership, read-only
API Store, collector status and the network-observable 1013 fix. None was merged or deployed.

Next: integrate both local commits in a third worktree; read docs/fastapi-business-integration.md.
Resolve Compose/startup/metrics/docs overlaps, preserve scoped provider fixes and the recovery
fan-out set fix, run both branch suites and isolated 0004-to-0005 migration/rollback checks.
The legacy deployed collector has no new advisory lock and MUST be stopped before starting the
lock-aware collector. Its Store requires exactly 0004, so a post-0005 rollback needs a tested
compatible old-registry image or reviewed status-only downgrade; no blind legacy restart.
Do not merge or modify either completed worktree now. Batch 3 stays paused.

No further automatic deployment. Obtain employer decisions on ranking/stablecoin markets,
pair list, commodity price definitions/volume and UI defaults; retain required sparse
sources and their documented limitations; qualify simultaneous ordinary business collection and deployment-host networking
before promotion. Current Count=99 business switch is NOT implemented: current helper accepts
17/34/64/100/128/256 presets. Prepare a reviewed business override/guarded registry-path switch
only after approval; do not accidentally target a missing instruments.tradingview-99.json.

Preserved exact original-image rollbacks, from this worktree, without deleting expanded data:
./scripts/Switch-OrdinaryCollector.ps1 -Count 34
./scripts/Switch-OrdinaryCollector.ps1 -Count 17
Tag tradingview-app:rollback-34-20261012; config SHA
7936134ccf5aea4f4f8ca6e5641b4ea773cf7ddf56f36d7eeae3f748af22bc42.
The old image retains the long-recovery fan-out defect; do not assume outage-free rollback.
Retain ordinary-100 exact image/config/volume as the current fallback. XAUT activation/history,
all expanded history and unresolved gaps remain preserved. Globalhealth remains degraded for
old coverage gaps, which is not a current PostgreSQL outage.
