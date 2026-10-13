# Employer business universe — provisional qualification, 2026-10-13

The proposed active universe has **99 independent source entries: 50 Binance spot USDT
markets, 21 OANDA forex pairs, the same 21 pairs from FXCM, four OTC metals and three energy
references/CFDs**. Seven further metal/energy source alternatives were also tested. Both
forex feeds stay active in the proposal; a preferred UI display does not replace either
history. The employer has not approved the particular list or commodity product choices.
**The proposal is not deployed. Production remains the accuracy-selected 100 profile.**

## Evidence and reproducibility

- [Proposed business decisions and ranking](business-universe-proposal.json).
- [All 99 proposed active sources, per-source JSON](business-active-qualification.json) and
  [CSV](business-active-qualification.csv).
- [Full 106-source qualification JSON](business-source-qualification.json) and
  [CSV, also including seven untested futures alternatives](business-source-qualification.csv).
- [Live captures, timestamps and immutable raw hashes](business-live-evidence.json).
- [All 58 unresolved targets, exact UTC timestamps and classifications](business-missing-targets.csv)
  and [JSON provenance ledger](business-missing-targets.json).
- [FastAPI integration risks and next steps](fastapi-business-integration.md).
- [Existing-versus-required registry inventory](business-registry-inventory.json).
- [Earlier ordinary scaling and accuracy-selected baseline](ordinary-collector-scaling.md).

All market-directory, ranking, native candle and quote data used for this proposal came
from TradingView. Before the TradingView-only clarification, one Binance request failed
TLS and its official public market-data host returned HTTP 451. Requests stopped; no proxy,
account, geographic workaround or rejected-source data was used. That is separate network
restriction evidence, not a TradingView request failure. The Iranian deployment server was
not accessed or tested; these results are from the existing local Windows/Docker host.

Freeze the same inputs offline:

```powershell
uv run python scripts/prepare_business_universe.py
```

The retained directory/ranking snapshots and SHA-256 hashes reproduce the exact proposal.
The accuracy-selected instruments.tradingview-100.json remains unchanged at SHA-256
0ef7f3837a8f6a18f5ea570cc84ffc792e69100d07fdfb574593b639ec8b5600. Of those 100 sources,
57 occur in the proposed business universe, 43 do not, and 40 of the latter were density-selected
additions. Forty-two proposed business sources are currently absent. None of their old
identities, source mappings or history were changed during inventory.

## Binance selection

Selection timestamp: **2026-10-13 07:41:10.599241 UTC**. The TradingView crypto directory
returned 506 listed Binance spot USDT pairs. The frozen 50 are sorted descending by its
`24h_vol|5`, with exact-symbol tie breaking, excluding null/nonpositive volume and other
venues/products/quote currencies. Every selected exact BINANCE symbol subsequently passed
native spot identity, currency and source-status validation. No perpetual or OKX replacement
is present. Stablecoin pairs remain included under the current definition.

This field is TradingView's rolling **24-hour USD volume**, not raw Binance USDT quote volume.
The initial raw-USDT ranking definition therefore remains an explicitly provisional decision;
no equivalence or independent exchange trading eligibility check is claimed. Source listing,
positive reported turnover and live native retrieval establish observed TradingView activity,
not the operator's eligibility to trade on Binance. See
[TradingView's own metric definition](https://www.tradingview.com/blog/en/two-new-ways-to-screen-crypto-pairs-34549/)
and [the captured full ranking](business-ranking-snapshot.json). No candle-completeness score
entered the ranking. In particular, RLUSD remains rank 15 despite sparse native history.

## Forex identity and default display

Majors: EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD, NZDUSD.
Crosses: EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD, GBPJPY, GBPCHF, GBPAUD,
GBPCAD, GBPNZD, AUDJPY, CADJPY, CHFJPY. These are a defensible provisional shortlist,
not a claim that the employer individually approved all pairs.

TradingView's exact FXCM namespace is **FX:**. The directory and
[EURUSD symbol page](https://www.tradingview.com/symbols/EURUSD/?exchange=FX) identify
FX:EURUSD as FXCM. The native chart reports full_name FX:EURUSD and listed_exchange FX;
the quote protocol reports exchange FXCM. The old quote gate compared that broker value
with the chart namespace FX and rejected otherwise valid streaming observations. The scoped
fix normalizes only the observed FX/FXCM venue relationship inside the quote adapter, while
retaining the exact configured symbol and type, currency and delay checks. Native chart
validation, canonical IDs, persistence and access gates were not relaxed.

Regression cases reject a different source symbol, exchange, currency, product type and
delayed status. Following the fix, all 26 tested FXCM forex/metal/energy sources emitted
accepted quote observations. The original rejected captures remain separate evidence.
Canonical identities such as FX:OANDA:EURUSD and FX:FXCM:EURUSD have distinct UUIDs; their
TradingView mappings remain OANDA:EURUSD and FX:EURUSD. The domain exchange field FX follows
native listed_exchange; the broker provenance FXCM is explicit in the report and quote
metadata. A future UI must display the broker, not mistake the FX namespace for an exchange.

The provisional UI recommendations favor OANDA for 20 pairs because tied native coverage
has verified tick-volume semantics there; CADJPY favors FXCM because the OANDA window has
one missing target. Quote freshness and measured request latency are also retained for every
comparison. These sequential short captures do not establish a sustained broker winner.
Both histories remain independently available; automatic cross-broker failover/backfill,
OHLC merging and volume combination are prohibited.

## Measured business coverage

The fixed historical window is **06:49–07:49 UTC** on 2026-10-13. Two independent native
captures were performed, with real Store transactions and acquired independent PostgreSQL
readback. The primary 99-source counts below retain every EXPECTED minute. Market-closed
and UNKNOWN minutes are separately represented; both were zero in this particular open
window according to captured session metadata. No closure was inferred from an absent bar.

| Proposed category | Sources | Expected | First capture durable | Recovered on second capture | Eventual durable | Unresolved | Eventual completeness |
|---|---:|---:|---:|---:|---:|---:|---:|
| Binance spot USDT | 50 | 3,000 | 2,963 | 0 | 2,963 | 37 | 98.7667% |
| OANDA forex | 21 | 1,260 | 1,259 | 0 | 1,259 | 1 | 99.9206% |
| FXCM forex | 21 | 1,260 | 1,260 | 0 | 1,260 | 0 | 100% |
| Primary metals, OANDA | 4 | 240 | 237 | 0 | 237 | 3 | 98.75% |
| Primary energy, TVC/OANDA | 3 | 180 | 163 | 0 | 163 | 17 | 90.5556% |
| **Total provisional active** | **99** | **5,940** | **5,882** | **0** | **5,882** | **58** | **99.0236%** |

The broader 106 sources supplied 6,302/6,360 targets per capture. All **212 requests**
succeeded; 12,604 native candle occurrences across both captures exactly matched durable
readback. Native transport/HTTP/DNS/handshake/DB errors and retries were zero in these
captures, and every worker/session/pending request cleaned up. The initial quote gate's
26 identity rejections were a separate application validation defect, preserved and corrected.
They were neither native source omissions nor native network failures.

The conservative source-snapshot health count is 91/99 proposed active (98/106 including
alternatives). This requires a source-timed fresh last price as well as native completeness,
identity and persistence, and is not an overall production-readiness count. FDUSD's captured
last trade was 68.664s old, while updated bid/ask observations were delivered. RLUSD's last
trade was 232.656s old, also with bid/ask deliveries. Those bid/ask deltas lack source time;
their receipt freshness is recorded separately. A stale last trade is not rewritten as a
fresh last, and a bid/ask-only delta does not borrow its timestamp or price.

No candidate is marked VERIFIED for mere resolution. Good captures are PARTIALLY VERIFIED;
required sparse sources are SOURCE_LIMITED. Ordinary simultaneous business-universe
scheduled collection, weekend/break/holiday session behavior and live UI streaming remain
NOT TESTED. One historical open-hour result cannot establish sustained 99.9% reliability.

## Gaps and recovery investigation

All retained native windows are EXPECTED under the captured session metadata. No request
or database failure explains these specific absences:

| Source | Native / expected | Unresolved | Further finding |
|---|---:|---:|---|
| BINANCE:RLUSDUSDT | 26 / 60 | 34 | Same gaps in both captures and later 104-bar retrieval |
| OANDA:XPDUSD (palladium OTC quote) | 57 / 60 | 3 | Same gaps in later 104-bar retrieval |
| OANDA:NATGASUSD (broker CFD) | 43 / 60 | 17 | Same gaps in later 104-bar retrieval |
| BINANCE:CTKUSDT | 59 / 60 | 1 | Unchanged on the second capture |
| BINANCE:UUSDT | 59 / 60 | 1 | Unchanged on the second capture |
| BINANCE:DOTUSDT | 59 / 60 | 1 | Includes the accuracy-run 07:15 bucket absence |
| OANDA:CADJPY | 59 / 60 | 1 | FXCM separately supplied its own 60/60, never used to fill OANDA |

The later targeted recheck completed **2026-10-13 08:31:43.372774 UTC**.
The four additional absent buckets start at **CTK 07:31, U 07:33, DOT 07:15 and OANDA
CADJPY 07:26 UTC**, all on 2026-10-13. The full 58-row CSV/JSON gap ledger records minute
starts/ends, successful capture counts, session classification and unknown underlying cause.
Each received all 104 requested native bars and a response spanning both sides of the fixed
window: RLUSD 03:38–08:30 UTC, palladium 06:44–08:31, gas 06:10–08:30. Thus insufficient
requested historical depth and the current forming bar do not explain these omissions.
**Zero of the 54 RLUSD/palladium/gas gaps recovered in that later recheck.** Missing bars
remain visible and source-absent observations are repeat observations, not additional unique
lost targets. The deeper cause—no source trades versus upstream bar publication behavior—is
UNKNOWN. There is no evidence proving permanent data loss. No OHLC was fabricated, filled,
interpolated, substituted or removed from the denominator.

Deferred reliability work, after FastAPI integration, may use longer retained raw/native-window
captures for these exact sources,
publication-lag measurement across new closes, and session-boundary checks. Same-underlying
alternatives can inform an employer decision, never silently repair another source's history.
FXCM NGAS had its own 60/60 sample; it is a different broker CFD and has unclassified volume.
It does not erase OANDA's gas gaps or establish identical prices/products.

## Volume and commodity product semantics

- Binance: validated native volume_type base; source-reported traded base-asset units per
  minute, not USDT quote turnover and not a guaranteed complete exchange trade tape.
- OANDA forex, four metals and three tested energy CFDs: source-reported tick volume.
  It measures source activity, not traded currency notional, metal ounces or oil contracts.
- FXCM and TVC: normalized volume is NULL because a validated volume type/unit was absent.
  Representative FX:EURUSD, FX:XAUUSD, FX:NGAS, TVC:USOIL and TVC:UKOIL nevertheless returned
  numeric raw volume vectors. **Do not describe them as simply having no source volume.**
  Their economic meaning/units remain UNKNOWN. The parser now preserves finite raw numbers
  as decimal strings in source_volume_raw, with unknown classification and non-additive
  semantics; NULL stays in the typed volume column. Fifty real native candles were persisted
  and independently read back with exact NULL/raw-metadata agreement in the isolated database.
- Quote volume has an unverified measurement window and is not combined with candle volume.

Primary metal proposals are OANDA XAUUSD, XAGUSD, XPTUSD and XPDUSD broker OTC price quotes.
FXCM XAUUSD/XAGUSD are separately labelled broker OTC alternatives. Chart spot labels do
not establish executable broker CFD terms. No quoted OTC instrument is reclassified as an
exchange futures contract. The inactive OKX XAUT token remains a separate retained crypto
identity/history; it is not substituted for physical/OTC gold coverage. Any tokenized-gold
market in the Binance top 50 likewise remains crypto rather than satisfying OTC metal coverage.

Primary energy proposals retain TVC USOIL/UKOIL reference construction limitations plus
OANDA NATGASUSD as a broker CFD. OANDA WTICOUSD/BCOUSD and FXCM USOIL/UKOIL/NGAS are distinct
broker alternatives. Names containing “Future” in a chart description do not establish a
concrete NYMEX contract or its expiry. Employer approval of reference-versus-CFD price
requirements is necessary before choosing a final primary source.

Seven verified-directory futures aliases (GC1!, SI1!, PL1!, PA1!, CL1!, BZ1!, NG1!) are
comparison candidates only. They were not requested through the unsupported native adapter,
not persisted as instruments, and not counted as tested capacity. Contract-volume availability,
real-time entitlement, contract unit/size, authoritative expiry and continuous-series rollover
construction remain unverified for this task. BZ is specifically a Brent financial futures
series, not a claim to be physical spot Brent or the ICE Brent contract. Concrete contracts
must receive independent identities only after authoritative metadata and authorized access
are established. No exchange real-time entitlement or usable exchange volume is claimed here.

## Duration, latency and simultaneous capacity

The full two-capture 106-source phase ran **07:52:03.100–07:58:43.395 UTC: 400.295s**. It
used confirmed idle phases of the running collector, two paced sockets, local windows two,
global concurrency four, 12s retrieval deadline and 3s cleanup. At most 16 candidates were
assigned per batch, not all 99 simultaneously. Native per-request maximum was 1.757s,
whole batch maximum 7.466s and DB acknowledgement maximum 0.153s. Per-source distributions
and first transaction acknowledgement ages are retained in JSON/CSV. Historical candle
close-to-ack ages include the intentional lookback; they are not normal current-minute latency.

A separate real two-broker ordinary CandleIngestion.cycle followed. It ran within the
08:28:05–08:30:38 verification phase, deliberately waiting for idle source windows. For
its target ending 08:29 UTC, both current bars committed: 72 received candles, 62 newly
changed/backfilled, fetch 11.827s, transaction acknowledgement 0.0746s, whole cycle 11.911s,
close-to-commit 42.620s. The 25.717s scheduler delay was deliberate idle-window deferral;
it is not a normal scheduled grace-five performance result. Both coverage rows independently
retain activation 07:53 and advance to 08:29, with 36 expected native minutes each, zero
remaining expected/unknown gaps. Independent native API queries returned all 36 per broker.
A deterministic DB regression separately makes different gaps in the two brokers and proves
that recovering OANDA changes only OANDA's gap count and preserves both activation starts.

The accuracy-selected ordinary 100 run, completed before this steer, achieved exactly
2,997/3,000 eventual targets after 30 scheduled cycles plus three settling cycles, including
25 recovered handshake losses. Current close-to-commit p50/p95/max was 22.984/33.052/39.161s;
maximum eventual acknowledgement 88.290s. It is the largest newly measured near-lossless
ordinary configuration in that short window, not sustained business-universe capacity proof.
Older 128/256 ordinary and prior isolated benchmark counts remain separate. No extra socket
count was used to evade a restriction; production image/configuration/container stayed unchanged.

## Quote persistence and API scope

After the FXCM namespace fix, accepted quotes for all 26 FXCM candidates were first
validated in isolation. Later, **14 actual normalized OANDA/FXCM EURUSD quote deltas** were
persisted, independently read back with exact payload identity, and retrieved through the
existing API in an in-process stored-only TestClient: eight OANDA and six FXCM ticks.
Native candles, quotes, source timestamps (including NULL bid/ask time) and broker identities
remain separate. The API client had zero providers and no public port, created no second
collector, and correctly marked latest observations stale without an active source.

This is real isolated persistence and historical API access; **continuous realtime UI
streaming is NOT implemented/qualified for the business universe**. The deployed candle-mode
application's latest candle close remains labelled candle_close. Native closes cannot be
presented as current live prices. No FastAPI source, process-separation work or migration
from the separate FastAPI worktree was merged to perform this readback.

## Approval decisions and safe activation plan

Employer decisions still required: accept TradingView USD-volume ranking instead of raw
USDT quote turnover; keep/exclude stablecoin markets explicitly; approve the 21 pair list
with both broker histories; decide OTC quotes versus executable CFDs versus concrete futures
for each metal/energy category; decide meaningful volume requirements where only ticks or
unclassified vectors are observed. Preferred UI broker choices are provisional. Commercial
source permissions and deployment-host connectivity/latency also require their own evidence.

The employer accepts the unchanged **5,882/5,940 = 99.0236%** as a provisional development
milestone. This permits moving to FastAPI integration; it does not qualify production or sustained
reliability. Keep 99.9% as a longer-term goal, preserve required sparse sources and stop open-ended
tuning. All 5,940 minutes are session-classified EXPECTED; NOT_EXPECTED=0 and UNKNOWN=0, so no
adjusted denominator is introduced. Source-absence causes remain UNKNOWN. Do not promote now. After
approval and a successful simultaneous ordinary qualification, use one collector on the same
project/DB/volume, initially the measured four-paced-socket/global-eight architecture and
five-second grace. Build a distinct reviewed business image containing the exact approved
registry, the FXCM quote identity fix and raw-volume provenance fix; keep ordinary-100 and
rollback-34-20261012 tags unchanged. Validate registry capacity and Compose configuration
before an idle-phase switch. Guard the sole app, database system identifier and unchanged
persistent volume, bound readiness and explicitly restore the exact previous 100 image on
failure. Do not use a Count=99 switch against the current helper: that preset is not implemented.
A reviewed business override/guarded registry-path switch is required after approval.

No database migration is needed by this work. Never drop/reset/recreate production or its
volume, clear old gaps, copy test data into production, or start a worktree Compose project.
New production sources receive their next-full-minute activation atomically; old starts remain
immutable. All inactive expanded/XAUT data remain queryable. Exact old-image rollback from
this worktree remains:

```powershell
./scripts/Switch-OrdinaryCollector.ps1 -Count 34
./scripts/Switch-OrdinaryCollector.ps1 -Count 17
```

Those scripts preserve expanded history and never touch DB lifecycle. They are available
procedures, not actions performed during business qualification. The separately prepared
70s interruption helper was not run after the business steer.

## Repository and FastAPI handoff

[Exact changed-file manifest](business-change-manifest.json) records paths and content hashes.
It includes earlier scaling work kept in this session as well as the business additions.

Worktree artifacts/ordinary-scale, branch ordinary-scale-256, based on
2c5d0e6e51f51d0ae7298501df79e38859f3a464. The finalized collector/business changes are
committed locally; use `git log -1` for the resulting commit identity. No merge, push or deployment.
The original deployed checkout and paused batch3-database-efficiency worktree are preserved.
This branch adds no migration; production and its isolated qualification schema remain 0004.

FastAPI Batch 1 is clean and locally committed on fastapi-batch1-isolation at
**6901bfadd44cb83ccdca49aac559e6f97a93a91d**. Its worktree was inspected read-only and left
untouched. Migration 0005, read-only API, advisory-lock collector ownership, status TTL and
network-observable WebSocket close 1013 belong to that branch and are not included/deployed here.
[The integration handoff](fastapi-business-integration.md) details file overlaps, schema/rollback
compatibility, provider fixes, recovery/fan-out, metrics, APIs and required third-worktree checks.
**The legacy production collector does not acquire the new lock: stop it before starting a
lock-aware collector.** Neither branch is merged by this task. Integrate the two local commits
in a separate third worktree, preserving both originals and paused Batch 3.

Final checks: 316 TimescaleDB tests passed (46.88s); PostgreSQL mode 313 passed (45.56s) plus three
expected aggregate skips. Both isolated schemas upgraded to 0004. PostgreSQL mode used the same Timescale host
with no continuous aggregates, not a separately qualified plain-PostgreSQL server. Ruff lint/format and strict mypy
passed; existing Starlette/httpx deprecation warning remains. HANDOFF.md records exact current
state, file groups, verified evidence and outstanding decisions. No production schema change.

Finalization performed no further live source requests, collector restart or production writes.
The new timestamp-ledger publisher runs entirely offline. Final Ruff/format checks cover 100
Python files; strict mypy covers 29 source files. PowerShell syntax passed without switching.
No existing tests were removed or weakened. Private exports, credentials, local environments,
raw artifact logs and generated databases are excluded from the local commit. Published JSON/CSV
contains source qualification evidence, not account credentials or employer-private exports.
