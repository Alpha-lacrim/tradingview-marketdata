# Native 1-minute candle verification - 2026-10-07

Initial guest-only probe: chart resolution `"1"`, no authentication/cookies/browser automation.
Source identity (full_name, type, listed_exchange, currency) matched all 17 registry mappings.
Each series reported `streaming`, `bar_source=trade`, `bar_transform=none`, `bar_fillgaps=false`.
This is reported native chart semantics, not an independent latency audit or proof of exchange executions.

| Instrument | Native 1m OHLC available | O | H | L | C | Volume | Timestamp verified (UTC) | Notes |
|---|---|---|---|---|---|---|---|---|
| BINANCE:BTCUSDT | yes | 83840.11 | 83852.01 | 83840.1 | 83852.01 | 19.65785 (base) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| BINANCE:ETHUSDT | yes | 2606.2 | 2606.82 | 2605.99 | 2606.82 | 47.8813 (base) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| BINANCE:SOLUSDT | yes | 118.15 | 118.16 | 118.14 | 118.15 | 175.757 (base) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| BINANCE:XRPUSDT | yes | 1.4596 | 1.4599 | 1.4595 | 1.4599 | 30983.4 (base) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:EURUSD | yes | 1.11917 | 1.1192 | 1.1191 | 1.11912 | 170.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:GBPUSD | yes | 1.32326 | 1.3233 | 1.32319 | 1.32325 | 141.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:USDJPY | yes | 158.234 | 158.242 | 158.228 | 158.236 | 217.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:USDCHF | yes | 0.83334 | 0.83334 | 0.83325 | 0.83332 | 44.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:AUDUSD | yes | 0.697 | 0.69703 | 0.69698 | 0.69701 | 41.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:USDCAD | yes | 1.42257 | 1.42258 | 1.4225 | 1.42254 | 59.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:NZDUSD | yes | 0.56056 | 0.56066 | 0.56056 | 0.56064 | 27.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:EURGBP | yes | 0.84576 | 0.84581 | 0.84573 | 0.84575 | 36.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:XAUUSD | yes | 4122.335 | 4123.25 | 4122.335 | 4122.485 | 286.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OANDA:XAGUSD | yes | 60.1925 | 60.211 | 60.1925 | 60.2095 | 108.0 (tick) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| OKX:XAUTUSDT | yes | 4125.2 | 4125.7 | 4125.2 | 4125.7 | 2.325057 (base) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| TVC:USOIL | yes | 89.58 | 89.67 | 89.57 | 89.61 | NULL (upstream field unspecified) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |
| TVC:UKOIL | yes | 101.48 | 101.6 | 101.45 | 101.56 | NULL (upstream field unspecified) | 2026-10-07T09:26:00+00:00 | streaming; untransformed, no fill |

The checked-in fixture retains these real captured metadata/bar vectors with chart session IDs renamed and internal node IDs removed.
The final forming bar is retained in the fixture to test its exclusion. Prices and source timestamps were not replaced with synthetic values.

Live end-to-end comparison on 2026-10-07: all 17 symbols matched independent native requests, PostgreSQL OHLC and REST OHLC for consecutive buckets 09:39, 09:40 and 09:41 UTC; application WebSocket delivered all 17 persisted candles each minute.
There were exactly 17 stored rows per bucket from 09:33 through 09:41; raw guest ticks stayed at 26,216.
Reproduce with `uv run python scripts/smoke_tradingview_candles.py --minutes 3 --output artifacts/native-candle-smoke.json`.

Final rebuilt-collector repeat passed for 09:57 and 09:58 UTC: all 17 independent native/DB/REST OHLC matched and all 17 WebSocket candles arrived per minute. `artifacts/native-candle-final-smoke.json` records 442 candles across 26 consecutive buckets from 09:33 through 09:58, with raw ticks unchanged at 26,216 and no unresolved gaps. The verification script rechecks its wall-clock deadline to tolerate slightly early Windows timer wakeups.

Historical receipt-window measurement: 11,138 guest tick rows during [09:00,09:20) UTC = 556.9 rows/minute. Compared with measured 17 candle rows/minute, history row creation fell 96.947% (32.76x fewer rows). This compares different observed windows, is workload dependent, and is not a disk compression measurement.
17 x 60 x 24 = 24,480 rows/day only when every instrument supplies a candle every minute; quiet/closed markets can omit bars.
