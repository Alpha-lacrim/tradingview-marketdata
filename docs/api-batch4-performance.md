# Batch 4 measured API performance

Generated from retained reports. Scope: [qualification](api-batch4-verification.md).

Latencies are milliseconds. Each REST row has 120 requests; normal failures are zero.

| Engine | Request | Concurrent clients | p50 | p95 | p99 | RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| pg | instruments100 | 1 | 12.593 | 17.349 | 33.957 | 73.765 |
| pg | latest100 | 1 | 21.35 | 25.109 | 26.517 | 46.865 |
| pg | history500 | 1 | 31.886 | 41.238 | 50.37 | 31.217 |
| pg | history1000 | 1 | 44.803 | 75.964 | 81.907 | 20.624 |
| pg | coverage100 | 1 | 84.083 | 99.532 | 111.443 | 11.831 |
| pg | instruments100 | 8 | 55.486 | 116.457 | 122.058 | 124.478 |
| pg | latest100 | 8 | 85.457 | 106.354 | 112.616 | 91.694 |
| pg | history500 | 8 | 141.014 | 210.894 | 230.793 | 51.974 |
| pg | history1000 | 8 | 300.576 | 359.653 | 372.153 | 26.702 |
| pg | coverage100 | 8 | 169.17 | 248.847 | 264.212 | 45.427 |
| pg | instruments100 | 24 | 132.297 | 489.361 | 544.769 | 115.614 |
| pg | latest100 | 24 | 183.674 | 351.773 | 411.954 | 101.631 |
| pg | history500 | 24 | 347.711 | 558.84 | 637.67 | 58.949 |
| pg | history1000 | 24 | 598.581 | 943.713 | 1102.723 | 35.345 |
| pg | coverage100 | 24 | 380.892 | 515.359 | 552.443 | 57.868 |
| timescale | instruments100 | 1 | 10.258 | 12.955 | 25.564 | 91.464 |
| timescale | latest100 | 1 | 14.716 | 16.719 | 18.686 | 66.386 |
| timescale | history500 | 1 | 22.268 | 26.367 | 39.897 | 43.289 |
| timescale | history1000 | 1 | 38.327 | 56.729 | 61.761 | 24.519 |
| timescale | coverage100 | 1 | 62.514 | 75.513 | 82.257 | 15.852 |
| timescale | instruments100 | 8 | 45.34 | 71.199 | 90.42 | 157.646 |
| timescale | latest100 | 8 | 58.624 | 78.964 | 82.56 | 128.93 |
| timescale | history500 | 8 | 117.297 | 152.524 | 159.375 | 64.735 |
| timescale | history1000 | 8 | 196.046 | 238.059 | 251.463 | 39.855 |
| timescale | coverage100 | 8 | 105.8 | 149.706 | 158.483 | 68.688 |
| timescale | instruments100 | 24 | 126.829 | 203.351 | 259.388 | 160.098 |
| timescale | latest100 | 24 | 117.426 | 529.941 | 595.037 | 116.468 |
| timescale | history500 | 24 | 313.034 | 435.702 | 519.294 | 70.067 |
| timescale | history1000 | 24 | 605.568 | 891.272 | 988.196 | 36.075 |
| timescale | coverage100 | 24 | 259.112 | 440.681 | 485.736 | 76.75 |

| Engine | WS clients × 100 symbols | Delivered/expected | ACK→client p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| pg | 5 | 1500/1500 | 861.022 | 870.495 | 872.361 |
| pg | 20 | 6000/6000 | 1056.417 | 1093.526 | 1095.916 |
| pg | 50 | 15000/15000 | 920.177 | 1366.275 | 1373.329 |
| timescale | 5 | 1500/1500 | 904.241 | 1017.478 | 1018.216 |
| timescale | 20 | 6000/6000 | 367.01 | 455.292 | 457.456 |
| timescale | 50 | 15000/15000 | 1432.586 | 1687.67 | 1693.621 |

| Engine | Elapsed s | CPU s / % one core | RSS MiB | Pool max | Queue max/bound |
| --- | ---: | --- | ---: | ---: | --- |
| pg | 72.092 | 44.578 / 61.84% | 119.37 | 10 | 100/256 |
| timescale | 59.744 | 36.078 / 60.39% | 116.16 | 10 | 100/256 |

| Engine | Metric | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | ---: | ---: | ---: | ---: |
| pg | event_loop_lag | 0 | 41.0 | 103.0 | 291.0 |
| pg | database_queries | 15.0 | 78.0 | 125.0 | 5016.0 |
| timescale | event_loop_lag | 0 | 25.0 | 56.0 | 212.0 |
| timescale | database_queries | 15.0 | 63.0 | 110.0 | 5031.0 |

DB query samples include timeout injection and client decoding, not only execution.
Windows asyncpg/event-loop clocks are coarse (including zero elapsed samples).

| Engine | Fault requests | 503 p50/p95/p99 ms | Recovery ms | History SQL ms |
| --- | ---: | --- | ---: | ---: |
| pg | 16 | 5009.899/5042.267/5042.267 | 9.482 | 3.204 |
| timescale | 16 | 5021.078/5051.988/5051.988 | 10.850 | 3.515 |

[Full evidence, query plans and hashes](api-batch4-evidence.json).
