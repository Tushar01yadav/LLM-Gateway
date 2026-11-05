# Phase 4 load test — BEFORE vs AFTER concurrency control

- Generated: 2026-11-05T06:38:20.580824+00:00
- Machine: COGLAP021 (Windows-10-10.0.26200-SP0), 8 CPUs, Python 3.10.11
- Redis: {'available': True, 'version': '7.4.11', 'rtt_ms_avg_10_gets': 3.24}
- Providers: mocked (alias 'mock', MockAdapter); latency: fixed 500 ms
- Load: 120 requests, concurrency 60, same seed 20261104, 3 interleaved repetitions per scenario

## Every run

| Run | Scenario | Served | Rate-limit 429 | Load-shed 429 | 5xx | Timeouts | Wall (s) | Req/s | Served / Error / Shed % | Client p50 | Client p95 | Client p99 | Server p50/p95/p99 (200s) | Peak global / mock in-flight (queue) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | BEFORE (off) | 120 | 0 | 0 | 0 | 0 | 9.969 | 12.0 | 100.0 / 0.0 / 0.0 | 3837.99 | 5738.93 | 6362.08 | 801.07/1126.24/1130.1 (n=120) | 0 / 0 (0) |
| 2 | BEFORE (off) | 120 | 0 | 0 | 0 | 0 | 10.778 | 11.1 | 100.0 / 0.0 / 0.0 | 4591.84 | 5733.98 | 6222.97 | 905.15/1203.59/1222.55 (n=120) | 0 / 0 (0) |
| 3 | BEFORE (off) | 120 | 0 | 0 | 0 | 0 | 11.113 | 10.8 | 100.0 / 0.0 / 0.0 | 4920.1 | 5946.8 | 6743.27 | 888.27/1379.76/1387.45 (n=120) | 0 / 0 (0) |
| 1 | AFTER (on) | 80 | 0 | 40 | 0 | 0 | 7.753 | 15.5 | 66.7 / 0.0 / 33.3 | 3275.08 | 4809.5 | 5578.04 | 1365.02/2376.75/2390.18 (n=80) | 50 / 10 (20) |
| 2 | AFTER (on) | 84 | 0 | 36 | 0 | 0 | 11.894 | 10.1 | 70.0 / 0.0 / 30.0 | 5293.14 | 6649.2 | 7505.03 | 1540.79/2594.36/2603.23 (n=84) | 48 / 10 (20) |
| 3 | AFTER (on) | 80 | 0 | 40 | 0 | 0 | 8.156 | 14.7 | 66.7 / 0.0 / 33.3 | 3497.2 | 4797.32 | 5019.1 | 1587.71/2536.37/2544.21 (n=80) | 48 / 10 (20) |

## Median across repetitions

| Scenario | Served | Shed 429 | Served / Error / Shed % | Wall (s) | Req/s | Client p50 ms | Client p95 ms | Client p99 ms | Served-only p50/p95/p99 ms | Server p50/p95/p99 ms | Peak global / mock in-flight (queue) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| BEFORE (off) | 120 | 0 | 100.0 / 0.0 / 0.0 | 10.78 | 11.1 | 4591.84 | 5738.93 | 6362.08 | 4591.84/5738.93/6362.08 | 888.27/1203.59/1222.55 | 0 / 0 (0) |
| AFTER (on) | 80 | 40 | 67.0 / 0.0 / 33.0 | 8.16 | 14.7 | 3497.2 | 4809.5 | 5578.04 | 3737.67/4943.32/6339.81 | 1540.79/2536.37/2544.21 | 50 / 10 (20) |

Notes:
- Host baseline (measured, not assumed): Redis GET RTT 3.24 ms; sequential `/health` RTT 7.61 ms; 60 concurrent `/health` wall 671.9 ms. This host is slow at loopback concurrency, so absolute numbers are not production capacity.
- Repetitions are interleaved (BEFORE, AFTER, BEFORE, AFTER, ...) to spread host drift across both scenarios; medians are reported.
- Client latency is observed at the load generator; server-side percentiles are scenario-attributed `request_logs` rows (status 200). The full `/api/stats` snapshot is saved in the JSON.
- Peak mock in-flight is the provider bulkhead's counter; with control off the bulkhead is bypassed, so 0 means "not measured", not "no provider concurrency".
- AFTER is expected to shed under this load: that is the control doing its job, not a regression. BEFORE overloads the provider.
- `GATEWAY_CONCURRENCY_CONTROL=off` exists only for this comparison; never use it in production.
