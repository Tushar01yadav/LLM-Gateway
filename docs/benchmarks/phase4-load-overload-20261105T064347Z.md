# Phase 4 load test — BEFORE vs AFTER concurrency control (overload profile)

- Generated: 2026-11-05T06:43:47.469879+00:00
- Machine: COGLAP021 (Windows-10-10.0.26200-SP0), 8 CPUs, Python 3.10.11
- Redis: {'available': True, 'version': '7.4.11', 'rtt_ms_avg_10_gets': 1.29}
- Providers: mocked (alias 'mock', MockAdapter); latency: grows 500 ms + 150 ms per in-flight request; 503 above 12 in-flight
- Load: 120 requests, concurrency 60, same seed 20261104, 3 interleaved repetitions per scenario

## Every run

| Run | Scenario | Served | Rate-limit 429 | Load-shed 429 | 5xx | Timeouts | Wall (s) | Req/s | Served / Error / Shed % | Client p50 | Client p95 | Client p99 | Server p50/p95/p99 (200s) | Peak global / mock in-flight (queue) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | BEFORE (off) | 50 | 0 | 0 | 70 | 0 | 12.186 | 9.8 | 41.7 / 58.3 / 0.0 | 5052.98 | 6097.52 | 7545.37 | 3337.29/6199.34/6374.26 (n=50) | 0 / 0 (0) |
| 2 | BEFORE (off) | 37 | 0 | 0 | 83 | 0 | 9.377 | 12.8 | 30.8 / 69.2 / 0.0 | 1625.98 | 6008.41 | 6861.76 | 2326.61/6013.3/6228.29 (n=37) | 0 / 0 (0) |
| 3 | BEFORE (off) | 49 | 0 | 0 | 71 | 0 | 11.24 | 10.7 | 40.8 / 59.2 / 0.0 | 4004.25 | 5826.47 | 7428.71 | 3287.24/6070.41/6484.76 (n=49) | 0 / 0 (0) |
| 1 | AFTER (on) | 42 | 0 | 78 | 0 | 0 | 10.632 | 11.3 | 35.0 / 0.0 / 65.0 | 2649.58 | 8378.14 | 8770.82 | 5047.27/6933.02/6936.77 (n=42) | 50 / 10 (20) |
| 2 | AFTER (on) | 42 | 0 | 78 | 0 | 0 | 9.828 | 12.2 | 35.0 / 0.0 / 65.0 | 1462.78 | 7743.01 | 8204.1 | 5037.17/6468.12/6474.39 (n=42) | 41 / 10 (20) |
| 3 | AFTER (on) | 37 | 0 | 83 | 0 | 0 | 8.71 | 13.8 | 30.8 / 0.0 / 69.2 | 1589.64 | 6496.89 | 7556.68 | 4401.38/6017.36/6020.41 (n=37) | 43 / 10 (20) |

## Median across repetitions

| Scenario | Served | Shed 429 | Served / Error / Shed % | Wall (s) | Req/s | Client p50 ms | Client p95 ms | Client p99 ms | Served-only p50/p95/p99 ms | Server p50/p95/p99 ms | Peak global / mock in-flight (queue) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| BEFORE (off) | 49 | 0 | 41.0 / 59.0 / 0.0 | 11.24 | 10.7 | 4004.25 | 6008.41 | 7428.71 | 3997.44/7071.69/7444.04 | 3287.24/6070.41/6374.26 | 0 / 0 (0) |
| AFTER (on) | 42 | 78 | 35.0 / 0.0 / 65.0 | 9.83 | 12.2 | 1589.64 | 7743.01 | 8204.1 | 6678.6/8137.71/8315.39 | 5037.17/6468.12/6474.39 | 50 / 10 (20) |

## Honest reading (overload profile)

- Mock behaviour: base 500 ms + 150 ms per in-flight request, rejecting with 503 above 12 in-flight. BEFORE has no bulkhead, so it drives the provider into that unhealthy band; AFTER keeps admitted calls below it.
- BEFORE: served 41.0%, errors 59.0%, shed 0.0%, served-only p99 7444.04 ms, wall 11.24 s.
- AFTER: served 35.0%, errors 0.0%, shed 65.0%, served-only p99 8315.39 ms, wall 9.83 s.
- What the control buys here is error-free serving, not more served requests: AFTER converts provider 5xx into explicit 429 shedding, and its admitted calls can wait in the bulkhead queue (up to 5 s), so served-only p99 is not better than BEFORE. That is reported as measured.
- A 429 is deliberate load shedding: the caller is told to retry. A 5xx from an overloaded provider is a failure. AFTER refuses work it cannot run healthily instead of pushing the provider past its limit.
- Served-only p99 is the useful latency comparison; all-request p99 includes retry and backoff time.

Notes:
- Host baseline (measured, not assumed): Redis GET RTT 1.29 ms; sequential `/health` RTT 9.31 ms; 60 concurrent `/health` wall 668.7 ms. This host is slow at loopback concurrency, so absolute numbers are not production capacity.
- Repetitions are interleaved (BEFORE, AFTER, BEFORE, AFTER, ...) to spread host drift across both scenarios; medians are reported.
- Client latency is observed at the load generator; server-side percentiles are scenario-attributed `request_logs` rows (status 200). The full `/api/stats` snapshot is saved in the JSON.
- Peak mock in-flight is the provider bulkhead's counter; with control off the bulkhead is bypassed, so 0 means "not measured", not "no provider concurrency".
- AFTER is expected to shed under this load: that is the control doing its job, not a regression. BEFORE overloads the provider.
- `GATEWAY_CONCURRENCY_CONTROL=off` exists only for this comparison; never use it in production.
