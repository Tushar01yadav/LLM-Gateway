# Phase 4.3 — Chaos under concurrent load

Source data: `docs/benchmarks/phase4-chaos.json` (generated 2026-11-05T05:23:14Z).
Reproduce: `python scripts/chaos_load.py`.

Environment: win32, Python 3.10.11, Redis 7.4 (Docker `llm-gateway-redis`, db 4), two
uvicorn workers on ports 8051/8052, mock provider latency 500 ms, load 300 requests at
concurrency 30, fault injected 8 s into the run for 5 s. Latency numbers below are
client-observed and include this host's loopback cost (see `phase4-load-before-after.md`
for measured host baselines: Redis GET RTT ≈ 2 ms, sequential `/health` ≈ 12 ms,
60-concurrent `/health` wall ≈ 530 ms). Windows are attributed by **request start time**.

## Summary

| Scenario | Success during fault | p99 during fault (ms) | Time to recover | Errors by type | Notes |
|---|---|---|---|---|---|
| Provider death (mock returns 503) | **18.9 %** (17/90) | 4743 (all), 3894 (served only) | 0 s | 73 × 5xx (502 to client) | ~81 % of requests started in the fault window failed. Circuit breaker CLOSED after heal. |
| Provider latency (500 → 5000 ms) | **100 %** (17/17) | 8316 (all) | 0 s | none | No errors, but only 17 requests *started* in the 5 s window vs 132 in the 8 s pre-window (~79 % throughput collapse). Latency absorbed by waiting, not shed. |
| Redis kill + restart | **100 %** (56/56) | 4295 (all) | 0 s | none | 8 PING failures, first at +9.1 s, restored at +15.2 s. Requests kept being served via fallbacks. |
| Worker kill (TerminateProcess) | Jobs **12/12 done** | n/a | **37.5 s** | 15 × connection errors (HTTP only) | 1 job needed 2 attempts after lease expiry; cost rows delta = 12 (no duplicate side effects). In-flight HTTP requests on the killed worker were lost — no front proxy in this setup. |

"Time to recover" = first sustained 1 s bucket with ≥ 90 % success after the fault was
lifted. It reads 0 s for the three provider/Redis scenarios because requests already
in flight during the fault window were still completing successfully at heal time; it is
not evidence that the fault had no effect.

## Window detail

| Scenario | Window | Requests | Success | p50 / p95 / p99 (ms) | Error classes |
|---|---|---|---|---|---|
| provider_death | pre | 107 | 93.5 % | 2499 / 4555 / 4775 | 7 × 5xx |
| provider_death | during | 90 | **18.9 %** | 1542 / 3894 / 4743 | **73 × 5xx** |
| provider_death | post | 103 | 100 % | 2910 / 3617 / 3869 | — |
| provider_latency | pre | 132 | 100 % | 2010 / 5874 / 7082 | — |
| provider_latency | during | 17 | 100 % | 5793 / 8316 / 8316 | — |
| provider_latency | post | 151 | 100 % | 1899 / 2777 / 3281 | — |
| redis_kill | pre | 130 | 100 % | 2111 / 3251 / 3737 | — |
| redis_kill | during | 56 | 100 % | 3303 / 4156 / 4295 | — |
| redis_kill | post | 114 | 100 % | 1970 / 3018 / 3153 | — |

## Honest findings

1. **Provider death is not gracefully absorbed.** With a single provider returning 503,
   81 % of requests that began during the fault failed as 502s. The circuit breaker
   opened during the fault and returned to CLOSED after the provider healed. There is no
   failover provider configured in this run, so this is the expected (and reported) result,
   not a test artifact.
2. **Latency injection degrades throughput, not correctness.** Every request that
   reached the provider eventually succeeded, but the 5000 ms call cap meant only 17
   requests started during the 5 s fault vs 132 in the 8 s pre-window. The tail p99
   during the fault (8.3 s) is ~1.2× the pre-fault p99 (7.1 s); the bigger cost is lost
   throughput, which the success-rate metric alone hides.
3. **Redis kill did not take the gateway down.** All 56 requests started during the
   outage were served, at ~1.5 s higher p99 than pre-fault. Fallbacks (in-memory
   limiter state, cost path) covered the 8 failed PINGs. This does mean rate-limit
   accounting is approximate while Redis is down.
4. **Worker kill preserves jobs exactly once, loses in-flight HTTP.** All 12 jobs
   completed (one retried once after its 30 s lease expired), with exactly 12 cost rows.
   15 HTTP requests sent to the killed process failed with connection errors; without a
   front proxy/load balancer, an HTTP request in flight on a dead process cannot be
   salvaged. Recovery took 37.5 s because it waits for the lease to expire (30 s) plus the
   5 s recovery scan.
5. **No tuning was applied to hide degradations.** Numbers above are the final
   unmodified run; the chaos harness reports start-time windows and all error classes.

## Reproduction notes

- `scripts/chaos_load.py` starts its own workers on 8051/8052 with Redis db 4 and
  `GATEWAY_ADMIN_TOKEN=phase4-chaos-admin-token`; it kills/starts the
  `llm-gateway-redis` Docker container for the Redis scenario.
- Provider faults use `POST /api/reliability/inject-failure` on both ports (healed at
  +13 s). Latency fault rewrites `scratch/mock_latency_ms.txt` mid-run.
- Worker kill uses `psutil.Process(pid).kill()` (Windows TerminateProcess, no shutdown
  hooks) on the worker that owns the observed lease.
