# Phase 4 chaos — WITH fallback chain (primary mock -> mock2)

- Generated: 2026-11-05T06:30:47.195939+00:00
- Machine: win32, Python 3.10.11, 2 workers on ports [8051, 8052]
- Redis db 4; mock latency file `scratch/mock_latency_ms.json`
- Alias under test: `mock_failover` (mock -> mock2); real-provider alias: `fast` (groq -> openrouter)
- Load: 300 requests, concurrency 30, fault at 8 s for 5 s

## Without fallback (baseline, from phase4-chaos.json)

| Scenario | Requests during fault | Success during fault | Served p99 (ms) | Errors by type | Recovery (s) |
|---|---|---|---|---|---|
| provider_death | 90 | 0.1889 | 3894.34 | {'error_5xx': 73, 'served': 17} | 0 |
| provider_latency | 17 | 1.0 | 8316.31 | {'served': 17} | 0 |

The baseline's Redis-kill and worker-kill scenarios are unchanged and not re-run here.

## With fallback — summary

| Scenario | Success during fault | Served by fallback | Failover first served (s) | Breaker OPEN (s) | Served p99 (ms) | Post-fault providers | Recovery (s) |
|---|---|---|---|---|---|---|---|
| provider_death_fallback | 1.0 | 40/50 (0.8) | 2.68 | 2.24 | 9625.09 | {'mock': 162} | 0 |
| provider_latency_fallback | 1.0 | 0/17 (0.0) | None | None | 8203.34 | {'mock': 218} | 0 |
| provider_latency_severe | 1.0 | 52/52 (1.0) | 6.83 | None | 18234.51 | {'mock': 135} | 2 |

## Per-window detail

### provider_death_fallback

primary mock returns 503 mid-load; fallback mock2 stays healthy. Fault window 5.53 s.

| Window | Requests | Success | By class | Served by provider | Fallback share | p50/p95/p99 served (ms) |
|---|---|---|---|---|---|---|
| pre_fault | 88 | 1.0 | {'served': 88} | {'mock': 82, 'mock2': 6} | 0.0682 | 3039.27/5868.04/7232.8 |
| during_fault | 50 | 1.0 | {'served': 50} | {'mock2': 40, 'mock': 10} | 0.8 | 2020.67/7152.46/9625.09 |
| post_fault | 162 | 1.0 | {'served': 162} | {'mock': 162} | 0.0 | 2369.48/3625.69/5010.72 |

- First fallback-served response: provider `mock2`, latency 1669.9 ms, 2.68 s after fault start.
- Primary breaker observed OPEN 2.24 s after fault start ({'state': 'OPEN', 'consecutive_failures': 8, 'cooldown_remaining_s': 59.8}).
- Primary/fallback attempt outcomes during the fault window: {'mock:skipped:circuit_breaker_open': 36, 'mock:error:server_error_503': 19}.

### provider_latency_fallback

primary mock latency -> 5000 ms mid-load; fallback mock2 stays at 500 ms. Fault window 5.01 s.

| Window | Requests | Success | By class | Served by provider | Fallback share | p50/p95/p99 served (ms) |
|---|---|---|---|---|---|---|
| pre_fault | 65 | 1.0 | {'served': 65} | {'mock': 65} | 0.0 | 5183.51/9531.0/9932.5 |
| during_fault | 17 | 1.0 | {'served': 17} | {'mock': 17} | 0.0 | 6990.49/8203.34/8203.34 |
| post_fault | 218 | 1.0 | {'served': 218} | {'mock': 218} | 0.0 | 3508.7/4907.23/5623.15 |

- No request was served by the fallback during the run.
- Primary breaker never opened during the run.

### provider_latency_severe

primary mock latency -> 16000 ms mid-load; fallback mock2 stays at 500 ms. Fault window 25.01 s.

| Window | Requests | Success | By class | Served by provider | Fallback share | p50/p95/p99 served (ms) |
|---|---|---|---|---|---|---|
| pre_fault | 53 | 1.0 | {'served': 53} | {'mock': 45, 'mock2': 8} | 0.1509 | 2793.85/17387.48/17440.21 |
| during_fault | 52 | 1.0 | {'served': 52} | {'mock2': 52} | 1.0 | 6352.07/17814.74/18234.51 |
| post_fault | 135 | 1.0 | {'served': 135} | {'mock': 135} | 0.0 | 1332.4/2525.72/3673.31 |

- First fallback-served response: provider `mock2`, latency 6788.06 ms, 6.83 s after fault start.
- Primary breaker never opened during the run.
- Primary/fallback attempt outcomes during the fault window: {'mock:saturated:queue_wait_exceeded': 40, 'mock:error:unknown_error': 12}.

## Real providers (low N)

3 real Groq calls, then 3 calls with Groq injected as 503 so the real OpenRouter fallback serves; alias 'fast', max_tokens 16. Served 6/6; providers {'groq': 3, 'openrouter': 3}; served p50/p95/p99 623.67/4603.47/4603.47 ms.

| Phase | # | Status | Class | Provider | Model | Fallback used | Latency (ms) |
|---|---|---|---|---|---|---|---|
| primary_real | 0 | 200 | served | groq | openai/gpt-oss-120b | False | 623.4 |
| primary_real | 1 | 200 | served | groq | openai/gpt-oss-120b | False | 623.67 |
| primary_real | 2 | 200 | served | groq | openai/gpt-oss-120b | False | 937.54 |
| groq_injected_503 | 0 | 200 | served | openrouter | meta-llama/llama-3.1-8b-instruct | True | 4603.47 |
| groq_injected_503 | 1 | 200 | served | openrouter | meta-llama/llama-3.1-8b-instruct | True | 4321.27 |
| groq_injected_503 | 2 | 200 | served | openrouter | meta-llama/llama-3.1-8b-instruct | True | 591.6 |

## Honest reading

- Provider death: success during fault 1.0 with fallback vs 0.1889 without it; 40/50 requests (0.8) were served by the fallback, first at 2.68 s, and the primary breaker opened at 2.24 s. Requests already queued on the primary pay the retry backoff before moving over; later requests skip it immediately.
- Latency at 5000 ms does NOT trigger failover: success stays 1.0 on the primary, fallback share is 0.0, and throughput drops from 65 to 17 requests in comparable windows with p99 at 8203.34 ms. The primary is slow but still succeeds inside the 30 s budget, so this is a degradation scenario, not a failover scenario.
- Latency at 16000 ms: success during fault stays 1.0 and the fallback absorbs 52/52 requests, but served p99 is 18234.51 ms and the breaker never opened. Attempt outcomes: {'mock:saturated:queue_wait_exceeded': 40, 'mock:error:unknown_error': 12}. Latency remains multi-second for requests caught on the slow primary; the fallback prevents hard failure but does not make the fault invisible.
- The provider-death fault is healed through the admin toggle, which also resets the primary breaker; a real outage would leave it OPEN for its 60 s cooldown, so post-heal traffic would stay on the fallback longer than shown here.
- Real providers: 6/6 served; the Groq-injected phase produced 3/3 served responses, of which 3 came through the real fallback ({'groq': 3, 'openrouter': 3}). The first injected call paid the retry/backoff path (4603.47 ms); once the Groq breaker opened, the last completed in 591.6 ms. Small N (max_tokens 16) keeps quota use minimal.
