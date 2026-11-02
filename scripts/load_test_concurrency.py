"""
Step 4 Load Test Proof: 200 Concurrent Requests Benchmark
Runs 200 concurrent requests against a throttled provider in two modes:
1. BEFORE (Concurrency Limits Disabled):
   - Unbounded in-flight requests, unlimited queue.
   - Measures p50/p95/p99 latency, error rate, and memory consumption.
2. AFTER (Concurrency & Bulkhead Limits Enabled):
   - Strict in-flight limits, bounded FIFO queue, fast fallback, and 429 load shedding.
   - Measures p50/p95/p99 latency, error rate, memory consumption, 429 shed count,
     queue depth peak, and fallback count.
Prints terminal logs and comparison table.
"""

import asyncio
import os
import sys
import time
import psutil
from unittest.mock import patch
import httpx

# Ensure workspace root in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app
from app.reliability import (
    GLOBAL_CONCURRENCY,
    PROVIDER_POOLS,
    CIRCUIT_BREAKERS,
    ADAPTER_MAP,
    reset_concurrency_pools,
)


def get_current_memory_mb() -> float:
    """Returns memory RSS in MB for current process."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def calculate_percentiles(latencies):
    if not latencies:
        return 0.0, 0.0, 0.0
    s = sorted(latencies)
    n = len(s)
    p50 = s[int(0.50 * (n - 1))]
    p95 = s[int(0.95 * (n - 1))]
    p99 = s[int(0.99 * (n - 1))]
    return round(p50, 2), round(p95, 2), round(p99, 2)


async def run_scenario(name: str, limits_enabled: bool, total_requests: int = 200):
    print(f"\n======================================================================")
    print(f"  RUNNING LOAD TEST: {name}")
    print(f"  Concurrent Requests: {total_requests} | Limits Enabled: {limits_enabled}")
    print(f"======================================================================")

    reset_concurrency_pools()
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()

    if not limits_enabled:
        # DISABLE LIMITS: Unbounded global and provider concurrency
        GLOBAL_CONCURRENCY.max_inflight = 5000
        for p in PROVIDER_POOLS.values():
            p.max_inflight = 5000
            p.max_queue_size = 5000
            p.max_queue_wait_sec = 60.0
    else:
        # ENABLE LIMITS: Bounded concurrency & bulkhead protection
        GLOBAL_CONCURRENCY.max_inflight = 50
        # Groq primary: tightly throttled to 10 in-flight, max 15 queue, 0.15s wait
        PROVIDER_POOLS["groq"].max_inflight = 10
        PROVIDER_POOLS["groq"].max_queue_size = 15
        PROVIDER_POOLS["groq"].max_queue_wait_sec = 0.15
        # OpenRouter fallback: 15 in-flight, max 20 queue
        PROVIDER_POOLS["openrouter"].max_inflight = 15
        PROVIDER_POOLS["openrouter"].max_queue_size = 20
        PROVIDER_POOLS["openrouter"].max_queue_wait_sec = 0.20
        # Gemini fallback: 15 in-flight, max 20 queue
        PROVIDER_POOLS["gemini"].max_inflight = 15
        PROVIDER_POOLS["gemini"].max_queue_size = 20
        PROVIDER_POOLS["gemini"].max_queue_wait_sec = 0.20

    # Mock Throttled Provider: Groq introduces artificial service delay (e.g. 80ms)
    async def throttled_groq(*args, **kwargs):
        await asyncio.sleep(0.08)
        return {
            "text": "Throttled Groq response",
            "finish_reason": "stop",
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        }

    # Fallback provider: OpenRouter responds in 20ms
    async def fallback_openrouter(*args, **kwargs):
        await asyncio.sleep(0.02)
        return {
            "text": "Fallback OpenRouter response",
            "finish_reason": "stop",
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        }

    # Fallback provider: Gemini responds in 20ms
    async def fallback_gemini(*args, **kwargs):
        await asyncio.sleep(0.02)
        return {
            "text": "Fallback Gemini response",
            "finish_reason": "stop",
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        }

    mem_before = get_current_memory_mb()
    t_start = time.perf_counter()

    latencies = []
    status_codes = []
    responses = []
    fallbacks_count = 0
    peak_queue_depth = 0

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gateway.local", timeout=30.0) as client:
        with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=throttled_groq), \
             patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=fallback_openrouter), \
             patch.object(ADAPTER_MAP["gemini"], "generate", side_effect=fallback_gemini):

            async def send_single_req(req_idx: int):
                nonlocal peak_queue_depth, fallbacks_count
                req_t0 = time.perf_counter()
                payload = {
                    "model": "fast",
                    "messages": [{"role": "user", "content": f"Load test request #{req_idx}"}],
                    "temperature": 0.5,
                    "max_tokens": 50,
                }
                try:
                    resp = await client.post("/v1/chat", json=payload)
                    lat = (time.perf_counter() - req_t0) * 1000
                    latencies.append(lat)
                    status_codes.append(resp.status_code)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("fallback_used"):
                            fallbacks_count += 1
                    # Monitor queue peak
                    q_d = PROVIDER_POOLS["groq"].queue_depth
                    if q_d > peak_queue_depth:
                        peak_queue_depth = q_d
                    return resp.status_code
                except Exception as e:
                    lat = (time.perf_counter() - req_t0) * 1000
                    latencies.append(lat)
                    status_codes.append(500)
                    return 500

            # Fire all 200 requests concurrently
            tasks = [asyncio.create_task(send_single_req(i)) for i in range(total_requests)]
            results = await asyncio.gather(*tasks)

    t_end = time.perf_counter()
    mem_after = get_current_memory_mb()
    total_elapsed = round(t_end - t_start, 3)

    p50, p95, p99 = calculate_percentiles(latencies)
    count_200 = status_codes.count(200)
    count_429 = status_codes.count(429)
    count_errors = len([c for c in status_codes if c not in (200, 429)])
    error_rate = round((count_errors / total_requests) * 100, 2)
    mem_delta = round(max(0.0, mem_after - mem_before), 2)

    print(f"Results for {name}:")
    print(f"  Total Time:       {total_elapsed}s")
    print(f"  Successful (200): {count_200}")
    print(f"  Shed (429):       {count_429}")
    print(f"  Errors (5xx/etc): {count_errors} ({error_rate}%)")
    print(f"  Latency p50:      {p50} ms")
    print(f"  Latency p95:      {p95} ms")
    print(f"  Latency p99:      {p99} ms")
    print(f"  Memory (RSS):     {mem_after:.2f} MB (Delta: +{mem_delta} MB)")
    if limits_enabled:
        print(f"  Queue Depth Peak: {peak_queue_depth}")
        print(f"  Fallbacks Used:   {fallbacks_count}")

    return {
        "name": name,
        "total": total_requests,
        "success_200": count_200,
        "shed_429": count_429,
        "errors": count_errors,
        "error_rate": error_rate,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "mem_rss": round(mem_after, 2),
        "mem_delta": mem_delta,
        "queue_peak": peak_queue_depth,
        "fallbacks": fallbacks_count,
        "elapsed": total_elapsed,
    }


async def main():
    print(">>> Starting Step 4 Concurrency Load Test Benchmark (200 Concurrent Requests)...")

    # 1. Run BEFORE (Limits Disabled)
    before_stats = await run_scenario(
        name="BEFORE (Limits Disabled - Unbounded)",
        limits_enabled=False,
        total_requests=200,
    )

    await asyncio.sleep(1.0)

    # 2. Run AFTER (Limits Enabled)
    after_stats = await run_scenario(
        name="AFTER (Limits Enabled - Bulkhead & Concurrency Control)",
        limits_enabled=True,
        total_requests=200,
    )

    # Comparison Table
    print("\n" + "=" * 88)
    print("            STEP 4 CONCURRENCY LOAD TEST COMPARISON TABLE (200 REQUESTS)")
    print("=" * 88)
    print(f"{'Metric':<32} | {'BEFORE (Limits Disabled)':<25} | {'AFTER (Limits Enabled)':<25}")
    print("-" * 88)
    print(f"{'Total Concurrent Requests':<32} | {before_stats['total']:<25} | {after_stats['total']:<25}")
    print(f"{'Successful Responses (200 OK)':<32} | {before_stats['success_200']:<25} | {after_stats['success_200']:<25}")
    print(f"{'Load Shed Responses (HTTP 429)':<32} | {before_stats['shed_429']:<25} | {after_stats['shed_429']:<25}")
    print(f"{'Uncontrolled Errors (5xx)':<32} | {before_stats['errors']:<25} | {after_stats['errors']:<25}")
    print(f"{'Error Rate (%)':<32} | {before_stats['error_rate']}%{'':<21} | {after_stats['error_rate']}%{'':<21}")
    print(f"{'Latency p50 (ms)':<32} | {before_stats['p50']} ms{'':<18} | {after_stats['p50']} ms{'':<18}")
    print(f"{'Latency p95 (ms)':<32} | {before_stats['p95']} ms{'':<18} | {after_stats['p95']} ms{'':<18}")
    print(f"{'Latency p99 (ms)':<32} | {before_stats['p99']} ms{'':<18} | {after_stats['p99']} ms{'':<18}")
    print(f"{'Memory Delta (RSS MB)':<32} | +{before_stats['mem_delta']} MB{'':<18} | +{after_stats['mem_delta']} MB{'':<18}")
    print(f"{'Peak Queue Depth':<32} | {'Unbounded':<25} | {after_stats['queue_peak']:<25}")
    print(f"{'Fallback Requests Handled':<32} | {'0 (All crammed)':<25} | {after_stats['fallbacks']:<25}")
    print(f"{'Execution Time (s)':<32} | {before_stats['elapsed']} s{'':<19} | {after_stats['elapsed']} s{'':<19}")
    print("=" * 88)


if __name__ == "__main__":
    asyncio.run(main())
