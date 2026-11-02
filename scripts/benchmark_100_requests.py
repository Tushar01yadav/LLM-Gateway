import argparse
import asyncio
import os
import sys
import time
import httpx

# Ensure workspace root in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app
from app.reliability import PROVIDER_POOLS, GLOBAL_CONCURRENCY, reset_concurrency_pools

async def run_stress_test(
    total_requests: int = 100,
    capacity: int = 10,
    queue_size: int = 20,
    queue_wait_sec: float = 5.0,
    base_url: str = "http://127.0.0.1:8000",
    use_asgi: bool = False,
):
    print("=" * 70)
    print("       STEP 4 STRESS TEST: MOCK PROVIDER (FIXED 500MS LATENCY)")
    print(f"       Requests: {total_requests} | Capacity: {capacity} | Queue: {queue_size}")
    print("=" * 70)

    # Configure capacity on mock provider pool
    if "mock" in PROVIDER_POOLS:
        PROVIDER_POOLS["mock"].max_inflight = capacity
        PROVIDER_POOLS["mock"].max_queue_size = queue_size
        PROVIDER_POOLS["mock"].max_queue_wait_sec = queue_wait_sec

    limits = httpx.Limits(max_keepalive_connections=120, max_connections=150)

    client_kwargs = {"timeout": 45.0, "limits": limits}
    if use_asgi:
        client_kwargs["transport"] = httpx.ASGITransport(app=app)
        client_kwargs["base_url"] = "http://test"
    else:
        client_kwargs["base_url"] = base_url
        try:
            async with httpx.AsyncClient(base_url=base_url, timeout=2.0) as chk:
                h = await chk.get("/health")
                if h.status_code != 200:
                    raise Exception()
        except Exception:
            client_kwargs["transport"] = httpx.ASGITransport(app=app)
            client_kwargs["base_url"] = "http://test"

    t_start = time.perf_counter()
    async with httpx.AsyncClient(**client_kwargs) as client:
        async def send_req(i):
            payload = {
                "model": "mock",
                "messages": [{"role": "user", "content": f"hi {i}"}],
                "max_tokens": 15
            }
            req_start = time.perf_counter()
            try:
                resp = await client.post("/v1/chat", json=payload)
                elapsed = (time.perf_counter() - req_start) * 1000
                data = resp.json() if resp.status_code == 200 else {}
                return {
                    "idx": i,
                    "status": resp.status_code,
                    "latency_ms": elapsed,
                    "server_latency_ms": data.get("latency_ms", 0.0),
                    "queue_wait_ms": data.get("queue_wait_ms", 0.0),
                    "attempts": len(data.get("attempts", [])),
                }
            except Exception as e:
                elapsed = (time.perf_counter() - req_start) * 1000
                return {
                    "idx": i,
                    "status": 599,
                    "latency_ms": elapsed,
                    "server_latency_ms": 0.0,
                    "queue_wait_ms": 0.0,
                    "attempts": 0,
                    "error": str(e),
                }

        tasks = [asyncio.create_task(send_req(i)) for i in range(total_requests)]
        results = await asyncio.gather(*tasks)

    t_total = time.perf_counter() - t_start

    # Status counts
    statuses = [r["status"] for r in results]
    c_200 = statuses.count(200)
    c_429 = statuses.count(429)
    c_504 = statuses.count(504)
    c_other = len([s for s in statuses if s not in (200, 429, 504)])

    # Percentiles for SERVED requests only (HTTP 200)
    served_latencies = sorted([r["latency_ms"] for r in results if r["status"] == 200])
    if served_latencies:
        n = len(served_latencies)
        p50 = served_latencies[int(0.50 * (n - 1))]
        p95 = served_latencies[int(0.95 * (n - 1))]
        p99 = served_latencies[int(0.99 * (n - 1))]
        min_lat = min(served_latencies)
        max_lat = max(served_latencies)
    else:
        p50 = p95 = p99 = min_lat = max_lat = 0.0

    print("\n" + "=" * 70)
    print("                    STRESS TEST REPORT")
    print("=" * 70)
    print(f"Total Requests:              {total_requests}")
    print(f"Total Wall-Clock Time:       {t_total:.2f} s")
    print("-" * 70)
    print("STATUS COUNTS:")
    print(f"  HTTP 200 (Served/Success): {c_200}")
    print(f"  HTTP 429 (Shed):           {c_429}")
    print(f"  HTTP 504 (Timeout):        {c_504}")
    print(f"  Other Errors:              {c_other}")
    print("-" * 70)
    print(f"SEPARATE SHED METRIC:")
    print(f"  Shed Count (HTTP 429):     {c_429} (Capacity Throttled / Limit Protected)")
    print("-" * 70)
    print("LATENCY PERCENTILES (SERVED REQUESTS ONLY):")
    print(f"  Served Count:              {len(served_latencies)}")
    print(f"  Min Latency:               {min_lat:.1f} ms")
    print(f"  p50 Latency:               {p50:.1f} ms")
    print(f"  p95 Latency:               {p95:.1f} ms")
    print(f"  p99 Latency:               {p99:.1f} ms")
    print(f"  Max Latency:               {max_lat:.1f} ms")
    print("=" * 70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 4 Stress Test with Mock Provider")
    parser.add_argument("--requests", type=int, default=100, help="Number of concurrent requests (default: 100)")
    parser.add_argument("--capacity", type=int, default=10, help="Mock provider max inflight capacity (default: 10)")
    parser.add_argument("--queue-size", type=int, default=20, help="Mock provider max queue size (default: 20)")
    parser.add_argument("--asgi", action="store_true", help="Force in-process ASGI transport")
    args = parser.parse_args()
    asyncio.run(run_stress_test(total_requests=args.requests, capacity=args.capacity, queue_size=args.queue_size, use_asgi=args.asgi))
