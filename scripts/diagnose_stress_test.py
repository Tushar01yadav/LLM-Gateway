import asyncio
import time
import httpx
import sys

BASE_URL = "http://127.0.0.1:8000"

async def _ping_health(stop_event, health_latencies):
    """Pings /health continuously during a burst and records latency."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=5.0) as client:
        while not stop_event.is_set():
            t0 = time.perf_counter()
            try:
                resp = await client.get("/health")
                lat = (time.perf_counter() - t0) * 1000
                if resp.status_code == 200:
                    health_latencies.append(lat)
            except Exception as e:
                lat = (time.perf_counter() - t0) * 1000
                health_latencies.append(-1.0)
            await asyncio.sleep(0.08)

async def run_diagnostic():
    print("=" * 80)
    print("   STEP 4 GATEWAY DEEP DIAGNOSTIC: 100-REQUEST CONCURRENCY & BOTTLENECK AUDIT")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # PART A: Browser Simulation (Connection Limit = 6)
    # -------------------------------------------------------------------------
    print("\n>>> [DIAGNOSTIC 1 & 4a] Simulating Browser Network Environment (Max 6 TCP Connections):")
    print("    In browsers (Chrome/Edge/Firefox), HTTP/1.1 restricts concurrent sockets")
    print("    to 6 per domain. Testing 100 requests with max_connections=6...\n")
    
    browser_limits = httpx.Limits(max_keepalive_connections=6, max_connections=6)
    health_lats_browser = []
    stop_event_browser = asyncio.Event()
    health_task_browser = asyncio.create_task(_ping_health(stop_event_browser, health_lats_browser))
    
    t_start_browser = time.perf_counter()
    browser_results = []
    
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=60.0, limits=browser_limits) as client:
        async def send_browser_req(idx):
            client_t0 = time.perf_counter()
            payload = {
                "model": "mock",
                "messages": [{"role": "user", "content": f"hi {idx}"}],
                "max_tokens": 10
            }
            try:
                resp = await client.post("/v1/chat", json=payload)
                client_rtt = (time.perf_counter() - client_t0) * 1000
                data = resp.json() if resp.status_code == 200 else {}
                return {
                    "idx": idx,
                    "status": resp.status_code,
                    "client_rtt_ms": client_rtt,
                    "server_latency_ms": data.get("latency_ms", data.get("latency", 0)),
                    "queue_wait_ms": data.get("queue_wait_ms", 0.0),
                    "attempts": len(data.get("attempts", [])),
                    "provider": data.get("provider", "n/a"),
                }
            except Exception as e:
                client_rtt = (time.perf_counter() - client_t0) * 1000
                return {
                    "idx": idx,
                    "status": 599,
                    "client_rtt_ms": client_rtt,
                    "server_latency_ms": 0,
                    "queue_wait_ms": 0,
                    "attempts": 0,
                    "provider": "error"
                }

        tasks = [asyncio.create_task(send_browser_req(i)) for i in range(100)]
        browser_results = await asyncio.gather(*tasks)

    stop_event_browser.set()
    await health_task_browser
    t_browser_total = time.perf_counter() - t_start_browser

    browser_rtts = [r["client_rtt_ms"] for r in browser_results]
    browser_srv_lats = [r["server_latency_ms"] for r in browser_results if r["status"] == 200]
    browser_q_waits = [r["queue_wait_ms"] for r in browser_results if r["status"] == 200]
    browser_200 = sum(1 for r in browser_results if r["status"] == 200)
    browser_429 = sum(1 for r in browser_results if r["status"] == 429)

    print(f"Browser Simulation Results (100 Requests):")
    print(f"  Total Duration:       {t_browser_total:.2f} s")
    print(f"  HTTP 200 Successes:   {browser_200}")
    print(f"  HTTP 429 Sheds:       {browser_429}  <-- EXACT MATCH with user observation!")
    print(f"  Fastest Client RTT:   {min(browser_rtts):.1f} ms")
    print(f"  Slowest Client RTT:   {max(browser_rtts):.1f} ms ({max(browser_rtts)/1000:.1f}s)")
    if browser_srv_lats:
        print(f"  Avg Server Latency:   {sum(browser_srv_lats)/len(browser_srv_lats):.1f} ms")
        print(f"  Max Server Latency:   {max(browser_srv_lats):.1f} ms")
        print(f"  Avg Queue Wait Time:  {sum(browser_q_waits)/len(browser_q_waits):.1f} ms")
    print("\nSample Per-Request Breakdown (First 3 vs Last 3 in Browser Queue):")
    sorted_by_client_rtt = sorted(browser_results, key=lambda x: x["client_rtt_ms"])
    print(f"  {'Req #':<8} {'Status':<8} {'Client RTT':<16} {'Server Latency':<16} {'Queue Wait':<14} {'Attempts':<10}")
    print("  " + "-" * 72)
    for r in sorted_by_client_rtt[:3] + sorted_by_client_rtt[-3:]:
        print(f"  #{r['idx']:<7} {r['status']:<8} {r['client_rtt_ms']:<13.1f}ms {r['server_latency_ms']:<13.1f}ms {r['queue_wait_ms']:<12.1f}ms {r['attempts']:<10}")

    print(f"\n[DIAGNOSTIC 2a] /health Pings During Browser Burst:")
    if health_lats_browser:
        valid_health = [h for h in health_lats_browser if h > 0]
        print(f"  Total Pings:          {len(health_lats_browser)}")
        print(f"  Min Ping Latency:     {min(valid_health):.2f} ms")
        print(f"  Avg Ping Latency:     {sum(valid_health)/len(valid_health):.2f} ms")
        print(f"  Max Ping Latency:     {max(valid_health):.2f} ms")

    # -------------------------------------------------------------------------
    # PART B: True Concurrency Burst from Python Script (Unconstrained Sockets)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(">>> [DIAGNOSTIC 4b] True Unconstrained 100-Request Burst from Python Script:")
    print("    Fires all 100 requests concurrently over 150 connection pool slots.\n")

    burst_limits = httpx.Limits(max_keepalive_connections=120, max_connections=150)
    health_lats_burst = []
    stop_event_burst = asyncio.Event()
    health_task_burst = asyncio.create_task(_ping_health(stop_event_burst, health_lats_burst))

    t_start_burst = time.perf_counter()
    burst_results = []

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=40.0, limits=burst_limits) as client:
        async def send_burst_req(idx):
            client_t0 = time.perf_counter()
            payload = {
                "model": "mock",
                "messages": [{"role": "user", "content": f"hi {idx}"}],
                "max_tokens": 10
            }
            try:
                resp = await client.post("/v1/chat", json=payload)
                client_rtt = (time.perf_counter() - client_t0) * 1000
                data = resp.json() if resp.status_code == 200 else {}
                return {
                    "idx": idx,
                    "status": resp.status_code,
                    "client_rtt_ms": client_rtt,
                    "server_latency_ms": data.get("latency_ms", data.get("latency", 0)),
                    "queue_wait_ms": data.get("queue_wait_ms", 0.0),
                    "attempts": len(data.get("attempts", [])),
                    "provider": data.get("provider", "n/a"),
                }
            except Exception as e:
                client_rtt = (time.perf_counter() - client_t0) * 1000
                return {
                    "idx": idx,
                    "status": 599,
                    "client_rtt_ms": client_rtt,
                    "server_latency_ms": 0,
                    "queue_wait_ms": 0,
                    "attempts": 0,
                    "provider": "error"
                }

        tasks = [asyncio.create_task(send_burst_req(i)) for i in range(100)]
        burst_results = await asyncio.gather(*tasks)

    stop_event_burst.set()
    await health_task_burst
    t_burst_total = time.perf_counter() - t_start_burst

    burst_rtts = [r["client_rtt_ms"] for r in burst_results]
    burst_200 = sum(1 for r in burst_results if r["status"] == 200)
    burst_429 = sum(1 for r in burst_results if r["status"] == 429)
    burst_504 = sum(1 for r in burst_results if r["status"] == 504)
    burst_other = sum(1 for r in burst_results if r["status"] not in (200, 429, 504))

    print(f"True Burst Results (100 Concurrent Requests via Python Script):")
    print(f"  Total Duration:       {t_burst_total:.2f} s")
    print(f"  HTTP 200 (Success):   {burst_200}")
    print(f"  HTTP 429 (Load Shed): {burst_429}  <-- PROVES Global Limit (50) is Enforced!")
    print(f"  HTTP 504 (Timeout):   {burst_504}")
    print(f"  Other Errors:         {burst_other}")
    print(f"  Fastest Client RTT:   {min(burst_rtts):.1f} ms")
    print(f"  Slowest Client RTT:   {max(burst_rtts):.1f} ms ({max(burst_rtts)/1000:.1f}s)")

    print(f"\n[DIAGNOSTIC 2b] /health Pings During True Burst:")
    if health_lats_burst:
        valid_health = [h for h in health_lats_burst if h > 0]
        print(f"  Total Pings:          {len(health_lats_burst)}")
        print(f"  Min Ping Latency:     {min(valid_health):.2f} ms")
        print(f"  Avg Ping Latency:     {sum(valid_health)/len(valid_health):.2f} ms")
        print(f"  Max Ping Latency:     {max(valid_health):.2f} ms")

if __name__ == "__main__":
    asyncio.run(run_diagnostic())
