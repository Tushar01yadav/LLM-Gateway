import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.benchmark_100_requests import run_stress_test, argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 4 Stress Test with Mock Provider")
    parser.add_argument("--requests", type=int, default=100, help="Number of concurrent requests (default: 100)")
    parser.add_argument("--capacity", type=int, default=10, help="Mock provider max inflight capacity (default: 10)")
    parser.add_argument("--queue-size", type=int, default=20, help="Mock provider max queue size (default: 20)")
    parser.add_argument("--asgi", action="store_true", help="Force in-process ASGI transport")
    args = parser.parse_args()
    asyncio.run(run_stress_test(total_requests=args.requests, capacity=args.capacity, queue_size=args.queue_size, use_asgi=args.asgi))
