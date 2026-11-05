"""One-command demo load generator for docker-compose.

Run via `docker compose up --build` (see README). Exercises the gateway
end-to-end against the built-in mock providers, so no API keys are needed:

1. health check
2. happy path chat (model=mock) and response-header metadata
3. inject a 503 outage on mock, then call the failover alias (mock_failover)
   to show the fallback chain (mock -> mock2) serving traffic
4. clear the outage
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

GATEWAY = os.getenv("GATEWAY_URL", "http://gateway:8000")
ADMIN_TOKEN = os.getenv("GATEWAY_ADMIN_TOKEN", "demo-admin-token")

failures = []


def request(path, method="GET", body=None, headers=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(GATEWAY + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers or {}), exc.read().decode()


def check(label, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" :: {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def wait_for_health(deadline=60):
    started = time.time()
    while time.time() - started < deadline:
        try:
            status, _, _ = request("/health", timeout=3)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def chat(model, prompt):
    return request(
        "/v1/chat",
        method="POST",
        body={"model": model, "messages": [{"role": "user", "content": prompt}]},
    )


def main():
    print(f"Waiting for gateway at {GATEWAY} ...")
    if not wait_for_health():
        print("Gateway did not become healthy within 60s")
        return 1

    print("\n[1] Health")
    status, _, _ = request("/health")
    check("GET /health", status == 200, f"http={status}")

    print("\n[2] Happy path (built-in mock provider, no API keys)")
    status, headers, body = chat("mock", "Hello from docker compose")
    check("POST /v1/chat model=mock", status == 200, f"http={status}")
    for name in ("X-Request-Id", "X-Provider", "X-Model", "X-Fallback-Used", "X-Latency-Ms", "X-Cost-USD"):
        check(f"header {name}", name.lower() in {k.lower() for k in headers})
    if status == 200:
        print("      body:", body[:120])

    print("\n[3] Provider outage -> fallback chain (mock -> mock2)")
    status, _, body = request(
        "/api/reliability/inject-failure",
        method="POST",
        body={"provider": "mock", "enabled": True, "mode": "503"},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    check("inject 503 on mock", status == 200, f"http={status} {body[:100]}")

    status, headers, body = chat("mock_failover", "Fallback should serve this")
    fallback_used = {k.lower(): v for k, v in headers.items()}.get("x-fallback-used")
    provider = {k.lower(): v for k, v in headers.items()}.get("x-provider")
    check("POST /v1/chat model=mock_failover during outage", status == 200, f"http={status}")
    check("served by fallback", fallback_used == "true", f"x-fallback-used={fallback_used} x-provider={provider}")

    status, _, _ = request(
        "/api/reliability/inject-failure",
        method="POST",
        body={"provider": "mock", "enabled": False, "mode": "503"},
        headers={"X-Admin-Token": ADMIN_TOKEN},
    )
    check("clear injected outage", status == 200, f"http={status}")

    print("\n" + ("ALL CHECKS PASSED" if not failures else f"{len(failures)} CHECK(S) FAILED: {failures}"))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
