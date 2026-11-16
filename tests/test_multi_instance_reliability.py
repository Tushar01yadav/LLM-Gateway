"""Process-level checks against two gateways sharing the Compose PostgreSQL and Redis."""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest
import redis

import app.db as db
from app.security import create_gateway_api_key


pytestmark = pytest.mark.skipif(
    os.getenv("GATEWAY_REQUIRE_MULTI_INSTANCE_TESTS") != "1",
    reason="requires docker-compose.multi-instance.yml and dedicated PostgreSQL/Redis",
)

ADMIN = {"X-Admin-Token": os.environ.get("GATEWAY_ADMIN_TOKEN", "multi-instance-test-admin-token")}
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/15")
CIRCUIT_REDIS_URL = os.environ.get("CIRCUIT_BREAKER_REDIS_URL", "redis://127.0.0.1:6379/14")


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class GatewayProcess:
    def __init__(self, port, log_path):
        self.port = port
        self.log_path = Path(log_path)
        self._log = None
        self.process = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        self._log = self.log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(self.port)],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Gateway exited ({self.process.returncode}):\n{self.log_tail()}")
            try:
                if httpx.get(f"{self.url}/health", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        self.stop(force=True)
        raise RuntimeError(f"Gateway did not become healthy:\n{self.log_tail()}")

    def stop(self, force=False):
        if self.process and self.process.poll() is None:
            self.process.kill() if force else self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self._log:
            self._log.close()
            self._log = None

    def log_tail(self):
        try:
            return "\n".join(self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-60:])
        except OSError:
            return "<no process log>"


@pytest.fixture(scope="module")
def gateways(tmp_path_factory):
    root = tmp_path_factory.mktemp("multi-instance")
    first, second = GatewayProcess(_free_port(), root / "gateway-a.log"), GatewayProcess(_free_port(), root / "gateway-b.log")
    try:
        first.start()
        second.start()
        assert first.process.pid != second.process.pid
        yield first, second
    finally:
        first.stop()
        second.stop()


def _key(quota=200, budget=1000.0):
    raw, info = create_gateway_api_key(f"mi-{uuid.uuid4().hex}", quota=quota, daily_budget=budget)
    return raw, str(info["id"])


def _chat_payload(content=None):
    return {"model": "mock", "messages": [{"role": "user", "content": content or f"hello-{uuid.uuid4().hex[:8]}"}], "max_tokens": 3}


async def _post_chat(client, api_key, content=None):
    return await client.post("/v1/chat", headers={"Authorization": f"Bearer {api_key}"}, json=_chat_payload(content))


def _pg_row(sql, params=()):
    conn = db.sqlite3.connect("gateway.db", timeout=15)
    try:
        return conn.execute(sql, params).fetchone()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_budget_reservations_are_shared_and_atomic(gateways):
    first, second = gateways
    reports = []
    for attempt in range(3):
        api_key, key_id = _key(quota=100, budget=10.0)
        barrier = asyncio.Event()

        async def request(base):
            async with httpx.AsyncClient(base_url=base.url, timeout=30) as client:
                await barrier.wait()
                return await _post_chat(client, api_key, f"budget{attempt:06d}")

        tasks = [asyncio.create_task(request(first)), asyncio.create_task(request(second))]
        await asyncio.sleep(0.02)
        barrier.set()
        responses = await asyncio.gather(*tasks)
        codes = [response.status_code for response in responses]
        assert codes.count(200) <= 1, codes
        assert codes.count(429) >= 1, [(response.status_code, response.text) for response in responses]
        account = _pg_row("SELECT spent,reserved FROM budget_accounts WHERE key_id=? ORDER BY budget_date DESC LIMIT 1", (key_id,))
        assert account is not None and float(account[0]) + float(account[1]) <= 10.000001
        reports.append(codes)
    print("MULTI_INSTANCE_BUDGET", json.dumps({"two_pids": [first.process.pid, second.process.pid], "attempts": reports}))


@pytest.mark.asyncio
async def test_rate_limit_is_shared_across_instances(gateways):
    first, second = gateways
    api_key, _ = _key(quota=5, budget=10000.0)
    async def request(index):
        base = first if index % 2 == 0 else second
        async with httpx.AsyncClient(base_url=base.url, timeout=40) as client:
            return await _post_chat(client, api_key, f"rate-{index}-{uuid.uuid4().hex[:5]}")
    responses = await asyncio.gather(*(request(i) for i in range(8)))
    codes = [response.status_code for response in responses]
    assert codes.count(200) == 5, [(response.status_code, response.text) for response in responses]
    rejected = [response for response in responses if response.status_code == 429]
    assert len(rejected) == 3
    assert all(response.headers.get("Retry-After") for response in rejected)
    print("MULTI_INSTANCE_RATE_LIMIT", json.dumps({"accepted": codes.count(200), "rejected": len(rejected)}))


@pytest.mark.asyncio
async def test_breaker_state_and_half_open_probe_are_shared(gateways):
    first, second = gateways
    async with httpx.AsyncClient(base_url=first.url, headers=ADMIN) as client:
        await client.post("/api/reliability/circuit-breaker/reset")
        result = await client.post("/api/reliability/circuit-breaker/trip", params={"provider": "mock"})
        assert result.status_code == 200, result.text
    async with httpx.AsyncClient(base_url=second.url, headers=ADMIN) as client:
        status = (await client.get("/api/reliability/status")).json()["circuit_breakers"]
        assert status["mock"]["state"] == "OPEN", status
        await client.post("/api/reliability/circuit-breaker/reset")

    from app.reliability import get_model_circuit_breaker
    breaker = get_model_circuit_breaker("mock", "mock-multi-instance")
    client = redis.Redis.from_url(CIRCUIT_REDIS_URL, decode_responses=True)
    client.delete(breaker.key)
    client.hset(breaker.key, mapping={"state": "OPEN", "version": 0, "open_until_ms": 0, "failures": 2,
                                      "threshold": 2, "cooldown_ms": 60000, "total_requests": 0,
                                      "total_failures": 2, "total_successes": 0})
    client.expire(breaker.key, 120)
    for base in (first, second):
        async with httpx.AsyncClient(base_url=base.url, headers=ADMIN) as admin:
            changed = await admin.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 900})
            assert changed.status_code == 200, changed.text
    api_a, _ = _key(budget=1000.0)
    api_b, _ = _key(budget=1000.0)
    async def probe(base, key):
        async with httpx.AsyncClient(base_url=base.url, timeout=20) as session:
            return await _post_chat(session, key, f"probe-{uuid.uuid4().hex[:7]}")
    try:
        responses = await asyncio.gather(probe(first, api_a), probe(second, api_b))
        codes = [response.status_code for response in responses]
        assert codes.count(200) == 1 and codes.count(502) == 1, [(r.status_code, r.text) for r in responses]
        state = client.hgetall(breaker.key)
        assert state.get("state") in {"CLOSED", "HALF_OPEN"}, state
    finally:
        client.close()
        for base in (first, second):
            async with httpx.AsyncClient(base_url=base.url, headers=ADMIN) as session:
                await session.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 0})
        async with httpx.AsyncClient(base_url=first.url, headers=ADMIN) as session:
            await session.post("/api/reliability/circuit-breaker/reset")
    print("MULTI_INSTANCE_BREAKER", json.dumps({"probe_statuses": codes, "breaker_key": breaker.key}))


@pytest.mark.asyncio
async def test_killed_gateway_probe_lease_expires_and_is_reclaimed(gateways):
    first, second = gateways
    from app.reliability import get_model_circuit_breaker
    breaker = get_model_circuit_breaker("mock", "mock-multi-instance")
    redis_client = redis.Redis.from_url(CIRCUIT_REDIS_URL, decode_responses=True)
    redis_client.delete(breaker.key)
    redis_client.hset(breaker.key, mapping={"state": "OPEN", "version": 0, "open_until_ms": 0, "failures": 2,
                                            "threshold": 2, "cooldown_ms": 60000, "total_requests": 0,
                                            "total_failures": 2, "total_successes": 0})
    redis_client.expire(breaker.key, 120)
    api_key, _ = _key(budget=100000.0)
    async with httpx.AsyncClient(base_url=first.url, headers=ADMIN) as admin:
        await admin.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 12000})
    request_task = asyncio.create_task(
        httpx.AsyncClient(base_url=first.url, timeout=20).__aenter__()
    )
    session = await request_task
    pending = asyncio.create_task(_post_chat(session, api_key, "kill-half-open-probe"))
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            state = redis_client.hgetall(breaker.key)
            if state.get("state") == "HALF_OPEN" and state.get("probe_owner"):
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Gateway A never acquired the shared probe lease: {state}")
        lease_expiry = int(state["probe_expires_at_ms"])
        killed_pid = first.process.pid
        first.stop(force=True)
        try:
            await pending
        except httpx.HTTPError:
            pass
        while time.time() < lease_expiry / 1000 + 0.2:
            await asyncio.sleep(0.05)
        async with httpx.AsyncClient(base_url=second.url, timeout=25) as survivor:
            recovered = await _post_chat(survivor, api_key, "replacement-half-open-probe")
        assert recovered.status_code == 200, (recovered.status_code, recovered.text)
        state = redis_client.hgetall(breaker.key)
        assert int(state.get("expired_probe_leases", "0")) >= 1, state
        first.start()
        async with httpx.AsyncClient(base_url=first.url, headers=ADMIN) as admin:
            await admin.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 0})
            await admin.post("/api/reliability/circuit-breaker/reset")
        print("MULTI_INSTANCE_PROBE_CRASH", json.dumps({"killed_pid": killed_pid, "reclaimed": True,
                                                          "expired_probe_leases": state.get("expired_probe_leases")}))
    finally:
        if not pending.done():
            pending.cancel()
        await session.aclose()
        redis_client.close()
        if first.process and first.process.poll() is not None:
            first.start()


@pytest.mark.asyncio
async def test_worker_process_crash_recovers_database_lease(gateways):
    first, second = gateways
    api_key, key_id = _key(quota=500, budget=100000.0)
    for base in (first, second):
        async with httpx.AsyncClient(base_url=base.url, headers=ADMIN) as admin:
            changed = await admin.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 12000})
            assert changed.status_code == 200, changed.text
    async with httpx.AsyncClient(base_url=first.url, headers={"Authorization": f"Bearer {api_key}"}) as client:
        job_response = await client.post("/v1/jobs", json=_chat_payload("crash-recovery-single-job"))
    assert job_response.status_code == 200, job_response.text
    job_id = job_response.json()["id"]
    deadline = time.monotonic() + 20
    claimed = None
    victim = survivor = None
    while time.monotonic() < deadline:
        claimed = _pg_row("SELECT id,attempts,lease_owner FROM async_jobs WHERE id=? AND status='running'", (job_id,))
        if claimed:
            victim = first if f":{first.process.pid}:" in claimed[2] else second
            survivor = second if victim is first else first
            active_count = _pg_row("SELECT COUNT(*) FROM budget_reservations WHERE key_id=? AND status='active'", (key_id,))[0]
            if active_count:
                break
        await asyncio.sleep(0.1)
    assert claimed and victim and survivor, "Neither gateway claimed the job before the deadline"
    assert first.process.poll() is None and second.process.poll() is None
    assert active_count == 1
    # Expire this reservation early so the test exercises durable recovery
    # without waiting for the production 120-second budget lease.
    conn = db.sqlite3.connect("gateway.db", timeout=15)
    try:
        conn.execute("UPDATE budget_reservations SET expires_at=? WHERE key_id=? AND status='active'", (time.time() - 1, key_id))
        conn.commit()
    finally:
        conn.close()
    killed_pid = victim.process.pid
    victim.stop(force=True)
    deadline = time.monotonic() + 45
    final = None
    while time.monotonic() < deadline:
        final = _pg_row("SELECT status,attempts,lease_owner FROM async_jobs WHERE id=?", (job_id,))
        if final and final[0] in ("done", "dlq"):
            break
        await asyncio.sleep(0.25)
    assert final and final[0] == "done" and int(final[1]) == 2, final
    assert final[2] is None
    assert _pg_row("SELECT COUNT(*) FROM budget_reservations WHERE key_id=? AND status='active'", (key_id,))[0] == 0
    assert float(_pg_row("SELECT reserved FROM budget_accounts WHERE key_id=?", (key_id,))[0]) == pytest.approx(0.0)
    victim.start()
    for base in (first, second):
        async with httpx.AsyncClient(base_url=base.url, headers=ADMIN) as admin:
            await admin.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 0})
    print("MULTI_INSTANCE_JOB_CRASH", json.dumps({"killed_pid": killed_pid, "survivor_pid": survivor.process.pid,
                                                   "restarted_pid": victim.process.pid, "job_id": job_id,
                                                   "status": final[0], "attempts": final[1], "active_budget_reservations": 0}))


@pytest.mark.asyncio
async def test_two_independent_workers_claim_shared_jobs_and_idempotency(gateways):
    first, second = gateways
    api_key, _ = _key(quota=500, budget=100000.0)
    idem = f"shared-{uuid.uuid4().hex}"
    async def enqueue_same(base):
        async with httpx.AsyncClient(base_url=base.url, headers={"Authorization": f"Bearer {api_key}"}) as client:
            return await client.post("/v1/jobs", headers={"Idempotency-Key": idem}, json=_chat_payload("same-idempotent-job"))
    duplicates = await asyncio.gather(enqueue_same(first), enqueue_same(second))
    assert all(response.status_code == 200 for response in duplicates), [r.text for r in duplicates]
    assert duplicates[0].json()["id"] == duplicates[1].json()["id"]
    assert _pg_row("SELECT COUNT(*) FROM async_jobs WHERE idempotency_key=?", (idem,))[0] == 1
    async def enqueue_batch(base, start):
        async with httpx.AsyncClient(base_url=base.url, headers={"Authorization": f"Bearer {api_key}"}, timeout=20) as client:
            return await asyncio.gather(*(client.post("/v1/jobs", json=_chat_payload(f"worker-{start+i}")) for i in range(16)))
    batches = await asyncio.gather(enqueue_batch(first, 0), enqueue_batch(second, 16))
    assert all(response.status_code == 200 for batch in batches for response in batch)
    ids = [response.json()["id"] for batch in batches for response in batch]
    deadline = time.monotonic() + 45
    states = []
    while time.monotonic() < deadline:
        states = [_pg_row("SELECT status,attempts FROM async_jobs WHERE id=?", (job_id,)) for job_id in ids]
        if all(row and row[0] == "done" for row in states):
            break
        await asyncio.sleep(0.2)
    assert all(row and row[0] == "done" and int(row[1]) == 1 for row in states)
    time.sleep(0.1)  # allow logging handlers to flush worker claim records
    first_claimed = any("Job claimed" in line and f":{first.process.pid}:" in line for line in first.log_tail().splitlines())
    second_claimed = any("Job claimed" in line and f":{second.process.pid}:" in line for line in second.log_tail().splitlines())
    assert first_claimed and second_claimed, (first.log_tail(), second.log_tail())
    print("MULTI_INSTANCE_JOBS", json.dumps({"workers": [first.process.pid, second.process.pid],
                                             "independent_jobs": len(ids), "duplicate_idempotency_rows": 1,
                                             "duplicate_job_claims": 0}))


@pytest.mark.asyncio
async def test_concurrent_workloads_10_50_100_report_observed_metrics(gateways):
    first, second = gateways
    results = []
    for size in (10, 50, 100):
        api_key, _ = _key(quota=1000, budget=1000000.0)
        start = time.perf_counter()
        async def send(index):
            base = first if index % 2 == 0 else second
            async with httpx.AsyncClient(base_url=base.url, timeout=90) as session:
                return await _post_chat(session, api_key, f"load-{size}-{index}-{uuid.uuid4().hex[:5]}")
        maximum_active = 0
        sampling = True
        async def sample_concurrency():
            nonlocal maximum_active
            while sampling:
                sampled_instances = []
                for base in (first, second):
                    try:
                        async with httpx.AsyncClient(base_url=base.url, headers=ADMIN, timeout=2) as monitor:
                            data = (await monitor.get("/api/concurrency/status")).json()
                        sampled_instances.append(int(data["global"]["in_flight"]))
                    except (httpx.HTTPError, KeyError, ValueError):
                        continue
                maximum_active = max(maximum_active, sum(sampled_instances))
                await asyncio.sleep(0.01)
        sampler = asyncio.create_task(sample_concurrency())
        responses = await asyncio.gather(*(send(i) for i in range(size)))
        sampling = False
        await sampler
        elapsed = time.perf_counter() - start
        latencies = sorted(float(response.headers.get("X-Latency-Ms", "0")) for response in responses if response.status_code == 200)
        successes = sum(response.status_code == 200 for response in responses)
        results.append({"requests": size, "success": successes,
                        "rejected": sum(response.status_code == 429 for response in responses),
                        "failed": size - successes - sum(response.status_code == 429 for response in responses),
                        "throughput_rps": round(size / elapsed, 2),
                        "max_sampled_active_sum": maximum_active,
                        "p50_ms": latencies[int(.50 * (len(latencies) - 1))] if latencies else None,
                        "p95_ms": latencies[int(.95 * (len(latencies) - 1))] if latencies else None,
                        "p99_ms": latencies[int(.99 * (len(latencies) - 1))] if latencies else None})
        assert successes == size, [(r.status_code, r.text) for r in responses if r.status_code != 200]
    print("MULTI_INSTANCE_LOAD_REPORT", json.dumps({"pids": [first.process.pid, second.process.pid], "workloads": results,
                                                       "concurrency_note": "sum of process-local global in-flight gauges sampled every ~20ms; samples can miss peaks"}))


@pytest.mark.asyncio
async def test_redis_pause_is_fail_closed_and_limiter_recovers(gateways):
    first, second = gateways
    api_key, _ = _key(quota=100, budget=10000.0)
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        client.execute_command("CLIENT", "PAUSE", 1500, "ALL")
        async with httpx.AsyncClient(base_url=first.url, timeout=5) as session:
            response = await _post_chat(session, api_key, "redis-pause")
        assert response.status_code == 429, (response.status_code, response.text)
        assert response.headers.get("Retry-After")
    finally:
        client.close()
    await asyncio.sleep(10.5)  # limiter's documented Redis-failure retry cooldown
    async with httpx.AsyncClient(base_url=second.url, timeout=30) as session:
        recovered = await _post_chat(session, api_key, "redis-recovered")
    assert recovered.status_code == 200, (recovered.status_code, recovered.text)
