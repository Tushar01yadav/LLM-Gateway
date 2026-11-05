"""
Phase 4.3 chaos under concurrent load.

Each scenario runs the same continuous, authenticated load (alias 'mock') while a
fault is injected mid-run and healed. Metrics are computed per window:
success rate, p50/p95/p99, errors by status, and time-to-recover.

Scenarios:
  1. provider_death      mock provider returns 503 (failure injection) mid-load
  2. provider_latency    mock latency 500 ms -> 5000 ms mid-load, then back
  3. redis_kill          the llm-gateway-redis Docker container is killed and restarted
  4. worker_kill         2 workers, 12 async jobs + HTTP load, one worker is hard-killed

Honest output: no tuning to hide degradation. Writes docs/benchmarks/phase4-chaos.json.
"""
import asyncio
import json
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psutil
import redis as redis_sync

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

REDIS_DB = 4
REDIS_URL = f"redis://127.0.0.1:6379/{REDIS_DB}"
ADMIN = "phase4-chaos-admin-token"
PORTS = (8051, 8052)
REDIS_CONTAINER = "llm-gateway-redis"
LATENCY_FILE = ROOT / "scratch" / "mock_latency_ms.txt"
OUT = ROOT / "docs" / "benchmarks" / "phase4-chaos.json"
LOG_DIR = ROOT / "scratch"
LEASE_KEY = "gw:jobs:leases"

scenarios = []


def set_latency(ms):
    LATENCY_FILE.write_text(str(ms), encoding="utf-8")


def wait_health(port, timeout=40.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def start_worker(port):
    env = dict(os.environ)
    env.update({
        "GATEWAY_ADMIN_TOKEN": ADMIN,
        "APP_ENV": "development",
        "REDIS_URL": REDIS_URL,
        "GATEWAY_MOCK_LATENCY_FILE": str(LATENCY_FILE),
    })
    log = open(LOG_DIR / f"chaos-worker-{port}.log", "w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--no-proxy-headers"],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    proc._log = log
    if not wait_health(port):
        raise RuntimeError(f"worker {port} did not start")
    return proc


def stop_worker(proc):
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=15)
    except Exception:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
    finally:
        try:
            proc._log.close()
        except Exception:
            pass


def inject(port, enabled, mode="503"):
    try:
        httpx.post(f"http://127.0.0.1:{port}/api/reliability/inject-failure",
                   json={"provider": "mock", "enabled": enabled, "mode": mode},
                   headers={"X-Admin-Token": ADMIN}, timeout=10)
    except Exception:
        pass


def classify(status, detail=""):
    if status == 200:
        return "served"
    if status == 429:
        return "rate_limit_429" if "rate limit" in detail.lower() else "load_shed_429"
    if status is None:
        return "timeout_or_conn_error"
    if status >= 500:
        return "error_5xx"
    return "other"


def percentiles(values):
    vals = sorted(values)
    if not vals:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    def pct(p):
        rank = max(1, min(len(vals), math.ceil(p / 100.0 * len(vals))))
        return vals[rank - 1]
    return {"count": len(vals), "p50": pct(50), "p95": pct(95), "p99": pct(99), "max": vals[-1]}


def summarize_records(records, window_start, window_end):
    """Start-time windows: which requests the caller began in this period."""
    window = [r for r in records if window_start <= r["started"] <= window_end]
    buckets = {}
    for r in window:
        buckets[r["class"]] = buckets.get(r["class"], 0) + 1
    total = len(window) or 1
    served_lat = [r["latency_ms"] for r in window if r["class"] == "served"]
    return {
        "requests": len(window),
        "success_rate": round(buckets.get("served", 0) / total, 4) if window else None,
        "by_class": buckets,
        "served_latency_ms": percentiles(served_lat),
        "all_latency_ms": percentiles([r["latency_ms"] for r in window]),
    }


async def run_load_with_fault(port, raw_key, total, concurrency, fault_after_s, fault_duration_s,
                              apply_fault, heal_fault, mode_label):
    """
    Continuous load; apply_fault() runs once at fault_after_s, heal_fault() after
    fault_duration_s. Records wall-clock start/end for window attribution.
    """
    sem = asyncio.Semaphore(concurrency)
    records = []
    loop = asyncio.get_running_loop()

    async def one(client, index):
        payload = {
            "alias": "mock",
            "messages": [{"role": "user", "content": f"chaos {mode_label} request {index}"}],
            "temperature": 0.7,
            "max_tokens": 10,
            "session_id": f"chaos-session-{mode_label}-{index}",
        }
        async with sem:
            started = time.time()
            t0 = time.perf_counter()
            status = None
            detail = ""
            try:
                res = await client.post(f"http://127.0.0.1:{port}/v1/chat", json=payload,
                                        headers={"Authorization": f"Bearer {raw_key}"}, timeout=60.0)
                status = res.status_code
                try:
                    detail = str(res.json().get("detail", ""))
                except Exception:
                    detail = res.text[:200]
            except Exception as exc:
                detail = type(exc).__name__
            ended = time.time()
            records.append({
                "started": started, "ended": ended,
                "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                "status": status, "class": classify(status, detail),
            })

    async def scheduler():
        await asyncio.sleep(fault_after_s)
        fault_start = time.time()
        if asyncio.iscoroutinefunction(apply_fault):
            await apply_fault()
        else:
            apply_fault()
        await asyncio.sleep(fault_duration_s)
        fault_end = time.time()
        if asyncio.iscoroutinefunction(heal_fault):
            await heal_fault()
        else:
            heal_fault()
        return fault_start, fault_end

    async with httpx.AsyncClient() as client:
        sched = asyncio.create_task(scheduler())
        await asyncio.gather(*(one(client, i) for i in range(total)))
        fault_start, fault_end = await sched

    # Time-to-recover: from heal until a whole 1s bucket started after heal is >=90% served,
    # and every later bucket stays >=90%.
    recovery_seconds = None
    post = sorted([r for r in records if r["started"] >= fault_end], key=lambda r: r["started"])
    if post:
        bucket = {}
        for r in post:
            b = int(r["started"] - fault_end)
            bucket.setdefault(b, []).append(r["class"] == "served")
        max_bucket = max(bucket)
        last_full = int(post[-1]["started"] - fault_end) - 1
        for b in range(0, last_full + 1):
            vals = bucket.get(b, [])
            if vals and sum(vals) / len(vals) >= 0.9:
                tail = [v for bb, vs in bucket.items() if bb >= b for v in vs]
                if tail and sum(tail) / len(tail) >= 0.9:
                    recovery_seconds = b
                    break
    return {
        "mode": mode_label,
        "fault_window_s": round(fault_end - fault_start, 2),
        "window_basis": "request start time",
        "pre_fault": summarize_records(records, 0, fault_start),
        "during_fault": summarize_records(records, fault_start, fault_end),
        "post_fault": summarize_records(records, fault_end, float("inf")),
        "time_to_recover_s": recovery_seconds,
        "total_requests": len(records),
        "error_detail_sample": next((r for r in records if r["class"] != "served"), None),
    }


def ensure_redis_up(timeout=45.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            redis_sync.Redis.from_url(REDIS_URL, socket_connect_timeout=1, socket_timeout=1).ping()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def docker(cmd, timeout=60):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def breaker_state(port):
    try:
        res = httpx.get(f"http://127.0.0.1:{port}/api/reliability/status",
                        headers={"X-Admin-Token": ADMIN}, timeout=5)
        cb = res.json().get("circuit_breakers", {}).get("mock", {})
        return {"state": cb.get("state"), "consecutive_failures": cb.get("consecutive_failures"),
                "cooldown_remaining_s": cb.get("cooldown_remaining_sec")}
    except Exception as exc:
        return {"error": type(exc).__name__}


async def scenario_provider_fault(port, key, name, fault_after, fault_dur):
    set_latency(500)
    for p in PORTS:
        inject(p, False)
    if name == "provider_death":
        apply_fault = lambda: [inject(p, True, "503") for p in PORTS]
        heal_fault = lambda: [inject(p, False) for p in PORTS]
        description = "mock provider returns 503 mid-load"
    else:
        apply_fault = lambda: set_latency(5000)
        heal_fault = lambda: set_latency(500)
        description = "mock latency raised 500 ms -> 5000 ms mid-load"

    started = time.time()
    result = await run_load_with_fault(
        port, key, total=300, concurrency=30,
        fault_after_s=fault_after, fault_duration_s=fault_dur,
        apply_fault=apply_fault, heal_fault=heal_fault, mode_label=name,
    )
    result["scenario"] = name
    result["description"] = description
    result["circuit_breaker_after_run"] = {str(p): breaker_state(p) for p in PORTS}
    result["run_seconds"] = round(time.time() - started, 2)
    scenarios.append(result)
    print(f"[chaos] {name}: during_fault={result['during_fault']['by_class']} "
          f"success={result['during_fault']['success_rate']} recover={result['time_to_recover_s']}s "
          f"cb={result['circuit_breaker_after_run']}")


async def scenario_redis_kill(port, key):
    if not ensure_redis_up():
        scenarios.append({"scenario": "redis_kill", "skipped": "redis unavailable before test"})
        return
    started = time.time()
    outage = {"kill_returncode": None, "kill_stderr": "", "ping_failures": 0,
              "first_ping_fail_s": None, "ping_restored_s": None}
    monitor_stop = asyncio.Event()

    async def kill_redis():
        proc = docker(["docker", "kill", REDIS_CONTAINER])
        outage["kill_returncode"] = proc.returncode
        outage["kill_stderr"] = (proc.stderr or "").strip()[:300]

    async def restore_redis():
        docker(["docker", "start", REDIS_CONTAINER])
        await asyncio.to_thread(lambda: _wait_up(30.0))

    async def monitor():
        while not monitor_stop.is_set():
            ok = await asyncio.to_thread(_ping)
            now = time.time()
            if not ok:
                outage["ping_failures"] += 1
                if outage["first_ping_fail_s"] is None:
                    outage["first_ping_fail_s"] = round(now - started, 2)
            elif outage["first_ping_fail_s"] is not None and outage["ping_restored_s"] is None:
                outage["ping_restored_s"] = round(now - started, 2)
            await asyncio.sleep(0.25)

    async def run():
        return await run_load_with_fault(
            port, key, total=300, concurrency=30,
            fault_after_s=8.0, fault_duration_s=5.0,
            apply_fault=kill_redis, heal_fault=restore_redis, mode_label="redis_kill",
        )

    mon = asyncio.create_task(monitor())
    try:
        result = await run()
    finally:
        monitor_stop.set()
        await mon
    result["scenario"] = "redis_kill"
    result["description"] = "llm-gateway-redis container killed (docker kill) and restarted mid-load"
    result["redis_outage"] = outage
    result["run_seconds"] = round(time.time() - started, 2)
    scenarios.append(result)
    print(f"[chaos] redis_kill: during_fault={result['during_fault']['by_class']} "
          f"success={result['during_fault']['success_rate']} recover={result['time_to_recover_s']}s "
          f"redis_failures={outage['ping_failures']}")


def _ping():
    try:
        return bool(redis_sync.Redis.from_url(REDIS_URL, socket_connect_timeout=0.5, socket_timeout=0.5).ping())
    except Exception:
        return False


def _wait_up(timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            redis_sync.Redis.from_url(REDIS_URL, socket_connect_timeout=1, socket_timeout=1).ping()
            return True
        except Exception:
            time.sleep(0.3)
    return False


async def scenario_worker_kill(port, job_key, job_key_id, http_key):
    """12 jobs on job_key + HTTP load on both ports; kill the worker owning a lease."""
    set_latency(3000)
    for p in PORTS:
        inject(p, False)
    started = time.time()

    import sqlite3
    db = ROOT / "gateway.db"
    conn = sqlite3.connect(db, timeout=15)
    rows_before = conn.execute("SELECT COUNT(*) FROM cost_records WHERE key_id = ?", (str(job_key_id),)).fetchone()[0]
    conn.close()

    def submit_jobs():
        ids = []
        with httpx.Client(timeout=20.0) as client:
            for i in range(12):
                res = client.post(
                    f"http://127.0.0.1:{port}/v1/jobs",
                    json={"alias": "mock", "messages": [{"role": "user", "content": f"chaos job {i}"}],
                          "max_tokens": 10, "max_attempts": 3},
                    headers={"Authorization": f"Bearer {job_key}"},
                )
                ids.append(res.json()["id"])
        return ids

    job_ids = await asyncio.to_thread(submit_jobs)
    r = redis_sync.Redis.from_url(REDIS_URL, socket_connect_timeout=1, socket_timeout=3, decode_responses=True)

    async def http_load():
        # 30 requests split across both ports, so killing one worker does not
        # hide the survivor's behaviour.
        async with httpx.AsyncClient() as client:
            sem = asyncio.Semaphore(10)
            async def one(i):
                target_port = PORTS[i % len(PORTS)]
                async with sem:
                    try:
                        res = await client.post(
                            f"http://127.0.0.1:{target_port}/v1/chat",
                            json={"alias": "mock", "messages": [{"role": "user", "content": f"chaos worker http {i}"}]},
                            headers={"Authorization": f"Bearer {http_key}"}, timeout=60.0)
                        return target_port, classify(res.status_code)
                    except Exception:
                        return target_port, "timeout_or_conn_error"
            return await asyncio.gather(*(one(i) for i in range(30)))

    load_task = asyncio.create_task(http_load())

    # Wait until at least one job is leased, then kill that worker process.
    victim_pid = None
    deadline = time.time() + 30
    while time.time() < deadline and victim_pid is None:
        leases = r.hgetall(LEASE_KEY)
        if leases:
            victim_pid = int(json.loads(next(iter(leases.values())))["owner"].split(":")[1])
            break
        time.sleep(0.1)
    kill_ts = time.time()
    killed = False
    if victim_pid:
        try:
            psutil.Process(victim_pid).kill()
            killed = True
        except Exception:
            pass

    # Wait for all jobs to become terminal.
    terminal = {}
    deadline = time.time() + 150
    while time.time() < deadline:
        conn = sqlite3.connect(db, timeout=15)
        rows = conn.execute(
            f"SELECT id, status, attempts FROM async_jobs WHERE id IN ({','.join('?' * len(job_ids))})",
            job_ids,
        ).fetchall()
        conn.close()
        terminal = {row[0]: {"status": row[1], "attempts": row[2]} for row in rows}
        if len(terminal) == len(job_ids) and all(v["status"] in ("done", "dlq") for v in terminal.values()):
            break
        time.sleep(1.0)
    done_at = time.time()

    http_pairs = await load_task
    http_counts = {}
    http_by_port = {str(p): {} for p in PORTS}
    for target_port, cls in http_pairs:
        http_counts[cls] = http_counts.get(cls, 0) + 1
        http_by_port[str(target_port)][cls] = http_by_port[str(target_port)].get(cls, 0) + 1

    conn = sqlite3.connect(db, timeout=15)
    rows_after = conn.execute("SELECT COUNT(*) FROM cost_records WHERE key_id = ?", (str(job_key_id),)).fetchone()[0]
    conn.close()

    result = {
        "scenario": "worker_kill",
        "description": "hard-kill (TerminateProcess) the worker owning a lease while 12 jobs and HTTP load run",
        "worker_killed_pid": victim_pid,
        "killed": killed,
        "jobs": {
            "submitted": len(job_ids),
            "done": sum(1 for v in terminal.values() if v["status"] == "done"),
            "dlq": sum(1 for v in terminal.values() if v["status"] == "dlq"),
            "attempts": {jid: v["attempts"] for jid, v in terminal.items()},
            "all_terminal": len(terminal) == len(job_ids) and all(
                v["status"] in ("done", "dlq") for v in terminal.values()),
        },
        "http_load": http_counts,
        "http_load_by_port": http_by_port,
        "time_to_recover_s": round(done_at - kill_ts, 2),
        "job_cost_rows_delta": rows_after - rows_before,
        "run_seconds": round(time.time() - started, 2),
    }
    scenarios.append(result)
    print(f"[chaos] worker_kill: killed={killed} jobs_done={result['jobs']['done']}/{len(job_ids)} "
          f"http={http_counts} by_port={http_by_port} recover={result['time_to_recover_s']}s "
          f"cost_rows={result['job_cost_rows_delta']}")


async def main():
    set_latency(500)
    r = redis_sync.Redis.from_url(REDIS_URL, socket_connect_timeout=2, socket_timeout=5)
    r.flushdb()
    if not ensure_redis_up(10):
        raise SystemExit("Redis is required for chaos tests")

    os.environ["REDIS_URL"] = REDIS_URL
    os.environ["GATEWAY_ADMIN_TOKEN"] = ADMIN
    from app.security import create_gateway_api_key

    raw_key, info = create_gateway_api_key("phase4-chaos-client", quota=100000, daily_budget=1000.0)
    job_key, job_info = create_gateway_api_key("phase4-chaos-jobs", quota=100000, daily_budget=1000.0)
    http_key, http_info = create_gateway_api_key("phase4-chaos-http", quota=100000, daily_budget=1000.0)

    conn = __import__("sqlite3").connect(ROOT / "gateway.db", timeout=15)
    conn.execute("DELETE FROM async_jobs WHERE key_name IN ('phase4-chaos-client', 'phase4-chaos-jobs', 'phase4-chaos-http')")
    conn.execute("DELETE FROM cost_records WHERE key_id IN (?, ?, ?)",
                 (str(info["id"]), str(job_info["id"]), str(http_info["id"])))
    conn.commit()
    conn.close()

    workers = [start_worker(p) for p in PORTS]
    try:
        await scenario_provider_fault(PORTS[0], raw_key, "provider_death", 8.0, 5.0)
        await asyncio.sleep(1)
        await scenario_provider_fault(PORTS[0], raw_key, "provider_latency", 8.0, 5.0)
        set_latency(500)
        for p in PORTS:
            inject(p, False)
        await asyncio.sleep(1)
        await scenario_redis_kill(PORTS[0], raw_key)
        await asyncio.sleep(1)
        await scenario_worker_kill(PORTS[0], job_key, job_info["id"], http_key)
    finally:
        for w in workers:
            stop_worker(w)
        set_latency(500)
        try:
            r.flushdb()
        except Exception:
            pass

    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "redis_db": REDIS_DB,
            "redis_container": REDIS_CONTAINER,
            "workers": len(PORTS),
            "ports": PORTS,
            "mock_latency_ms_default": 500,
            "load": {"total_requests": 300, "concurrency": 30, "fault_after_s": 8, "fault_duration_s": 5},
        },
        "scenarios": scenarios,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
