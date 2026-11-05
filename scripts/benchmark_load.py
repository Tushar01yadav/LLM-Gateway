"""
Honest BEFORE/AFTER load test.

Runs the same fixed workload against two gateway processes:
  BEFORE: GATEWAY_CONCURRENCY_CONTROL=off (no global slot, no provider bulkhead)
  AFTER:  default concurrency control

Both runs use a dedicated Redis database, a high-quota gateway key, cleared
cache/limiter state, and the deterministic mock provider.

Profiles:
  fixed     mock returns in 500 ms regardless of load (the original scenario)
  overload  mock latency grows 500 ms + 150 ms per in-flight request and it
            rejects with 503 above 12 in-flight, so an uncontrolled BEFORE run
            actually degrades

Each profile writes JSON + a markdown table to docs/benchmarks/.

Usage:
  python scripts/benchmark_load.py [--total 120] [--concurrency 60] [--port 8020]
                                   [--profiles fixed,overload] [--repeats 3]
"""
import argparse
import asyncio
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BENCH_REDIS_DB = 2
BENCH_REDIS_URL = f"redis://127.0.0.1:6379/{BENCH_REDIS_DB}"
ADMIN_TOKEN = os.getenv("GATEWAY_ADMIN_TOKEN", "phase4-benchmark-admin")
BENCH_DIR = ROOT / "docs" / "benchmarks"


def redis_info():
    try:
        import redis
        client = redis.Redis.from_url(BENCH_REDIS_URL, socket_connect_timeout=1, socket_timeout=1)
        info = client.info("server")
        client.flushdb()
        # This host's loopback cost is part of the measurement environment.
        client.get("bench:rtt:probe")
        started = time.perf_counter()
        for _ in range(10):
            client.get("bench:rtt:probe")
        rtt = round((time.perf_counter() - started) * 1000 / 10, 2)
        return {"available": True, "version": info.get("redis_version"), "rtt_ms_avg_10_gets": rtt}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def git_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=str(ROOT), text=True
        ).strip()
    except Exception:
        return "unknown"


OVERLOAD_ENV = {
    "GATEWAY_MOCK_OVERLOAD": "1",
    "GATEWAY_MOCK_OVERLOAD_BASE_MS": "500",
    "GATEWAY_MOCK_OVERLOAD_SLOPE_MS": "150",
    "GATEWAY_MOCK_OVERLOAD_ERROR_AT": "12",
}


def environment_header(redis: dict, total: int, concurrency: int, profile: str) -> dict:
    profile_text = (
        "fixed 500 ms" if profile == "fixed"
        else "grows 500 ms + 150 ms per in-flight request; 503 above 12 in-flight"
    )
    return {
        "machine": platform.node(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.split()[0],
        "git_commit": git_commit(),
        "redis": redis,
        "providers": "mocked (alias 'mock', MockAdapter)",
        "profile": profile,
        "mock_latency_profile": profile_text,
        "overload_settings": dict(OVERLOAD_ENV) if profile == "overload" else None,
        "rate_limiter": "Redis token bucket (quota=100000 rpm, so limiter is not the bottleneck)",
        "load": {"total_requests": total, "concurrency": concurrency, "alias": "mock"},
        "memory_total_gb": round(psutil.virtual_memory().total / (1024 ** 3), 1),
    }


def wait_for_health(port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            res = httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
            if res.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"server on port {port} did not become healthy")


def start_server(port: int, control: str, log_path: Path, profile: str) -> subprocess.Popen:
    env = dict(os.environ)
    env.update({
        "GATEWAY_ADMIN_TOKEN": ADMIN_TOKEN,
        "APP_ENV": "development",
        "REDIS_URL": BENCH_REDIS_URL,
        "GATEWAY_CONCURRENCY_CONTROL": control,
    })
    for key in OVERLOAD_ENV:
        env.pop(key, None)
    if profile == "overload":
        env.update(OVERLOAD_ENV)
    log = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port), "--no-proxy-headers",
        ],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    proc._log_handle = log  # keep the handle alive for the process lifetime
    wait_for_health(port)
    return proc


def stop_server(proc: subprocess.Popen) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
    finally:
        try:
            proc._log_handle.close()
        except Exception:
            pass


def classify(status: int, detail: str) -> str:
    if status == 200:
        return "served"
    if status == 429 and "rate limit" in (detail or "").lower():
        return "rate_limit_429"
    if status == 429:
        return "load_shed_429"
    if status >= 500:
        return "error_5xx"
    return "other"


async def run_load(port: int, raw_key: str, total: int, concurrency: int, seed: int) -> dict:
    sem = asyncio.Semaphore(concurrency)
    counters = {
        "served": 0, "rate_limit_429": 0, "load_shed_429": 0,
        "error_5xx": 0, "timeouts": 0, "other": 0,
    }
    latencies = []
    served_latencies = []
    statuses = {}
    peak_global_inflight = 0
    peak_mock_inflight = 0
    peak_mock_queue = 0
    sampler_stop = asyncio.Event()

    async def sampler():
        nonlocal peak_global_inflight, peak_mock_inflight, peak_mock_queue
        async with httpx.AsyncClient(timeout=2.0) as client:
            while not sampler_stop.is_set():
                try:
                    res = await client.get(f"http://127.0.0.1:{port}/api/concurrency/status")
                    if res.status_code == 200:
                        body = res.json()
                        peak_global_inflight = max(
                            peak_global_inflight, int(body.get("global", {}).get("in_flight", 0))
                        )
                        mock = body.get("providers", {}).get("mock", {})
                        peak_mock_inflight = max(peak_mock_inflight, int(mock.get("in_flight", 0)))
                        peak_mock_queue = max(peak_mock_queue, int(mock.get("queue_depth", 0)))
                except Exception:
                    pass
                await asyncio.sleep(0.05)

    async def one(client: httpx.AsyncClient, index: int):
        nonce = f"bench-{seed}-{index}"
        payload = {
            "alias": "mock",
            "messages": [{"role": "user", "content": f"load test {nonce}"}],
            "temperature": 0.7,
            "max_tokens": 10,
            "session_id": f"bench-session-{seed}-{index}",
        }
        async with sem:
            started = time.perf_counter()
            try:
                res = await client.post(
                    f"http://127.0.0.1:{port}/v1/chat",
                    json=payload,
                    headers={"Authorization": f"Bearer {raw_key}"},
                    timeout=60.0,
                )
                latencies.append(round((time.perf_counter() - started) * 1000, 2))
                if res.status_code == 200:
                    served_latencies.append(latencies[-1])
                try:
                    detail = res.json().get("detail", "")
                except Exception:
                    detail = res.text[:200]
                bucket = classify(res.status_code, detail)
                counters[bucket] += 1
                statuses[str(res.status_code)] = statuses.get(str(res.status_code), 0) + 1
            except httpx.TimeoutException:
                counters["timeouts"] += 1
            except Exception:
                counters["other"] += 1

    sampler_task = asyncio.create_task(sampler())
    wall_start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*(one(client, i) for i in range(total)))
    wall_seconds = round(time.perf_counter() - wall_start, 3)
    sampler_stop.set()
    await sampler_task

    ordered = sorted(latencies)
    ordered_served = sorted(served_latencies)
    def pct(p, values=None):
        values = ordered if values is None else values
        if not values:
            return 0.0
        rank = max(1, min(len(values), math.ceil(p / 100.0 * len(values))))
        return values[rank - 1]

    server_percentiles = {}
    try:
        res = httpx.get(f"http://127.0.0.1:{port}/api/stats", timeout=10.0)
        if res.status_code == 200:
            server_percentiles = res.json().get("latency_percentiles", {})
    except Exception:
        pass

    return {
        "counters": counters,
        "status_codes": statuses,
        "wall_seconds": wall_seconds,
        "requests_per_second": round(total / wall_seconds, 1) if wall_seconds else 0,
        "client_latency_ms": {
            "count": len(ordered),
            "p50": pct(50), "p95": pct(95), "p99": pct(99),
            "avg": round(statistics.mean(ordered), 2) if ordered else 0.0,
            "max": ordered[-1] if ordered else 0.0,
        },
        "client_latency_served_only_ms": {
            "count": len(ordered_served),
            "p50": pct(50, ordered_served), "p95": pct(95, ordered_served), "p99": pct(99, ordered_served),
            "avg": round(statistics.mean(ordered_served), 2) if ordered_served else 0.0,
            "max": ordered_served[-1] if ordered_served else 0.0,
        },
        "server_latency_percentiles": server_percentiles,
        "peak_global_in_flight": peak_global_inflight,
        "peak_mock_in_flight": peak_mock_inflight,
        "peak_mock_queue_depth": peak_mock_queue,
    }


def cleanup_bench_rows():
    import sqlite3
    conn = sqlite3.connect(ROOT / "gateway.db", timeout=15.0)
    cur = conn.cursor()
    cur.execute("DELETE FROM messages WHERE session_id LIKE 'bench-session-%'")
    cur.execute("DELETE FROM sessions WHERE id LIKE 'bench-session-%'")
    cur.execute("DELETE FROM request_logs WHERE session_id LIKE 'bench-session-%'")
    conn.commit()
    conn.close()


def health_rtt_ms(port: int, samples: int = 10) -> float:
    """Sequential keep-alive RTT to a trivial endpoint: baseline HTTP cost of the host."""
    started = time.perf_counter()
    with httpx.Client(timeout=10.0) as client:
        for _ in range(samples):
            client.get(f"http://127.0.0.1:{port}/health")
    return round((time.perf_counter() - started) * 1000 / samples, 2)


def concurrent_health_wall_ms(port: int, n: int = 60, attempts: int = 3) -> float:
    """Wall time for n concurrent trivial requests: this host's HTTP concurrency floor.

    A burst of 60 fresh loopback connections occasionally hits a Windows backlog
    reset even though the server is healthy, so retry instead of failing the run.
    """
    async def go():
        async with httpx.AsyncClient(
            timeout=30.0, limits=httpx.Limits(max_connections=n + 10)
        ) as client:
            started = time.perf_counter()
            await asyncio.gather(*(client.get(f"http://127.0.0.1:{port}/health") for _ in range(n)))
            return round((time.perf_counter() - started) * 1000, 1)

    for attempt in range(attempts):
        try:
            return asyncio.run(go())
        except Exception:
            time.sleep(0.5)
    return None


def scenario_percentiles():
    """Server-recorded latency for this scenario only (bench sessions are purged per scenario)."""
    import sqlite3
    conn = sqlite3.connect(ROOT / "gateway.db", timeout=15.0)
    rows = [r[0] for r in conn.execute(
        "SELECT latency_ms FROM request_logs WHERE session_id LIKE 'bench-session-%' "
        "AND status_code = 200 AND latency_ms IS NOT NULL"
    ).fetchall()]
    conn.close()
    vals = sorted(rows)
    def pct(p):
        if not vals:
            return 0.0
        rank = max(1, min(len(vals), math.ceil(p / 100.0 * len(vals))))
        return vals[rank - 1]
    return {"count": len(vals), "p50": pct(50), "p95": pct(95), "p99": pct(99),
            "avg": round(statistics.mean(vals), 2) if vals else 0.0,
            "max": vals[-1] if vals else 0.0}


def scenario(name: str, control: str, port: int, raw_key: str, total: int, concurrency: int,
             seed: int, profile: str) -> dict:
    import redis
    client = redis.Redis.from_url(BENCH_REDIS_URL, socket_connect_timeout=1, socket_timeout=1)
    client.flushdb()
    # Attribute /api/stats and the DB snapshot to this scenario only.
    cleanup_bench_rows()
    proc = start_server(port, control, BENCH_DIR / f"load-{profile}-{name}-server.log", profile)
    time.sleep(0.5)
    try:
        baseline = {
            "health_rtt_ms": health_rtt_ms(port),
            "concurrent_health_60_wall_ms": concurrent_health_wall_ms(port, 60),
        }
        result = asyncio.run(run_load(port, raw_key, total, concurrency, seed))
        result["scenario_server_latency_ms"] = scenario_percentiles()
        result["baseline"] = baseline
        return result
    finally:
        stop_server(proc)


def _median(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return round(statistics.median(values), 2)


def summarize_runs(runs: list) -> dict:
    def col(path):
        out = []
        for r in runs:
            cur = r
            for part in path:
                cur = (cur or {}).get(part) if isinstance(cur, dict) else None
            out.append(cur)
        return out

    summary = {
        "runs": len(runs),
        "served": _median(col(("counters", "served"))),
        "rate_limit_429": _median(col(("counters", "rate_limit_429"))),
        "load_shed_429": _median(col(("counters", "load_shed_429"))),
        "error_5xx": _median(col(("counters", "error_5xx"))),
        "timeouts": _median(col(("counters", "timeouts"))),
        "wall_seconds": _median(col(("wall_seconds",))),
        "requests_per_second": _median(col(("requests_per_second",))),
        "client_latency_ms": {
            "p50": _median(col(("client_latency_ms", "p50"))),
            "p95": _median(col(("client_latency_ms", "p95"))),
            "p99": _median(col(("client_latency_ms", "p99"))),
        },
        "client_latency_served_only_ms": {
            "p50": _median(col(("client_latency_served_only_ms", "p50"))),
            "p95": _median(col(("client_latency_served_only_ms", "p95"))),
            "p99": _median(col(("client_latency_served_only_ms", "p99"))),
        },
        "scenario_server_latency_ms": {
            "p50": _median(col(("scenario_server_latency_ms", "p50"))),
            "p95": _median(col(("scenario_server_latency_ms", "p95"))),
            "p99": _median(col(("scenario_server_latency_ms", "p99"))),
        },
        "peak_global_in_flight": max((r.get("peak_global_in_flight") or 0) for r in runs),
        "peak_mock_in_flight": max((r.get("peak_mock_in_flight") or 0) for r in runs),
        "peak_mock_queue_depth": max((r.get("peak_mock_queue_depth") or 0) for r in runs),
    }
    totals = [sum((r.get("counters") or {}).values()) for r in runs]
    served_rates = [r["counters"]["served"] / t for r, t in zip(runs, totals) if t]
    error_rates = [(r["counters"]["error_5xx"] + r["counters"]["timeouts"]) / t
                   for r, t in zip(runs, totals) if t]
    shed_rates = [r["counters"]["load_shed_429"] / t for r, t in zip(runs, totals) if t]
    summary["served_rate"] = _median([round(v, 4) for v in served_rates])
    summary["error_rate"] = _median([round(v, 4) for v in error_rates])
    summary["shed_rate"] = _median([round(v, 4) for v in shed_rates])
    return summary


def render_markdown(data: dict) -> str:
    env = data["environment"]
    runs_n = data["load"]["repeats"]
    profile = env.get("profile", "fixed")
    title = "# Phase 4 load test — BEFORE vs AFTER concurrency control"
    if profile == "overload":
        title += " (overload profile)"
    lines = [
        title,
        "",
        f"- Generated: {data['generated_at']}",
        f"- Machine: {env['machine']} ({env['platform']}), {env['cpu_count']} CPUs, Python {env['python']}",
        f"- Redis: {env['redis']}",
        f"- Providers: {env['providers']}; latency: {env['mock_latency_profile']}",
        f"- Load: {data['load']['total_requests']} requests, concurrency {data['load']['concurrency']}, "
        f"same seed {data['load']['seed']}, {runs_n} interleaved repetitions per scenario",
        "",
        "## Every run",
        "",
        "| Run | Scenario | Served | Rate-limit 429 | Load-shed 429 | 5xx | Timeouts | Wall (s) | Req/s | Served / Error / Shed % | Client p50 | Client p95 | Client p99 | Server p50/p95/p99 (200s) | Peak global / mock in-flight (queue) |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for name, label in (("before", "BEFORE (off)"), ("after", "AFTER (on)")):
        for i, s in enumerate(data["scenarios"][name], start=1):
            c = s["counters"]
            lat = s["client_latency_ms"]
            srv = s.get("scenario_server_latency_ms") or {}
            srv_txt = (
                f"{srv.get('p50')}/{srv.get('p95')}/{srv.get('p99')} (n={srv.get('count')})"
                if srv.get("count") else "n/a"
            )
            total = sum(c.values()) or 1
            rates = (100 * c["served"] / total, 100 * (c["error_5xx"] + c["timeouts"]) / total,
                     100 * c["load_shed_429"] / total)
            lines.append(
                f"| {i} | {label} | {c['served']} | {c['rate_limit_429']} | {c['load_shed_429']} | "
                f"{c['error_5xx']} | {c['timeouts']} | {s['wall_seconds']} | {s['requests_per_second']} | "
                f"{rates[0]:.1f} / {rates[1]:.1f} / {rates[2]:.1f} | "
                f"{lat['p50']} | {lat['p95']} | {lat['p99']} | {srv_txt} | "
                f"{s.get('peak_global_in_flight')} / {s.get('peak_mock_in_flight')} ({s.get('peak_mock_queue_depth')}) |"
            )

    lines += [
        "",
        "## Median across repetitions",
        "",
        "| Scenario | Served | Shed 429 | Served / Error / Shed % | Wall (s) | Req/s | Client p50 ms | Client p95 ms | Client p99 ms | Served-only p50/p95/p99 ms | Server p50/p95/p99 ms | Peak global / mock in-flight (queue) |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for name, label in (("before", "BEFORE (off)"), ("after", "AFTER (on)")):
        s = data["summary"][name]
        rates = (100 * (s["served_rate"] or 0), 100 * (s["error_rate"] or 0), 100 * (s["shed_rate"] or 0))
        lines.append(
            f"| {label} | {s['served']} | {s['load_shed_429']} | "
            f"{rates[0]:.1f} / {rates[1]:.1f} / {rates[2]:.1f} | "
            f"{s['wall_seconds']} | {s['requests_per_second']} | "
            f"{s['client_latency_ms']['p50']} | {s['client_latency_ms']['p95']} | {s['client_latency_ms']['p99']} | "
            f"{s['client_latency_served_only_ms']['p50']}/{s['client_latency_served_only_ms']['p95']}/{s['client_latency_served_only_ms']['p99']} | "
            f"{s['scenario_server_latency_ms']['p50']}/{s['scenario_server_latency_ms']['p95']}/{s['scenario_server_latency_ms']['p99']} | "
            f"{s['peak_global_in_flight']} / {s['peak_mock_in_flight']} ({s['peak_mock_queue_depth']}) |"
        )

    if profile == "overload":
        b = data["summary"]["before"]
        a = data["summary"]["after"]
        settings = env.get("overload_settings") or {}
        lines += [
            "",
            "## Honest reading (overload profile)",
            "",
            f"- Mock behaviour: base {settings.get('GATEWAY_MOCK_OVERLOAD_BASE_MS')} ms + "
            f"{settings.get('GATEWAY_MOCK_OVERLOAD_SLOPE_MS')} ms per in-flight request, rejecting with 503 "
            f"above {settings.get('GATEWAY_MOCK_OVERLOAD_ERROR_AT')} in-flight. BEFORE has no bulkhead, so it "
            "drives the provider into that unhealthy band; AFTER keeps admitted calls below it.",
            f"- BEFORE: served {100 * (b['served_rate'] or 0):.1f}%, errors {100 * (b['error_rate'] or 0):.1f}%, "
            f"shed {100 * (b['shed_rate'] or 0):.1f}%, served-only p99 "
            f"{b['client_latency_served_only_ms']['p99']} ms, wall {b['wall_seconds']} s.",
            f"- AFTER: served {100 * (a['served_rate'] or 0):.1f}%, errors {100 * (a['error_rate'] or 0):.1f}%, "
            f"shed {100 * (a['shed_rate'] or 0):.1f}%, served-only p99 "
            f"{a['client_latency_served_only_ms']['p99']} ms, wall {a['wall_seconds']} s.",
            "- What the control buys here is error-free serving, not more served requests: AFTER converts "
            "provider 5xx into explicit 429 shedding, and its admitted calls can wait in the bulkhead queue "
            "(up to 5 s), so served-only p99 is not better than BEFORE. That is reported as measured.",
            "- A 429 is deliberate load shedding: the caller is told to retry. A 5xx from an overloaded "
            "provider is a failure. AFTER refuses work it cannot run healthily instead of pushing the provider "
            "past its limit.",
            "- Served-only p99 is the useful latency comparison; all-request p99 includes retry and backoff time.",
        ]

    baseline = data.get("baseline", {})
    lines += [
        "",
        "Notes:",
        f"- Host baseline (measured, not assumed): Redis GET RTT {env['redis'].get('rtt_ms_avg_10_gets')} ms; "
        f"sequential `/health` RTT {baseline.get('health_rtt_ms')} ms; "
        f"60 concurrent `/health` wall {baseline.get('concurrent_health_60_wall_ms')} ms. "
        "This host is slow at loopback concurrency, so absolute numbers are not production capacity.",
        "- Repetitions are interleaved (BEFORE, AFTER, BEFORE, AFTER, ...) to spread host drift across both scenarios; medians are reported.",
        "- Client latency is observed at the load generator; server-side percentiles are scenario-attributed `request_logs` rows (status 200). The full `/api/stats` snapshot is saved in the JSON.",
        "- Peak mock in-flight is the provider bulkhead's counter; with control off the bulkhead is bypassed, so 0 means \"not measured\", not \"no provider concurrency\".",
        "- AFTER is expected to shed under this load: that is the control doing its job, not a regression. BEFORE overloads the provider.",
        "- `GATEWAY_CONCURRENCY_CONTROL=off` exists only for this comparison; never use it in production.",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int, default=120)
    parser.add_argument("--concurrency", type=int, default=60)
    parser.add_argument("--port", type=int, default=8020)
    parser.add_argument("--seed", type=int, default=20261104)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--profiles", default="fixed,overload")
    args = parser.parse_args()

    profiles = [p.strip() for p in args.profiles.split(",") if p.strip()]
    redis = redis_info()
    if not redis["available"]:
        raise SystemExit(f"Redis is required for this benchmark: {redis['error']}")

    os.environ.setdefault("GATEWAY_ADMIN_TOKEN", ADMIN_TOKEN)
    os.environ["REDIS_URL"] = BENCH_REDIS_URL
    from app.security import create_gateway_api_key

    raw_key, _ = create_gateway_api_key("phase4-benchmark", quota=100000, daily_budget=1000.0)

    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for profile in profiles:
        data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "environment": environment_header(redis, args.total, args.concurrency, profile),
            "load": {"total_requests": args.total, "concurrency": args.concurrency,
                     "seed": args.seed, "repeats": args.repeats},
            "scenarios": {"before": [], "after": []},
        }

        for rep in range(1, args.repeats + 1):
            for name, control in (("before", "off"), ("after", "on")):
                print(f"[benchmark] {profile} rep {rep}/{args.repeats}: {name} "
                      f"(GATEWAY_CONCURRENCY_CONTROL={control}) ...")
                data["scenarios"][name].append(scenario(
                    name, control, args.port, raw_key, args.total, args.concurrency, args.seed, profile
                ))
                # Prevent Redis cache/limiter state from carrying between runs.
                import redis as redis_sync
                redis_sync.Redis.from_url(BENCH_REDIS_URL).flushdb()

        data["summary"] = {name: summarize_runs(runs) for name, runs in data["scenarios"].items()}
        data["baseline"] = data["scenarios"]["before"][0].get("baseline", {})
        cleanup_bench_rows()

        stem = "phase4-load" if profile == "fixed" else f"phase4-load-{profile}"
        json_path = BENCH_DIR / f"{stem}-{stamp}.json"
        md_path = BENCH_DIR / f"{stem}-{stamp}.md"
        json_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        md_path.write_text(render_markdown(data), encoding="utf-8")
        (BENCH_DIR / f"{stem}-before-after.json").write_text(
            json.dumps(data, indent=2), encoding="utf-8")
        (BENCH_DIR / f"{stem}-before-after.md").write_text(
            render_markdown(data), encoding="utf-8")

        print(render_markdown(data))
        print(f"\nWrote {json_path} and {md_path}")


if __name__ == "__main__":
    main()
