"""
Phase 4.7.1 chaos with a configured fallback chain.

Primary mock -> fallback mock2 via alias 'mock_failover'. The no-fallback run in
docs/benchmarks/phase4-chaos.* stays untouched as the baseline; this script adds
docs/benchmarks/phase4-chaos-fallback.json/.md including that baseline for
side-by-side reading.

Scenarios:
  1. provider_death_fallback      mock returns 503 mid-load
  2. provider_latency_fallback    mock 500 ms -> 5000 ms mid-load, then back
  3. provider_latency_severe      mock 500 ms -> 16000 ms (past the 15 s per-attempt cap)
  4. real_groq_openrouter         3 real Groq calls + 3 with Groq injected as 503
                                  so real OpenRouter serves the fallback (low N, small cost)

Honest output: the latency-5000 case is expected NOT to fail over because the
primary still succeeds inside the 30 s budget; that is reported, not tuned away.
"""
import asyncio
import json
import math
import os
import signal
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
TARGET = PORTS[0]
REDIS_CONTAINER = "llm-gateway-redis"
LATENCY_FILE = ROOT / "scratch" / "mock_latency_ms.json"
OUT_JSON = ROOT / "docs" / "benchmarks" / "phase4-chaos-fallback.json"
OUT_MD = ROOT / "docs" / "benchmarks" / "phase4-chaos-fallback.md"
BASELINE_JSON = ROOT / "docs" / "benchmarks" / "phase4-chaos.json"
LOG_DIR = ROOT / "scratch"
ALIAS = "mock_failover"
REAL_ALIAS = "fast"

scenarios = []


def set_latency(profile):
    LATENCY_FILE.write_text(json.dumps(profile), encoding="utf-8")


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
    log = open(LOG_DIR / f"chaos-fallback-worker-{port}.log", "w", encoding="utf-8")
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


def inject(port, provider, enabled, mode="503"):
    try:
        httpx.post(f"http://127.0.0.1:{port}/api/reliability/inject-failure",
                   json={"provider": provider, "enabled": enabled, "mode": mode},
                   headers={"X-Admin-Token": ADMIN}, timeout=10)
    except Exception:
        pass


def reset_breakers(port):
    try:
        httpx.post(f"http://127.0.0.1:{port}/api/reliability/circuit-breaker/reset",
                   headers={"X-Admin-Token": ADMIN}, timeout=10)
    except Exception:
        pass


def breaker_state(port, provider):
    try:
        res = httpx.get(f"http://127.0.0.1:{port}/api/reliability/status",
                        headers={"X-Admin-Token": ADMIN}, timeout=5)
        cb = res.json().get("circuit_breakers", {}).get(provider, {})
        return {"state": cb.get("state"), "consecutive_failures": cb.get("consecutive_failures"),
                "cooldown_remaining_s": cb.get("cooldown_remaining_sec")}
    except Exception as exc:
        return {"error": type(exc).__name__}


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
    by_class = {}
    by_provider = {}
    by_fallback = {"primary": 0, "fallback": 0}
    for r in window:
        by_class[r["class"]] = by_class.get(r["class"], 0) + 1
        if r["class"] == "served":
            by_provider[r["provider"]] = by_provider.get(r["provider"], 0) + 1
            by_fallback["fallback" if r["fallback_used"] else "primary"] += 1
    total = len(window) or 1
    served = [r for r in window if r["class"] == "served"]
    attempt_outcomes = {}
    for r in window:
        for a in r.get("attempts") or []:
            if a.get("status") == "success":
                continue
            key = f"{a.get('provider')}:{a.get('status')}:{a.get('error_type')}"
            attempt_outcomes[key] = attempt_outcomes.get(key, 0) + 1
    return {
        "requests": len(window),
        "success_rate": round(len(served) / total, 4) if window else None,
        "by_class": by_class,
        "served_by_provider": by_provider,
        "served_primary": by_fallback["primary"],
        "served_fallback": by_fallback["fallback"],
        "fallback_share": round(by_fallback["fallback"] / len(served), 4) if served else None,
        "served_latency_ms": percentiles([r["latency_ms"] for r in served]),
        "all_latency_ms": percentiles([r["latency_ms"] for r in window]),
        "attempt_outcomes": attempt_outcomes,
    }


def first_fallback_after(records, fault_start):
    candidates = [r for r in records
                  if r["started"] >= fault_start and r["class"] == "served" and r["fallback_used"]]
    if not candidates:
        return None
    first = min(candidates, key=lambda r: r["ended"])
    return {
        "provider": first["provider"],
        "latency_ms": first["latency_ms"],
        "first_served_s_after_fault": round(first["ended"] - fault_start, 2),
    }


async def run_load_with_fault(port, raw_key, alias, total, concurrency, fault_after_s,
                              fault_duration_s, apply_fault, heal_fault, mode_label,
                              fault_mark, max_tokens=10):
    sem = asyncio.Semaphore(concurrency)
    records = []

    async def one(client, index):
        payload = {
            "alias": alias,
            "messages": [{"role": "user", "content": f"chaos {mode_label} request {index}"}],
            "temperature": 0.7,
            "max_tokens": max_tokens,
            "session_id": f"chaos-fb-session-{mode_label}-{index}",
        }
        async with sem:
            started = time.time()
            t0 = time.perf_counter()
            status = None
            detail = ""
            provider = None
            model = None
            fallback_used = False
            attempts = []
            try:
                res = await client.post(f"http://127.0.0.1:{port}/v1/chat", json=payload,
                                        headers={"Authorization": f"Bearer {raw_key}"}, timeout=60.0)
                status = res.status_code
                try:
                    body = res.json()
                    detail = str(body.get("detail", ""))
                    provider = body.get("provider")
                    model = body.get("model")
                    fallback_used = bool(body.get("fallback_used"))
                    attempts = body.get("attempts") or []
                except Exception:
                    detail = res.text[:200]
            except Exception as exc:
                detail = type(exc).__name__
            ended = time.time()
            records.append({
                "started": started, "ended": ended,
                "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                "status": status, "class": classify(status, detail),
                "provider": provider, "model": model, "fallback_used": fallback_used,
                "attempts": attempts,
            })

    async def scheduler():
        await asyncio.sleep(fault_after_s)
        fault_start = time.time()
        fault_mark["start"] = fault_start
        if asyncio.iscoroutinefunction(apply_fault):
            await apply_fault()
        else:
            apply_fault()
        await asyncio.sleep(fault_duration_s)
        fault_mark["end"] = time.time()
        if asyncio.iscoroutinefunction(heal_fault):
            await heal_fault()
        else:
            heal_fault()

    async with httpx.AsyncClient() as client:
        sched = asyncio.create_task(scheduler())
        await asyncio.gather(*(one(client, i) for i in range(total)))
        await sched

    fault_start, fault_end = fault_mark["start"], fault_mark["end"]
    recovery_seconds = None
    post = sorted([r for r in records if r["started"] >= fault_end], key=lambda r: r["started"])
    if post:
        bucket = {}
        for r in post:
            b = int(r["started"] - fault_end)
            bucket.setdefault(b, []).append(r["class"] == "served")
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
        "alias": alias,
        "fault_window_s": round(fault_end - fault_start, 2),
        "window_basis": "request start time",
        "pre_fault": summarize_records(records, 0, fault_start),
        "during_fault": summarize_records(records, fault_start, fault_end),
        "post_fault": summarize_records(records, fault_end, float("inf")),
        "first_fallback": first_fallback_after(records, fault_start),
        "time_to_recover_s": recovery_seconds,
        "total_requests": len(records),
        "error_detail_sample": next(({k: r[k] for k in ("status", "class", "provider", "model")}
                                     for r in records if r["class"] != "served"), None),
    }


async def poll_breaker(stop_event, fault_mark, provider, port, out):
    """Record when the primary breaker is first seen OPEN, relative to fault start."""
    while not stop_event.is_set():
        if fault_mark.get("start"):
            st = await asyncio.to_thread(breaker_state, port, provider)
            if st.get("state") == "OPEN" and out.get("breaker_open_s") is None:
                out["breaker_open_s"] = round(time.time() - fault_mark["start"], 2)
                out["breaker_state_when_open"] = st
        await asyncio.sleep(0.2)


async def scenario_mock_fault(name, mode, latency_during, fault_duration, total=300,
                              concurrency=30, fault_after=8.0, max_tokens=10):
    set_latency({"mock": 500, "mock2": 500})
    for p in PORTS:
        inject(p, "mock", False)
        reset_breakers(p)
    await asyncio.sleep(0.5)

    if mode == "death":
        apply_fault = lambda: [inject(p, "mock", True, "503") for p in PORTS]
        heal_fault = lambda: [inject(p, "mock", False) for p in PORTS]
        description = "primary mock returns 503 mid-load; fallback mock2 stays healthy"
    else:
        apply_fault = lambda: set_latency(latency_during)
        heal_fault = lambda: set_latency({"mock": 500, "mock2": 500})
        description = (
            f"primary mock latency -> {latency_during['mock']} ms mid-load; "
            f"fallback mock2 stays at {latency_during['mock2']} ms"
        )

    raw_key = os.environ["CHAOS_CLIENT_KEY"]
    fault_mark = {}
    breaker_watch = {}
    stop = asyncio.Event()
    watcher = asyncio.create_task(poll_breaker(stop, fault_mark, "mock", TARGET, breaker_watch))
    started = time.time()
    try:
        result = await run_load_with_fault(
            TARGET, raw_key, ALIAS, total=total, concurrency=concurrency,
            fault_after_s=fault_after, fault_duration_s=fault_duration,
            apply_fault=apply_fault, heal_fault=heal_fault, mode_label=name,
            fault_mark=fault_mark, max_tokens=max_tokens,
        )
    finally:
        stop.set()
        await watcher

    result["scenario"] = name
    result["description"] = description
    result["breaker_watch"] = breaker_watch
    result["breaker_after_run"] = {str(p): {"mock": breaker_state(p, "mock"),
                                            "mock2": breaker_state(p, "mock2")} for p in PORTS}
    result["run_seconds"] = round(time.time() - started, 2)
    scenarios.append(result)
    during = result["during_fault"]
    print(f"[chaos-fb] {name}: ok={during['success_rate']} by={during['by_class']} "
          f"providers={during['served_by_provider']} fallback_share={during['fallback_share']} "
          f"p99={during['served_latency_ms']['p99']} first_fb={result['first_fallback']} "
          f"breaker_open={breaker_watch.get('breaker_open_s')}s")
    return result


async def scenario_real_providers(port, raw_key):
    """3 real Groq calls, then 3 more with Groq injected as 503 -> real OpenRouter."""
    inject(port, "groq", False)
    reset_breakers(port)
    await asyncio.sleep(0.3)

    def call(index, label):
        t0 = time.perf_counter()
        status = None
        provider = model = None
        fallback_used = False
        detail = ""
        try:
            res = httpx.post(
                f"http://127.0.0.1:{port}/v1/chat",
                json={"alias": REAL_ALIAS, "temperature": 0.0, "max_tokens": 16,
                      "messages": [{"role": "user", "content":
                                    f"Reply with the single word: pong. Probe {label}-{index}-{time.time_ns()}"}]},
                headers={"Authorization": f"Bearer {raw_key}"}, timeout=90.0,
            )
            status = res.status_code
            body = res.json()
            detail = str(body.get("detail", ""))[:200]
            provider = body.get("provider")
            model = body.get("model")
            fallback_used = bool(body.get("fallback_used"))
        except Exception as exc:
            detail = type(exc).__name__
        return {
            "phase": label, "index": index, "status": status,
            "class": classify(status, detail), "provider": provider, "model": model,
            "fallback_used": fallback_used,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
            "detail": detail,
        }

    normal = [await asyncio.to_thread(call, i, "primary_real") for i in range(3)]

    inject(port, "groq", True, "503")
    try:
        injected = [await asyncio.to_thread(call, i, "groq_injected_503") for i in range(3)]
    finally:
        inject(port, "groq", False)
        reset_breakers(port)

    records = normal + injected
    served = [r for r in records if r["class"] == "served"]
    result = {
        "scenario": "real_groq_openrouter",
        "description": ("3 real Groq calls, then 3 calls with Groq injected as 503 so the real "
                        "OpenRouter fallback serves; alias 'fast', max_tokens 16"),
        "requests": records,
        "served": len(served),
        "success_rate": round(len(served) / len(records), 4),
        "by_provider": {p: sum(1 for r in served if r["provider"] == p)
                        for p in sorted({r["provider"] for r in served if r["provider"]})},
        "served_latency_ms": percentiles([r["latency_ms"] for r in served]),
        "failures": [r for r in records if r["class"] != "served"],
    }
    scenarios.append(result)
    print(f"[chaos-fb] real_groq_openrouter: served={result['served']}/{len(records)} "
          f"by_provider={result['by_provider']} p99={result['served_latency_ms']['p99']}")
    return result


def load_baseline():
    if not BASELINE_JSON.exists():
        return None
    try:
        data = json.loads(BASELINE_JSON.read_text(encoding="utf-8"))
        return data.get("scenarios", [])
    except Exception:
        return None


def render_markdown(data):
    env = data["environment"]
    lines = [
        "# Phase 4 chaos — WITH fallback chain (primary mock -> mock2)",
        "",
        f"- Generated: {data['generated_at']}",
        f"- Machine: {env['platform']}, Python {env['python']}, {env['workers']} workers on ports {env['ports']}",
        f"- Redis db {env['redis_db']}; mock latency file `scratch/mock_latency_ms.json`",
        f"- Alias under test: `{ALIAS}` (mock -> mock2); real-provider alias: `{REAL_ALIAS}` (groq -> openrouter)",
        f"- Load: {env['load']['total_requests']} requests, concurrency {env['load']['concurrency']}, "
        f"fault at {env['load']['fault_after_s']} s for {env['load']['fault_duration_s']} s",
        "",
    ]

    baseline = data.get("baseline_without_fallback")
    if baseline:
        lines += [
            "## Without fallback (baseline, from phase4-chaos.json)",
            "",
            "| Scenario | Requests during fault | Success during fault | Served p99 (ms) | Errors by type | Recovery (s) |",
            "|---|---|---|---|---|---|",
        ]
        for s in baseline:
            if s.get("scenario") not in ("provider_death", "provider_latency") or "during_fault" not in s:
                continue
            d = s["during_fault"]
            lines.append(
                f"| {s.get('scenario')} | {d.get('requests')} | {d.get('success_rate')} | "
                f"{(d.get('served_latency_ms') or {}).get('p99')} | {d.get('by_class')} | "
                f"{s.get('time_to_recover_s')} |"
            )
        lines += ["", "The baseline's Redis-kill and worker-kill scenarios are unchanged and not re-run here.", ""]

    lines += [
        "## With fallback — summary",
        "",
        "| Scenario | Success during fault | Served by fallback | Failover first served (s) | Breaker OPEN (s) | Served p99 (ms) | Post-fault providers | Recovery (s) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for s in data["scenarios"]:
        if "during_fault" not in s:
            continue
        d = s["during_fault"]
        fb = s.get("first_fallback") or {}
        bw = s.get("breaker_watch") or {}
        lines.append(
            f"| {s['scenario']} | {d['success_rate']} | {d['served_fallback']}/{d['requests']} "
            f"({d['fallback_share']}) | {fb.get('first_served_s_after_fault')} | "
            f"{bw.get('breaker_open_s')} | {d['served_latency_ms']['p99']} | "
            f"{s['post_fault'].get('served_by_provider')} | {s.get('time_to_recover_s')} |"
        )

    lines += ["", "## Per-window detail", ""]
    for s in data["scenarios"]:
        if "during_fault" not in s:
            continue
        lines += [
            f"### {s['scenario']}",
            "",
            f"{s['description']}. Fault window {s['fault_window_s']} s.",
            "",
            "| Window | Requests | Success | By class | Served by provider | Fallback share | p50/p95/p99 served (ms) |",
            "|---|---|---|---|---|---|---|",
        ]
        for w in ("pre_fault", "during_fault", "post_fault"):
            x = s[w]
            lat = x["served_latency_ms"]
            lines.append(
                f"| {w} | {x['requests']} | {x['success_rate']} | {x['by_class']} | "
                f"{x['served_by_provider']} | {x['fallback_share']} | "
                f"{lat['p50']}/{lat['p95']}/{lat['p99']} |"
            )
        fb = s.get("first_fallback") or {}
        bw = s.get("breaker_watch") or {}
        lines.append("")
        if fb.get("provider"):
            lines.append(
                f"- First fallback-served response: provider `{fb.get('provider')}`, "
                f"latency {fb.get('latency_ms')} ms, {fb.get('first_served_s_after_fault')} s after fault start."
            )
        else:
            lines.append("- No request was served by the fallback during the run.")
        if bw.get("breaker_open_s") is not None:
            lines.append(f"- Primary breaker observed OPEN {bw['breaker_open_s']} s after fault start "
                         f"({bw.get('breaker_state_when_open')}).")
        else:
            lines.append("- Primary breaker never opened during the run.")
        outcomes = s["during_fault"].get("attempt_outcomes") or {}
        if outcomes:
            lines.append(f"- Primary/fallback attempt outcomes during the fault window: {outcomes}.")
        if s.get("error_detail_sample"):
            lines.append(f"- First non-served sample: {s['error_detail_sample']}.")
        lines.append("")

    real = next((s for s in data["scenarios"] if s.get("scenario") == "real_groq_openrouter"), None)
    if real:
        lines += [
            "## Real providers (low N)",
            "",
            f"{real['description']}. Served {real['served']}/{len(real['requests'])}; "
            f"providers {real['by_provider']}; served p50/p95/p99 "
            f"{real['served_latency_ms']['p50']}/{real['served_latency_ms']['p95']}/{real['served_latency_ms']['p99']} ms.",
            "",
            "| Phase | # | Status | Class | Provider | Model | Fallback used | Latency (ms) |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for r in real["requests"]:
            lines.append(
                f"| {r['phase']} | {r['index']} | {r['status']} | {r['class']} | {r['provider']} | "
                f"{r['model']} | {r['fallback_used']} | {r['latency_ms']} |"
            )
        lines.append("")

    def milestone(name):
        return next((s for s in data["scenarios"] if s.get("scenario") == name and "during_fault" in s), None)

    def baseline_for(name):
        return next((s for s in (data.get("baseline_without_fallback") or [])
                     if s.get("scenario") == name and "during_fault" in s), None)

    death, lat5, lat16 = (milestone("provider_death_fallback"),
                          milestone("provider_latency_fallback"),
                          milestone("provider_latency_severe"))
    lines += ["## Honest reading", ""]
    if death:
        d = death["during_fault"]
        base = (baseline_for("provider_death") or {}).get("during_fault") or {}
        fb = death.get("first_fallback") or {}
        bw = death.get("breaker_watch") or {}
        lines.append(
            f"- Provider death: success during fault {d['success_rate']} with fallback vs "
            f"{base.get('success_rate')} without it; {d['served_fallback']}/{d['requests']} requests "
            f"({d['fallback_share']}) were served by the fallback, first at "
            f"{fb.get('first_served_s_after_fault')} s, and the primary breaker opened at "
            f"{bw.get('breaker_open_s')} s. Requests already queued on the primary pay the retry "
            f"backoff before moving over; later requests skip it immediately."
        )
    if lat5:
        d = lat5["during_fault"]
        pre = lat5["pre_fault"]
        lines.append(
            f"- Latency at 5000 ms does NOT trigger failover: success stays {d['success_rate']} on the "
            f"primary, fallback share is {d['fallback_share']}, and throughput drops from "
            f"{pre['requests']} to {d['requests']} requests in comparable windows with p99 at "
            f"{d['served_latency_ms']['p99']} ms. The primary is slow but still succeeds inside the "
            "30 s budget, so this is a degradation scenario, not a failover scenario."
        )
    if lat16:
        d = lat16["during_fault"]
        bw = lat16.get("breaker_watch") or {}
        lines.append(
            f"- Latency at 16000 ms: success during fault stays {d['success_rate']} and the fallback "
            f"absorbs {d['served_fallback']}/{d['requests']} requests, but served p99 is "
            f"{d['served_latency_ms']['p99']} ms and the breaker "
            f"{'opened at ' + str(bw.get('breaker_open_s')) + ' s' if bw.get('breaker_open_s') is not None else 'never opened'}. "
            f"Attempt outcomes: {d.get('attempt_outcomes')}. Latency remains multi-second for "
            "requests caught on the slow primary; the fallback prevents hard failure but does not "
            "make the fault invisible."
        )
    if death:
        lines.append(
            "- The provider-death fault is healed through the admin toggle, which also resets the "
            "primary breaker; a real outage would leave it OPEN for its 60 s cooldown, so post-heal "
            "traffic would stay on the fallback longer than shown here."
        )
    real = next((s for s in data["scenarios"] if s.get("scenario") == "real_groq_openrouter"), None)
    if real and real.get("requests"):
        injected = [r for r in real["requests"] if r["phase"] == "groq_injected_503"]
        injected_served = [r for r in injected if r["class"] == "served"]
        injected_via_fallback = [r for r in injected_served if r["fallback_used"]]
        cold_warm = ""
        if len(injected_served) >= 2:
            cold_warm = (
                f" The first injected call paid the retry/backoff path ({injected_served[0]['latency_ms']} ms); "
                f"once the Groq breaker opened, the last completed in {injected_served[-1]['latency_ms']} ms."
            )
        lines.append(
            f"- Real providers: {real['served']}/{len(real['requests'])} served; the Groq-injected "
            f"phase produced {len(injected_served)}/{len(injected)} served responses, of which "
            f"{len(injected_via_fallback)} came through the real fallback "
            f"({real['by_provider']}).{cold_warm} Small N (max_tokens 16) keeps quota use minimal."
        )
    lines.append("")
    return "\n".join(lines)


async def main():
    set_latency({"mock": 500, "mock2": 500})
    r = redis_sync.Redis.from_url(REDIS_URL, socket_connect_timeout=2, socket_timeout=5)
    r.flushdb()

    os.environ["REDIS_URL"] = REDIS_URL
    os.environ["GATEWAY_ADMIN_TOKEN"] = ADMIN
    from app.security import create_gateway_api_key

    raw_key, info = create_gateway_api_key("phase4-chaos-fallback", quota=100000, daily_budget=1000.0)

    import sqlite3
    conn = sqlite3.connect(ROOT / "gateway.db", timeout=15)
    conn.execute("DELETE FROM async_jobs WHERE key_name = 'phase4-chaos-fallback'")
    conn.execute("DELETE FROM cost_records WHERE key_id = ?", (str(info["id"]),))
    conn.commit()
    conn.close()

    os.environ["CHAOS_CLIENT_KEY"] = raw_key
    real_key = None
    if os.getenv("GROQ_API_KEY") and os.getenv("OPENROUTER_API_KEY"):
        real_key, real_info = create_gateway_api_key("phase4-chaos-real", quota=100000, daily_budget=1000.0)
        conn = sqlite3.connect(ROOT / "gateway.db", timeout=15)
        conn.execute("DELETE FROM cost_records WHERE key_id = ?", (str(real_info["id"]),))
        conn.commit()
        conn.close()

    workers = [start_worker(p) for p in PORTS]
    started_at = time.time()
    try:
        await scenario_mock_fault("provider_death_fallback", "death",
                                  {"mock": 500, "mock2": 500}, 5.0)
        await asyncio.sleep(1)
        await scenario_mock_fault("provider_latency_fallback", "latency",
                                  {"mock": 5000, "mock2": 500}, 5.0)
        await asyncio.sleep(1)
        await scenario_mock_fault("provider_latency_severe", "latency",
                                  {"mock": 16000, "mock2": 500}, 25.0,
                                  total=240, concurrency=20, fault_after=6.0)
        for i, w in enumerate(workers):
            if w.poll() is not None:
                workers[i] = start_worker(PORTS[i])
        if real_key:
            await scenario_real_providers(TARGET, real_key)
        else:
            scenarios.append({"scenario": "real_groq_openrouter",
                              "skipped": "GROQ_API_KEY or OPENROUTER_API_KEY not configured"})
    finally:
        for w in workers:
            stop_worker(w)
        set_latency({"mock": 500, "mock2": 500})
        try:
            r.flushdb()
        except Exception:
            pass
        os.environ.pop("CHAOS_CLIENT_KEY", None)

    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "redis_db": REDIS_DB,
            "redis_container": REDIS_CONTAINER,
            "workers": len(PORTS),
            "ports": list(PORTS),
            "alias": ALIAS,
            "real_alias": REAL_ALIAS,
            "load": {"total_requests": 300, "concurrency": 30,
                     "fault_after_s": 8, "fault_duration_s": 5},
        },
        "baseline_without_fallback": load_baseline(),
        "scenarios": scenarios,
        "wall_seconds": round(time.time() - started_at, 2),
    }
    OUT_JSON.write_text(json.dumps(data, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_markdown(data), encoding="utf-8")
    print(render_markdown(data))
    print(f"\nWrote {OUT_JSON} and {OUT_MD}")


if __name__ == "__main__":
    asyncio.run(main())
