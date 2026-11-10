import math
import os
from app.db import sqlite3
import time
from typing import Any, Dict, Iterable, List

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")

DEFAULT_WINDOW_SECONDS = 3600
DEFAULT_MAX_SAMPLES = 5000


def percentile(values: Iterable[float], p: float) -> float:
    """
    Nearest-rank percentile. For 1..1000 this returns exactly 500 at p50,
    950 at p95 and 990 at p99 (no interpolation, no AVG shortcut).
    """
    ordered = sorted(float(v) for v in values)
    if not ordered:
        return 0.0
    rank = math.ceil((p / 100.0) * len(ordered))
    index = min(len(ordered) - 1, max(0, rank - 1))
    return ordered[index]


def summarize(values: Iterable[float]) -> Dict[str, Any]:
    ordered = sorted(float(v) for v in values)
    if not ordered:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "max": 0.0}
    return {
        "count": len(ordered),
        "p50": round(percentile(ordered, 50), 2),
        "p95": round(percentile(ordered, 95), 2),
        "p99": round(percentile(ordered, 99), 2),
        "avg": round(sum(ordered) / len(ordered), 2),
        "max": round(ordered[-1], 2),
    }


def _grouped_summary(samples: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, List[float]] = {}
    for row in samples:
        groups.setdefault(row[key], []).append(row["latency_ms"])
    return {name: summarize(vals) for name, vals in groups.items()}


def latency_percentiles(
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    max_samples: int = DEFAULT_MAX_SAMPLES,
) -> Dict[str, Any]:
    """
    Exact percentiles over the most recent `max_samples` completed requests
    inside a rolling time window. Computed server-side; AVG is still reported
    only as a separate diagnostic, never used as the percentile.
    """
    window_seconds = max(1, int(window_seconds))
    max_samples = max(1, int(max_samples))

    rows: List[Dict[str, Any]] = []
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COALESCE(endpoint, '/v1/chat') AS endpoint,
                   LOWER(COALESCE(provider, 'unknown')) AS provider,
                   COALESCE(model, '') AS model,
                   latency_ms
            FROM request_logs
            WHERE latency_ms IS NOT NULL AND latency_ms >= 0
              AND datetime(timestamp) >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT ?
            """,
            (f"-{window_seconds} seconds", max_samples),
        )
        for endpoint, provider, model, latency in cursor.fetchall():
            rows.append({
                "endpoint": endpoint,
                "provider": provider,
                "model": model,
                "latency_ms": float(latency),
            })
        conn.close()
    except Exception:
        rows = []

    routes: Dict[str, List[float]] = {}
    for row in rows:
        route = f"{row['endpoint']}|{row['provider']}|{row['model']}"
        routes.setdefault(route, []).append(row["latency_ms"])

    by_route = []
    for route, values in routes.items():
        endpoint, provider, model = route.split("|", 2)
        item = summarize(values)
        item.update({"endpoint": endpoint, "provider": provider, "model": model})
        by_route.append(item)
    by_route.sort(key=lambda item: item["count"], reverse=True)

    return {
        "window_seconds": window_seconds,
        "max_samples": max_samples,
        "sample_size": len(rows),
        "generated_at": time.time(),
        "overall": summarize([r["latency_ms"] for r in rows]),
        "by_endpoint": _grouped_summary(rows, "endpoint"),
        "by_provider": _grouped_summary(rows, "provider"),
        "by_model": _grouped_summary(rows, "model"),
        "by_route": by_route,
    }
