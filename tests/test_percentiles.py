import sqlite3
import time

import pytest

from app import metrics


def test_nearest_rank_percentiles_of_1_to_1000():
    values = list(range(1, 1001))
    assert metrics.percentile(values, 50) == 500
    assert metrics.percentile(values, 95) == 950
    assert metrics.percentile(values, 99) == 990
    summary = metrics.summarize(values)
    assert summary["count"] == 1000
    assert summary["p50"] == 500
    assert summary["p95"] == 950
    assert summary["p99"] == 990
    assert summary["avg"] == 500.5
    assert summary["max"] == 1000


def test_percentile_handles_empty_and_single():
    assert metrics.percentile([], 99) == 0.0
    assert metrics.percentile([42], 99) == 42.0


def test_latency_percentiles_grouped_by_route(tmp_path, monkeypatch):
    db_file = tmp_path / "metrics.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE request_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            endpoint TEXT,
            provider TEXT,
            model TEXT,
            latency_ms REAL
        )
        """
    )
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    rows = []
    for i in range(1, 101):
        rows.append((now, "/v1/chat", "groq", "llama", float(i)))
    for i in range(1, 51):
        rows.append((now, "/v1/chat", "gemini", "flash", float(i * 10)))
    conn.executemany(
        "INSERT INTO request_logs (timestamp, endpoint, provider, model, latency_ms) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(metrics, "DB_PATH", str(db_file))
    snap = metrics.latency_percentiles(window_seconds=60, max_samples=5000)

    assert snap["sample_size"] == 150
    assert snap["overall"]["count"] == 150
    assert snap["by_provider"]["groq"]["p99"] == 99.0
    assert snap["by_provider"]["gemini"]["p99"] == 500.0
    routes = {(r["endpoint"], r["provider"], r["model"]): r for r in snap["by_route"]}
    assert routes[("/v1/chat", "groq", "llama")]["p50"] == 50.0
    assert routes[("/v1/chat", "gemini", "flash")]["p95"] == 480.0


def test_latency_percentiles_ignore_rows_outside_window(tmp_path, monkeypatch):
    db_file = tmp_path / "metrics_old.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        """
        CREATE TABLE request_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP,
            endpoint TEXT,
            provider TEXT,
            model TEXT,
            latency_ms REAL
        )
        """
    )
    old = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - 7200))
    conn.execute(
        "INSERT INTO request_logs (timestamp, endpoint, provider, model, latency_ms) VALUES (?, ?, ?, ?, ?)",
        (old, "/v1/chat", "groq", "llama", 999.0),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(metrics, "DB_PATH", str(db_file))
    snap = metrics.latency_percentiles(window_seconds=60)
    assert snap["sample_size"] == 0
    assert snap["overall"]["p99"] == 0.0
