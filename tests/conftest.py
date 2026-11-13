import os
import asyncio
import pytest

# The application refuses to import without an admin token. Tests use a local value.
os.environ.setdefault("GATEWAY_ADMIN_TOKEN", "test-admin-token")
os.environ.setdefault("APP_ENV", "development")

# Never let a developer's .env point the test suite at a remote database.
# CI sets DATABASE_URL explicitly for Stage C, and setdefault() preserves it;
# here the default prevents load_dotenv() from injecting a live URL into
# imports such as app.security, which initialize their schema on import.
os.environ.setdefault("DATABASE_URL", "")

# Breakers inherit REDIS_URL when this is unset, which makes timing-sensitive
# unit tests wait on an unreachable Redis. Stage D sets the URL explicitly.
os.environ.setdefault("CIRCUIT_BREAKER_REDIS_URL", "")

# Keep test jobs/cache/limiter keys out of the dev Redis database (db 0) so a
# locally running server cannot consume work enqueued by the test suite.
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/15")

# ASGITransport does not run the FastAPI lifespan, which is where the server
# creates its SQLite schema. On a fresh clone there is no gateway.db yet, so
# create the same schema here once for the whole test session.
from app.main import init_db  # noqa: E402

init_db()


def _reset_loop_bound_resources():
    from app.cost import COST_TRACKER, REQUEST_COALESCER, RESPONSE_CACHE
    from app.jobs import JOB_MANAGER
    from app.reliability import (
        CIRCUIT_BREAKERS,
        MODEL_CIRCUIT_BREAKERS,
        GLOBAL_CONCURRENCY,
        PROVIDER_POOLS,
        TRAFFIC_BROADCASTER,
    )
    from app.security import RATE_LIMITER

    for breaker in list(CIRCUIT_BREAKERS.values()) + list(MODEL_CIRCUIT_BREAKERS.values()):
        breaker._lock = asyncio.Lock()
        breaker._redis = None
        breaker.reset()
        breaker._degraded = False
        breaker._version = 0
        breaker.total_requests = 0
        breaker.total_failures = 0
        breaker.total_successes = 0
    GLOBAL_CONCURRENCY._lock = asyncio.Lock()
    GLOBAL_CONCURRENCY.in_flight = 0
    GLOBAL_CONCURRENCY.rejected_count = 0
    for pool in PROVIDER_POOLS.values():
        pool._lock = asyncio.Lock()
        pool._waiters.clear()
        pool.in_flight = 0
        pool.queue_depth = 0
        pool.rejected_count = 0
        pool._wait_history.clear()
    TRAFFIC_BROADCASTER._lock = asyncio.Lock()
    TRAFFIC_BROADCASTER._subscribers.clear()
    REQUEST_COALESCER._lock = asyncio.Lock()
    REQUEST_COALESCER._in_flight.clear()
    RATE_LIMITER._lock = asyncio.Lock()
    RATE_LIMITER._redis_client = None
    RESPONSE_CACHE._redis = None
    COST_TRACKER._redis = None
    JOB_MANAGER._memory_queue = asyncio.Queue()
    JOB_MANAGER._memory_notified.clear()
    JOB_MANAGER._memory_processing.clear()
    JOB_MANAGER._memory_dlq.clear()
    JOB_MANAGER._redis = None
    JOB_MANAGER._worker_running = False
    JOB_MANAGER._worker_task = None
    JOB_MANAGER._current_job_id = None
    JOB_MANAGER._orphan_seen.clear()


@pytest.fixture(autouse=True)
def isolate_application_async_resources(monkeypatch):
    """Keep process-wide app singletons from retaining loop-bound state between tests."""
    _reset_loop_bound_resources()
    if os.getenv("GATEWAY_TEST_DISABLE_REDIS") == "1":
        from app.cost import COST_TRACKER, RESPONSE_CACHE
        from app.jobs import JOB_MANAGER
        from app.security import RATE_LIMITER

        monkeypatch.setattr(JOB_MANAGER, "get_client", lambda: None)
        monkeypatch.setattr(COST_TRACKER, "get_client", lambda: None)
        monkeypatch.setattr(RESPONSE_CACHE, "get_client", lambda: None)

        async def no_rate_limit_client():
            return None

        monkeypatch.setattr(RATE_LIMITER, "get_client", no_rate_limit_client)
    yield
    _reset_loop_bound_resources()

