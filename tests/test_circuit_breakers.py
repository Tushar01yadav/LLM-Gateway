import asyncio
import os
import time
import uuid

import pytest
from redis.asyncio import Redis as RedisClient

from app.reliability import CircuitBreaker, RedisError, classify_error, should_count_circuit_failure


@pytest.mark.asyncio
async def test_half_open_probe_is_exclusive_and_recovers():
    cb = CircuitBreaker("test", failure_threshold=1, cooldown_seconds=0, redis_url="")
    await cb.record_failure()
    results = await asyncio.gather(*(cb.acquire_permit() for _ in range(40)))
    permits = [permit for allowed, _, permit in results if allowed]
    assert len(permits) == 1
    assert cb.state == "HALF_OPEN"
    await cb.record_success(permits[0])
    assert cb.state == "CLOSED"


@pytest.mark.asyncio
async def test_stale_local_result_cannot_close_newer_open_state():
    cb = CircuitBreaker("stale", failure_threshold=1, cooldown_seconds=60, redis_url="")
    _, _, old_permit = await cb.acquire_permit()
    await cb.record_failure(permit=old_permit)
    assert cb.state == "OPEN"
    await cb.record_success(permit=old_permit)
    assert cb.state == "OPEN"


@pytest.mark.asyncio
async def test_redis_outage_uses_explicit_local_degraded_state():
    class UnavailableRedis:
        async def eval(self, *args, **kwargs):
            raise RedisError("unavailable")

    cb = CircuitBreaker("degraded", failure_threshold=1, cooldown_seconds=60, redis_url="redis://unavailable")
    cb._redis = UnavailableRedis()
    allowed, _, permit = await cb.acquire_permit()
    assert allowed
    assert cb._degraded is True
    await cb.record_failure(permit=permit)
    assert cb.state == "OPEN"
    allowed, reason, _ = await cb.acquire_permit()
    assert allowed is False
    assert "circuit_open" in reason


@pytest.mark.asyncio
async def test_fallback_success_does_not_close_primary_model_breaker():
    primary = CircuitBreaker("provider-a", failure_threshold=1, cooldown_seconds=60, redis_url="", model="model-a")
    fallback = CircuitBreaker("provider-b", failure_threshold=1, cooldown_seconds=60, redis_url="", model="model-b")
    await primary.record_failure()
    _, _, fallback_permit = await fallback.acquire_permit()
    await fallback.record_success(permit=fallback_permit)
    assert primary.state == "OPEN"
    assert fallback.state == "CLOSED"


@pytest.mark.asyncio
async def test_real_redis_probe_is_exclusive_across_instances():
    redis_url = os.getenv("CIRCUIT_TEST_REDIS_URL") or os.getenv("REDIS_URL")
    if not redis_url:
        if os.getenv("GATEWAY_REQUIRE_REDIS_TESTS") == "1":
            pytest.fail("Redis integration is required but CIRCUIT_TEST_REDIS_URL/REDIS_URL is missing")
        pytest.skip("CIRCUIT_TEST_REDIS_URL is not configured")
    domain = f"integration-{uuid.uuid4().hex}"
    first = CircuitBreaker(domain, failure_threshold=1, cooldown_seconds=0, redis_url=redis_url, probe_lease_seconds=2)
    second = CircuitBreaker(domain, failure_threshold=1, cooldown_seconds=0, redis_url=redis_url, probe_lease_seconds=2)
    client = RedisClient.from_url(redis_url, decode_responses=True)
    try:
        try:
            await client.ping()
        except Exception as exc:
            if os.getenv("GATEWAY_REQUIRE_REDIS_TESTS") == "1":
                pytest.fail(f"Required Redis integration service is unavailable: {type(exc).__name__}")
            pytest.skip(f"Redis unavailable for distributed integration test: {type(exc).__name__}")
        allowed, _, first_permit = await first.acquire_permit()
        assert allowed
        await first.record_failure(permit=first_permit)
        ready = 0
        ready_event = asyncio.Event()
        start_event = asyncio.Event()

        async def compete(cb):
            nonlocal ready
            ready += 1
            if ready == 2:
                ready_event.set()
            await start_event.wait()
            return await cb.acquire_permit()

        competitors = [asyncio.create_task(compete(first)), asyncio.create_task(compete(second))]
        await ready_event.wait()
        start_event.set()
        outcomes = await asyncio.gather(*competitors)
        assert sum(1 for allowed, _, _ in outcomes if allowed) == 1
        winner = first if outcomes[0][0] else second
        winning_permit = outcomes[0][2] if outcomes[0][0] else outcomes[1][2]
        await winner.record_success(permit=winning_permit)
        assert (await first.get_status_shared())["state"] == "CLOSED"

        # Reopen, force the active lease to expire using Redis server time, and
        # prove that the old owner cannot complete after a replacement is issued.
        allowed, _, permit = await first.acquire_permit()
        assert allowed
        await first.record_failure(permit=permit)
        allowed, _, stale_permit = await first.acquire_permit()
        assert allowed
        server_time = await first._redis.time()
        expired_at = server_time[0] * 1000 + server_time[1] // 1000 - 1
        await first._redis.hset(first.key, "probe_expires_at_ms", expired_at)
        allowed, _, replacement_permit = await second.acquire_permit()
        assert allowed
        await first.record_success(permit=stale_permit)
        assert (await second.get_status_shared())["state"] == "HALF_OPEN"
        await second.record_failure(permit=replacement_permit)
        assert (await first.get_status_shared())["state"] == "OPEN"
        # Loss of Redis state (for example, a restart without persistence) must
        # not leave an orphaned HALF_OPEN owner permanently blocking recovery.
        old_permit = replacement_permit
        await first._redis.delete(first.key)
        restarted = CircuitBreaker(domain, failure_threshold=1, cooldown_seconds=0, redis_url=redis_url)
        allowed, _, _ = await restarted.acquire_permit()
        assert allowed
        await first.record_success(permit=old_permit)
        assert (await restarted.get_status_shared())["state"] == "CLOSED"
        if restarted._redis:
            await restarted._redis.aclose()

        failure_domain = f"{domain}:concurrent-failures"
        workers = [CircuitBreaker(failure_domain, failure_threshold=3, cooldown_seconds=60, redis_url=redis_url) for _ in range(8)]
        permits = []
        for worker in workers:
            allowed, _, permit = await worker.acquire_permit()
            assert allowed
            permits.append(permit)
        await asyncio.gather(*(worker.record_failure(permit=permit) for worker, permit in zip(workers, permits)))
        failure_status = await workers[0].get_status_shared()
        assert failure_status["state"] == "OPEN"
        assert failure_status["consecutive_failures"] == 3
    finally:
        await first.reset_shared()
        for worker in locals().get("workers", []):
            await worker.reset_shared()
            if worker._redis:
                await worker._redis.aclose()
        if first._redis:
            await first._redis.aclose()
        if second._redis:
            await second._redis.aclose()
        await client.aclose()


def test_error_classification_excludes_invalid_client_requests():
    assert classify_error(400) == (False, "client_error_400")
    assert classify_error(401) == (False, "client_error_401")
    assert classify_error(503) == (True, "server_error_503")
    assert classify_error(429) == (True, "rate_limit_429")
    assert should_count_circuit_failure(429, True, "groq", {"circuit_breakers": {"count_429": False}}) is False
    assert should_count_circuit_failure(429, True, "groq", {"circuit_breakers": {"count_429": True}}) is True
