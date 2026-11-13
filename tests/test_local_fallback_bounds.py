import asyncio
import time

import pytest

import app.security as security
from app.cost import ResponseCache


@pytest.mark.asyncio
async def test_response_cache_fallback_is_bounded_thread_safe_and_expires(monkeypatch):
    cache = ResponseCache(redis_url="redis://unused")
    cache._local_cache_max_entries = 32
    monkeypatch.setattr(cache, "get_client", lambda: None)
    await asyncio.gather(*(
        asyncio.to_thread(asyncio.run, cache.set(f"key-{index}", {"text": "value"}))
        for index in range(128)
    ))
    assert len(cache._local_cache) == 32
    assert cache._local_cache_bytes <= cache._local_cache_max_bytes
    assert await cache.get("key-127") == {"text": "value"}
    await cache.set("expired", {"text": "old"}, ttl=1)
    cache._local_cache["expired"] = (cache._local_cache["expired"][0], time.time() - 1)
    assert await cache.get("expired") is None
    assert "expired" not in cache._local_cache

    cache._local_cache_max_bytes = 32
    assert await cache.set("too-large", {"text": "x" * 128}) is False


@pytest.mark.asyncio
async def test_rate_limit_fallback_capacity_expiry_and_concurrent_access(monkeypatch):
    limiter = security.RedisTokenBucketRateLimiter()
    limiter._mem_max_buckets = 32
    limiter._mem_bucket_ttl_seconds = 60
    async def no_redis():
        return None
    monkeypatch.setattr(limiter, "get_client", no_redis)
    monkeypatch.setattr(security, "get_security_config", lambda: {"enabled": True})
    monkeypatch.setattr(security, "rate_limit_fail_closed", lambda: False)

    results = await asyncio.gather(*(
        asyncio.to_thread(asyncio.run, limiter.check_rate_limit(f"tenant-{index}", quota_rpm=2))
        for index in range(128)
    ))
    assert all(result[0] for result in results)
    assert len(limiter._mem_buckets) == 32

    expired_bucket = next(iter(limiter._mem_buckets))
    limiter._mem_buckets[expired_bucket]["last_time"] -= 61
    await limiter.check_rate_limit("replacement", quota_rpm=2)
    assert expired_bucket not in limiter._mem_buckets
    assert len(limiter._mem_buckets) <= limiter._mem_max_buckets


@pytest.mark.asyncio
async def test_rate_limiter_uses_redis_again_after_recovery(monkeypatch):
    limiter = security.RedisTokenBucketRateLimiter()
    monkeypatch.setattr(security, "get_security_config", lambda: {"enabled": True})
    monkeypatch.setattr(security, "rate_limit_fail_closed", lambda: False)
    available = False

    class RecoveredRedis:
        async def eval(self, *args):
            return [1, 0, 5]

    async def client():
        return RecoveredRedis() if available else None

    monkeypatch.setattr(limiter, "get_client", client)
    assert (await limiter.check_rate_limit("same-key", 5))[0] is True
    assert limiter._mem_buckets["same-key"]["tokens"] == 4
    available = True
    assert (await limiter.check_rate_limit("same-key", 5))[0] is True
    assert limiter._mem_buckets["same-key"]["tokens"] == 4


@pytest.mark.asyncio
async def test_rate_limiter_fails_closed_on_transient_connect_error_and_recovers(monkeypatch):
    limiter = security.RedisTokenBucketRateLimiter()
    monkeypatch.setattr(security, "get_security_config", lambda: {"enabled": True})
    monkeypatch.setattr(security, "rate_limit_fail_closed", lambda: True)
    healthy = False

    class FlakyRedis:
        async def eval(self, *args):
            if not healthy:
                raise ConnectionError("Timeout connecting to server")
            return [1, 0, 99]

    async def client():
        return FlakyRedis()

    monkeypatch.setattr(limiter, "get_client", client)
    assert await limiter.check_rate_limit("bucket", quota_rpm=100) == (False, 5, 0.0)
    healthy = True
    assert await limiter.check_rate_limit("bucket", quota_rpm=100) == (True, 0, 99.0)


@pytest.mark.asyncio
async def test_rate_limiter_redis_client_uses_load_tolerant_timeouts(monkeypatch):
    captured = {}

    class DummyClient:
        async def aclose(self):
            pass

    def fake_from_url(url, **kwargs):
        captured.update(kwargs)
        return DummyClient()

    monkeypatch.setattr(security.aioredis, "from_url", fake_from_url)
    limiter = security.RedisTokenBucketRateLimiter()
    assert await limiter.get_client() is not None
    assert captured["socket_connect_timeout"] >= 2.0
    assert captured["socket_timeout"] >= 1.0


@pytest.mark.asyncio
async def test_concurrent_rate_limit_checks_with_redis_latency_are_not_rejected(monkeypatch):
    limiter = security.RedisTokenBucketRateLimiter()
    monkeypatch.setattr(security, "get_security_config", lambda: {"enabled": True})
    monkeypatch.setattr(security, "rate_limit_fail_closed", lambda: True)

    class SlowRedis:
        async def eval(self, *args):
            await asyncio.sleep(0.05)
            return [1, 0, 50]

    async def client():
        return SlowRedis()

    monkeypatch.setattr(limiter, "get_client", client)
    results = await asyncio.gather(*(
        limiter.check_rate_limit(f"concurrent-{index}", quota_rpm=100) for index in range(100)
    ))
    assert all(result[0] for result in results)
