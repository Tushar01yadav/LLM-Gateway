import pytest

from app.cost import ResponseCache


@pytest.mark.asyncio
async def test_cache_clear_removes_redis_and_local_entries(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.deleted = []

        async def scan_iter(self, match):
            assert match == "cache:resp:*"
            for key in ("cache:resp:a", "cache:resp:b"):
                yield key

        async def delete(self, *keys):
            self.deleted.extend(keys)

    cache = ResponseCache(redis_url="redis://unused")
    cache._local_cache["a"] = ({"text": "value"}, 9999999999)
    redis = FakeRedis()
    monkeypatch.setattr(cache, "get_client", lambda: redis)

    await cache.clear()

    assert cache._local_cache == {}
    assert redis.deleted == ["cache:resp:a", "cache:resp:b"]


@pytest.mark.asyncio
async def test_cache_returns_to_redis_after_recovery(monkeypatch):
    class RecoveredRedis:
        def __init__(self):
            self.values = {}

        async def set(self, key, value, ex):
            self.values[key] = value

        async def get(self, key):
            return self.values.get(key)

    cache = ResponseCache(redis_url="redis://unused")
    cache._local_cache["key"] = ({"text": "local"}, 9999999999)
    cache._local_cache["local-only"] = ({"text": "stale local"}, 9999999999)
    redis = RecoveredRedis()
    monkeypatch.setattr(cache, "get_client", lambda: redis)
    assert await cache.set("key", {"text": "redis"}) is True
    assert await cache.get("key") == {"text": "redis"}
    assert cache._local_cache["key"][0] == {"text": "local"}
    assert await cache.get("local-only") is None
