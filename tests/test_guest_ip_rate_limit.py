"""
Regression tests for the anonymous guest quota bypass.

An anonymous caller can discard the ``llm_gateway_guest_id`` cookie, receive a
freshly minted identity on the next request, and previously got a fresh rate
limit bucket (and budget tenant) every time. These tests pin the fix: anonymous
requests must satisfy both the per-guest bucket and a bucket scoped to the
client IP resolved through the trusted-proxy configuration.
"""

import asyncio
import os
import types
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

import app.security as security
from app.auth import db_create_user
from app.main import app
from app.security import (
    GUEST_COOKIE_NAME,
    RATE_LIMITER,
    RedisTokenBucketRateLimiter,
    create_gateway_api_key,
    generate_guest_identity,
)

PEER_IP = "127.0.0.1"


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _payload(text: str = "anonymous quota probe") -> dict:
    return {"model": "mock", "messages": [{"role": "user", "content": text}]}


async def _chat(client: AsyncClient, **kwargs):
    return await client.post("/v1/chat", json=_payload(), **kwargs)


def _force_memory_limiter(monkeypatch):
    """Pin the limiter to its in-memory path so buckets are inspectable."""

    async def _no_client():
        return None

    monkeypatch.setattr(RATE_LIMITER, "get_client", _no_client)


async def _drain(bucket: str, quota: int = 60, leave: float = 0.0):
    """Spend a bucket through the limiter, leaving `leave` tokens behind."""
    allowed, _, remaining = await RATE_LIMITER.check_rate_limit(
        bucket, quota_rpm=quota, cost=0.0
    )
    assert allowed is True
    # Stay a hair under the reported balance: the value is rounded to 2dp and
    # could otherwise exceed the real token count.
    spend = max(0.0, remaining - leave - 0.01)
    allowed, _, remaining = await RATE_LIMITER.check_rate_limit(
        bucket, quota_rpm=quota, cost=spend
    )
    assert allowed is True
    return remaining


@pytest.mark.asyncio
async def test_anonymous_request_consumes_guest_and_ip_buckets(monkeypatch):
    """Both limits are charged on an anonymous request, even with a valid identity."""
    _force_memory_limiter(monkeypatch)
    guest_id = generate_guest_identity()

    async with _client() as client:
        response = await _chat(client, headers={"X-Guest-ID": guest_id})

    assert response.status_code == 200, response.text
    assert response.headers["X-Guest-ID"] == guest_id
    assert f"guest:{guest_id}" in RATE_LIMITER._mem_buckets
    assert f"ip:{PEER_IP}" in RATE_LIMITER._mem_buckets
    assert RATE_LIMITER._mem_buckets[f"guest:{guest_id}"]["tokens"] < 60
    assert RATE_LIMITER._mem_buckets[f"ip:{PEER_IP}"]["tokens"] < 60


@pytest.mark.asyncio
async def test_discarded_guest_cookies_share_one_ip_bucket(monkeypatch):
    """Rotating identities by dropping Set-Cookie cannot reset the IP quota."""
    _force_memory_limiter(monkeypatch)
    await _drain(f"ip:{PEER_IP}")

    guest_ids = []
    for _ in range(3):
        # A new client per request discards every Set-Cookie, so each call is
        # a newly minted guest identity.
        async with _client() as client:
            response = await _chat(client)
        assert response.status_code == 429, response.text
        assert "Anonymous client IP rate limit exceeded" in response.json()["detail"]
        assert response.headers["Retry-After"]
        guest_ids.append(response.headers["X-Guest-ID"])

    assert len(set(guest_ids)) == 3


@pytest.mark.asyncio
async def test_rotating_valid_guest_identities_cannot_bypass_ip_limit(monkeypatch):
    """Distinct, correctly signed guest identities still share the IP bucket."""
    _force_memory_limiter(monkeypatch)
    await _drain(f"ip:{PEER_IP}")

    async with _client() as client:
        for _ in range(3):
            response = await _chat(
                client, headers={"X-Guest-ID": generate_guest_identity()}
            )
            assert response.status_code == 429, response.text
            assert "Anonymous client IP rate limit exceeded" in response.json()["detail"]

    # Rejected requests still consumed their per-guest bucket before the IP
    # check, proving both limits are evaluated.
    guest_buckets = [b for b in RATE_LIMITER._mem_buckets if b.startswith("guest:")]
    assert len(guest_buckets) == 3


@pytest.mark.asyncio
async def test_untrusted_proxy_ignores_spoofed_forwarded_for(monkeypatch):
    """With no trusted proxies, X-Forwarded-For cannot select a fresh bucket."""
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "0")
    _force_memory_limiter(monkeypatch)
    await _drain(f"ip:{PEER_IP}")

    async with _client() as client:
        response = await _chat(client, headers={"X-Forwarded-For": "203.0.113.9"})

    assert response.status_code == 429, response.text
    assert "Anonymous client IP rate limit exceeded" in response.json()["detail"]
    assert "ip:203.0.113.9" not in RATE_LIMITER._mem_buckets


@pytest.mark.asyncio
async def test_trusted_proxy_uses_appended_client_ip(monkeypatch):
    """With one trusted proxy, only the proxy-appended (right-most) IP is used."""
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "1")
    _force_memory_limiter(monkeypatch)

    async with _client() as client:
        first = await _chat(client, headers={"X-Forwarded-For": "198.51.100.23"})
        assert first.status_code == 200, first.text
        assert "ip:198.51.100.23" in RATE_LIMITER._mem_buckets

        await _drain("ip:198.51.100.23")

        # A forged left-most entry does not help: the right-most hop is used.
        blocked = await _chat(
            client, headers={"X-Forwarded-For": "203.0.113.9, 198.51.100.23"}
        )
        assert blocked.status_code == 429, blocked.text
        assert "Anonymous client IP rate limit exceeded" in blocked.json()["detail"]

    # A different real client behind the same proxy has its own bucket.
    async with _client() as other_client:
        other = await _chat(
            other_client, headers={"X-Forwarded-For": "198.51.100.24"}
        )
    assert other.status_code == 200, other.text


@pytest.mark.asyncio
async def test_guest_specific_limit_is_preserved(monkeypatch):
    """The per-guest bucket still rejects on its own, before the IP check runs."""
    _force_memory_limiter(monkeypatch)
    guest_id = generate_guest_identity()
    await _drain(f"guest:{guest_id}")

    async with _client() as client:
        response = await _chat(client, headers={"X-Guest-ID": guest_id})

    assert response.status_code == 429, response.text
    detail = response.json()["detail"]
    assert "Rate limit quota exceeded for key" in detail
    assert "Anonymous client IP" not in detail
    # The guest check rejected the request, so the IP bucket was never touched.
    assert f"ip:{PEER_IP}" not in RATE_LIMITER._mem_buckets


@pytest.mark.asyncio
async def test_api_key_and_session_requests_skip_ip_bucket(monkeypatch):
    """Authenticated callers keep their existing per-key/per-user behavior."""
    _force_memory_limiter(monkeypatch)
    await _drain(f"ip:{PEER_IP}")

    raw_key, key_info = create_gateway_api_key(
        name=f"ip-scope-key-{uuid.uuid4().hex[:8]}", quota=60, daily_budget=10.0
    )
    async with _client() as client:
        key_response = await _chat(
            client, headers={"Authorization": f"Bearer {raw_key}"}
        )
    assert key_response.status_code == 200, key_response.text
    assert f"key:{key_info['id']}" in RATE_LIMITER._mem_buckets
    assert RATE_LIMITER._mem_buckets[f"ip:{PEER_IP}"]["tokens"] < 1.0

    email = f"ip-scope-{uuid.uuid4().hex[:10]}@example.com"
    db_create_user(
        name="IP Scope User",
        email=email,
        password="secret123",
        auth_provider="local",
        is_verified=True,
    )
    async with _client() as user_client:
        login = await user_client.post(
            "/api/auth/login", json={"email": email, "password": "secret123"}
        )
        assert login.status_code == 200, login.text
        user_id = login.json()["user"]["id"]
        user_response = await _chat(user_client)

    assert user_response.status_code == 200, user_response.text
    assert f"user:{user_id}" in RATE_LIMITER._mem_buckets
    assert RATE_LIMITER._mem_buckets[f"ip:{PEER_IP}"]["tokens"] < 1.0


@pytest.mark.asyncio
async def test_concurrent_requests_cannot_over_admit_last_ip_token(monkeypatch):
    """Ten concurrent identities racing for one IP token yield exactly one call."""
    _force_memory_limiter(monkeypatch)
    # Freeze the limiter clock so refill is deterministic for this race.
    monkeypatch.setattr(security, "time", types.SimpleNamespace(time=lambda: 1000.0))
    await _drain(f"ip:{PEER_IP}", leave=1.0)

    async def _attempt():
        async with _client() as client:
            return await _chat(
                client, headers={"X-Guest-ID": generate_guest_identity()}
            )

    responses = await asyncio.gather(*[_attempt() for _ in range(10)])

    allowed = [r for r in responses if r.status_code == 200]
    rejected = [r for r in responses if r.status_code == 429]
    assert len(allowed) == 1
    assert len(rejected) == 9
    assert all(
        "Anonymous client IP rate limit exceeded" in r.json()["detail"]
        for r in rejected
    )


@pytest.mark.asyncio
async def test_response_cache_stays_scoped_per_guest_not_per_ip(monkeypatch):
    """Sharing an IP must not leak cached responses between guest identities."""
    _force_memory_limiter(monkeypatch)
    prompt = f"cache-scope-{uuid.uuid4().hex}"
    payload = {"model": "mock", "messages": [{"role": "user", "content": prompt}]}

    async with _client() as guest_a, _client() as guest_b:
        first = await guest_a.post("/v1/chat", json=payload)
        second = await guest_a.post("/v1/chat", json=payload)
        other = await guest_b.post("/v1/chat", json=payload)

    assert first.status_code == second.status_code == other.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert other.json()["cached"] is False


@pytest.mark.asyncio
async def test_redis_and_in_memory_limits_agree(monkeypatch):
    """Both limiter backends reject the same over-budget sequence, end to end."""
    if os.getenv("GATEWAY_TEST_DISABLE_REDIS") == "1":
        pytest.skip("Redis disabled for this test run")
    redis_limiter = RedisTokenBucketRateLimiter()
    client = await redis_limiter.get_client()
    if client is None:
        pytest.skip("Redis rate-limit backend unavailable")

    try:
        await client.ping()
    except Exception as exc:
        await redis_limiter.aclose()
        if os.getenv("GATEWAY_REQUIRE_REDIS_TESTS") == "1":
            pytest.fail(
                f"Required Redis integration service is unavailable: {type(exc).__name__}"
            )
        pytest.skip("Redis integration service is unavailable")

    mem_limiter = RedisTokenBucketRateLimiter()

    async def _no_client():
        return None

    mem_limiter.get_client = _no_client

    bucket = f"consistency:{uuid.uuid4().hex}"
    try:
        for limiter in (mem_limiter, redis_limiter):
            assert (await limiter.check_rate_limit(bucket, quota_rpm=10, cost=10))[0] is True
            assert (await limiter.check_rate_limit(bucket, quota_rpm=10, cost=1))[0] is False
            assert (
                await limiter.check_rate_limit(f"{bucket}:fresh", quota_rpm=10, cost=1)
            )[0] is True

        # End-to-end through the endpoint on the Redis backend.
        RATE_LIMITER._redis_client = None
        RATE_LIMITER._last_redis_fail = 0.0
        await client.delete(
            f"gw:rate:ip:{PEER_IP}:tokens", f"gw:rate:ip:{PEER_IP}:last_time"
        )
        await _drain(f"ip:{PEER_IP}")
        assert await client.exists(f"gw:rate:ip:{PEER_IP}:tokens") == 1

        async with _client() as http_client:
            blocked = await _chat(http_client)
        assert blocked.status_code == 429, blocked.text
        assert "Anonymous client IP rate limit exceeded" in blocked.json()["detail"]
    finally:
        keys = [f"gw:rate:{bucket}:tokens", f"gw:rate:{bucket}:last_time"]
        keys += [f"gw:rate:{bucket}:fresh:tokens", f"gw:rate:{bucket}:fresh:last_time"]
        keys += [f"gw:rate:ip:{PEER_IP}:tokens", f"gw:rate:ip:{PEER_IP}:last_time"]
        await client.delete(*keys)
        await redis_limiter.aclose()


@pytest.mark.asyncio
async def test_invalid_guest_cookie_is_rejected_without_touching_ip_bucket(monkeypatch):
    """A forged cookie still gets 403; it can neither bypass nor reset quota."""
    _force_memory_limiter(monkeypatch)
    await _drain(f"ip:{PEER_IP}")

    async with _client() as client:
        response = await _chat(
            client,
            cookies={GUEST_COOKIE_NAME: "guest_" + "0" * 32 + "_" + "f" * 16},
        )

    assert response.status_code == 403
    assert "Invalid or forged guest identity" in response.json()["detail"]
    assert RATE_LIMITER._mem_buckets[f"ip:{PEER_IP}"]["tokens"] < 1.0
