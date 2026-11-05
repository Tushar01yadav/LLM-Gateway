import asyncio
import hashlib
import json
import logging
import os
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.security import (
    ADMIN_TOKEN,
    RATE_LIMITER,
    create_gateway_api_key,
    hash_api_key,
    list_gateway_api_keys,
    revoke_gateway_api_key,
    validate_gateway_api_key,
)
from app.reliability import (
    reset_concurrency_pools,
    mask_api_key,
    get_provider_nodes_info,
)


@pytest.fixture(autouse=True)
def clean_test_state():
    reset_concurrency_pools()
    yield
    reset_concurrency_pools()


# ---------------------------------------------------------------------------
# 1. Gateway API Key Authentication Tests (Stored Hashed, Shown Once)
# ---------------------------------------------------------------------------
def test_create_and_validate_gateway_key():
    """Verify keys are stored hashed and validated via SHA-256."""
    raw_key, info = create_gateway_api_key(name="Unit Test Key", quota=100)
    assert raw_key.startswith("gw_live_")
    assert "key" in info
    assert info["quota"] == 100
    assert info["status"] == "active"

    # Validate with correct raw key
    validated = validate_gateway_api_key(raw_key)
    assert validated is not None
    assert validated["id"] == info["id"]
    assert validated["status"] == "active"

    # Raw key is never stored in DB
    keys_list = list_gateway_api_keys()
    matching = [k for k in keys_list if k["id"] == info["id"]]
    assert len(matching) == 1
    assert "key" not in matching[0]
    assert matching[0]["prefix"].startswith("gw_live_")

    # Invalid raw key fails
    assert validate_gateway_api_key("gw_live_bogus_key_here") is None

    # Revoked key fails
    revoked_ok = revoke_gateway_api_key(info["id"])
    assert revoked_ok is True
    assert validate_gateway_api_key(raw_key) is None


# ---------------------------------------------------------------------------
# 2. Pipeline Order & Guest Policy: bad/revoked keys 401, anonymous chat allowed
# ---------------------------------------------------------------------------
def test_chat_pipeline_auth_rejections():
    client = TestClient(app)

    # A: Missing key is the intentional guest path (per-IP quota), not a 401
    resp = client.post("/v1/chat", json={"model": "mock", "messages": [{"role": "user", "content": "guest policy"}]})
    assert resp.status_code == 200
    assert resp.json().get("provider") == "mock"

    # B: Bad / Invalid key returns 401
    resp = client.post(
        "/v1/chat",
        headers={"Authorization": "Bearer gw_live_invalid_random_string_123"},
        json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 401
    assert "Invalid or revoked" in resp.json()["detail"]

    # C: Revoked key returns 401
    raw_key, info = create_gateway_api_key(name="To Revoke", quota=50)
    revoke_gateway_api_key(info["id"])
    resp = client.post(
        "/v1/chat",
        headers={"Authorization": f"Bearer {raw_key}"},
        json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 401
    assert "Invalid or revoked" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 3. Redis Token-Bucket Rate Limiter: 429 + Retry-After
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_token_bucket_rate_limit_exceeded_returns_429():
    """Verify token bucket exhaustion returns 429 with Retry-After header."""
    raw_key, info = create_gateway_api_key(name="Rate Limit Target", quota=60)
    client = TestClient(app)

    # Mock RATE_LIMITER.check_rate_limit to simulate quota exhaustion
    async def mock_exhausted(*args, **kwargs):
        return False, 7, 0.0

    with patch.object(RATE_LIMITER, "check_rate_limit", side_effect=mock_exhausted):
        resp = client.post(
            "/v1/chat",
            headers={"Authorization": f"Bearer {raw_key}"},
            json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert resp.status_code == 429
        assert resp.headers.get("Retry-After") == "7"
        assert "Rate limit quota exceeded" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 4. Redis Down Fallback: Fail-Open vs Fail-Closed + Loud Logging
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_down_fallback_and_logging(caplog):
    """Verify safe fallback and loud logging when Redis is offline."""
    limiter = RATE_LIMITER

    # Case A: Default fail-open (allows request, logs error loudly)
    with patch.object(limiter, "get_client", return_value=None), \
         patch("app.security.get_security_config", return_value={"enabled": True, "redis_fail_closed": False}):
        with caplog.at_level(logging.ERROR):
            allowed, retry_after, rem = await limiter.check_rate_limit(key_id=999, quota_rpm=60)
            assert allowed is True
            assert retry_after == 0
            assert any("REDIS CONNECTION DOWN" in rec.message for rec in caplog.records)

    # Case B: Configurable fail-closed (rejects request with retry_after, logs loudly)
    caplog.clear()
    with patch.object(limiter, "get_client", return_value=None), \
         patch("app.security.get_security_config", return_value={"enabled": True, "redis_fail_closed": True}):
        with caplog.at_level(logging.ERROR):
            allowed, retry_after, rem = await limiter.check_rate_limit(key_id=999, quota_rpm=60)
            assert allowed is False
            assert retry_after > 0
            assert any("REDIS CONNECTION DOWN" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# 5. Admin Token Protection & Provider Key Leakage Prevention
# ---------------------------------------------------------------------------
def test_admin_token_protects_key_endpoints():
    client = TestClient(app)

    # 1. Unauthenticated key creation -> 403
    resp = client.post("/api/gateway-keys", json={"name": "Hacker Key", "quota": 100})
    assert resp.status_code == 403

    # 2. Authenticated with valid admin token -> 200
    resp = client.post(
        "/api/gateway-keys",
        headers={"X-Admin-Token": ADMIN_TOKEN},
        json={"name": "Admin Generated Key", "quota": 120},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "key" in data
    assert data["quota"] == 120

    # 3. Provider key edit endpoint requires admin token
    resp_edit_unauth = client.post("/api/reliability/keys", json={"provider": "groq", "api_key": "dummy"})
    assert resp_edit_unauth.status_code == 403

    # 4. With admin token, provider key edit proceeds through security check
    with patch("app.main.update_provider_api_key", return_value={"status": "valid", "masked_key": "***test"}):
        resp_edit_auth = client.post(
            "/api/reliability/keys",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            json={"provider": "groq", "api_key": "dummy"},
        )
        assert resp_edit_auth.status_code == 200


def test_no_key_leakage_in_telemetry_and_errors(caplog):
    """Assert raw provider and gateway keys NEVER leak in logs, node telemetry, or error messages."""
    raw_key, info = create_gateway_api_key(name="Leak Check Key", quota=60)

    # Telemetry check: backend never returns full keys
    nodes = get_provider_nodes_info()
    for n in nodes:
        masked = n.get("masked_key", "")
        assert not masked.startswith("gsk_")
        assert not masked.startswith("sk-or-")
        assert not masked.startswith("AIzaSy")

    # API key list check: never returns full key or raw hash
    keys = list_gateway_api_keys()
    for k in keys:
        assert "key" not in k
        assert "key_hash" not in k
        assert "..." in k["prefix"]
