import asyncio
import json
import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.reliability import (
    CIRCUIT_BREAKERS,
    INJECTED_FAILURES,
    NODE_METRICS,
    TRAFFIC_BROADCASTER,
    execute_reliable_completion,
    get_provider_nodes_info,
    mask_api_key,
    set_failure_injection,
    update_provider_api_key,
    validate_candidate_key,
)


@pytest.fixture(autouse=True)
def clean_state():
    """Reset circuit breakers and failure injections before and after each test."""
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()
    for prov in INJECTED_FAILURES:
        set_failure_injection(prov, enabled=False)
    yield
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()
    for prov in INJECTED_FAILURES:
        set_failure_injection(prov, enabled=False)


# ---------------------------------------------------------------------------
# 1. Test Key Masking (Backend Never Returns Full Keys)
# ---------------------------------------------------------------------------
def test_key_masking_security():
    """Ensure keys are masked to ***yd and full keys are never exposed."""
    assert mask_api_key("gsk_abc12345yd") == "***45yd"
    assert mask_api_key("sk-or-v1-mykey99") == "***ey99"
    assert mask_api_key("AIzaSy1234") == "***1234"
    assert mask_api_key("") == "Not Set"
    assert mask_api_key(None) == "Not Set"

    nodes = get_provider_nodes_info()
    assert len(nodes) == 3
    for node in nodes:
        assert "provider" in node
        assert "masked_key" in node
        masked = node["masked_key"]
        if masked != "Not Set":
            assert masked.startswith("***")
            # Ensure full raw key from os.environ is NOT exposed
            raw_key = os.getenv(f"{node['provider'].upper()}_API_KEY", "")
            if len(raw_key) > 6:
                assert masked != raw_key


# ---------------------------------------------------------------------------
# 2. Test Key Validation Call (Valid vs Rejected)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_validate_candidate_key():
    """Test lightweight validation test call for candidate keys."""
    # Empty key must be rejected
    is_valid, msg = await validate_candidate_key("groq", "   ")
    assert is_valid is False
    assert "cannot be empty" in msg

    # Mock valid Groq response (200 OK)
    req = httpx.Request("GET", "https://api.groq.com/openai/v1/models")
    resp_200 = httpx.Response(status_code=200, json={"data": []}, request=req)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=resp_200)):
        is_val, msg = await validate_candidate_key("groq", "gsk_valid_mock_key")
        assert is_val is True
        assert "Valid" in msg

    # Mock invalid Groq response (401 Unauthorized)
    resp_401 = httpx.Response(status_code=401, json={"error": "Invalid API key"}, request=req)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=resp_401)):
        is_val, msg = await validate_candidate_key("groq", "gsk_bad_mock_key")
        assert is_val is False
        assert "Unauthorized" in msg


# ---------------------------------------------------------------------------
# 3. Test Key Update & Breaker Reset Flow
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_key_update_resets_breaker_and_applies_runtime():
    """Test valid key updates os.environ immediately and resets tripped circuit breaker."""
    groq_cb = CIRCUIT_BREAKERS["groq"]
    orig_key = os.getenv("GROQ_API_KEY", "")

    # Trip breaker
    groq_cb.state = "OPEN"
    groq_cb.consecutive_failures = 5

    # Mock test call to succeed
    req = httpx.Request("GET", "https://api.groq.com/openai/v1/models")
    resp_200 = httpx.Response(status_code=200, json={"data": []}, request=req)

    try:
        with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=resp_200)):
            result = await update_provider_api_key("groq", "gsk_new_working_key_9999")

            assert result["status"] == "valid"
            assert result["masked_key"].startswith("***")
            # Runtime key updated without restart
            assert os.getenv("GROQ_API_KEY") == "gsk_new_working_key_9999"
            # Breaker reset to CLOSED
            assert groq_cb.state == "CLOSED"
            assert groq_cb.consecutive_failures == 0
    finally:
        # Restore original valid key
        if orig_key:
            with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=resp_200)):
                await update_provider_api_key("groq", orig_key)


# ---------------------------------------------------------------------------
# 4. Test Key Update Security (Admin or Localhost Enforcement)
# ---------------------------------------------------------------------------
def test_key_endpoint_security_rejection():
    """Ensure remote unauthenticated callers cannot modify provider keys."""
    client = TestClient(app)

    # Remote untrusted client (non-localhost) without auth
    response = client.post(
        "/api/reliability/keys",
        json={"provider": "groq", "api_key": "hacked_key"},
        headers={"X-Forwarded-For": "198.51.100.1"},
    )
    assert response.status_code == 403
    assert "Forbidden" in response.text


# ---------------------------------------------------------------------------
# 5. Test Failure Injection Toggle & Fallback Trigger
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_failure_injection_and_fallback():
    """Verify demo failure injection triggers red dot error and fallback."""
    # Break Groq
    set_failure_injection("groq", enabled=True, mode="503")

    openrouter_success = {
        "text": "Fallback rescued by OpenRouter",
        "finish_reason": "stop",
        "token_usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
    }

    from app.reliability import ADAPTER_MAP

    with patch.object(ADAPTER_MAP["openrouter"], "generate", new=AsyncMock(return_value=openrouter_success)):
        res = await execute_reliable_completion(
            target="fast",
            messages=[{"role": "user", "content": "Ping"}],
            max_retries_per_provider=0,
            request_id="test_injection_req",
        )

        assert res["fallback_used"] is True
        assert res["provider"] == "openrouter"
        assert res["text"] == "Fallback rescued by OpenRouter"

    # Reset failure injection
    set_failure_injection("groq", enabled=False)


# ---------------------------------------------------------------------------
# 6. Test SSE Traffic Event Broadcasting
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_traffic_event_broadcasting():
    """Verify reliable completion broadcasts all SSE event types."""
    q = await TRAFFIC_BROADCASTER.subscribe()

    mock_resp = {
        "text": "SSE broadcast test",
        "finish_reason": "stop",
        "token_usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
    }

    from app.reliability import ADAPTER_MAP

    with patch.object(ADAPTER_MAP["groq"], "generate", new=AsyncMock(return_value=mock_resp)):
        await execute_reliable_completion(
            target="fast",
            messages=[{"role": "user", "content": "Hello"}],
            max_retries_per_provider=0,
            request_id="req_sse_test_123",
        )

    # Collect broadcasted events
    events = []
    while not q.empty():
        events.append(q.get_nowait())

    await TRAFFIC_BROADCASTER.unsubscribe(q)

    event_names = [e["event"] for e in events]
    assert "request_started" in event_names
    assert "attempt_started" in event_names
    assert "request_served" in event_names

    start_event = next(e for e in events if e["event"] == "request_started")
    assert start_event["data"]["request_id"] == "req_sse_test_123"
    assert start_event["data"]["target"] == "fast"


# ---------------------------------------------------------------------------
# 7. Test Provider Nodes Telemetry Endpoint
# ---------------------------------------------------------------------------
def test_provider_nodes_endpoint():
    """Ensure /api/reliability/provider-nodes returns telemetry without full keys."""
    client = TestClient(app)
    resp = client.get("/api/reliability/provider-nodes")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 3

    providers = {item["provider"] for item in data}
    assert providers == {"groq", "openrouter", "gemini"}

    for item in data:
        assert "masked_key" in item
        assert "circuit_breaker" in item
        assert "failure_injection" in item
        assert "metrics" in item
        assert "state" in item["circuit_breaker"]
