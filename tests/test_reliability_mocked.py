import asyncio
import json
import os
import tempfile
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.reliability import (
    ADAPTER_MAP,
    CIRCUIT_BREAKERS,
    BaseAdapter,
    CircuitBreaker,
    GeminiAdapter,
    GroqAdapter,
    OpenRouterAdapter,
    classify_error,
    execute_reliable_completion,
    load_models_config,
    resolve_fallback_chain,
)


@pytest.fixture(autouse=True)
def reset_breakers():
    """Reset all circuit breakers before and after each test."""
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()
    yield
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()


# ---------------------------------------------------------------------------
# 1. Test: models.json is the Only Routing Config (Single Source of Truth)
# ---------------------------------------------------------------------------
def test_models_json_sole_source_of_truth():
    """
    Ensure routing config is loaded dynamically and exclusively from models.json.
    Creating a temporary models.json alters routing without any Python code change.
    """
    config = load_models_config()
    assert isinstance(config, dict)
    assert "fast" in config
    assert "smart" in config
    assert "cheap" in config
    assert "complex" not in config

    # Custom routing JSON to prove single source of truth
    custom_routing = {
        "custom-lane": {
            "provider": "openrouter",
            "model": "meta-llama/llama-3.3-70b-instruct",
            "fallback": [
                {"provider": "groq", "model": "llama-3.3-70b-versatile"}
            ]
        }
    }

    with tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False) as tf:
        json.dump(custom_routing, tf)
        temp_path = tf.name

    try:
        with patch("app.reliability.REGISTRY_FILE", temp_path):
            reloaded = load_models_config()
            assert "custom-lane" in reloaded
            chain = resolve_fallback_chain("custom-lane")
            assert len(chain) == 2
            assert chain[0] == {"provider": "openrouter", "model": "meta-llama/llama-3.3-70b-instruct"}
            assert chain[1] == {"provider": "groq", "model": "llama-3.3-70b-versatile"}
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# ---------------------------------------------------------------------------
# 2. Test: Provider Adapters Wired into the Reliability Path
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_adapters_wired_in_reliability_path():
    """
    Ensure GroqAdapter, OpenRouterAdapter, and GeminiAdapter are subclasses of
    BaseAdapter and are directly invoked by execute_reliable_completion via ADAPTER_MAP.
    """
    assert issubclass(GroqAdapter, BaseAdapter)
    assert issubclass(OpenRouterAdapter, BaseAdapter)
    assert issubclass(GeminiAdapter, BaseAdapter)

    assert "groq" in ADAPTER_MAP
    assert "openrouter" in ADAPTER_MAP
    assert "gemini" in ADAPTER_MAP

    mock_response = {
        "text": "Wired adapter test response",
        "finish_reason": "stop",
        "token_usage": {"prompt_tokens": 12, "completion_tokens": 6, "total_tokens": 18},
    }

    with patch.object(ADAPTER_MAP["groq"], "generate", new=AsyncMock(return_value=mock_response)) as mock_groq:
        result = await execute_reliable_completion(
            target="fast",
            messages=[{"role": "user", "content": "Hello adapter"}],
            temperature=0.5,
            max_tokens=100,
        )

        assert mock_groq.called
        assert result["text"] == "Wired adapter test response"
        assert result["provider"] == "groq"
        assert result["fallback_used"] is False
        assert result["token_usage"]["total_tokens"] == 18


# ---------------------------------------------------------------------------
# 3. Test: Circuit Breaker Ignores Non-Retryable Failures
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_breaker_ignores_non_retryable_errors():
    """
    Verify that 400 Bad Request, 401 Unauthorized, 403 Forbidden, 404, 422
    do NOT increment consecutive_failures and do NOT trip the breaker to OPEN.
    """
    cb = CircuitBreaker("test_provider", failure_threshold=5, cooldown_seconds=60.0)
    assert cb.state == "CLOSED"

    # Simulate 10 non-retryable failures (400, 401, 403, 404, 422)
    for code in [400, 401, 403, 404, 422, 400, 401, 403, 404, 422]:
        is_retryable, err_type = classify_error(code)
        assert is_retryable is False
        await cb.record_failure(is_retryable=is_retryable)

    # Circuit breaker MUST remain CLOSED and consecutive_failures must be 0
    assert cb.state == "CLOSED"
    assert cb.consecutive_failures == 0
    assert cb.total_failures == 0

    can_exec, reason = await cb.can_execute()
    assert can_exec is True
    assert reason == "closed_healthy"


# ---------------------------------------------------------------------------
# 4. Test: Circuit Breaker Counts Only Retryable Failures and Trips
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_breaker_counts_retryable_failures_and_trips():
    """
    Verify that 5xx, 429, timeouts, and network errors DO increment consecutive_failures
    and trip the breaker to OPEN after reaching the failure threshold (5).
    """
    cb = CircuitBreaker("test_provider", failure_threshold=5, cooldown_seconds=60.0)

    # Retryable errors: 500, 502, 503, 429, ReadTimeout
    retryable_errors = [
        (500, None),
        (502, None),
        (503, None),
        (429, None),
    ]

    for code, exc in retryable_errors:
        is_retryable, _ = classify_error(code, exc)
        assert is_retryable is True
        await cb.record_failure(is_retryable=is_retryable)
        assert cb.state == "CLOSED"

    assert cb.consecutive_failures == 4

    # 5th retryable failure: timeout
    is_retryable, _ = classify_error(None, httpx.ReadTimeout("Timeout"))
    assert is_retryable is True
    await cb.record_failure(is_retryable=is_retryable)

    # Now breaker MUST trip to OPEN
    assert cb.state == "OPEN"
    assert cb.consecutive_failures == 5
    assert cb.total_failures == 5

    # Any new call should be denied during cooldown
    can_exec, reason = await cb.can_execute()
    assert can_exec is False
    assert "circuit_open" in reason


# ---------------------------------------------------------------------------
# 5. Test: Automatic Fallback on Primary Retryable Failure
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_fallback_chain_execution_mocked():
    """
    Verify that when primary provider encounters retryable 503 error,
    it automatically falls back to the secondary provider in the chain.
    """
    req = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
    resp_503 = httpx.Response(status_code=503, text="Service Unavailable", request=req)
    groq_error = httpx.HTTPStatusError("Groq 503", request=req, response=resp_503)

    openrouter_success = {
        "text": "OpenRouter Fallback Success",
        "finish_reason": "stop",
        "token_usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }

    with patch.object(ADAPTER_MAP["groq"], "generate", new=AsyncMock(side_effect=groq_error)), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", new=AsyncMock(return_value=openrouter_success)):

        result = await execute_reliable_completion(
            target="fast",  # fast chain: groq -> openrouter -> gemini
            messages=[{"role": "user", "content": "Test fallback"}],
            max_retries_per_provider=0,  # 1 attempt before fallback
        )

        assert result["text"] == "OpenRouter Fallback Success"
        assert result["provider"] == "openrouter"
        assert result["fallback_used"] is True
        assert len(result["attempts"]) == 2
        assert result["attempts"][0]["provider"] == "groq"
        assert result["attempts"][0]["status"] == "error"
        assert result["attempts"][1]["provider"] == "openrouter"
        assert result["attempts"][1]["status"] == "success"


# ---------------------------------------------------------------------------
# 6. Test: Non-Retryable Error Aborts Retries and Immediately Falls Back
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_non_retryable_error_aborts_retries():
    """
    Verify that a client error (e.g. 401 Unauthorized) immediately moves to the
    fallback provider without wasting retries or tripping the circuit breaker.
    """
    groq_cb = CIRCUIT_BREAKERS["groq"]
    assert groq_cb.consecutive_failures == 0

    req = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
    resp_401 = httpx.Response(status_code=401, text="Invalid API Key", request=req)
    groq_401 = httpx.HTTPStatusError("Groq 401", request=req, response=resp_401)

    openrouter_success = {
        "text": "OpenRouter rescued the request",
        "finish_reason": "stop",
        "token_usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
    }

    with patch.object(ADAPTER_MAP["groq"], "generate", new=AsyncMock(side_effect=groq_401)), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", new=AsyncMock(return_value=openrouter_success)):

        result = await execute_reliable_completion(
            target="fast",
            messages=[{"role": "user", "content": "Test 401 abort"}],
            max_retries_per_provider=2,  # Configured for 2 retries, but 401 must abort immediately!
        )

        assert result["text"] == "OpenRouter rescued the request"
        assert result["provider"] == "openrouter"
        # Groq should only have had 1 attempt because 401 is not retryable
        groq_attempts = [a for a in result["attempts"] if a["provider"] == "groq"]
        assert len(groq_attempts) == 1
        assert groq_attempts[0]["error_type"] == "client_error_401"

        # Breaker must NOT have counted the 401 failure
        assert groq_cb.consecutive_failures == 0
        assert groq_cb.state == "CLOSED"


# ---------------------------------------------------------------------------
# 7. Test: All Providers Fail Raises HTTP 502 with Summary
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_all_providers_fail_raises_502():
    """
    Verify that when all providers in the chain fail, execute_reliable_completion
    raises httpx.HTTPStatusError with 502 status code and detailed attempts summary.
    """
    req = httpx.Request("POST", "https://api.example.com")
    resp_500 = httpx.Response(status_code=500, text="Internal Error", request=req)
    err = httpx.HTTPStatusError("500", request=req, response=resp_500)

    with patch.object(ADAPTER_MAP["groq"], "generate", new=AsyncMock(side_effect=err)), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", new=AsyncMock(side_effect=err)), \
         patch.object(ADAPTER_MAP["gemini"], "generate", new=AsyncMock(side_effect=err)):

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "Fail test"}],
                max_retries_per_provider=0,
            )

        assert exc_info.value.response.status_code == 502
        assert "All LLM providers failed" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 8. Test: No Stale Models in Config
# ---------------------------------------------------------------------------
def test_no_stale_models_in_config():
    """
    Verify models.json contains active models and no stale or non-existent models.
    """
    config = load_models_config()
    stale_names = [
        "gemini-1.5-flash",
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite",
        "qwen/qwen3.8-27b",
        "openai/gpt-oss-20b",
    ]

    for key, val in config.items():
        if key == "concurrency":
            continue
        assert key not in stale_names, f"Stale key '{key}' found in models.json"
        assert val.get("model") not in stale_names, f"Stale model '{val.get('model')}' found in models.json"
        for fb in val.get("fallback", []):
            assert fb.get("model") not in stale_names, f"Stale fallback model '{fb.get('model')}' found in models.json"
