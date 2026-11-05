import asyncio
import os
import time
import httpx
import pytest
from dotenv import load_dotenv

load_dotenv()

from app.reliability import (
    classify_error,
    CircuitBreaker,
    CIRCUIT_BREAKERS,
    resolve_fallback_chain,
    execute_reliable_completion,
    load_models_config,
)


# ---------------------------------------------------------------------------
# 1. Test Error Classification
# ---------------------------------------------------------------------------
def test_error_classification():
    # Retryable errors
    is_ret, err_type = classify_error(429)
    assert is_ret is True
    assert "429" in err_type

    for code in (500, 502, 503, 504):
        is_ret, err_type = classify_error(code)
        assert is_ret is True, f"Code {code} should be retryable"
        assert str(code) in err_type

    is_ret, err_type = classify_error(None, httpx.ReadTimeout("timeout"))
    assert is_ret is True
    assert err_type == "timeout"

    is_ret, err_type = classify_error(None, httpx.ConnectError("connection refused"))
    assert is_ret is True
    assert err_type == "network_error"

    # Not retryable errors
    for code in (400, 401, 403, 404, 422):
        is_ret, err_type = classify_error(code)
        assert is_ret is False, f"Code {code} should NOT be retryable"
        assert str(code) in err_type


# ---------------------------------------------------------------------------
# 2. Test Fallback Chains Registry & Fast Alias Hits Groq First
# ---------------------------------------------------------------------------
def test_alias_fast_hits_groq_first():
    """
    Proves that alias 'fast' priority chain hits Groq first.
    Only fast / smart / cheap remain; complex is removed.
    """
    config = load_models_config()
    assert "fast" in config
    assert "smart" in config
    assert "cheap" in config
    assert "complex" not in config

    fast_chain = resolve_fallback_chain("fast")
    assert len(fast_chain) == 3
    assert fast_chain[0]["provider"] == "groq"
    assert fast_chain[0]["model"] == "openai/gpt-oss-120b"
    assert fast_chain[1]["provider"] == "openrouter"
    assert fast_chain[2]["provider"] == "gemini"


def test_fallback_chains_registry():
    fast_chain = resolve_fallback_chain("fast")
    assert len(fast_chain) == 3
    assert fast_chain[0]["provider"] == "groq"
    assert fast_chain[1]["provider"] == "openrouter"
    assert fast_chain[2]["provider"] == "gemini"

    smart_chain = resolve_fallback_chain("smart")
    assert len(smart_chain) == 3
    assert smart_chain[0]["provider"] == "gemini"

    cheap_chain = resolve_fallback_chain("cheap")
    assert len(cheap_chain) == 3
    assert cheap_chain[0]["provider"] == "openrouter"

    # Direct model mapping test
    groq_model_chain = resolve_fallback_chain("openai/gpt-oss-120b")
    assert groq_model_chain[0]["provider"] == "groq"
    assert groq_model_chain[1]["provider"] == "openrouter"

    openrouter_model_chain = resolve_fallback_chain("meta-llama/llama-3.3-70b-instruct")
    assert openrouter_model_chain[0]["provider"] == "openrouter"
    assert openrouter_model_chain[1]["provider"] == "groq"

    gemini_model_chain = resolve_fallback_chain("gemini-2.5-flash")
    assert gemini_model_chain[0]["provider"] == "gemini"
    assert gemini_model_chain[1]["provider"] == "groq"


def test_mock_failover_lane_and_bulkhead():
    """Chaos fallback uses mock -> mock2; the fallback provider needs its own pool."""
    from app.reliability import ADAPTER_MAP, PROVIDER_POOLS

    chain = resolve_fallback_chain("mock_failover")
    assert chain == [
        {"provider": "mock", "model": "mock-primary"},
        {"provider": "mock2", "model": "mock-fallback"},
    ]
    assert "mock2" in ADAPTER_MAP
    assert "mock2" in PROVIDER_POOLS
    assert PROVIDER_POOLS["mock2"].max_inflight == 20


# ---------------------------------------------------------------------------
# 3. Test Circuit Breaker Logic
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_circuit_breaker_transitions():
    cb = CircuitBreaker("test_provider", failure_threshold=5, cooldown_seconds=2.0)
    assert cb.state == "CLOSED"

    # 4 failures should remain CLOSED
    for _ in range(4):
        await cb.record_failure()
        assert cb.state == "CLOSED"

    # 5th failure trips to OPEN
    await cb.record_failure()
    assert cb.state == "OPEN"
    assert cb.consecutive_failures == 5

    # Should deny execution during cooldown
    can_exec, reason = await cb.can_execute()
    assert can_exec is False
    assert "circuit_open" in reason

    # Wait for cooldown (2 seconds)
    await asyncio.sleep(2.1)

    # After cooldown, transitions to HALF_OPEN (probe allowed)
    can_exec, reason = await cb.can_execute()
    assert can_exec is True
    assert cb.state == "HALF_OPEN"
    assert reason == "half_open_probe"

    # If probe succeeds -> reverts to CLOSED
    await cb.record_success()
    assert cb.state == "CLOSED"
    assert cb.consecutive_failures == 0

    # Test failure during HALF_OPEN trips immediately back to OPEN
    for _ in range(5):
        await cb.record_failure()
    assert cb.state == "OPEN"

    await asyncio.sleep(2.1)
    await cb.can_execute() # Transitions to HALF_OPEN
    assert cb.state == "HALF_OPEN"

    await cb.record_failure() # Probe failed
    assert cb.state == "OPEN" # Should immediately trip back to OPEN
    status = cb.get_status()
    assert status["state"] == "OPEN"
    assert status["cooldown_remaining_sec"] > 0


# ---------------------------------------------------------------------------
# 4. Test Live Reliable Execution
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("GROQ_API_KEY"), reason="live test: requires a real GROQ_API_KEY")
async def test_live_reliable_execution():
    # Test primary success without fallback
    res = await execute_reliable_completion(
        target="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": "Reply with only: ReliableGroq"}],
        max_tokens=20,
    )
    assert "text" in res
    assert res["provider"] == "groq"
    assert res["fallback_used"] is False
    assert len(res["attempts"]) >= 1
    assert res["attempts"][-1]["status"] == "success"


# ---------------------------------------------------------------------------
# 5. Test Live Fallback Chain Triggering
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.skipif(
    not (os.getenv("OPENROUTER_API_KEY") or os.getenv("GEMINI_API_KEY")),
    reason="live test: needs a real fallback provider key (OPENROUTER_API_KEY or GEMINI_API_KEY)",
)
async def test_live_fallback_trigger():
    # Force primary Groq circuit breaker to OPEN to trigger fallback
    groq_cb = CIRCUIT_BREAKERS["groq"]
    groq_cb.state = "OPEN"
    groq_cb.last_failure_time = time.time()
    groq_cb.cooldown_seconds = 60.0

    try:
        # Requesting fast chain: Groq is open -> should fallback to OpenRouter or Gemini!
        res = await execute_reliable_completion(
            target="fast",
            messages=[{"role": "user", "content": "Reply with only: FallbackWorks"}],
            max_tokens=20,
        )
        assert res["fallback_used"] is True
        assert res["provider"] in ("openrouter", "gemini")
        # Check attempts logged Groq skipped due to circuit breaker
        skipped_attempts = [a for a in res["attempts"] if a["status"] == "skipped"]
        assert len(skipped_attempts) >= 1
        assert skipped_attempts[0]["provider"] == "groq"
        assert skipped_attempts[0]["error_type"] == "circuit_breaker_open"
    finally:
        groq_cb.reset()
