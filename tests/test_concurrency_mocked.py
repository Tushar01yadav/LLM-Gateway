"""
Mocked unit tests for Step 4: Concurrency & Bulkhead Control.
Verifies all concurrency invariants without making live API calls:
1. In-flight never exceeds limit.
2. Queue wait timeout rejects correctly.
3. Load shedding returns HTTP 429 with Retry-After header.
4. Slots are released after error, timeout, and completion.
5. Bulkhead isolation: slow provider does not affect other providers.
6. Saturation does not trip the circuit breaker.
"""

import asyncio
import time
import pytest
from unittest.mock import AsyncMock, patch
import httpx

from app.reliability import (
    execute_reliable_completion,
    LoadShedError,
    GatewayTimeoutError,
    ADAPTER_MAP,
    PROVIDER_POOLS,
    GLOBAL_CONCURRENCY,
    CIRCUIT_BREAKERS,
    reset_concurrency_pools,
    load_models_config,
)


@pytest.fixture(autouse=True)
def reset_concurrency_and_breakers():
    """Ensure clean state before and after each test."""
    reset_concurrency_pools()
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()
    yield
    reset_concurrency_pools()
    for cb in CIRCUIT_BREAKERS.values():
        cb.reset()


# ---------------------------------------------------------------------------
# 1. Test: In-Flight Never Exceeds Limit
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_inflight_never_exceeds_limit():
    """
    Ensure the number of concurrent executions for a provider never exceeds
    its configured max_inflight limit under heavy concurrent load.
    """
    groq_pool = PROVIDER_POOLS["groq"]
    original_max = groq_pool.max_inflight
    original_q_max = groq_pool.max_queue_size
    groq_pool.max_inflight = 3
    groq_pool.max_queue_size = 20
    groq_pool.max_queue_wait_sec = 2.0

    current_concurrent = 0
    max_observed = 0
    lock = asyncio.Lock()

    async def mock_generate(*args, **kwargs):
        nonlocal current_concurrent, max_observed
        async with lock:
            current_concurrent += 1
            if current_concurrent > max_observed:
                max_observed = current_concurrent
        await asyncio.sleep(0.05)
        async with lock:
            current_concurrent -= 1
        return {
            "text": "test output",
            "finish_reason": "stop",
            "token_usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10},
        }

    try:
        with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=mock_generate):
            # Run 10 concurrent requests to fast lane (Groq primary)
            tasks = [
                execute_reliable_completion(
                    target="fast",
                    messages=[{"role": "user", "content": f"msg {i}"}],
                    max_retries_per_provider=0,
                )
                for i in range(10)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        successful = [r for r in results if isinstance(r, dict)]
        assert len(successful) == 10
        assert max_observed <= 3, f"Max concurrent observed {max_observed} exceeded limit 3"
        assert groq_pool.in_flight == 0, f"Remaining in_flight should be 0, got {groq_pool.in_flight}"
    finally:
        groq_pool.max_inflight = original_max
        groq_pool.max_queue_size = original_q_max


# ---------------------------------------------------------------------------
# 2. Test: Queue Wait Timeout Rejects Correctly
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_wait_timeout_rejects_correctly():
    """
    Verify that when a provider's slot is held longer than max_queue_wait_sec,
    queued requests time out waiting and fall back to the next provider.
    """
    groq_pool = PROVIDER_POOLS["groq"]
    orig_inflight = groq_pool.max_inflight
    orig_q_wait = groq_pool.max_queue_wait_sec

    groq_pool.max_inflight = 1
    groq_pool.max_queue_wait_sec = 0.05  # 50ms queue wait limit

    async def slow_groq(*args, **kwargs):
        await asyncio.sleep(0.20)  # Slot held for 200ms (> 50ms wait limit)
        return {
            "text": "slow groq output",
            "finish_reason": "stop",
            "token_usage": {"total_tokens": 10},
        }

    async def fast_openrouter(*args, **kwargs):
        return {
            "text": "openrouter fallback output",
            "finish_reason": "stop",
            "token_usage": {"total_tokens": 10},
        }

    try:
        with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=slow_groq), \
             patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=fast_openrouter):

            # Start request 1 (occupies Groq slot for 200ms)
            task1 = asyncio.create_task(
                execute_reliable_completion(target="fast", messages=[{"role": "user", "content": "req1"}])
            )
            await asyncio.sleep(0.01)  # Ensure task1 acquires the slot

            # Request 2 enters queue, will exceed 50ms max_queue_wait_sec, then fall back to OpenRouter
            task2 = asyncio.create_task(
                execute_reliable_completion(target="fast", messages=[{"role": "user", "content": "req2"}])
            )

            res1, res2 = await asyncio.gather(task1, task2)

            assert res1["provider"] == "groq"
            assert res2["provider"] == "openrouter"
            assert res2["fallback_used"] is True
            # Check attempts: Groq must show saturated
            groq_attempts = [a for a in res2["attempts"] if a["provider"] == "groq"]
            assert len(groq_attempts) == 1
            assert groq_attempts[0]["status"] == "saturated"
            assert groq_attempts[0]["reason"] in ("queue_wait_exceeded", "queue_wait_timeout")
    finally:
        groq_pool.max_inflight = orig_inflight
        groq_pool.max_queue_wait_sec = orig_q_wait


# ---------------------------------------------------------------------------
# 3. Test: Load Shedding Returns HTTP 429 With Retry-After
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_load_shedding_returns_429_with_retry_after():
    """
    Verify that when all providers in a fallback chain are saturated (or global
    capacity is exhausted), the gateway immediately sheds the request with HTTP 429
    and sets a Retry-After header.
    """
    # 1. Test Provider Chain Saturation Shedding
    for p in ("groq", "openrouter", "gemini"):
        pool = PROVIDER_POOLS[p]
        pool.max_inflight = 1
        pool.max_queue_size = 0  # No queue allowed
        # Pre-acquire the single slot so provider is immediately saturated
        await pool.acquire()

    try:
        with pytest.raises(LoadShedError) as exc_info:
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "shed test"}],
            )

        err = exc_info.value
        assert err.status_code == 429
        assert err.retry_after == 5
        assert err.headers.get("Retry-After") == "5"
        assert "All LLM providers saturated" in err.detail

    finally:
        reset_concurrency_pools()

    # 2. Test Global Semaphore Saturation Shedding
    orig_global_max = GLOBAL_CONCURRENCY.max_inflight
    GLOBAL_CONCURRENCY.max_inflight = 1
    acquired = await GLOBAL_CONCURRENCY.acquire()
    assert acquired is True

    try:
        with pytest.raises(LoadShedError) as exc_info:
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "global shed test"}],
            )
        err = exc_info.value
        assert err.status_code == 429
        assert err.retry_after == 5
        assert err.headers.get("Retry-After") == "5"
        assert "Global gateway concurrency limit reached" in err.detail
    finally:
        GLOBAL_CONCURRENCY.max_inflight = orig_global_max
        reset_concurrency_pools()


# ---------------------------------------------------------------------------
# 4. Test: Slot Released After Error and Timeout
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_slot_released_after_error_and_timeout():
    """
    Ensure slots in both the provider pool and the global concurrency manager
    are guaranteed to be released (in_flight == 0) even when adapters fail
    with 500 errors, timeouts, or exceptions.
    """
    groq_pool = PROVIDER_POOLS["groq"]
    assert groq_pool.in_flight == 0
    assert GLOBAL_CONCURRENCY.in_flight == 0

    # Case A: HTTP 500 Status Error
    async def failing_adapter(*args, **kwargs):
        req = httpx.Request("POST", "https://api.groq.com")
        resp = httpx.Response(500, request=req, text="Internal Server Error")
        raise httpx.HTTPStatusError("500 Error", request=req, response=resp)

    with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=failing_adapter), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=failing_adapter), \
         patch.object(ADAPTER_MAP["gemini"], "generate", side_effect=failing_adapter):

        with pytest.raises(httpx.HTTPStatusError):
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "error test"}],
                max_retries_per_provider=0,
            )

    assert groq_pool.in_flight == 0, "Groq pool slot leaked after HTTP 500"
    assert GLOBAL_CONCURRENCY.in_flight == 0, "Global slot leaked after HTTP 500"

    # Case B: ReadTimeout
    async def timeout_adapter(*args, **kwargs):
        raise httpx.ReadTimeout("Connection timed out reading from provider")

    with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=timeout_adapter), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=timeout_adapter), \
         patch.object(ADAPTER_MAP["gemini"], "generate", side_effect=timeout_adapter):

        with pytest.raises(httpx.HTTPStatusError):
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "timeout test"}],
                max_retries_per_provider=0,
            )

    assert groq_pool.in_flight == 0, "Groq pool slot leaked after Timeout"
    assert GLOBAL_CONCURRENCY.in_flight == 0, "Global slot leaked after Timeout"


# ---------------------------------------------------------------------------
# 5. Test: Slow Provider Bulkhead Isolation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_slow_provider_bulkhead_isolation():
    """
    Verify that a slow, bogged-down provider (Groq) does not starve or block
    unrelated requests directed to other healthy providers (Gemini / OpenRouter).
    """
    groq_pool = PROVIDER_POOLS["groq"]
    gemini_pool = PROVIDER_POOLS["gemini"]

    orig_groq_inflight = groq_pool.max_inflight
    groq_pool.max_inflight = 1

    # Groq adapter sleeps for 0.4s (simulating a slow provider)
    async def slow_groq(*args, **kwargs):
        await asyncio.sleep(0.4)
        return {"text": "slow groq output", "finish_reason": "stop", "token_usage": {"total_tokens": 5}}

    # Gemini adapter responds immediately (10ms)
    async def fast_gemini(*args, **kwargs):
        await asyncio.sleep(0.01)
        return {"text": "fast gemini output", "finish_reason": "stop", "token_usage": {"total_tokens": 5}}

    try:
        with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=slow_groq), \
             patch.object(ADAPTER_MAP["gemini"], "generate", side_effect=fast_gemini):

            # Start slow request holding Groq's capacity
            groq_task = asyncio.create_task(
                execute_reliable_completion(target="fast", messages=[{"role": "user", "content": "slow"}])
            )
            await asyncio.sleep(0.02)  # Give time for groq to acquire slot

            # Now send request to smart lane (Gemini primary)
            t_start = time.perf_counter()
            gemini_res = await execute_reliable_completion(
                target="smart",
                messages=[{"role": "user", "content": "fast gemini"}],
            )
            elapsed_gemini = time.perf_counter() - t_start

            # Gemini request must complete quickly and not wait for Groq
            assert gemini_res["provider"] == "gemini"
            assert elapsed_gemini < 0.20, f"Gemini was blocked by slow Groq! Took {elapsed_gemini}s"

            # Wait for slow groq task to finish
            groq_res = await groq_task
            assert groq_res["provider"] == "groq"
    finally:
        groq_pool.max_inflight = orig_groq_inflight


# ---------------------------------------------------------------------------
# 6. Test: Saturation Does Not Trip Circuit Breaker
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_saturation_does_not_trip_circuit_breaker():
    """
    Verify that capacity saturation (queue full or queue wait timeout) is treated
    strictly as traffic load, NOT a provider failure, and therefore does NOT
    increment the circuit breaker's failure counter or trip it to OPEN.
    """
    groq_pool = PROVIDER_POOLS["groq"]
    groq_cb = CIRCUIT_BREAKERS["groq"]

    orig_inflight = groq_pool.max_inflight
    orig_q_size = groq_pool.max_queue_size

    groq_pool.max_inflight = 1
    groq_pool.max_queue_size = 0  # Zero queue: any 2nd request will immediately be rejected as queue_full

    assert groq_cb.state == "CLOSED"
    assert groq_cb.consecutive_failures == 0
    initial_failures = groq_cb.total_failures

    async def holding_groq(*args, **kwargs):
        await asyncio.sleep(0.25)
        return {"text": "holding slot", "finish_reason": "stop", "token_usage": {"total_tokens": 5}}

    async def openrouter_fallback(*args, **kwargs):
        return {"text": "openrouter ok", "finish_reason": "stop", "token_usage": {"total_tokens": 5}}

    try:
        with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=holding_groq), \
             patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=openrouter_fallback):

            # Request 1 holds the only Groq slot
            task1 = asyncio.create_task(
                execute_reliable_completion(target="fast", messages=[{"role": "user", "content": "req1"}])
            )
            await asyncio.sleep(0.01)

            # Fire 5 concurrent requests while Groq is full.
            # All 5 will find Groq queue full and fall back to OpenRouter.
            saturated_tasks = [
                execute_reliable_completion(
                    target="fast",
                    messages=[{"role": "user", "content": f"sat_{i}"}],
                    max_retries_per_provider=0,
                )
                for i in range(5)
            ]
            results = await asyncio.gather(*saturated_tasks)
            await task1

            for r in results:
                assert r["provider"] == "openrouter"
                assert r["fallback_used"] is True

            # CRITICAL ASSERTION: Groq circuit breaker must remain completely healthy and CLOSED!
            assert groq_cb.state == "CLOSED"
            assert groq_cb.consecutive_failures == 0
            assert groq_cb.total_failures == initial_failures
            assert groq_pool.rejected_count >= 5
    finally:
        groq_pool.max_inflight = orig_inflight
        groq_pool.max_queue_size = orig_q_size


# ---------------------------------------------------------------------------
# 7. Test: Hard End-to-End 30s Deadline Cancels Hanging Provider & Releases Slots
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_hard_deadline_cancels_at_30s_when_provider_hangs():
    """
    Mocked test where all providers in the fallback chain hang indefinitely (100s).
    Asserts:
    1. The outer timeout strictly cancels the entire chain at 30s.
    2. Total request elapsed time never exceeds 30s plus a small margin (assert <= 31.0s).
    3. Returns HTTP 504 (GatewayTimeoutError).
    4. All execution slots (provider bulkhead and global gateway) are strictly released on cancel.
    """
    groq_pool = PROVIDER_POOLS["groq"]
    global_conc = GLOBAL_CONCURRENCY

    # Mock all provider generate methods to hang indefinitely
    async def hanging_provider(*args, **kwargs):
        await asyncio.sleep(120.0)
        return {"text": "never reached", "finish_reason": "stop", "token_usage": {}}

    t0 = time.perf_counter()
    with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=hanging_provider), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=hanging_provider), \
         patch.object(ADAPTER_MAP["gemini"], "generate", side_effect=hanging_provider):

        with pytest.raises(GatewayTimeoutError) as exc_info:
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "hang test"}],
                total_time_budget=30.0,
            )

    elapsed = time.perf_counter() - t0

    # 1. Assert status code is 504 Gateway Timeout
    assert exc_info.value.status_code == 504
    assert "Gateway Timeout" in exc_info.value.detail

    # 2. Assert total request time never exceeds 30s plus a small margin (margin <= 1.0s)
    assert 29.8 <= elapsed <= 31.0, f"Total elapsed time {elapsed:.3f}s exceeded 30s plus margin!"

    # 3. Assert all slots are strictly released on cancel
    assert groq_pool.in_flight == 0, f"Groq in_flight was {groq_pool.in_flight}, expected 0"
    assert groq_pool.queue_depth == 0, f"Groq queue_depth was {groq_pool.queue_depth}, expected 0"
    assert global_conc.in_flight == 0, f"Global in_flight was {global_conc.in_flight}, expected 0"


@pytest.mark.asyncio
async def test_hard_deadline_fast_budget_releases_slots():
    """
    Fast unit test: verifies that a 0.5s total time budget immediately aborts a hanging
    provider and releases all slots cleanly.
    """
    groq_pool = PROVIDER_POOLS["groq"]
    global_conc = GLOBAL_CONCURRENCY

    async def hanging_provider(*args, **kwargs):
        await asyncio.sleep(10.0)
        return {"text": "never reached", "finish_reason": "stop", "token_usage": {}}

    t0 = time.perf_counter()
    with patch.object(ADAPTER_MAP["groq"], "generate", side_effect=hanging_provider), \
         patch.object(ADAPTER_MAP["openrouter"], "generate", side_effect=hanging_provider), \
         patch.object(ADAPTER_MAP["gemini"], "generate", side_effect=hanging_provider):

        with pytest.raises(GatewayTimeoutError) as exc_info:
            await execute_reliable_completion(
                target="fast",
                messages=[{"role": "user", "content": "fast hang test"}],
                total_time_budget=0.5,
            )

    elapsed = time.perf_counter() - t0
    assert exc_info.value.status_code == 504
    assert 0.5 <= elapsed <= 1.2, f"Total elapsed time {elapsed:.3f}s exceeded 0.5s plus margin!"

    # Assert all slots are strictly released on cancel
    assert groq_pool.in_flight == 0
    assert groq_pool.queue_depth == 0
    assert global_conc.in_flight == 0


# ---------------------------------------------------------------------------
# 8. Test: Queue Acquire Race & Cancellation Handling
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_queue_acquire_race_slot_handed_over_at_timeout_releases_slot():
    """
    Simulate race condition where a slot is handed over (ev.set()) at the exact
    moment timeout occurs in acquire().
    Verifies:
    1. The handed-over slot is strictly released (not leaked in in_flight).
    2. queue_depth is properly decremented to 0.
    """
    from app.reliability import ProviderBulkheadPool
    pool = ProviderBulkheadPool(provider="test_race", max_inflight=1, max_queue_size=5, max_queue_wait_sec=0.05)

    # Acquire the single slot so pool is full
    acq1, _, _ = await pool.acquire()
    assert acq1 is True
    assert pool.in_flight == 1

    # Start a waiter that will wait in queue
    waiter_task = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0.01)
    assert pool.queue_depth == 1
    assert len(pool._waiters) == 1

    # Simulate release occurring right as waiter is about to time out
    # Wait until just before the 0.05s timeout
    await asyncio.sleep(0.045)
    await pool.release()

    # The waiter task finishes (either acquired or timed out with slot release)
    result = await waiter_task
    # Either way: in_flight must be 0 if rejected or 1 if acquired, and queue_depth must be 0
    assert pool.queue_depth == 0, f"queue_depth leaked: {pool.queue_depth}"
    if result[0] is False:
        # Rejected due to timeout, handed-over slot must have been released!
        assert pool.in_flight == 0, f"Handed-over slot leaked! in_flight is {pool.in_flight}"
    else:
        # Acquired in time
        assert pool.in_flight == 1
        await pool.release()
        assert pool.in_flight == 0


@pytest.mark.asyncio
async def test_queue_wait_cancellation_releases_slot_and_decrements_queue_depth():
    """
    Simulate client disconnect (cancellation) while waiting in the bounded queue.
    Verifies:
    1. CancelledError propagates to caller.
    2. queue_depth is properly decremented to 0.
    3. Waiter is removed from _waiters.
    4. If slot was handed over, it is immediately released.
    """
    from app.reliability import ProviderBulkheadPool
    pool = ProviderBulkheadPool(provider="test_cancel", max_inflight=1, max_queue_size=5, max_queue_wait_sec=5.0)

    # Fill the single execution slot
    acq1, _, _ = await pool.acquire()
    assert acq1 is True
    assert pool.in_flight == 1

    # Enqueue a waiter
    waiter_task = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0.02)
    assert pool.queue_depth == 1
    assert len(pool._waiters) == 1

    # Simulate client disconnect (cancel task)
    waiter_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter_task

    # Verify queue_depth was decremented and waiter was removed
    assert pool.queue_depth == 0, f"queue_depth leaked: {pool.queue_depth}"
    assert len(pool._waiters) == 0

    # Clean up initial slot
    await pool.release()
    assert pool.in_flight == 0

