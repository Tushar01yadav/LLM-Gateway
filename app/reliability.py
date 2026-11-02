import asyncio
import collections
import json
import logging
import os
import random
import sys
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from fastapi import HTTPException

# Ensure environment variables are loaded
load_dotenv()

class LoadShedError(HTTPException):
    """
    HTTP 429 Too Many Requests raised when all providers or the global gateway
    are saturated. Always includes a Retry-After header.
    """
    def __init__(self, detail: str = "All providers saturated. Please retry later.", retry_after: int = 5):
        super().__init__(
            status_code=429,
            detail=detail,
            headers={"Retry-After": str(retry_after)},
        )
        self.retry_after = retry_after
        self.queue_wait_ms = 0.0


class GatewayTimeoutError(HTTPException):
    """
    HTTP 504 Gateway Timeout raised when request exceeds hard total time budget (30s).
    """
    def __init__(self, detail: str = "Gateway Timeout: Request exceeded hard 30s deadline."):
        super().__init__(
            status_code=504,
            detail=detail,
        )
        self.queue_wait_ms = 0.0

logger = logging.getLogger("llm_gateway.reliability")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [RELIABILITY] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# 1. Error Classification
# ---------------------------------------------------------------------------
def classify_error(status_code: Optional[int], exc: Optional[Exception] = None) -> Tuple[bool, str]:
    """
    Classify an error as retryable or not retryable.

    Retryable:
      - Timeouts (httpx.TimeoutException, TimeoutError)
      - HTTP 429 (Rate Limit / Quota)
      - HTTP 5xx (500, 502, 503, 504 server errors)
      - Network / Connection errors (httpx.NetworkError, ConnectError, etc.)

    Not retryable:
      - HTTP 400 (Bad Request), 401 (Invalid/Unauthorized Key), 403 (Forbidden), 404 (Not Found), 422
    """
    if exc is not None:
        if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
            return True, "timeout"
        if isinstance(exc, (httpx.ConnectError, httpx.NetworkError, ConnectionError)):
            return True, "network_error"
        if isinstance(exc, httpx.RequestError):
            return True, "request_error"

    if status_code is not None:
        if status_code == 429:
            return True, "rate_limit_429"
        if 500 <= status_code <= 599:
            return True, f"server_error_{status_code}"
        if status_code in (400, 401, 403, 404, 422):
            return False, f"client_error_{status_code}"
        if status_code < 400:
            return False, f"status_{status_code}"
        return False, f"http_error_{status_code}"

    return False, "unknown_error"


# ---------------------------------------------------------------------------
# 2. Circuit Breaker
# ---------------------------------------------------------------------------
class CircuitBreaker:
    """
    Circuit breaker per provider.
    - Counts only retryable failures (5xx, 429, timeouts, network errors).
    - 5 consecutive retryable failures -> OPEN for 60s cooldown.
    - After 60s cooldown -> HALF_OPEN (1 test probe allowed).
    - Test probe success -> resets to CLOSED.
    - Test probe failure -> trips back to OPEN for 60s.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        cooldown_seconds: float = 60.0,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.state = "CLOSED"  # "CLOSED", "OPEN", "HALF_OPEN"
        self.consecutive_failures = 0
        self.last_failure_time = 0.0
        self.total_requests = 0
        self.total_failures = 0
        self.total_successes = 0
        self._lock = asyncio.Lock()

    async def can_execute(self) -> Tuple[bool, str]:
        """
        Check if request is allowed through circuit breaker.
        Returns (can_execute: bool, reason: str).
        """
        async with self._lock:
            self.total_requests += 1
            now = time.time()

            if self.state == "CLOSED":
                return True, "closed_healthy"

            elif self.state == "OPEN":
                elapsed = now - self.last_failure_time
                if elapsed >= self.cooldown_seconds:
                    self.state = "HALF_OPEN"
                    logger.info(
                        f"Circuit for '{self.name}' transitioned from OPEN to HALF_OPEN after {elapsed:.1f}s cooldown. Allowing probe."
                    )
                    return True, "half_open_probe"
                else:
                    remaining = self.cooldown_seconds - elapsed
                    logger.warning(
                        f"Circuit for '{self.name}' is OPEN. Skipping request ({remaining:.1f}s cooldown remaining)."
                    )
                    return False, f"circuit_open_{remaining:.1f}s_remaining"

            elif self.state == "HALF_OPEN":
                # Only 1 test probe at a time in HALF_OPEN
                return True, "half_open_probe"

            return True, "unknown"

    async def record_success(self):
        """Record successful execution."""
        async with self._lock:
            self.total_successes += 1
            if self.state in ("HALF_OPEN", "OPEN"):
                logger.info(
                    f"Circuit for '{self.name}' recovered to CLOSED after successful request."
                )
            self.state = "CLOSED"
            self.consecutive_failures = 0

    async def record_failure(self, is_retryable: bool = True):
        """
        Record failed execution.
        Only retryable errors increment failure count and trip the circuit breaker.
        Non-retryable client errors (400, 401, 403, 404, 422) are ignored by the breaker.
        """
        if not is_retryable:
            logger.debug(f"Circuit breaker '{self.name}' ignoring non-retryable error.")
            return

        async with self._lock:
            self.total_failures += 1
            self.consecutive_failures += 1
            self.last_failure_time = time.time()

            if self.state == "HALF_OPEN":
                self.state = "OPEN"
                logger.warning(
                    f"Test probe failed for '{self.name}'. Circuit tripped back to OPEN for {self.cooldown_seconds}s."
                )
            elif self.consecutive_failures >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(
                    f"Circuit for '{self.name}' TRIPPED to OPEN after {self.consecutive_failures} consecutive failures. Cooldown: {self.cooldown_seconds}s."
                )

    def get_status(self) -> Dict[str, Any]:
        """Return circuit breaker health telemetry."""
        now = time.time()
        cooldown_remaining = max(
            0.0,
            round(self.cooldown_seconds - (now - self.last_failure_time), 1),
        ) if self.state == "OPEN" else 0.0

        return {
            "name": self.name,
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "failure_threshold": self.failure_threshold,
            "cooldown_remaining_sec": cooldown_remaining,
            "total_requests": self.total_requests,
            "total_successes": self.total_successes,
            "total_failures": self.total_failures,
        }

    def reset(self):
        """Manually reset circuit breaker for testing or administrative actions."""
        self.state = "CLOSED"
        self.consecutive_failures = 0
        self.last_failure_time = 0.0


# Provider Circuit Breakers Registry
CIRCUIT_BREAKERS: Dict[str, CircuitBreaker] = {
    "groq": CircuitBreaker("groq", failure_threshold=5, cooldown_seconds=60.0),
    "openrouter": CircuitBreaker("openrouter", failure_threshold=5, cooldown_seconds=60.0),
    "gemini": CircuitBreaker("gemini", failure_threshold=5, cooldown_seconds=60.0),
    "mock": CircuitBreaker("mock", failure_threshold=5, cooldown_seconds=60.0),
}


# ---------------------------------------------------------------------------
# 2b. Concurrency & Bulkhead Architecture (Configured strictly via models.json)
# ---------------------------------------------------------------------------
class ProviderBulkheadPool:
    """
    Per-provider concurrency pool and bounded queue (Bulkhead pattern).
    - Own max in-flight limit
    - Bounded queue with max_queue_size
    - Max wait time (default 5.0s, bounded by overall budget)
    - Detailed telemetry: in_flight, limit, queue_depth, queue_limit, avg_wait_ms, p95_wait_ms, rejected_count
    """

    def __init__(
        self,
        provider: str,
        max_inflight: int = 10,
        max_queue_size: int = 20,
        max_queue_wait_sec: float = 5.0,
    ):
        self.provider = provider
        self.max_inflight = max_inflight
        self.max_queue_size = max_queue_size
        self.max_queue_wait_sec = max_queue_wait_sec
        self.in_flight = 0
        self.queue_depth = 0
        self.rejected_count = 0
        self._wait_history: collections.deque = collections.deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._waiters: List[asyncio.Event] = []

    async def _broadcast_metrics(self):
        try:
            m = {
                "provider": self.provider,
                "in_flight": self.in_flight,
                "limit": self.max_inflight,
                "queue_depth": self.queue_depth,
                "queue_limit": self.max_queue_size,
            }
            await TRAFFIC_BROADCASTER.broadcast("concurrency_update", {
                "provider": self.provider,
                "concurrency": m,
                "providers": {self.provider: m}
            })
        except Exception:
            pass

    async def acquire(self, timeout_budget: Optional[float] = None) -> Tuple[bool, float, str]:
        """
        Attempts to acquire an execution slot.
        Flow:
        - Slot free -> run immediately (queue_wait_ms = 0.0)
        - Slot full -> wait in bounded queue
        - Queue full -> reject immediately
        - Wait exceeded -> reject, remove waiter
        Returns: (acquired: bool, queue_wait_ms: float, reason: str)
        """
        wait_start = time.perf_counter()
        async with self._lock:
            # 1. Slot free -> run
            if self.in_flight < self.max_inflight:
                self.in_flight += 1
                await self._broadcast_metrics()
                return True, 0.0, "slot_free"

            # 2. Slot full -> check bounded queue
            if self.queue_depth >= self.max_queue_size:
                self.rejected_count += 1
                await self._broadcast_metrics()
                return False, 0.0, "queue_full"

            # Enqueue waiter
            ev = asyncio.Event()
            self._waiters.append(ev)
            self.queue_depth += 1

        # Max wait bounded by default (5s) and remaining overall budget
        effective_wait = self.max_queue_wait_sec
        if timeout_budget is not None:
            effective_wait = max(0.0, min(effective_wait, timeout_budget))

        # Broadcast SSE event
        await TRAFFIC_BROADCASTER.broadcast("request_queued", {
            "provider": self.provider,
            "queue_depth": self.queue_depth,
            "queue_limit": self.max_queue_size,
        })
        await self._broadcast_metrics()

        try:
            await asyncio.wait_for(ev.wait(), timeout=effective_wait)
            queue_wait_ms = round((time.perf_counter() - wait_start) * 1000, 2)
            async with self._lock:
                self.queue_depth = max(0, self.queue_depth - 1)
                self._wait_history.append(queue_wait_ms)

            await TRAFFIC_BROADCASTER.broadcast("request_dequeued", {
                "provider": self.provider,
                "wait_ms": queue_wait_ms,
                "queue_depth": self.queue_depth,
            })
            await self._broadcast_metrics()
            return True, queue_wait_ms, "acquired_from_queue"

        except (asyncio.TimeoutError, TimeoutError, asyncio.CancelledError) as exc:
            is_cancelled = isinstance(exc, asyncio.CancelledError)
            queue_wait_ms = round((time.perf_counter() - wait_start) * 1000, 2)
            slot_to_release = False
            async with self._lock:
                self.queue_depth = max(0, self.queue_depth - 1)
                if ev in self._waiters:
                    self._waiters.remove(ev)
                elif ev.is_set():
                    # Slot transferred right as timeout or cancellation occurred; release it
                    slot_to_release = True
                self.rejected_count += 1
                self._wait_history.append(queue_wait_ms)

            if slot_to_release:
                await self.release()

            await self._broadcast_metrics()
            if is_cancelled:
                raise
            return False, queue_wait_ms, "queue_wait_exceeded"

    async def release(self):
        """
        Release execution slot. Slots must ALWAYS be released.
        Hands slot to waiting request or decrements in_flight.
        """
        async with self._lock:
            if self._waiters:
                next_waiter = self._waiters.pop(0)
                next_waiter.set()
            else:
                self.in_flight = max(0, self.in_flight - 1)
        await self._broadcast_metrics()

    def get_metrics(self) -> Dict[str, Any]:
        """Telemetry metrics for provider concurrency & queue."""
        history = list(self._wait_history)
        avg_wait = round(sum(history) / len(history), 2) if history else 0.0
        p95_wait = 0.0
        if history:
            s = sorted(history)
            idx = int(round(0.95 * (len(s) - 1)))
            p95_wait = round(s[min(idx, len(s) - 1)], 2)

        return {
            "provider": self.provider,
            "in_flight": self.in_flight,
            "limit": self.max_inflight,
            "queue_depth": self.queue_depth,
            "queue_limit": self.max_queue_size,
            "max_queue_wait_sec": self.max_queue_wait_sec,
            "avg_wait_ms": avg_wait,
            "p95_wait_ms": p95_wait,
            "rejected_count": self.rejected_count,
        }

    def reset(self):
        self.in_flight = 0
        self.queue_depth = 0
        self.rejected_count = 0
        self._waiters.clear()
        self._wait_history.clear()


class GlobalConcurrencyManager:
    """
    Gateway-wide global concurrency controller.
    Ensures total in-flight requests never exceed max_inflight.
    """

    def __init__(self, max_inflight: int = 50):
        self.max_inflight = max_inflight
        self.in_flight = 0
        self.rejected_count = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            if self.in_flight < self.max_inflight:
                self.in_flight += 1
                return True
            self.rejected_count += 1
            return False

    async def release(self):
        async with self._lock:
            self.in_flight = max(0, self.in_flight - 1)

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "in_flight": self.in_flight,
            "limit": self.max_inflight,
            "rejected_count": self.rejected_count,
        }

    def reset(self):
        self.in_flight = 0
        self.rejected_count = 0


# ---------------------------------------------------------------------------
# 3. Routing Configuration (models.json as Sole Source of Truth)
# ---------------------------------------------------------------------------
REGISTRY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models.json")


def load_models_config() -> Dict[str, Any]:
    """
    Loads routing and model configuration exclusively from models.json.
    models.json is the single source of truth for routing config.
    """
    resolved_path = os.path.abspath(REGISTRY_FILE)
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f"Routing configuration file not found at {resolved_path}")
    with open(resolved_path, "r", encoding="utf-8") as f:
        return json.load(f)


def init_concurrency_pools() -> Tuple[GlobalConcurrencyManager, Dict[str, ProviderBulkheadPool]]:
    """
    Loads concurrency configuration strictly from models.json (nothing hardcoded).
    Initializes global semaphore and per-provider bulkhead pools.
    """
    try:
        cfg = load_models_config()
        c_cfg = cfg.get("concurrency", {})
    except Exception:
        c_cfg = {}

    g_limit = c_cfg.get("global_max_inflight", 50)
    global_mgr = GlobalConcurrencyManager(max_inflight=g_limit)

    prov_pools: Dict[str, ProviderBulkheadPool] = {}
    p_cfgs = c_cfg.get("providers", {})

    for p in ("groq", "openrouter", "gemini", "mock"):
        p_data = p_cfgs.get(p, {})
        prov_pools[p] = ProviderBulkheadPool(
            provider=p,
            max_inflight=p_data.get("max_inflight", 10),
            max_queue_size=p_data.get("max_queue_size", 20),
            max_queue_wait_sec=p_data.get("max_queue_wait_sec", 5.0),
        )

    return global_mgr, prov_pools


GLOBAL_CONCURRENCY, PROVIDER_POOLS = init_concurrency_pools()


def get_concurrency_metrics() -> Dict[str, Any]:
    """Return complete real-time concurrency metrics across global gateway and all providers."""
    return {
        "global": GLOBAL_CONCURRENCY.get_metrics(),
        "providers": {name: pool.get_metrics() for name, pool in PROVIDER_POOLS.items()},
    }


def reset_concurrency_pools():
    """Reset concurrency controllers for testing and administrative resets."""
    GLOBAL_CONCURRENCY.reset()
    for pool in PROVIDER_POOLS.values():
        pool.reset()
        pool.max_inflight = 10
        pool.max_queue_size = 20
        pool.max_queue_wait_sec = 5.0


def resolve_fallback_chain(target: str) -> List[Dict[str, str]]:
    """
    Resolve target alias (e.g. 'fast', 'smart', 'cheap') or direct model ID
    into a priority fallback chain exclusively from models.json.
    Ignores non-model config sections (such as 'concurrency').
    """
    config = load_models_config()
    clean_target = target.strip().lower()

    # 1. Exact match on alias or model key
    for key, data in config.items():
        if key in ("concurrency", "_concurrency") or not isinstance(data, dict) or "provider" not in data:
            continue
        if key.lower() == clean_target:
            chain = [{"provider": data["provider"], "model": data["model"]}]
            if "fallback" in data and isinstance(data["fallback"], list):
                chain.extend(data["fallback"])
            elif "chain" in data and isinstance(data["chain"], list):
                chain = data["chain"]
            return chain

    # 2. Match on model attribute inside any entry
    for key, data in config.items():
        if key in ("concurrency", "_concurrency") or not isinstance(data, dict) or "provider" not in data:
            continue
        if data.get("model", "").lower() == clean_target:
            chain = [{"provider": data["provider"], "model": data["model"]}]
            if "fallback" in data and isinstance(data["fallback"], list):
                chain.extend(data["fallback"])
            return chain

    # 3. Default to 'fast' if defined
    if "fast" in config and isinstance(config["fast"], dict) and "provider" in config["fast"]:
        fast_data = config["fast"]
        chain = [{"provider": fast_data["provider"], "model": fast_data["model"]}]
        if "fallback" in fast_data:
            chain.extend(fast_data["fallback"])
        return chain

    # 4. Fallback to first available model entry
    valid_keys = [k for k, v in config.items() if isinstance(v, dict) and "provider" in v]
    if valid_keys:
        first_data = config[valid_keys[0]]
        chain = [{"provider": first_data["provider"], "model": first_data["model"]}]
        if "fallback" in first_data:
            chain.extend(first_data["fallback"])
        return chain

    raise ValueError("No valid providers configured in models.json")


# ---------------------------------------------------------------------------
# 4. Provider Adapters (Wired into Reliability Path)
# ---------------------------------------------------------------------------
class BaseAdapter(ABC):
    @abstractmethod
    async def generate(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        """Execute chat completion for this provider."""
        pass


class GroqAdapter(BaseAdapter):
    async def generate(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        groq_key = os.getenv("GROQ_API_KEY")
        if not groq_key or not groq_key.strip():
            raise ValueError("GROQ_API_KEY not configured")

        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {groq_key}",
            "Content-Type": "application/json",
        }
        formatted_messages = [
            {"role": m.get("role", "user") if isinstance(m, dict) else m.role,
             "content": m.get("content", "") if isinstance(m, dict) else m.content}
            for m in messages
        ]
        payload = {
            "model": model,
            "messages": formatted_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        async def _do_call(c: httpx.AsyncClient):
            resp = await c.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                try:
                    err_msg = resp.json().get("error", {}).get("message", resp.text)
                except Exception:
                    err_msg = resp.text
                raise httpx.HTTPStatusError(
                    f"Groq API ({resp.status_code}): {err_msg}",
                    request=resp.request,
                    response=resp,
                )
            data = resp.json()
            choice = data.get("choices", [{}])[0]
            msg_obj = choice.get("message", {})
            content = (msg_obj.get("content") or "").strip()
            reasoning = (msg_obj.get("reasoning") or "").strip()
            text = content if content else (reasoning or "")
            finish_reason = choice.get("finish_reason", "stop")
            usage = data.get("usage", {})
            return {
                "text": text,
                "finish_reason": finish_reason,
                "token_usage": {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                },
            }

        if client is not None:
            return await _do_call(client)
        async with httpx.AsyncClient(timeout=30.0) as local_client:
            return await _do_call(local_client)


class OpenRouterAdapter(BaseAdapter):
    async def generate(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        openrouter_key = os.getenv("OPENROUTER_API_KEY")
        if not openrouter_key or not openrouter_key.strip():
            raise ValueError("OPENROUTER_API_KEY not configured")

        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {openrouter_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://127.0.0.1:8000",
            "X-Title": "LLM Gateway",
        }
        formatted_messages = [
            {"role": m.get("role", "user") if isinstance(m, dict) else m.role,
             "content": m.get("content", "") if isinstance(m, dict) else m.content}
            for m in messages
        ]
        payload = {
            "model": model,
            "messages": formatted_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        async def _do_call(c: httpx.AsyncClient):
            resp = await c.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                try:
                    err_msg = resp.json().get("error", {}).get("message", resp.text)
                except Exception:
                    err_msg = resp.text
                raise httpx.HTTPStatusError(
                    f"OpenRouter API ({resp.status_code}): {err_msg}",
                    request=resp.request,
                    response=resp,
                )
            data = resp.json()
            choice = data.get("choices", [{}])[0]
            msg_obj = choice.get("message", {})
            text = msg_obj.get("content") or ""
            finish_reason = choice.get("finish_reason", "stop")
            usage = data.get("usage", {})
            return {
                "text": text,
                "finish_reason": finish_reason,
                "token_usage": {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                },
            }

        if client is not None:
            return await _do_call(client)
        async with httpx.AsyncClient(timeout=30.0) as local_client:
            return await _do_call(local_client)


class GeminiAdapter(BaseAdapter):
    async def generate(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        gemini_key = os.getenv("GEMINI_API_KEY")
        if not gemini_key or not gemini_key.strip():
            raise ValueError("GEMINI_API_KEY not configured")

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
        contents = []
        for m in messages:
            role_val = m.get("role", "user") if isinstance(m, dict) else m.role
            content_val = m.get("content", "") if isinstance(m, dict) else m.content
            role = "user" if role_val == "user" else "model"
            contents.append({"role": role, "parts": [{"text": content_val}]})

        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }

        async def _do_call(c: httpx.AsyncClient):
            resp = await c.post(url, headers={"Content-Type": "application/json"}, json=payload)
            if resp.status_code != 200:
                try:
                    err_msg = resp.json().get("error", {}).get("message", resp.text)
                except Exception:
                    err_msg = resp.text
                raise httpx.HTTPStatusError(
                    f"Gemini API ({resp.status_code}): {err_msg}",
                    request=resp.request,
                    response=resp,
                )
            data = resp.json()
            candidates = data.get("candidates", [])
            text = ""
            finish_reason = "stop"
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts)
                finish_reason = candidates[0].get("finishReason", "stop").lower()
            usage_meta = data.get("usageMetadata", {})
            return {
                "text": text,
                "finish_reason": finish_reason,
                "token_usage": {
                    "prompt_tokens": usage_meta.get("promptTokenCount", 0),
                    "completion_tokens": usage_meta.get("candidatesTokenCount", 0),
                    "total_tokens": usage_meta.get("totalTokenCount", 0),
                },
            }

        if client is not None:
            return await _do_call(client)
        async with httpx.AsyncClient(timeout=30.0) as local_client:
            return await _do_call(local_client)


class MockAdapter(BaseAdapter):
    """Mock provider with configurable/fixed latency (default 500ms) for deterministic stress tests."""
    def __init__(self, latency_sec: float = 0.5):
        self.latency_sec = latency_sec

    async def generate(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        await asyncio.sleep(self.latency_sec)
        return {
            "text": "Mock response for stress testing",
            "finish_reason": "stop",
            "token_usage": {
                "prompt_tokens": 5,
                "completion_tokens": 5,
                "total_tokens": 10,
            },
        }


ADAPTER_MAP: Dict[str, BaseAdapter] = {
    "groq": GroqAdapter(),
    "openrouter": OpenRouterAdapter(),
    "gemini": GeminiAdapter(),
    "mock": MockAdapter(),
}


# ---------------------------------------------------------------------------
# 5. Unified Dispatcher (Delegates to Wired Adapters)
# ---------------------------------------------------------------------------
async def dispatch_single_attempt(
    client: httpx.AsyncClient,
    provider: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: int,
) -> Tuple[str, str, Dict[str, int]]:
    """
    Executes a single HTTP call to the specified provider via its wired adapter.
    Eliminates duplicated dispatch code.
    Returns (text, finish_reason, token_usage).
    """
    adapter = ADAPTER_MAP.get(provider.lower())
    if not adapter:
        raise ValueError(f"Unknown provider '{provider}'")

    res = await adapter.generate(
        messages=messages,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        client=client,
    )
    return res["text"], res["finish_reason"], res["token_usage"]


# ---------------------------------------------------------------------------
# 5b. Visualize & Traffic Event Broadcaster (SSE Real-Time Flow)
# ---------------------------------------------------------------------------
class TrafficBroadcaster:
    """
    Async event broadcaster for SSE traffic animation.
    Distributes real-time gateway events to connected visualize clients.
    Events: request_started, attempt_started, attempt_failed, fallback, request_served.
    """

    def __init__(self):
        self._subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=200)
        async with self._lock:
            self._subscribers.append(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue):
        async with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    async def broadcast(self, event_type: str, data: Dict[str, Any]):
        message = {
            "event": event_type,
            "data": data,
            "timestamp": time.time(),
        }
        async with self._lock:
            dead = []
            for q in self._subscribers:
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    dead.append(q)
            for d in dead:
                self._subscribers.remove(d)


TRAFFIC_BROADCASTER = TrafficBroadcaster()

# Live metrics per node (served, failed, in_flight)
NODE_METRICS: Dict[str, Dict[str, int]] = {
    "router": {"served": 0, "failed": 0, "in_flight": 0},
    "groq": {"served": 0, "failed": 0, "in_flight": 0},
    "openrouter": {"served": 0, "failed": 0, "in_flight": 0},
    "gemini": {"served": 0, "failed": 0, "in_flight": 0},
    "mock": {"served": 0, "failed": 0, "in_flight": 0},
}

# Failure injection registry for interactive demos
INJECTED_FAILURES: Dict[str, Dict[str, Any]] = {
    "groq": {"enabled": False, "mode": "503"},        # "503", "timeout", "circuit_open"
    "openrouter": {"enabled": False, "mode": "503"},
    "gemini": {"enabled": False, "mode": "503"},
    "mock": {"enabled": False, "mode": "503"},
}


def set_failure_injection(provider: str, enabled: bool, mode: str = "503") -> Dict[str, Any]:
    """Configure demo failure injection for a provider."""
    provider = provider.lower()
    if provider in INJECTED_FAILURES:
        INJECTED_FAILURES[provider] = {"enabled": enabled, "mode": mode}
        if enabled and mode == "circuit_open":
            cb = CIRCUIT_BREAKERS.get(provider)
            if cb:
                cb.state = "OPEN"
                cb.last_failure_time = time.time()
        elif not enabled:
            cb = CIRCUIT_BREAKERS.get(provider)
            if cb and cb.state == "OPEN":
                cb.reset()
    return INJECTED_FAILURES.get(provider, {"enabled": False, "mode": "503"})


# ---------------------------------------------------------------------------
# 5c. Masking & Secure Provider Key Management
# ---------------------------------------------------------------------------
PROVIDER_ENV_VARS = {
    "groq": "GROQ_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "gemini": "GEMINI_API_KEY",
}
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")


def mask_api_key(key: Optional[str]) -> str:
    """
    Safely mask an API key, showing only the last 2-4 characters.
    Example: 'gsk_12345678yd' -> '***yd'.
    Backend never exposes full keys to UI or telemetry.
    """
    if not key or not key.strip():
        return "Not Set"
    clean = key.strip()
    if len(clean) <= 4:
        return "***" + clean
    return "***" + clean[-4:]


async def validate_candidate_key(provider: str, key: str) -> Tuple[bool, str]:
    """
    Performs a lightweight validation test call to the provider using candidate key.
    Returns (is_valid: bool, reason: str).
    Does not consume prompt tokens or leak raw key in errors.
    """
    provider = provider.lower()
    clean_key = key.strip()
    if not clean_key:
        return False, "API key cannot be empty"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            if provider == "groq":
                url = "https://api.groq.com/openai/v1/models"
                headers = {"Authorization": f"Bearer {clean_key}"}
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    return True, "Valid Groq API key"
                elif resp.status_code in (401, 403):
                    return False, f"Groq rejected key (HTTP {resp.status_code}: Unauthorized)"
                else:
                    return False, f"Groq returned unexpected HTTP {resp.status_code}"

            elif provider == "openrouter":
                url = "https://openrouter.ai/api/v1/auth/key"
                headers = {"Authorization": f"Bearer {clean_key}"}
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    return True, "Valid OpenRouter API key"
                elif resp.status_code in (401, 403):
                    return False, f"OpenRouter rejected key (HTTP {resp.status_code}: Unauthorized)"
                else:
                    return False, f"OpenRouter returned unexpected HTTP {resp.status_code}"

            elif provider == "gemini":
                url = f"https://generativelanguage.googleapis.com/v1beta/models?key={clean_key}"
                resp = await client.get(url)
                if resp.status_code == 200:
                    return True, "Valid Gemini API key"
                elif resp.status_code in (400, 401, 403):
                    return False, f"Gemini rejected key (HTTP {resp.status_code}: Invalid key)"
                else:
                    return False, f"Gemini returned unexpected HTTP {resp.status_code}"

            else:
                return False, f"Unknown provider '{provider}'"

    except Exception as e:
        return False, f"Validation network error: {type(e).__name__}"


async def update_provider_api_key(provider: str, new_key: str) -> Dict[str, Any]:
    """
    Validates candidate key first with a test call.
    If valid: applies to runtime environment immediately, updates .env file, resets breaker.
    If invalid: rejects and retains old key.
    Returns status payload with masked key. Full key is NEVER returned.
    """
    provider = provider.lower()
    env_var = PROVIDER_ENV_VARS.get(provider)
    if not env_var:
        return {"status": "rejected", "provider": provider, "error": f"Unknown provider '{provider}'"}

    is_valid, msg = await validate_candidate_key(provider, new_key)
    if not is_valid:
        return {
            "status": "rejected",
            "provider": provider,
            "error": msg,
            "masked_key": mask_api_key(os.getenv(env_var)),
        }

    # Apply to runtime environment immediately (no restart needed)
    clean_key = new_key.strip()
    os.environ[env_var] = clean_key

    # Persist to .env safely
    resolved_env = os.path.abspath(ENV_PATH)
    if os.path.exists(resolved_env):
        try:
            with open(resolved_env, "r", encoding="utf-8") as f:
                lines = f.readlines()
            updated = False
            new_lines = []
            prefix = f"{env_var}="
            for line in lines:
                if line.strip().startswith(prefix):
                    new_lines.append(f"{env_var}={clean_key}\n")
                    updated = True
                else:
                    new_lines.append(line)
            if not updated:
                new_lines.append(f"{env_var}={clean_key}\n")
            with open(resolved_env, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        except Exception as exc:
            logger.error(f"Failed to persist {env_var} to .env: {exc}")

    # Reset circuit breaker for this provider so the flow restarts clean!
    cb = CIRCUIT_BREAKERS.get(provider)
    if cb:
        cb.reset()

    masked = mask_api_key(clean_key)
    logger.info(f"Updated key for '{provider}'. Validated & applied: {masked}. Circuit breaker reset.")
    return {
        "status": "valid",
        "provider": provider,
        "masked_key": masked,
        "message": "Key validated and applied successfully. Circuit breaker reset.",
    }


def get_provider_nodes_info() -> List[Dict[str, Any]]:
    """
    Returns topology node telemetry for Visualize tab.
    Backend NEVER returns the full key, only the masked version (***yd).
    Includes real-time bulkhead & concurrency metrics.
    """
    nodes = []
    for provider, env_var in PROVIDER_ENV_VARS.items():
        raw_key = os.getenv(env_var, "")
        masked = mask_api_key(raw_key)
        cb = CIRCUIT_BREAKERS.get(provider)
        cb_status = cb.get_status() if cb else {}
        inj = INJECTED_FAILURES.get(provider, {"enabled": False, "mode": "503"})
        metrics = NODE_METRICS.get(provider, {"served": 0, "failed": 0, "in_flight": 0})
        has_key = bool(raw_key and raw_key.strip())
        pool = PROVIDER_POOLS.get(provider)
        pool_metrics = pool.get_metrics() if pool else {}

        nodes.append({
            "provider": provider,
            "name": "Groq" if provider == "groq" else ("OpenRouter" if provider == "openrouter" else "Google Gemini"),
            "masked_key": masked,
            "key_status": "valid" if has_key else "not_configured",
            "circuit_breaker": cb_status,
            "failure_injection": inj,
            "metrics": metrics,
            "concurrency": pool_metrics,
        })
    return nodes


# ---------------------------------------------------------------------------
# 5d. Startup Smoke Test Engine
# ---------------------------------------------------------------------------
SMOKE_TEST_RESULTS: List[Dict[str, Any]] = []
SMOKE_TEST_FAILURES: List[Dict[str, Any]] = []


async def run_startup_smoke_test(client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
    """
    Sends a tiny call to every model in every fallback chain.
    Logs loudly any that fail and exposes them in /api/reliability/status.
    """
    global SMOKE_TEST_RESULTS, SMOKE_TEST_FAILURES
    config = load_models_config()
    pairs = []
    seen = set()

    for alias in ("fast", "smart", "cheap"):
        if alias in config and isinstance(config[alias], dict):
            data = config[alias]
            chain = [{"provider": data.get("provider"), "model": data.get("model")}]
            if "fallback" in data and isinstance(data["fallback"], list):
                chain.extend(data["fallback"])
            for c in chain:
                prov = (c.get("provider") or "").lower()
                mod = c.get("model") or ""
                if prov and mod and (prov, mod) not in seen:
                    seen.add((prov, mod))
                    pairs.append((prov, mod, alias))

    results = []
    failures = []
    logger.info(f"==> [STARTUP SMOKE TEST] Validating {len(pairs)} models across all fallback chains...")

    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=10.0)
        should_close = True

    try:
        for prov, mod, alias in pairs:
            t0 = time.perf_counter()
            adapter = ADAPTER_MAP.get(prov)
            if not adapter:
                err_msg = f"Unknown provider adapter '{prov}'"
                logger.error(f"🚨 [SMOKE TEST FAILURE] {prov}:{mod} (chain: {alias}) -> {err_msg}")
                entry = {"provider": prov, "model": mod, "alias": alias, "status": "failed", "error": err_msg}
                results.append(entry)
                failures.append(entry)
                continue

            try:
                res = await adapter.generate(
                    messages=[{"role": "user", "content": "ping"}],
                    model=mod,
                    temperature=0.0,
                    max_tokens=2,
                    client=client,
                )
                lat = round((time.perf_counter() - t0) * 1000, 2)
                entry = {"provider": prov, "model": mod, "alias": alias, "status": "passed", "latency_ms": lat}
                results.append(entry)
                logger.info(f"✅ [SMOKE TEST PASS] {prov}:{mod} ({lat}ms)")
            except Exception as e:
                lat = round((time.perf_counter() - t0) * 1000, 2)
                err_msg = str(e)
                logger.error(
                    f"🚨 [SMOKE TEST FAILURE] Model '{mod}' on provider '{prov}' (chain '{alias}') FAILED smoke test: {err_msg}"
                )
                entry = {"provider": prov, "model": mod, "alias": alias, "status": "failed", "error": err_msg, "latency_ms": lat}
                results.append(entry)
                failures.append(entry)
    finally:
        if should_close:
            await client.aclose()

    SMOKE_TEST_RESULTS = results
    SMOKE_TEST_FAILURES = failures
    logger.info(
        f"==> [STARTUP SMOKE TEST COMPLETE] Total: {len(results)} | Passed: {len(results)-len(failures)} | Failed: {len(failures)}"
    )
    return {
        "total": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "failures": failures,
        "results": results,
    }


# ---------------------------------------------------------------------------
# 6. Core Reliability & Concurrency Execution Engine
# ---------------------------------------------------------------------------
async def execute_reliable_completion(
    target: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.7,
    max_tokens: int = 1000,
    max_retries_per_provider: int = 2,
    total_time_budget: float = 30.0,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Executes a chat completion request with complete Step 4 Concurrency & Reliability guarantees:
    - Dynamic config strictly from models.json (nothing hardcoded)
    - Global semaphore limit on total in-flight requests (sheds with 429 + Retry-After)
    - Per-provider bulkhead semaphore + bounded queue with max wait time (default 5s)
    - Fast path / queue / fallback / load shedding flow
    - Slots ALWAYS released on success, error, timeout, and cancellation (try/finally)
    - Queue wait time counted against 30s total budget
    - Saturation never trips provider circuit breaker
    - Structured telemetry in response, logs, and real SSE stream
    """
    overall_start_time = time.perf_counter()
    deadline = overall_start_time + total_time_budget
    req_id = request_id or f"req_{int(time.time()*1000)}_{random.randint(100, 999)}"

    # 1. Acquire Global Slot
    global_acquired = await GLOBAL_CONCURRENCY.acquire()
    if not global_acquired:
        logger.warning(
            f"[{req_id}] Global gateway concurrency limit reached ({GLOBAL_CONCURRENCY.max_inflight}). Shedding request with 429."
        )
        NODE_METRICS["router"]["failed"] += 1
        await TRAFFIC_BROADCASTER.broadcast("request_shed", {
            "request_id": req_id,
            "target": target,
            "reason": "global_limit_exceeded",
            "status_code": 429,
        })
        raise LoadShedError(
            "Global gateway concurrency limit reached. Request shed.",
            retry_after=5,
        )

    router_in_flight_tracked = False
    try:
        NODE_METRICS["router"]["in_flight"] += 1
        router_in_flight_tracked = True
        await TRAFFIC_BROADCASTER.broadcast("request_started", {
            "request_id": req_id,
            "target": target,
            "timestamp": time.time(),
        })

        async def _run_chain() -> Dict[str, Any]:
            chain = resolve_fallback_chain(target)
            chain_repr = " -> ".join(f"{c['provider']}:{c['model']}" for c in chain)
            logger.info(f"[{req_id}] Starting reliable execution for target '{target}'. Chain: {chain_repr}")

            attempts_log: List[Dict[str, Any]] = []
            fallback_used = False
            total_queue_wait_ms = 0.0
            saturated_count = 0

            async with httpx.AsyncClient(timeout=total_time_budget) as client:
                for provider_idx, target_config in enumerate(chain):
                    provider = target_config["provider"]
                    model = target_config["model"]
                    cb = CIRCUIT_BREAKERS.get(provider)
                    pool = PROVIDER_POOLS.get(provider)

                    if provider_idx > 0:
                        fallback_used = True

                    # Check hard deadline before starting provider
                    remaining_budget = deadline - time.perf_counter()
                    if remaining_budget <= 0.01:
                        raise asyncio.TimeoutError("Hard total deadline exhausted before provider attempt")

                    # Check Circuit Breaker
                    if cb:
                        can_run, reason = await cb.can_execute()
                        if not can_run:
                            logger.warning(
                                f"[{req_id}] [CIRCUIT BREAKER] Skipping '{provider}' (reason: {reason}). Moving to next provider."
                            )
                            NODE_METRICS[provider]["failed"] += 1
                            await TRAFFIC_BROADCASTER.broadcast("attempt_failed", {
                                "request_id": req_id,
                                "provider": provider,
                                "model": model,
                                "attempt": 0,
                                "error_type": "circuit_breaker_open",
                                "status_code": 503,
                                "is_retryable": True,
                                "reason": reason,
                            })

                            attempts_log.append({
                                "provider": provider,
                                "model": model,
                                "attempt": 0,
                                "status": "skipped",
                                "error_type": "circuit_breaker_open",
                                "latency_ms": 0.0,
                                "queue_wait_ms": 0.0,
                                "reason": reason,
                            })

                            if provider_idx + 1 < len(chain):
                                next_prov = chain[provider_idx + 1]["provider"]
                                await TRAFFIC_BROADCASTER.broadcast("fallback", {
                                    "request_id": req_id,
                                    "from_provider": provider,
                                    "to_provider": next_prov,
                                })
                            continue

                    # 2. Concurrency Bulkhead & Bounded Queue (capped by remaining budget)
                    remaining_budget = deadline - time.perf_counter()
                    if remaining_budget <= 0.01:
                        raise asyncio.TimeoutError("Hard total deadline exhausted before acquiring queue slot")

                    slot_acquired = False
                    q_wait_ms = 0.0
                    q_reason = "slot_free"

                    if pool:
                        slot_acquired, q_wait_ms, q_reason = await pool.acquire(timeout_budget=remaining_budget)
                        total_queue_wait_ms += q_wait_ms

                        if not slot_acquired:
                            saturated_count += 1
                            logger.warning(
                                f"[{req_id}] [CONCURRENCY] Provider '{provider}' saturated ({q_reason}, wait={q_wait_ms}ms). "
                                f"Moving to next provider without tripping circuit breaker."
                            )

                            attempts_log.append({
                                "provider": provider,
                                "model": model,
                                "attempt": 0,
                                "status": "saturated",
                                "error_type": q_reason,
                                "latency_ms": 0.0,
                                "queue_wait_ms": q_wait_ms,
                                "reason": q_reason,
                            })

                            if provider_idx + 1 < len(chain):
                                next_prov = chain[provider_idx + 1]["provider"]
                                await TRAFFIC_BROADCASTER.broadcast("fallback", {
                                    "request_id": req_id,
                                    "from_provider": provider,
                                    "to_provider": next_prov,
                                    "reason": f"saturation_{q_reason}",
                                })
                            continue
                    else:
                        slot_acquired = True

                    # Slot acquired! Must ALWAYS release in finally block
                    try:
                        total_attempts_for_provider = 1 + max_retries_per_provider
                        provider_succeeded = False

                        for attempt_num in range(1, total_attempts_for_provider + 1):
                            # Check hard deadline before attempt
                            remaining_for_attempt = deadline - time.perf_counter()
                            if remaining_for_attempt <= 0.05:
                                logger.error(
                                    f"[{req_id}] Hard deadline reached ({time.perf_counter() - overall_start_time:.2f}s >= {total_time_budget}s). Aborting attempts."
                                )
                                raise asyncio.TimeoutError("Hard total deadline exhausted before attempt execution")

                            attempt_start = time.perf_counter()
                            status_code: Optional[int] = None
                            caught_exc: Optional[Exception] = None

                            logger.info(
                                f"[{req_id}] Attempt {attempt_num}/{total_attempts_for_provider} | Provider: {provider} | Model: {model} | State: {cb.state if cb else 'N/A'}"
                            )

                            # Track provider in-flight with guaranteed decrement in finally
                            prov_in_flight_tracked = False
                            try:
                                NODE_METRICS[provider]["in_flight"] += 1
                                prov_in_flight_tracked = True
                                await TRAFFIC_BROADCASTER.broadcast("attempt_started", {
                                    "request_id": req_id,
                                    "provider": provider,
                                    "model": model,
                                    "attempt": attempt_num,
                                })

                                # Demo Failure Injection Check
                                inj = INJECTED_FAILURES.get(provider, {})
                                if inj.get("enabled"):
                                    mode = inj.get("mode", "503")
                                    logger.warning(f"[{req_id}] [DEMO FAILURE INJECTION] Simulating {mode} error on {provider}")
                                    if mode == "timeout":
                                        raise httpx.ReadTimeout(f"Simulated timeout on {provider}")
                                    elif mode == "circuit_open":
                                        if cb:
                                            cb.state = "OPEN"
                                            cb.last_failure_time = time.time()
                                        req_dummy = httpx.Request("POST", f"https://api.{provider}.com")
                                        resp_dummy = httpx.Response(503, text="Simulated Circuit Breaker Trip", request=req_dummy)
                                        raise httpx.HTTPStatusError("Simulated Circuit Breaker Trip", request=req_dummy, response=resp_dummy)
                                    else:
                                        req_dummy = httpx.Request("POST", f"https://api.{provider}.com")
                                        resp_dummy = httpx.Response(503, text="Simulated 503 Service Unavailable", request=req_dummy)
                                        raise httpx.HTTPStatusError("Simulated 503 Service Unavailable", request=req_dummy, response=resp_dummy)

                                # Cap provider call by min(15.0, remaining_for_attempt)
                                call_timeout = min(15.0, max(0.01, remaining_for_attempt))

                                text, finish_reason, token_usage = await asyncio.wait_for(
                                    dispatch_single_attempt(
                                        client=client,
                                        provider=provider,
                                        model=model,
                                        messages=messages,
                                        temperature=temperature,
                                        max_tokens=max_tokens,
                                    ),
                                    timeout=call_timeout,
                                )

                                attempt_latency = round((time.perf_counter() - attempt_start) * 1000, 2)
                                overall_latency = round((time.perf_counter() - overall_start_time) * 1000, 2)

                                if cb:
                                    await cb.record_success()

                                NODE_METRICS[provider]["served"] += 1
                                NODE_METRICS["router"]["served"] += 1

                                await TRAFFIC_BROADCASTER.broadcast("request_served", {
                                    "request_id": req_id,
                                    "provider": provider,
                                    "model": model,
                                    "latency_ms": overall_latency,
                                    "queue_wait_ms": total_queue_wait_ms,
                                    "token_usage": token_usage,
                                    "fallback_used": fallback_used,
                                })

                                attempts_log.append({
                                    "provider": provider,
                                    "model": model,
                                    "attempt": attempt_num,
                                    "status": "success",
                                    "error_type": None,
                                    "latency_ms": attempt_latency,
                                    "queue_wait_ms": q_wait_ms,
                                })

                                logger.info(
                                    f"[{req_id}] SUCCESS on {provider} ({model}) in {attempt_latency}ms (queue_wait: {total_queue_wait_ms}ms). Elapsed: {overall_latency}ms | Fallback: {fallback_used}"
                                )

                                provider_succeeded = True
                                return {
                                    "request_id": req_id,
                                    "text": text,
                                    "finish_reason": finish_reason,
                                    "token_usage": token_usage,
                                    "usage": token_usage,
                                    "latency_ms": overall_latency,
                                    "latency": overall_latency,
                                    "queue_wait_ms": total_queue_wait_ms,
                                    "provider": provider,
                                    "model": model,
                                    "fallback_used": fallback_used,
                                    "attempts": attempts_log,
                                }
                            except asyncio.CancelledError:
                                raise
                            except httpx.HTTPStatusError as e:
                                status_code = e.response.status_code if e.response else 500
                                caught_exc = e
                            except Exception as e:
                                caught_exc = e
                            finally:
                                if prov_in_flight_tracked:
                                    NODE_METRICS[provider]["in_flight"] = max(0, NODE_METRICS[provider]["in_flight"] - 1)
                                    prov_in_flight_tracked = False

                            # Attempt failed: classify error
                            attempt_latency = round((time.perf_counter() - attempt_start) * 1000, 2)
                            is_retryable, error_type = classify_error(status_code, caught_exc)

                            NODE_METRICS[provider]["failed"] += 1
                            await TRAFFIC_BROADCASTER.broadcast("attempt_failed", {
                                "request_id": req_id,
                                "provider": provider,
                                "model": model,
                                "attempt": attempt_num,
                                "error_type": error_type,
                                "status_code": status_code,
                                "is_retryable": is_retryable,
                            })

                            if cb and is_retryable:
                                await cb.record_failure(is_retryable=True)

                            attempts_log.append({
                                "provider": provider,
                                "model": model,
                                "attempt": attempt_num,
                                "status": "error",
                                "error_type": error_type,
                                "status_code": status_code,
                                "latency_ms": attempt_latency,
                                "queue_wait_ms": q_wait_ms,
                                "detail": str(caught_exc),
                            })

                            logger.warning(
                                f"[{req_id}] FAILED Attempt {attempt_num}/{total_attempts_for_provider} on {provider} | Error: {error_type} ({status_code}) | Retryable: {is_retryable} | Latency: {attempt_latency}ms"
                            )

                            if not is_retryable:
                                logger.warning(
                                    f"[{req_id}] Error {error_type} is NOT retryable for {provider}. Switching to fallback provider immediately."
                                )
                                break

                            # Backoff sleep capped strictly by remaining deadline
                            if attempt_num < total_attempts_for_provider:
                                base_delay = 1.0 * (2 ** (attempt_num - 1))
                                jitter = random.uniform(0.1, 0.4)
                                nominal_wait = base_delay + jitter

                                remaining_before_sleep = deadline - time.perf_counter()
                                if remaining_before_sleep <= 0.1:
                                    logger.warning(
                                        f"[{req_id}] Remaining budget ({remaining_before_sleep:.2f}s) insufficient for backoff. Skipping retry to fallback."
                                    )
                                    break

                                wait_time = min(nominal_wait, max(0.01, remaining_before_sleep - 0.05))
                                logger.info(
                                    f"[{req_id}] Backing off for {wait_time:.2f}s (nominal: {nominal_wait:.2f}s) before attempt {attempt_num + 1} on {provider}..."
                                )
                                await asyncio.sleep(wait_time)

                        logger.info(
                            f"[{req_id}] Provider '{provider}' exhausted or failed. Proceeding to next provider in fallback chain..."
                        )

                        if not provider_succeeded and provider_idx + 1 < len(chain):
                            next_prov = chain[provider_idx + 1]["provider"]
                            await TRAFFIC_BROADCASTER.broadcast("fallback", {
                                "request_id": req_id,
                                "from_provider": provider,
                                "to_provider": next_prov,
                            })

                    finally:
                        # ALWAYS release provider slot
                        if pool and slot_acquired:
                            await pool.release()

                # If all providers in chain failed / saturated:
                overall_latency = round((time.perf_counter() - overall_start_time) * 1000, 2)
                if saturated_count >= len(chain) or (saturated_count > 0 and len([a for a in attempts_log if a["status"] == "error"]) == 0):
                    logger.warning(
                        f"[{req_id}] All providers in fallback chain saturated ({saturated_count}/{len(chain)}). Shedding request with HTTP 429."
                    )
                    await TRAFFIC_BROADCASTER.broadcast("request_shed", {
                        "request_id": req_id,
                        "target": target,
                        "reason": "all_providers_saturated",
                        "status_code": 429,
                    })
                    raise LoadShedError(
                        f"All LLM providers saturated (queue full or wait exceeded). Request shed after {overall_latency}ms.",
                        retry_after=5,
                    )

                error_summary = "; ".join(
                    f"{a['provider']} (attempt {a['attempt']}): {a.get('error_type', 'error')}"
                    for a in attempts_log if a['status'] not in ('skipped', 'saturated')
                )
                logger.error(
                    f"[{req_id}] All providers in fallback chain failed after {overall_latency}ms! Summary: {error_summary}"
                )

                raise httpx.HTTPStatusError(
                    f"All LLM providers failed. Summary: {error_summary}",
                    request=httpx.Request("POST", "http://127.0.0.1:8000/v1/chat"),
                    response=httpx.Response(status_code=502, text=error_summary),
                )

        # Wrap the whole chain in an outer timeout that cancels at deadline and returns 504
        try:
            remaining_outer = max(0.001, deadline - time.perf_counter())
            return await asyncio.wait_for(_run_chain(), timeout=remaining_outer)
        except (asyncio.TimeoutError, TimeoutError):
            elapsed_ms = round((time.perf_counter() - overall_start_time) * 1000, 2)
            logger.error(
                f"[{req_id}] Hard end-to-end deadline exceeded ({elapsed_ms}ms >= {total_time_budget*1000}ms). Cancelling chain and returning 504."
            )
            NODE_METRICS["router"]["failed"] += 1
            await TRAFFIC_BROADCASTER.broadcast("request_shed", {
                "request_id": req_id,
                "target": target,
                "reason": "gateway_timeout_504",
                "status_code": 504,
            })
            raise GatewayTimeoutError(
                f"Gateway Timeout: Request exceeded hard {total_time_budget}s deadline after {elapsed_ms}ms."
            )
        except Exception:
            NODE_METRICS["router"]["failed"] += 1
            raise

    finally:
        if router_in_flight_tracked:
            NODE_METRICS["router"]["in_flight"] = max(0, NODE_METRICS["router"]["in_flight"] - 1)
            router_in_flight_tracked = False
        # ALWAYS release global slot
        await GLOBAL_CONCURRENCY.release()
