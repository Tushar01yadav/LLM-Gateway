import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from fastapi import HTTPException, status

logger = logging.getLogger("llm_gateway.cost")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [COST] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(ch)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models.json")


def get_models_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read models.json for cost config: {e}")
        return {}


def init_cost_db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cost_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_id TEXT NOT NULL,
            key_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            cost_usd REAL NOT NULL,
            timestamp REAL NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cost_key_ts ON cost_records(key_id, timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cost_alias ON cost_records(alias);")

    # Add daily_budget column to gateway_api_keys if missing
    try:
        cursor.execute("ALTER TABLE gateway_api_keys ADD COLUMN daily_budget REAL DEFAULT 5.0")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    conn.commit()
    conn.close()


init_cost_db()


# ---------------------------------------------------------------------------
# 1. Exact-Match Response Cache with TTL (Redis + In-Memory Fallback)
# ---------------------------------------------------------------------------
def build_cache_key(
    scope: str,
    alias: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: Optional[int] = None,
) -> str:
    """
    Deterministic sha256 cache key. `scope` isolates callers ("key:3",
    "user:7", "ip:203.0.113.9") so one tenant can never read another's cached
    response. max_tokens is part of the key because it changes the output.
    """
    normalized = {
        "scope": (scope or "").strip(),
        "alias": (alias or "").strip().lower(),
        "model": (model or "").strip().lower(),
        "temperature": round(float(temperature or 0.7), 4),
        "max_tokens": int(max_tokens) if max_tokens is not None else None,
        "messages": [
            {"role": str(m.get("role", "")).strip().lower(), "content": str(m.get("content", "")).strip()}
            for m in (messages or [])
        ],
    }
    payload_str = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


class ResponseCache:
    """Exact-match response cache backed by Redis with local fallback. Never caches errors."""

    def __init__(self, redis_url: Optional[str] = None, default_ttl: int = 3600):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self.default_ttl = default_ttl
        self._redis: Optional[aioredis.Redis] = None
        self._local_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}

    def get_client(self) -> Optional[aioredis.Redis]:
        if self._redis is None:
            try:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    socket_connect_timeout=0.5,
                    socket_timeout=0.5,
                    decode_responses=True,
                )
            except Exception as e:
                logger.warning(f"Could not initialize Redis client for cache: {e}")
                self._redis = None
        return self._redis

    async def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        r_client = self.get_client()
        if r_client:
            try:
                val = await r_client.get(f"cache:resp:{cache_key}")
                if val:
                    return json.loads(val)
            except Exception as e:
                logger.warning(f"Redis cache get failed ({e}), checking memory fallback.")

        # Local fallback
        entry = self._local_cache.get(cache_key)
        if entry:
            data, expiry = entry
            if time.time() < expiry:
                return data
            else:
                self._local_cache.pop(cache_key, None)
        return None

    async def set(self, cache_key: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        # Never cache errors or empty responses
        if not data or not data.get("text") or data.get("error"):
            return False

        effective_ttl = ttl or self.default_ttl
        payload = json.dumps(data)

        r_client = self.get_client()
        if r_client:
            try:
                await r_client.set(f"cache:resp:{cache_key}", payload, ex=effective_ttl)
                return True
            except Exception as e:
                logger.warning(f"Redis cache set failed ({e}), saving to memory fallback.")

        # Local fallback
        self._local_cache[cache_key] = (data, time.time() + effective_ttl)
        return True

    def clear(self):
        self._local_cache.clear()


# ---------------------------------------------------------------------------
# 2. Request Coalescing (Identical in-flight requests share 1 provider call)
# ---------------------------------------------------------------------------
class RequestCoalescer:
    """Coalesces simultaneous identical in-flight requests to execute only one provider call."""

    def __init__(self, follower_timeout: float = 120.0):
        self._in_flight: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self.follower_timeout = follower_timeout

    async def execute(self, key: str, task_factory: Callable[[], Any]) -> Tuple[Dict[str, Any], bool]:
        """
        Executes task_factory if this is the first in-flight request for key.
        Subsequent callers await the ongoing future.
        Returns (result, was_coalesced: bool).

        Invariants: the shared future is always settled (result, exception or
        cancelled) before the key is dropped; followers never wait forever,
        and a cancelled leader releases followers with a retryable 503 instead
        of hanging them or leaking an unretrieved exception.
        """
        async with self._lock:
            if key in self._in_flight:
                future = self._in_flight[key]
                is_leader = False
            else:
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                self._in_flight[key] = future
                is_leader = True

        if not is_leader:
            try:
                result = await asyncio.wait_for(
                    asyncio.shield(future), timeout=self.follower_timeout
                )
                return result, True
            except asyncio.TimeoutError:
                raise HTTPException(
                    status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                    detail="Timed out waiting for an identical in-flight request.",
                )
            except asyncio.CancelledError:
                if future.cancelled():
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="The in-flight leader request was cancelled. Please retry.",
                    )
                raise

        # Leader executes the provider call
        try:
            result = await task_factory()
            if not future.done():
                future.set_result(result)
            return result, False
        except asyncio.CancelledError:
            if not future.done():
                future.cancel()
            raise
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)
                # Consume it so Python does not warn when there is no follower.
                future.exception()
            raise
        finally:
            async with self._lock:
                self._in_flight.pop(key, None)
            if not future.done():
                future.cancel()


# ---------------------------------------------------------------------------
# 3. Cost Tracking (Computed from models.json prices + token usage; Never $0)
# ---------------------------------------------------------------------------
def calculate_request_cost(
    alias: str,
    model: str,
    token_usage: Optional[Dict[str, Any]],
    messages: Optional[List[Dict[str, str]]] = None,
    completion_text: str = "",
) -> float:
    """
    Computes exact cost per request from models.json prices and token usage.
    Guarantees non-zero cost calculation: no valid request ever logs $0.
    """
    cfg = get_models_config()

    # Look up pricing by alias first, then fallback to model entry
    pricing = cfg.get(alias, {})
    if not pricing or not isinstance(pricing, dict) or "input_price_per_token" not in pricing:
        pricing = cfg.get(model, {})

    # Default baseline pricing if not specified (prevents $0 log paths)
    input_price = float(pricing.get("input_price_per_token", 0.00000008))
    output_price = float(pricing.get("output_price_per_token", 0.00000015))
    if input_price <= 0:
        input_price = 0.00000008
    if output_price <= 0:
        output_price = 0.00000015

    # Extract tokens or estimate from lengths
    usage = token_usage or {}
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")

    if prompt_tokens is None or prompt_tokens <= 0:
        msg_len = sum(len(m.get("content", "")) for m in (messages or []))
        prompt_tokens = max(1, msg_len // 4)

    if completion_tokens is None or completion_tokens <= 0:
        completion_tokens = max(1, len(completion_text) // 4) if completion_text else 1

    cost = (prompt_tokens * input_price) + (completion_tokens * output_price)
    # Strict safety floor: cost is always > 0
    return max(0.0000005, round(cost, 8))


def record_cost_sync(
    key_id: str,
    key_name: str,
    alias: str,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    cost_usd: float,
    timestamp: float,
):
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO cost_records (key_id, key_name, alias, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (str(key_id), key_name, alias, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, timestamp))
    conn.commit()
    conn.close()


class CostTracker:
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self._redis: Optional[aioredis.Redis] = None
        self._total_cost_memory: float = 0.0

    def get_client(self) -> Optional[aioredis.Redis]:
        if self._redis is None:
            try:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    socket_connect_timeout=0.5,
                    socket_timeout=0.5,
                    decode_responses=True,
                )
            except Exception:
                self._redis = None
        return self._redis

    async def record_cost(
        self,
        key_id: str,
        key_name: str,
        alias: str,
        provider: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        cost_usd: float,
    ):
        now = time.time()
        self._total_cost_memory += cost_usd

        # Asynchronously write to SQLite off event loop
        await asyncio.to_thread(
            record_cost_sync,
            str(key_id),
            key_name,
            alias,
            provider,
            model,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            cost_usd,
            now,
        )

        # Update Redis spend counters
        r_client = self.get_client()
        if r_client:
            try:
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                pipe = r_client.pipeline()
                pipe.incrbyfloat(f"cost:daily:{key_id}:{today}", cost_usd)
                pipe.expire(f"cost:daily:{key_id}:{today}", 86400 * 2)
                pipe.incrbyfloat(f"cost:alias:{alias}", cost_usd)
                pipe.incrbyfloat("cost:total:gateway", cost_usd)
                await pipe.execute()
            except Exception as e:
                logger.warning(f"Redis cost counter increment failed: {e}")

    async def get_daily_spend(self, key_id: str) -> float:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r_client = self.get_client()
        if r_client:
            try:
                val = await r_client.get(f"cost:daily:{key_id}:{today}")
                if val:
                    return float(val)
            except Exception:
                pass

        # Query SQLite off event loop
        def _get_sqlite_spend() -> float:
            conn = sqlite3.connect(DB_PATH, timeout=15.0)
            cursor = conn.cursor()
            start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            cursor.execute(
                "SELECT COALESCE(SUM(cost_usd), 0.0) FROM cost_records WHERE key_id = ? AND timestamp >= ?",
                (str(key_id), start_of_day),
            )
            val = cursor.fetchone()[0]
            conn.close()
            return float(val or 0.0)

        return await asyncio.to_thread(_get_sqlite_spend)

    async def get_total_spend(self) -> float:
        r_client = self.get_client()
        if r_client:
            try:
                val = await r_client.get("cost:total:gateway")
                if val:
                    return float(val)
            except Exception:
                pass

        def _get_total_sqlite() -> float:
            conn = sqlite3.connect(DB_PATH, timeout=15.0)
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(SUM(cost_usd), 0.0) FROM cost_records")
            val = cursor.fetchone()[0]
            conn.close()
            return float(val or 0.0)

        return await asyncio.to_thread(_get_total_sqlite)


# ---------------------------------------------------------------------------
# 4. Budget Guard (Per-key daily budget; returns 429 when exceeded)
# ---------------------------------------------------------------------------
class BudgetGuard:
    def __init__(self, tracker: CostTracker):
        self.tracker = tracker

    async def check_budget(self, key_id: str, key_name: str, daily_budget: float) -> Tuple[bool, float, float]:
        """
        Returns (is_allowed, current_spend, daily_budget).
        If current_spend >= daily_budget, rejects with False.
        """
        current_spend = await self.tracker.get_daily_spend(str(key_id))
        if current_spend >= daily_budget:
            return False, current_spend, daily_budget
        return True, current_spend, daily_budget


# Global Singletons
RESPONSE_CACHE = ResponseCache()
REQUEST_COALESCER = RequestCoalescer()
COST_TRACKER = CostTracker()
BUDGET_GUARD = BudgetGuard(COST_TRACKER)
