import asyncio
import hashlib
import json
import logging
import math
import os
from app import db
from app.db import DATABASE_URL, sqlite3
import threading
import time
import uuid
from decimal import Decimal
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from fastapi import HTTPException, status
from prometheus_client import Counter, Histogram

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
BUDGET_RESERVATIONS = Counter("gateway_budget_reservations_total", "Daily budget reservation outcomes", ("outcome",))
BUDGET_COST_USD = Histogram("gateway_budget_cost_usd", "Estimated and settled request cost", ("kind",))
CACHE_LOOKUPS = Counter("gateway_response_cache_lookups_total", "Response cache lookup outcomes", ("outcome", "backend"))


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
            timestamp DOUBLE PRECISION NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cost_key_ts ON cost_records(key_id, timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cost_alias ON cost_records(alias);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS budget_accounts (
            key_id TEXT PRIMARY KEY,
            budget_date TEXT NOT NULL,
            daily_budget NUMERIC(24,12) NOT NULL,
            spent NUMERIC(24,12) NOT NULL DEFAULT 0,
            reserved NUMERIC(24,12) NOT NULL DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS budget_reservations (
            id TEXT PRIMARY KEY,
            key_id TEXT NOT NULL,
            key_name TEXT NOT NULL,
            budget_date TEXT NOT NULL,
            amount NUMERIC(24,12) NOT NULL,
            status TEXT NOT NULL,
            expires_at DOUBLE PRECISION NOT NULL,
            created_at DOUBLE PRECISION NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_budget_reservations_expiry ON budget_reservations(status, expires_at);")

    if db.DATABASE_URL:
        for table, column in (
            ("budget_accounts", "daily_budget"),
            ("budget_accounts", "spent"),
            ("budget_accounts", "reserved"),
            ("budget_reservations", "amount"),
        ):
            cursor.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} TYPE NUMERIC(24,12) USING {column}::NUMERIC(24,12)"
            )
        # Epoch timestamps must be float8: float4 (REAL) rounds to 128-second
        # steps at current Unix epoch magnitudes, which breaks short leases.
        for table, column in (
            ("cost_records", "timestamp"),
            ("budget_reservations", "expires_at"),
            ("budget_reservations", "created_at"),
        ):
            cursor.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} TYPE DOUBLE PRECISION USING {column}::DOUBLE PRECISION"
            )

    # Add daily_budget column to gateway_api_keys if missing
    if db.DATABASE_URL:
        cursor.execute("SELECT to_regclass('llm_gateway.gateway_api_keys')")
        row = cursor.fetchone()
        if row and row[0]:
            cursor.execute("ALTER TABLE gateway_api_keys ADD COLUMN IF NOT EXISTS daily_budget REAL DEFAULT 5.0")
    else:
        cursor.execute("PRAGMA table_info(gateway_api_keys)")
        columns = {row[1] for row in cursor.fetchall()}
        if columns and "daily_budget" not in columns:
            cursor.execute("ALTER TABLE gateway_api_keys ADD COLUMN daily_budget REAL DEFAULT 5.0")

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
        self._local_cache: OrderedDict[str, Tuple[Dict[str, Any], float]] = OrderedDict()
        self._local_cache_lock = threading.RLock()
        self._local_cache_max_entries = 1000
        self._local_cache_max_bytes = 16 * 1024 * 1024
        self._local_cache_bytes = 0
        self._local_cache_sizes: Dict[str, int] = {}

    def get_client(self) -> Optional[aioredis.Redis]:
        if self._redis is None:
            try:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    socket_connect_timeout=2.0,
                    socket_timeout=1.0,
                    health_check_interval=30,
                    decode_responses=True,
                )
            except Exception as e:
                logger.warning(f"Could not initialize Redis client for cache: {e}")
                self._redis = None
        return self._redis

    async def aclose(self) -> None:
        client, self._redis = self._redis, None
        if client is not None:
            await client.aclose()

    async def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        r_client = self.get_client()
        if r_client:
            try:
                val = await r_client.get(f"cache:resp:{cache_key}")
                if val:
                    CACHE_LOOKUPS.labels("hit", "redis").inc()
                    return json.loads(val)
                CACHE_LOOKUPS.labels("miss", "redis").inc()
                return None
            except Exception as e:
                logger.warning(f"Redis cache get failed ({e}), checking memory fallback.")

        # Local fallback
        with self._local_cache_lock:
            entry = self._local_cache.get(cache_key)
            if entry:
                data, expiry = entry
                if time.time() < expiry:
                    self._local_cache.move_to_end(cache_key)
                    CACHE_LOOKUPS.labels("hit", "local").inc()
                    return data
                self._local_cache.pop(cache_key, None)
                self._local_cache_bytes -= self._local_cache_sizes.pop(cache_key, 0)
        CACHE_LOOKUPS.labels("miss", "none").inc()
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
        now = time.time()
        payload_size = len(payload.encode("utf-8"))
        with self._local_cache_lock:
            if payload_size > self._local_cache_max_bytes:
                return False
            self._local_cache.pop(cache_key, None)
            self._local_cache_bytes -= self._local_cache_sizes.pop(cache_key, 0)
            while (len(self._local_cache) >= self._local_cache_max_entries
                   or self._local_cache_bytes + payload_size > self._local_cache_max_bytes):
                evicted_key, _ = self._local_cache.popitem(last=False)
                self._local_cache_bytes -= self._local_cache_sizes.pop(evicted_key, 0)
            self._local_cache[cache_key] = (data, now + effective_ttl)
            self._local_cache_sizes[cache_key] = payload_size
            self._local_cache_bytes += payload_size
        return True

    async def clear(self):
        with self._local_cache_lock:
            self._local_cache.clear()
            self._local_cache_sizes.clear()
            self._local_cache_bytes = 0
        r_client = self.get_client()
        if r_client:
            try:
                keys = [key async for key in r_client.scan_iter(match="cache:resp:*")]
                if keys:
                    await r_client.delete(*keys)
            except Exception as exc:
                logger.warning("Redis response cache clear failed (%s)", type(exc).__name__)


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

    prompt_tokens, completion_tokens, _ = resolve_token_usage(token_usage, messages, completion_text)

    cost = (prompt_tokens * input_price) + (completion_tokens * output_price)
    # Strict safety floor: cost is always > 0
    return max(0.0000005, round(cost, 8))


def resolve_token_usage(token_usage: Optional[Dict[str, Any]], messages: Optional[List[Dict[str, str]]],
                        completion_text: str = "") -> Tuple[int, int, int]:
    """Return provider usage or the same documented text-length estimates used for billing."""
    usage = token_usage or {}
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    if prompt_tokens is None or int(prompt_tokens or 0) <= 0:
        message_length = sum(len(str(message.get("content", ""))) for message in messages or [])
        prompt_tokens = max(1, message_length // 4)
    else:
        prompt_tokens = int(prompt_tokens)
    if completion_tokens is None or int(completion_tokens or 0) <= 0:
        completion_tokens = max(1, len(completion_text) // 4) if completion_text else 1
    else:
        completion_tokens = int(completion_tokens)
    total_tokens = usage.get("total_tokens")
    if total_tokens is None or int(total_tokens or 0) <= 0:
        total_tokens = prompt_tokens + completion_tokens
    else:
        total_tokens = int(total_tokens)
    return prompt_tokens, completion_tokens, total_tokens


def estimate_request_cost(alias: str, model: str, messages: List[Dict[str, str]], max_tokens: int) -> float:
    prompt_tokens = max(1, sum(len(str(m.get("content", ""))) for m in messages or []) // 4)
    return calculate_request_cost(
        alias, model, {"prompt_tokens": prompt_tokens, "completion_tokens": max(1, int(max_tokens))}, messages
    )


def _database_epoch(cursor) -> float:
    cursor.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())" if DATABASE_URL else "SELECT (julianday('now') - 2440587.5) * 86400")
    return float(cursor.fetchone()[0])


def reserve_budget_sync(key_id: str, key_name: str, daily_budget: float, amount: float,
                        reservation_id: str, lease_seconds: float) -> Tuple[bool, float]:
    """Atomically reserve against a durable per-key daily account."""
    if not math.isfinite(daily_budget) or daily_budget < 0 or not math.isfinite(amount) or amount <= 0:
        raise ValueError("daily_budget must be non-negative and amount must be finite and positive")
    budget_value = str(Decimal(str(daily_budget)))
    amount_value = str(Decimal(str(amount)))
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        cursor = conn.cursor()
        # Acquire SQLite's write lock before reading the balance. PostgreSQL
        # serializes competing reservations on the account row UPDATE below.
        if not DATABASE_URL:
            cursor.execute("BEGIN IMMEDIATE")
        now = _database_epoch(cursor)
        stored_budget = budget_value if DATABASE_URL else float(daily_budget)
        stored_amount = amount_value if DATABASE_URL else float(amount)
        day_start = datetime.fromtimestamp(now, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        day = day_start.strftime("%Y-%m-%d")
        start = day_start.timestamp()
        cursor.execute(
            "UPDATE budget_reservations SET status = 'expired' WHERE status = 'active' AND expires_at <= ? RETURNING key_id,budget_date,amount",
            (now,),
        )
        expired = cursor.fetchall()
        for expired_key, expired_day, expired_amount in expired:
            cursor.execute(
                "UPDATE budget_accounts SET reserved = CASE WHEN reserved > ? THEN reserved-? ELSE 0 END WHERE key_id = ? AND budget_date = ?",
                (expired_amount, expired_amount, expired_key, expired_day),
            )
            BUDGET_RESERVATIONS.labels("expired").inc()
        cursor.execute("SELECT COALESCE(SUM(cost_usd),0) FROM cost_records WHERE key_id = ? AND timestamp >= ?", (str(key_id), start))
        existing_spend = float(cursor.fetchone()[0] or 0.0)
        cursor.execute(
            "INSERT INTO budget_accounts (key_id,budget_date,daily_budget,spent,reserved) VALUES (?,?,?,?,0) ON CONFLICT(key_id) DO NOTHING",
            (str(key_id), day, stored_budget, str(Decimal(str(existing_spend))) if DATABASE_URL else existing_spend),
        )
        cursor.execute(
            """UPDATE budget_accounts SET daily_budget = ?, budget_date = ?,
                   spent = CASE WHEN budget_date = ? THEN spent ELSE ? END,
                   reserved = CASE WHEN budget_date = ? THEN reserved + ? ELSE ? END
               WHERE key_id = ? AND
                   (CASE WHEN budget_date = ? THEN spent + reserved ELSE ? END) + ? <= ?
               RETURNING spent, reserved""",
            (stored_budget, day, day, str(Decimal(str(existing_spend))) if DATABASE_URL else existing_spend,
             day, stored_amount, stored_amount, str(key_id), day,
             str(Decimal(str(existing_spend))) if DATABASE_URL else existing_spend,
             stored_amount, stored_budget),
        )
        row = cursor.fetchone()
        if row is None:
            cursor.execute("SELECT spent,reserved FROM budget_accounts WHERE key_id = ?", (str(key_id),))
            current = cursor.fetchone()
            conn.commit()
            BUDGET_RESERVATIONS.labels("rejected").inc()
            return False, float(current[0] + current[1]) if current else existing_spend
        cursor.execute(
            "INSERT INTO budget_reservations (id,key_id,key_name,budget_date,amount,status,expires_at,created_at) VALUES (?,?,?,?,?,'active',?,?)",
            (reservation_id, str(key_id), key_name, day, stored_amount, now + lease_seconds, now),
        )
        conn.commit()
        BUDGET_RESERVATIONS.labels("reserved").inc()
        BUDGET_COST_USD.labels("estimated").observe(float(amount_value))
        return True, float(row[0] + row[1])
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def settle_budget_sync(reservation_id: str, actual_cost: float, key_id: str, key_name: str,
                       alias: str, provider: str, model: str, prompt_tokens: int,
                       completion_tokens: int, total_tokens: int) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        cursor = conn.cursor()
        now = _database_epoch(cursor)
        reservation_expiry_now = "EXTRACT(EPOCH FROM clock_timestamp())" if DATABASE_URL else "?"
        expiry_params = () if DATABASE_URL else (now,)
        cursor.execute(
            f"UPDATE budget_reservations SET status = 'settled' WHERE id = ? AND key_id = ? AND status = 'active' AND expires_at > {reservation_expiry_now} RETURNING key_id,budget_date,amount,created_at",
            (reservation_id, str(key_id), *expiry_params),
        )
        reservation = cursor.fetchone()
        if not reservation:
            conn.rollback()
            BUDGET_RESERVATIONS.labels("stale_settlement").inc()
            return False
        _, day, reserved, reserved_at = reservation
        cursor.execute(
            "UPDATE budget_accounts SET reserved = CASE WHEN reserved > ? THEN reserved-? ELSE 0 END, spent = spent+? WHERE key_id = ? AND budget_date = ?",
            (reserved, reserved, actual_cost, str(key_id), day),
        )
        cursor.execute(
            """INSERT INTO cost_records
               (key_id,key_name,alias,provider,model,prompt_tokens,completion_tokens,total_tokens,cost_usd,timestamp)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (str(key_id), key_name, alias, provider, model, prompt_tokens, completion_tokens,
             total_tokens, actual_cost, reserved_at),
        )
        conn.commit()
        BUDGET_RESERVATIONS.labels("settled").inc()
        BUDGET_COST_USD.labels("actual").observe(actual_cost)
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def release_budget_sync(reservation_id: str, key_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE budget_reservations SET status = 'released' WHERE id = ? AND key_id = ? AND status = 'active' RETURNING budget_date,amount",
            (reservation_id, str(key_id)),
        )
        reservation = cursor.fetchone()
        if not reservation:
            conn.rollback()
            BUDGET_RESERVATIONS.labels("already_finalized").inc()
            return False
        day, amount = reservation
        cursor.execute("UPDATE budget_accounts SET reserved = CASE WHEN reserved > ? THEN reserved-? ELSE 0 END WHERE key_id = ? AND budget_date = ?",
                       (amount, amount, str(key_id), day))
        conn.commit()
        BUDGET_RESERVATIONS.labels("released").inc()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
                    socket_connect_timeout=2.0,
                    socket_timeout=1.0,
                    health_check_interval=30,
                    decode_responses=True,
                )
            except Exception:
                self._redis = None
        return self._redis

    async def aclose(self) -> None:
        client, self._redis = self._redis, None
        if client is not None:
            await client.aclose()

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

    async def reserve_budget(self, key_id: str, key_name: str, daily_budget: float, amount: float,
                             lease_seconds: float = 120.0) -> Tuple[Optional[str], float]:
        reservation_id = uuid.uuid4().hex
        allowed, current = await asyncio.to_thread(
            reserve_budget_sync, str(key_id), key_name, daily_budget, amount,
            reservation_id, lease_seconds,
        )
        return (reservation_id if allowed else None), current

    async def settle_budget(self, reservation_id: str, key_id: str, key_name: str, alias: str,
                            provider: str, model: str, prompt_tokens: int,
                            completion_tokens: int, total_tokens: int, actual_cost: float) -> bool:
        settled = await asyncio.to_thread(
            settle_budget_sync, reservation_id, actual_cost, str(key_id), key_name,
            alias, provider, model, prompt_tokens, completion_tokens, total_tokens,
        )
        if settled:
            self._total_cost_memory += actual_cost
            r_client = self.get_client()
            if r_client:
                try:
                    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    pipe = r_client.pipeline()
                    pipe.incrbyfloat(f"cost:daily:{key_id}:{day}", actual_cost)
                    pipe.expire(f"cost:daily:{key_id}:{day}", 86400 * 2)
                    pipe.incrbyfloat(f"cost:alias:{alias}", actual_cost)
                    pipe.incrbyfloat("cost:total:gateway", actual_cost)
                    await pipe.execute()
                except Exception as exc:
                    logger.warning("Redis settled cost counter update failed: %s", type(exc).__name__)
        return settled

    async def release_budget(self, reservation_id: str, key_id: str) -> bool:
        return await asyncio.to_thread(release_budget_sync, reservation_id, str(key_id))

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
