import asyncio
import hashlib
import json
import logging
import math
import os
import secrets
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import HTTPException, Header, Request, status

load_dotenv()

logger = logging.getLogger("llm_gateway.security")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [SECURITY] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(ch)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models.json")
ADMIN_TOKEN = os.getenv("GATEWAY_ADMIN_TOKEN", os.getenv("ADMIN_TOKEN", "gw-admin-secret-key-production"))


def get_security_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            return cfg.get("rate_limiting", {})
    except Exception:
        return {}


def init_security_db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gateway_api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            key_hash TEXT UNIQUE NOT NULL,
            prefix TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            quota INTEGER NOT NULL DEFAULT 60,
            daily_budget REAL NOT NULL DEFAULT 5.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gw_key_hash ON gateway_api_keys(key_hash);")
    try:
        cursor.execute("ALTER TABLE gateway_api_keys ADD COLUMN daily_budget REAL DEFAULT 5.0")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


init_security_db()


def hash_api_key(raw_key: str) -> str:
    """Computes SHA-256 hash of API key for safe database storage."""
    return hashlib.sha256(raw_key.strip().encode("utf-8")).hexdigest()


def create_gateway_api_key(name: str, quota: int = 60, daily_budget: float = 5.0) -> Tuple[str, Dict[str, Any]]:
    """
    Generates a cryptographically secure gateway API key (gw_live_...).
    Stores only the SHA-256 hash. Returns plaintext raw key ONCE upon creation.
    """
    raw_key = f"gw_live_{secrets.token_urlsafe(32)}"
    key_hash = hash_api_key(raw_key)
    prefix = f"{raw_key[:12]}...{raw_key[-4:]}"

    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO gateway_api_keys (name, key_hash, prefix, status, quota, daily_budget) VALUES (?, ?, ?, 'active', ?, ?)",
        (name.strip() or "Default Key", key_hash, prefix, quota, daily_budget),
    )
    key_id = cursor.lastrowid
    conn.commit()

    cursor.execute("SELECT id, name, prefix, status, quota, daily_budget, created_at FROM gateway_api_keys WHERE id = ?", (key_id,))
    row = cursor.fetchone()
    conn.close()

    info = {
        "id": row[0],
        "name": row[1],
        "prefix": row[2],
        "status": row[3],
        "quota": row[4],
        "daily_budget": row[5],
        "created_at": row[6],
        "key": raw_key,
    }
    logger.info(f"Generated Gateway API key '{info['name']}' (ID: {key_id}, Prefix: {prefix}). Raw key presented once.")
    return raw_key, info


def validate_gateway_api_key(raw_key: str) -> Optional[Dict[str, Any]]:
    """Validates key by checking its SHA-256 hash and ensuring status == 'active'."""
    if not raw_key or not raw_key.strip():
        return None
    key_hash = hash_api_key(raw_key)

    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, name, prefix, status, quota, daily_budget, created_at FROM gateway_api_keys WHERE key_hash = ?",
        (key_hash,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None
    if row[3] != "active":
        return None

    return {
        "id": row[0],
        "name": row[1],
        "prefix": row[2],
        "status": row[3],
        "quota": row[4],
        "daily_budget": row[5] if row[5] is not None else 5.0,
        "created_at": row[6],
    }


def list_gateway_api_keys() -> List[Dict[str, Any]]:
    """Lists all stored gateway API keys. Never exposes plaintext keys or hashes."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, prefix, status, quota, daily_budget, created_at FROM gateway_api_keys ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "id": r[0],
            "name": r[1],
            "prefix": r[2],
            "status": r[3],
            "quota": r[4],
            "daily_budget": r[5] if r[5] is not None else 5.0,
            "created_at": r[6],
        }
        for r in rows
    ]


def revoke_gateway_api_key(key_id: int) -> bool:
    """Revokes a gateway API key."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("UPDATE gateway_api_keys SET status = 'revoked' WHERE id = ?", (key_id,))
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    if changed:
        logger.warning(f"Gateway API key ID {key_id} has been REVOKED.")
    return changed


class RedisTokenBucketRateLimiter:
    """
    Per-key token bucket rate limiter backed by Redis.
    Configurable capacity and replenishment rate derived from key quota.
    Gracefully handles Redis outage:
    - Logs loudly
    - Safely falls back according to redis_fail_closed policy (default fail-open).
    """

    def __init__(self):
        self._redis_client: Optional[aioredis.Redis] = None
        self._lock = asyncio.Lock()
        self._mem_buckets: Dict[int, Dict[str, float]] = {}
        self._last_redis_fail: float = 0.0

    async def get_client(self) -> Optional[aioredis.Redis]:
        now = time.time()
        if now - self._last_redis_fail < 10.0:
            return None

        cfg = get_security_config()
        redis_url = os.getenv("REDIS_URL", cfg.get("redis_url", "redis://127.0.0.1:6379/0"))
        async with self._lock:
            if self._redis_client is None:
                try:
                    self._redis_client = aioredis.from_url(
                        redis_url,
                        encoding="utf-8",
                        decode_responses=True,
                        socket_timeout=0.5,
                        socket_connect_timeout=0.5,
                    )
                except Exception as e:
                    logger.error(f"[SECURITY] Failed to initialize Redis client: {e}")
                    self._redis_client = None
                    self._last_redis_fail = now
            return self._redis_client

    async def check_rate_limit(self, key_id: int, quota_rpm: int) -> Tuple[bool, int, float]:
        """
        Token-bucket evaluation in Redis (with in-memory fallback on outage):
        Returns: (allowed: bool, retry_after: int, remaining: float)
        """
        cfg = get_security_config()
        if not cfg.get("enabled", True):
            return True, 0, quota_rpm

        fail_closed = cfg.get("redis_fail_closed", False)
        capacity = float(quota_rpm)
        refill_rate = capacity / 60.0  # tokens per second
        now = time.time()

        def _mem_check() -> Tuple[bool, int, float]:
            if key_id not in self._mem_buckets:
                self._mem_buckets[key_id] = {"tokens": capacity - 1.0, "last_time": now}
                return True, 0, capacity - 1.0
            b = self._mem_buckets[key_id]
            elapsed = max(0.0, now - b["last_time"])
            b["tokens"] = min(capacity, b["tokens"] + elapsed * refill_rate)
            b["last_time"] = now
            if b["tokens"] >= 1.0:
                b["tokens"] -= 1.0
                return True, 0, round(b["tokens"], 2)
            else:
                needed = 1.0 - b["tokens"]
                retry_after = max(1, math.ceil(needed / refill_rate))
                return False, retry_after, round(b["tokens"], 2)

        client = await self.get_client()
        if client is None:
            logger.error(
                f"[SECURITY] 🚨 REDIS CONNECTION DOWN! Falling back safely with fail_closed={fail_closed}."
            )
            if fail_closed:
                return False, 5, 0.0
            return _mem_check()

        token_key = f"gw:rate:{key_id}:tokens"
        time_key = f"gw:rate:{key_id}:last_time"

        try:
            # Atomic evaluation using Redis pipeline
            pipe = client.pipeline()
            pipe.get(token_key)
            pipe.get(time_key)
            results = await pipe.execute()

            raw_tokens, raw_last_time = results[0], results[1]

            if raw_tokens is None or raw_last_time is None:
                current_tokens = capacity - 1.0
                last_time = now
                pipe = client.pipeline()
                pipe.set(token_key, str(current_tokens), ex=120)
                pipe.set(time_key, str(now), ex=120)
                await pipe.execute()
                return True, 0, current_tokens

            last_time = float(raw_last_time)
            last_tokens = float(raw_tokens)
            elapsed = max(0.0, now - last_time)
            replenished = elapsed * refill_rate
            current_tokens = min(capacity, last_tokens + replenished)

            if current_tokens >= 1.0:
                current_tokens -= 1.0
                pipe = client.pipeline()
                pipe.set(token_key, str(current_tokens), ex=120)
                pipe.set(time_key, str(now), ex=120)
                await pipe.execute()
                return True, 0, round(current_tokens, 2)
            else:
                needed = 1.0 - current_tokens
                retry_after = max(1, math.ceil(needed / refill_rate))
                pipe = client.pipeline()
                pipe.set(token_key, str(current_tokens), ex=120)
                pipe.set(time_key, str(now), ex=120)
                await pipe.execute()
                return False, retry_after, round(current_tokens, 2)

        except Exception as exc:
            self._last_redis_fail = time.time()
            logger.error(
                f"[SECURITY] 🚨 REDIS ERROR during token bucket check: {exc}. "
                f"Falling back safely (fail_closed={fail_closed})."
            )
            if fail_closed:
                return False, 5, 0.0
            return _mem_check()

    async def close(self):
        async with self._lock:
            if self._redis_client is not None:
                try:
                    await self._redis_client.aclose()
                except Exception:
                    pass
                self._redis_client = None


RATE_LIMITER = RedisTokenBucketRateLimiter()


def verify_admin_auth(
    authorization: Optional[str] = Header(None),
    x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
) -> bool:
    """Requires valid admin token to access key management or provider key editing."""
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:].strip()
    elif x_admin_token:
        token = x_admin_token.strip()

    if not token or token != ADMIN_TOKEN:
        logger.warning("Unauthorized admin access attempt rejected (403 Forbidden).")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Valid Admin Token required for this operation.",
        )
    return True
