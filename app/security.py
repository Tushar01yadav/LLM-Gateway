import asyncio
import hashlib
import ipaddress
import json
import logging
import math
import os
import secrets
from app.db import sqlite3
import sys
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import HTTPException, Header, Request, status
from prometheus_client import Counter

load_dotenv()

logger = logging.getLogger("llm_gateway.security")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [SECURITY] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(ch)
RATE_LIMIT_RESULTS = Counter("gateway_rate_limit_checks_total", "API rate-limit decisions", ("outcome", "coordination"))
RATE_LIMIT_REDIS_FAILURES = Counter("gateway_rate_limit_redis_failures_total", "Rate limiter Redis failures")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models.json")
ADMIN_TOKEN = os.getenv("GATEWAY_ADMIN_TOKEN", "").strip()
if not ADMIN_TOKEN:
    raise RuntimeError(
        "GATEWAY_ADMIN_TOKEN is not set. Refusing to start: set a strong admin token in the environment."
    )


def get_security_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            return cfg.get("rate_limiting", {})
    except Exception:
        return {}


def get_trusted_proxy_count() -> int:
    """Number of reverse proxies in front of the app. 0 = ignore X-Forwarded-For."""
    try:
        return max(0, int(os.getenv("TRUSTED_PROXY_COUNT", "0").strip() or 0))
    except ValueError:
        return 0


def _proxy_headers_mode() -> Optional[bool]:
    """True = uvicorn proxy headers enabled, False = explicitly disabled, None = unknown.

    uvicorn exposes no runtime flag, so this inspects the launch command. Deployments
    that do not start through the uvicorn CLI must set GATEWAY_PROXY_HEADERS_DISABLED=1.
    """
    if os.getenv("GATEWAY_PROXY_HEADERS_DISABLED", "").strip().lower() in ("1", "true", "yes"):
        return False
    argv = [arg.lower() for arg in sys.argv]
    if "--no-proxy-headers" in argv:
        return False
    if "--proxy-headers" in argv:
        return True
    return None


def check_deploy_config() -> Dict[str, Any]:
    """Validate proxy/client-IP configuration at startup.

    Raises RuntimeError in production when TRUSTED_PROXY_COUNT > 0 but uvicorn
    proxy headers are not provably disabled: uvicorn would rewrite request.client
    from X-Forwarded-For before get_client_ip can apply the trusted-hop logic,
    defeating per-IP rate limiting. In non-production it logs a warning instead.
    """
    app_env = os.getenv("APP_ENV", "production").strip().lower()
    is_prod = app_env not in ("dev", "development", "local", "test")
    trusted = get_trusted_proxy_count()
    proxy_headers = _proxy_headers_mode()

    errors: List[str] = []
    warnings: List[str] = []

    if trusted > 0 and proxy_headers is not False:
        state = "enabled" if proxy_headers else "not provably disabled"
        errors.append(
            f"TRUSTED_PROXY_COUNT={trusted} but uvicorn proxy headers are {state}. "
            "Start the server with --no-proxy-headers (or set GATEWAY_PROXY_HEADERS_DISABLED=1 "
            "for non-CLI launchers), otherwise request.client is rewritten from "
            "X-Forwarded-For and the trusted-hop logic is bypassed."
        )
    if is_prod and trusted == 0:
        warnings.append(
            "APP_ENV=production with TRUSTED_PROXY_COUNT=0: X-Forwarded-For is ignored and "
            "every client resolves to the socket peer. If a reverse proxy sits in front, "
            "set TRUSTED_PROXY_COUNT to the number of trusted proxies."
        )

    result = {
        "app_env": app_env,
        "is_production": is_prod,
        "trusted_proxy_count": trusted,
        "proxy_headers": proxy_headers,
        "errors": errors,
        "warnings": warnings,
    }

    for warning in warnings:
        logger.warning(f"[DEPLOY] {warning}")
    for error in errors:
        if is_prod:
            logger.error(f"[DEPLOY] {error}")
        else:
            logger.warning(f"[DEPLOY] {error} (non-production: not fatal)")

    if errors and is_prod:
        raise RuntimeError("Invalid deploy configuration: " + " ".join(errors))
    return result


def get_client_ip(request: Request) -> str:
    """
    Resolve the client IP.

    X-Forwarded-For is only honored when TRUSTED_PROXY_COUNT > 0. Each proxy
    appends the address it received from, so with N trusted proxies the real
    client is the Nth entry from the right. Anything a client prepends spoofs
    only entries further left, which are never used. Invalid entries fall back
    to the socket peer.
    """
    peer = request.client.host if request.client else "unknown"
    trusted = get_trusted_proxy_count()
    if trusted <= 0:
        return peer

    forwarded = request.headers.get("x-forwarded-for", "")
    parts = [p.strip() for p in forwarded.split(",") if p.strip()]
    if len(parts) < trusted:
        return peer

    candidate = parts[-trusted]
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return peer
    return candidate


def rate_limit_fail_closed() -> bool:
    """
    Redis-outage policy. RATE_LIMIT_FAIL_MODE=open|closed wins; otherwise the
    models.json rate_limiting.redis_fail_closed flag is used. Documented in
    README/.env.example; open means allow on outage, closed rejects with 429.
    """
    mode = os.getenv("RATE_LIMIT_FAIL_MODE", "").strip().lower()
    if mode in ("open", "closed"):
        return mode == "closed"
    return bool(get_security_config().get("redis_fail_closed", False))


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
        cursor.execute("ALTER TABLE gateway_api_keys ADD COLUMN IF NOT EXISTS daily_budget REAL DEFAULT 5.0")
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
        "INSERT INTO gateway_api_keys (name, key_hash, prefix, status, quota, daily_budget) VALUES (?, ?, ?, 'active', ?, ?) RETURNING id",
        (name.strip() or "Default Key", key_hash, prefix, quota, daily_budget),
    )
    key_id = cursor.fetchone()[0]
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


# Single-round-trip token bucket. Read-modify-write happens inside Redis, and
# TIME comes from the server so clock skew between uvicorn workers cannot
# re-open the race the old GET/SET pipeline had.
_TOKEN_BUCKET_LUA = """
local tokens = redis.call('GET', KEYS[1])
local last = redis.call('GET', KEYS[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
tokens = tonumber(tokens)
last = tonumber(last)
if tokens == nil or last == nil then
  tokens = capacity
  last = now
end
local elapsed = now - last
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * refill)
local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) / refill)
  if retry < 1 then retry = 1 end
end
redis.call('SET', KEYS[1], tostring(tokens), 'EX', ttl)
redis.call('SET', KEYS[2], tostring(now), 'EX', ttl)
-- Redis truncates Lua floats in replies, so send the floats as strings.
return {allowed, tostring(retry), tostring(tokens)}
"""


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
        self._mem_lock = threading.Lock()
        self._mem_buckets: OrderedDict[str, Dict[str, float]] = OrderedDict()
        self._mem_max_buckets = 10000
        self._mem_bucket_ttl_seconds = 3600.0
        self._last_redis_fail: float = 0.0

    async def get_client(self) -> Optional[aioredis.Redis]:
        now = time.time()
        if now - self._last_redis_fail < 1.0:
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
                        # Under concurrent load, TCP setup in a container network
                        # measured up to ~0.43s; 0.5s rejected healthy connections.
                        socket_connect_timeout=2.0,
                        socket_timeout=1.0,
                        health_check_interval=30,
                    )
                except Exception as e:
                    logger.error(f"[SECURITY] Failed to initialize Redis client: {e}")
                    self._redis_client = None
                    self._last_redis_fail = now
            return self._redis_client

    async def aclose(self) -> None:
        client, self._redis_client = self._redis_client, None
        if client is not None:
            await client.aclose()

    async def check_rate_limit(self, key_id: Any, quota_rpm: int, cost: float = 1.0) -> Tuple[bool, int, float]:
        """
        Token-bucket evaluation. `key_id` is a bucket key such as "key:12",
        "user:7" or "ip:203.0.113.9" so every caller class gets its own bucket.
        Redis path is one atomic Lua script (server-side TIME), so concurrent
        workers can never over-admit. Returns: (allowed, retry_after, remaining).
        """
        cfg = get_security_config()
        if not cfg.get("enabled", True):
            RATE_LIMIT_RESULTS.labels("allowed", "disabled").inc()
            return True, 0, quota_rpm

        fail_closed = rate_limit_fail_closed()
        capacity = float(max(1, quota_rpm))
        refill_rate = capacity / 60.0  # tokens per second
        cost = max(0.0, float(cost))
        bucket = str(key_id)
        now = time.time()

        def _mem_check() -> Tuple[bool, int, float]:
            with self._mem_lock:
                b = self._mem_buckets.get(bucket)
                if b is not None and now - b["last_time"] >= self._mem_bucket_ttl_seconds:
                    self._mem_buckets.pop(bucket, None)
                    b = None
                if b is None:
                    while self._mem_buckets:
                        oldest_bucket = next(iter(self._mem_buckets))
                        oldest = self._mem_buckets[oldest_bucket]
                        if now - oldest["last_time"] < self._mem_bucket_ttl_seconds:
                            break
                        self._mem_buckets.popitem(last=False)
                    if len(self._mem_buckets) >= self._mem_max_buckets:
                        self._mem_buckets.popitem(last=False)
                    tokens = capacity
                    last = now
                else:
                    tokens = min(capacity, b["tokens"] + max(0.0, now - b["last_time"]) * refill_rate)
                    last = now
                if tokens >= cost:
                    tokens -= cost
                    self._mem_buckets[bucket] = {"tokens": tokens, "last_time": last}
                    self._mem_buckets.move_to_end(bucket)
                    return True, 0, round(tokens, 2)
                needed = cost - tokens
                self._mem_buckets[bucket] = {"tokens": tokens, "last_time": last}
                self._mem_buckets.move_to_end(bucket)
                return False, max(1, math.ceil(needed / refill_rate)), round(tokens, 2)

        client = await self.get_client()
        if client is None:
            logger.error(
                f"[SECURITY] 🚨 REDIS CONNECTION DOWN! Falling back safely with fail_closed={fail_closed}."
            )
            if fail_closed:
                RATE_LIMIT_RESULTS.labels("rejected", "fail_closed").inc()
                return False, 5, 0.0
            result = _mem_check()
            RATE_LIMIT_RESULTS.labels("allowed" if result[0] else "rejected", "local_degraded").inc()
            return result

        token_key = f"gw:rate:{bucket}:tokens"
        time_key = f"gw:rate:{bucket}:last_time"

        try:
            result = await client.eval(
                _TOKEN_BUCKET_LUA,
                2,
                token_key,
                time_key,
                capacity,
                refill_rate,
                cost,
                120,
            )
            allowed = int(result[0]) == 1
            retry_after = int(result[1])
            remaining = round(float(result[2]), 2)
            RATE_LIMIT_RESULTS.labels("allowed" if allowed else "rejected", "redis").inc()
            return allowed, retry_after, remaining
        except Exception as exc:
            self._last_redis_fail = time.time()
            RATE_LIMIT_REDIS_FAILURES.inc()
            logger.error(
                f"[SECURITY] 🚨 REDIS ERROR during token bucket check: {exc}. "
                f"Falling back safely (fail_closed={fail_closed})."
            )
            if fail_closed:
                RATE_LIMIT_RESULTS.labels("rejected", "fail_closed").inc()
                return False, 5, 0.0
            result = _mem_check()
            RATE_LIMIT_RESULTS.labels("allowed" if result[0] else "rejected", "local_degraded").inc()
            return result

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

    if not token or not secrets.compare_digest(token, ADMIN_TOKEN):
        logger.warning("Unauthorized admin access attempt rejected (403 Forbidden).")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Valid Admin Token required for this operation.",
        )
    return True
