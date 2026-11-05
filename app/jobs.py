import asyncio
import json
import logging
import os
import socket
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import HTTPException

from app.cost import BUDGET_GUARD, calculate_request_cost
from app.guardrails import GUARDRAILS
from app.reliability import TRAFFIC_BROADCASTER, execute_reliable_completion

logger = logging.getLogger("llm_gateway.jobs")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [JOBS] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(ch)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gateway.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models.json")


JOBS_MAX_ATTEMPTS = 5
DEFAULT_JOB_MAX_TOKENS = 1000

# Reliable-delivery keys. A job moves queue -> processing atomically via
# BLMOVE; ownership and lease expiry live in LEASE_KEY. A worker that dies
# leaves an expired lease behind, and any surviving worker requeues it.
QUEUE_KEY = "gw:jobs:queue"
PROCESSING_KEY = "gw:jobs:processing"
LEASE_KEY = "gw:jobs:leases"
DELAYED_KEY = "gw:jobs:delayed"
DLQ_KEY = "gw:jobs:dlq"
LEGACY_DLQ_QUEUE = "job_queue:dlq"

def _env_positive_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


# Configurable so a slow-but-alive worker can be distinguished from a dead one
# without redeploying. Heartbeat is capped at half the lease so at least one
# renewal is possible before expiry.
JOB_LEASE_SECONDS = _env_positive_float("GATEWAY_JOB_LEASE_SECONDS", 15.0)
JOB_HEARTBEAT_SECONDS = min(
    _env_positive_float("GATEWAY_JOB_HEARTBEAT_SECONDS", 5.0), JOB_LEASE_SECONDS / 2
)
JOB_RECOVERY_INTERVAL = 5.0

_EXTEND_LEASE_LUA = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local ok, lease = pcall(cjson.decode, raw)
if not ok then return 0 end
if lease.owner ~= ARGV[2] then return -1 end
lease.expires = tonumber(ARGV[3])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(lease))
return 1
"""

_RECOVER_LEASES_LUA = """
local now = tonumber(redis.call('TIME')[1])
local moved = 0
local leases = redis.call('HGETALL', KEYS[1])
for i = 1, #leases, 2 do
  local jid = leases[i]
  local raw = leases[i + 1]
  local ok, lease = pcall(cjson.decode, raw)
  if (not ok) or (tonumber(lease.expires) or 0) <= now then
    redis.call('LREM', KEYS[2], 0, jid)
    redis.call('HDEL', KEYS[1], jid)
    redis.call('RPUSH', KEYS[3], jid)
    moved = moved + 1
  end
end
local due = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, 100)
for i = 1, #due do
  local jid = due[i]
  if redis.call('ZREM', KEYS[4], jid) == 1 then
    redis.call('RPUSH', KEYS[3], jid)
    moved = moved + 1
  end
end
return moved
"""


class _TerminalJobError(Exception):
    """A rejection that retrying cannot fix (guardrail block, budget exhausted)."""


def _get_job_budget_limit(key_id: str) -> float:
    try:
        numeric_id = int(key_id)
    except (TypeError, ValueError):
        return 5.0
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("SELECT daily_budget FROM gateway_api_keys WHERE id = ?", (numeric_id,))
    row = cursor.fetchone()
    conn.close()
    if row and row[0] is not None:
        return float(row[0])
    return 5.0


def _has_global_idempotency_uniqueness(cursor) -> bool:
    """True if the legacy column-level UNIQUE(idempotency_key) autoindex exists."""
    cursor.execute("PRAGMA index_list(async_jobs)")
    for row in cursor.fetchall():
        # (seq, name, unique, origin, partial)
        if row[2] != 1:
            continue
        cursor.execute(f"PRAGMA index_info('{row[1]}')")
        columns = [r[2] for r in cursor.fetchall()]
        if columns == ["idempotency_key"]:
            return True
    return False


def _migrate_jobs_table(cursor):
    """
    Rebuild async_jobs so idempotency is unique per (key_id, idempotency_key).
    A column-level UNIQUE constraint cannot be dropped in SQLite, so the only
    migration path is create-copy-drop-rename.
    """
    if not _has_global_idempotency_uniqueness(cursor):
        return
    logger.info("[MIGRATION] Rebuilding async_jobs with UNIQUE(key_id, idempotency_key)")
    cursor.execute("""
        CREATE TABLE async_jobs_new (
            id TEXT PRIMARY KEY,
            idempotency_key TEXT,
            key_id TEXT,
            key_name TEXT,
            status TEXT NOT NULL,
            payload TEXT NOT NULL,
            result TEXT,
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            cost_usd REAL DEFAULT 0.0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(key_id, idempotency_key)
        )
    """)
    cursor.execute("""
        INSERT INTO async_jobs_new
            (id, idempotency_key, key_id, key_name, status, payload, result, error,
             attempts, max_attempts, cost_usd, created_at, updated_at)
        SELECT id, idempotency_key, key_id, key_name, status, payload, result, error,
               attempts, max_attempts, cost_usd, created_at, updated_at
        FROM async_jobs
    """)
    cursor.execute("DROP TABLE async_jobs")
    cursor.execute("ALTER TABLE async_jobs_new RENAME TO async_jobs")


def init_jobs_db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS async_jobs (
            id TEXT PRIMARY KEY,
            idempotency_key TEXT,
            key_id TEXT,
            key_name TEXT,
            status TEXT NOT NULL,
            payload TEXT NOT NULL,
            result TEXT,
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            cost_usd REAL DEFAULT 0.0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(key_id, idempotency_key)
        )
    """)
    _migrate_jobs_table(cursor)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON async_jobs(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_idem ON async_jobs(key_id, idempotency_key);")
    conn.commit()
    conn.close()


init_jobs_db()


def _finalize_success_sync(
    job_id: str,
    result_json: str,
    cost_usd: float,
    attempts: int,
    key_id: str,
    key_name: str,
    alias: str,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
) -> bool:
    """
    Commit the job result and its cost row in one SQLite transaction.

    The status check and both writes share `BEGIN IMMEDIATE`, so a job that is
    retried after a worker crash cannot be finalized or charged twice.
    """
    conn = sqlite3.connect(DB_PATH, timeout=15.0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT status FROM async_jobs WHERE id = ?", (job_id,)).fetchone()
        if not row or row[0] == "done":
            conn.execute("ROLLBACK")
            return False
        conn.execute(
            """INSERT INTO cost_records
               (key_id, key_name, alias, provider, model, prompt_tokens, completion_tokens,
                total_tokens, cost_usd, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(key_id), key_name, alias, provider, model, prompt_tokens,
                completion_tokens, total_tokens, cost_usd, time.time(),
            ),
        )
        conn.execute(
            """UPDATE async_jobs
               SET status = 'done', result = ?, attempts = ?, cost_usd = ?, updated_at = ?
               WHERE id = ?""",
            (result_json, attempts, cost_usd, time.time(), job_id),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _begin_attempt_sync(job_id: str) -> Optional[Dict[str, Any]]:
    """Atomically count the attempt and mark the job running. None if already terminal."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE async_jobs
           SET status = 'running', attempts = attempts + 1, updated_at = ?
           WHERE id = ? AND status NOT IN ('done', 'dlq')""",
        (time.time(), job_id),
    )
    changed = cursor.rowcount
    cursor.execute(
        "SELECT attempts, max_attempts, status FROM async_jobs WHERE id = ?", (job_id,)
    )
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    if not changed or not row:
        return None
    return {"attempts": row[0], "max_attempts": row[1], "status": row[2]}


def _set_status_sync(job_id: str, status_value: str, error: Optional[str] = None, reset_attempts: bool = False):
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    if reset_attempts:
        cursor.execute(
            "UPDATE async_jobs SET status = ?, error = ?, attempts = 0, updated_at = ? WHERE id = ?",
            (status_value, error, time.time(), job_id),
        )
    else:
        cursor.execute(
            "UPDATE async_jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?",
            (status_value, error, time.time(), job_id),
        )
    conn.commit()
    conn.close()


class JobQueueManager:
    """
    Redis-backed reliable queue with automatic in-memory fallback.

    Delivery: RPUSH enqueue -> BLMOVE queue/processing -> lease + heartbeat ->
    ack. Expired leases and delayed retries are requeued by any live worker, so
    killing a worker process cannot lose a job. Completion (result + cost row)
    is a single conditional SQLite transaction, so a retried job completes
    exactly once from the caller's perspective.
    """

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self._redis: Optional[aioredis.Redis] = None
        self._memory_queue: asyncio.Queue = asyncio.Queue()
        self._memory_processing: Dict[str, float] = {}
        self._memory_dlq: Dict[str, Dict[str, Any]] = {}
        self._worker_running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._last_recovery = 0.0
        self._orphan_seen: Dict[str, float] = {}
        self._current_job_id: Optional[str] = None

    def get_client(self) -> Optional[aioredis.Redis]:
        if self._redis is None:
            try:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    socket_connect_timeout=0.5,
                    # Must exceed the BLMOVE timeout below, otherwise the client
                    # can time out after Redis already moved the job and lose it.
                    socket_timeout=5.0,
                    decode_responses=True,
                )
            except Exception:
                self._redis = None
        return self._redis

    async def enqueue_job(
        self,
        payload: Dict[str, Any],
        key_id: str,
        key_name: str,
        idempotency_key: Optional[str] = None,
        max_attempts: int = 3,
    ) -> Dict[str, Any]:
        """
        Creates job record. If idempotency_key is provided and exists,
        returns the existing job without re-processing or double charging.
        """
        now = time.time()
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        key_id = str(key_id)
        max_attempts = max(1, min(int(max_attempts), JOBS_MAX_ATTEMPTS))

        def _existing_by_idempotency(cursor) -> Optional[Dict[str, Any]]:
            cursor.execute(
                """SELECT id, status, payload, result, error, attempts, max_attempts, cost_usd, created_at
                   FROM async_jobs WHERE key_id = ? AND idempotency_key = ?""",
                (key_id, idempotency_key),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "status": row[1],
                "idempotency_key": idempotency_key,
                "reused": True,
                "attempts": row[5],
                "cost_usd": row[7],
                "created_at": row[8],
            }

        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()

        # 1. Idempotency check, scoped to this caller only.
        if idempotency_key:
            existing_job = _existing_by_idempotency(cursor)
            if existing_job:
                conn.close()
                logger.info(
                    f"[IDEMPOTENCY] Reusing existing job {existing_job['id']} for key '{idempotency_key}' "
                    f"(scope: {key_id}, status: {existing_job['status']})"
                )
                await TRAFFIC_BROADCASTER.broadcast("idempotency_hit", {
                    "job_id": existing_job["id"],
                    "idempotency_key": idempotency_key,
                })
                return existing_job

        # 2. Insert new job. The composite UNIQUE index is the source of truth:
        # if two concurrent requests pass the SELECT above, one INSERT loses and
        # we return the winner's job instead of double-enqueueing.
        payload_json = json.dumps(payload)
        try:
            cursor.execute("""
                INSERT INTO async_jobs (id, idempotency_key, key_id, key_name, status, payload, attempts, max_attempts, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?)
            """, (job_id, idempotency_key, key_id, key_name, payload_json, max_attempts, now, now))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            existing_job = _existing_by_idempotency(cursor) if idempotency_key else None
            conn.close()
            if existing_job:
                logger.info(
                    f"[IDEMPOTENCY] Concurrent insert lost for key '{idempotency_key}' "
                    f"(scope: {key_id}); returning existing job {existing_job['id']}."
                )
                await TRAFFIC_BROADCASTER.broadcast("idempotency_hit", {
                    "job_id": existing_job["id"],
                    "idempotency_key": idempotency_key,
                })
                return existing_job
            raise
        conn.close()

        # 3. Push to queue
        r_client = self.get_client()
        enqueued_redis = False
        if r_client:
            try:
                await r_client.rpush(QUEUE_KEY, job_id)
                enqueued_redis = True
            except Exception as e:
                logger.warning(f"Redis rpush failed ({e}), falling back to memory queue.")

        if not enqueued_redis:
            await self._memory_queue.put(job_id)

        await TRAFFIC_BROADCASTER.broadcast("job_enqueued", {
            "job_id": job_id,
            "status": "queued",
            "idempotency_key": idempotency_key,
        })
        logger.info(f"Job {job_id} enqueued successfully (Idempotency: {idempotency_key})")

        return {
            "id": job_id,
            "status": "queued",
            "idempotency_key": idempotency_key,
            "reused": False,
            "attempts": 0,
            "max_attempts": max_attempts,
            "created_at": now,
        }

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, idempotency_key, key_id, key_name, status, payload, result, error, attempts, max_attempts, cost_usd, created_at, updated_at
            FROM async_jobs WHERE id = ?
        """, (job_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        result_obj = json.loads(row[6]) if row[6] else None
        payload_obj = json.loads(row[5]) if row[5] else {}

        return {
            "id": row[0],
            "idempotency_key": row[1],
            "key_id": row[2],
            "key_name": row[3],
            "status": row[4],
            "payload": payload_obj,
            "result": result_obj,
            "error": row[7],
            "attempts": row[8],
            "max_attempts": row[9],
            "cost_usd": row[10],
            "created_at": row[11],
            "updated_at": row[12],
        }

    async def list_dlq(self) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, idempotency_key, key_name, payload, error, attempts, max_attempts, created_at, updated_at
            FROM async_jobs WHERE status = 'dlq' ORDER BY updated_at DESC
        """)
        rows = cursor.fetchall()
        conn.close()

        items = []
        for r in rows:
            items.append({
                "id": r[0],
                "idempotency_key": r[1],
                "key_name": r[2],
                "payload": json.loads(r[3]) if r[3] else {},
                "error": r[4],
                "attempts": r[5],
                "max_attempts": r[6],
                "created_at": r[7],
                "updated_at": r[8],
            })
        return items

    async def clear_dlq(self) -> int:
        """Purges all dead-lettered jobs from database and queue."""
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM async_jobs WHERE status = 'dlq'")
        count = cursor.rowcount
        conn.commit()
        conn.close()

        self._memory_dlq.clear()
        r_client = self.get_client()
        if r_client:
            try:
                # Both calls must be awaited: the old code dropped the coroutine.
                await r_client.delete(DLQ_KEY)
                await r_client.delete(LEGACY_DLQ_QUEUE)
            except Exception as e:
                logger.warning(f"Redis DLQ clear failed: {e}")
        return count

    async def replay_dlq_job(self, job_id: str) -> Dict[str, Any]:
        """Requeues a dead-lettered job with a fresh attempt budget."""
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM async_jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        if not row or row[0] != "dlq":
            conn.close()
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not in DLQ")

        # Replay is an operator decision to retry: reset the attempt counter.
        cursor.execute(
            "UPDATE async_jobs SET status = 'queued', error = NULL, attempts = 0, updated_at = ? WHERE id = ?",
            (time.time(), job_id),
        )
        conn.commit()
        conn.close()

        r_client = self.get_client()
        replayed_redis = False
        if r_client:
            try:
                await r_client.lrem(PROCESSING_KEY, 0, job_id)
                await r_client.hdel(LEASE_KEY, job_id)
                await r_client.zrem(DELAYED_KEY, job_id)
                await r_client.hdel(DLQ_KEY, job_id)
                if await r_client.lpos(QUEUE_KEY, job_id) is None:
                    await r_client.rpush(QUEUE_KEY, job_id)
                replayed_redis = True
            except Exception as e:
                logger.warning(f"Redis replay push failed: {e}")

        if not replayed_redis:
            await self._memory_queue.put(job_id)

        await TRAFFIC_BROADCASTER.broadcast("job_replayed", {"job_id": job_id, "status": "queued"})
        logger.info(f"Replayed DLQ job {job_id} with attempts reset to 0")
        return {"id": job_id, "status": "queued", "replayed": True}

    # ------------------------------------------------------------------
    # Lease / recovery primitives
    # ------------------------------------------------------------------
    async def _set_lease(self, job_id: str, expires: Optional[float] = None) -> None:
        r_client = self.get_client()
        if not r_client:
            self._memory_processing[job_id] = expires or (time.time() + JOB_LEASE_SECONDS)
            return
        try:
            await r_client.hset(
                LEASE_KEY,
                job_id,
                json.dumps({
                    "owner": self._owner,
                    "expires": expires or (time.time() + JOB_LEASE_SECONDS),
                }),
            )
        except Exception as e:
            logger.warning(f"Could not set lease for job {job_id}: {e}")

    async def _ack(self, job_id: str) -> None:
        self._memory_processing.pop(job_id, None)
        self._orphan_seen.pop(job_id, None)
        r_client = self.get_client()
        if not r_client:
            return
        try:
            await r_client.lrem(PROCESSING_KEY, 0, job_id)
            await r_client.hdel(LEASE_KEY, job_id)
        except Exception as e:
            logger.warning(f"Could not acknowledge job {job_id}: {e}")

    async def _requeue_job(self, job_id: str, delay: float = 0.0) -> None:
        _set_status_sync(job_id, "queued")
        r_client = self.get_client()
        if r_client:
            try:
                await r_client.lrem(PROCESSING_KEY, 0, job_id)
                await r_client.hdel(LEASE_KEY, job_id)
                if delay > 0:
                    await r_client.zadd(DELAYED_KEY, {job_id: time.time() + delay})
                elif await r_client.lpos(QUEUE_KEY, job_id) is None:
                    await r_client.rpush(QUEUE_KEY, job_id)
                return
            except Exception as e:
                logger.warning(f"Redis requeue failed for {job_id}: {e}")
        self._memory_processing.pop(job_id, None)
        if delay > 0:
            asyncio.create_task(self._memory_requeue_later(job_id, delay))
        else:
            await self._memory_queue.put(job_id)

    async def _memory_requeue_later(self, job_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        await self._memory_queue.put(job_id)

    async def recover_stale_jobs(self):
        """Crash recovery for rows left 'running' with a stale heartbeat."""
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        stale_threshold = time.time() - 45.0
        cursor.execute(
            "SELECT id, attempts, max_attempts FROM async_jobs WHERE status = 'running' AND updated_at < ?",
            (stale_threshold,),
        )
        stale_jobs = cursor.fetchall()
        conn.close()

        for job_id, attempts, max_attempts in stale_jobs:
            if attempts >= max_attempts:
                _set_status_sync(
                    job_id, "dlq",
                    f"worker heartbeat expired after {attempts}/{max_attempts} attempts",
                )
                await self._ack(job_id)
                await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                    "job_id": job_id, "status": "dlq",
                    "error": "worker heartbeat expired", "attempts": attempts,
                })
                logger.error(f"[CRASH_RECOVERY] Job {job_id} exhausted attempts -> DLQ")
                continue
            await self._requeue_job(job_id)
            logger.info(f"[CRASH_RECOVERY] Restored orphaned job {job_id} back to queued.")

    async def _recover_redis(self) -> int:
        r_client = self.get_client()
        if not r_client:
            return 0
        try:
            moved = int(await r_client.eval(
                _RECOVER_LEASES_LUA,
                4,
                LEASE_KEY,
                PROCESSING_KEY,
                QUEUE_KEY,
                DELAYED_KEY,
            ))
            # Orphaned processing entry: BLMOVE succeeded but the lease write
            # never landed. Require two scans so a live pickup is never stolen.
            leases = set(await r_client.hkeys(LEASE_KEY))
            now = time.time()
            for job_id in await r_client.lrange(PROCESSING_KEY, 0, -1):
                if job_id in leases:
                    self._orphan_seen.pop(job_id, None)
                    continue
                first_seen = self._orphan_seen.setdefault(job_id, now)
                if now - first_seen >= JOB_RECOVERY_INTERVAL * 2:
                    if await r_client.lrem(PROCESSING_KEY, 1, job_id):
                        await r_client.rpush(QUEUE_KEY, job_id)
                        moved += 1
                    self._orphan_seen.pop(job_id, None)
            return moved
        except Exception as e:
            logger.warning(f"Redis lease recovery failed: {e}")
            return 0

    async def _recover_memory(self) -> int:
        now = time.time()
        expired = [jid for jid, exp in self._memory_processing.items() if exp <= now]
        for jid in expired:
            self._memory_processing.pop(jid, None)
            await self._memory_queue.put(jid)
        return len(expired)

    async def _maybe_recover(self) -> None:
        now = time.monotonic()
        if now - self._last_recovery < JOB_RECOVERY_INTERVAL:
            return
        self._last_recovery = now
        await self._recover_redis()
        await self._recover_memory()

    async def _next_job(self) -> Optional[str]:
        r_client = self.get_client()
        if r_client:
            try:
                job_id = await r_client.blmove(QUEUE_KEY, PROCESSING_KEY, 1, "LEFT", "RIGHT")
                if job_id:
                    await self._set_lease(job_id)
                    logger.info(f"Picked job {job_id} from Redis queue (lease {JOB_LEASE_SECONDS:.0f}s).")
                    return job_id
            except Exception as e:
                logger.warning(f"Redis BLMOVE failed ({e}), checking memory queue.")

        try:
            job_id = await asyncio.wait_for(self._memory_queue.get(), timeout=0.2)
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return None
        self._memory_processing[job_id] = time.time() + JOB_LEASE_SECONDS
        logger.info(f"Picked job {job_id} from memory queue.")
        return job_id

    async def _heartbeat(self, job_id: str) -> None:
        while True:
            await asyncio.sleep(JOB_HEARTBEAT_SECONDS)
            r_client = self.get_client()
            if r_client:
                try:
                    res = await r_client.eval(
                        _EXTEND_LEASE_LUA,
                        1,
                        LEASE_KEY,
                        job_id,
                        self._owner,
                        time.time() + JOB_LEASE_SECONDS,
                    )
                    if int(res) == 0:
                        logger.warning(f"Heartbeat: lease for {job_id} is gone; stopping heartbeat.")
                        return
                except Exception as e:
                    logger.warning(f"Heartbeat lease extension failed for {job_id}: {e}")
                    return
            else:
                self._memory_processing[job_id] = time.time() + JOB_LEASE_SECONDS

            # Keep the DB heartbeat fresh so recover_stale_jobs never mistakes
            # a live long-running job for an orphan.
            try:
                await asyncio.to_thread(
                    _set_status_sync, job_id, "running"
                )
            except Exception:
                pass

    async def start_worker(self):
        if self._worker_running:
            return
        self._worker_running = True
        await self.recover_stale_jobs()
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop_worker(self):
        self._worker_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        # Graceful shutdown: hand the in-flight job back immediately instead of
        # waiting for the lease to expire. Requeue first so there is no window
        # where the job is in neither the queue nor the processing list.
        job_id = self._current_job_id
        if job_id:
            await self._requeue_job(job_id)
            self._current_job_id = None

    async def _worker_loop(self):
        logger.info(f"Async job worker started processing (owner={self._owner}).")
        while self._worker_running:
            try:
                job_id = await self._next_job()
                if job_id:
                    await self._process_job(job_id)
                await self._maybe_recover()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job worker loop: {e}")
                await asyncio.sleep(0.5)

    async def _process_job(self, job_id: str):
        job = await self.get_job(job_id)
        if not job or job["status"] in ("done", "dlq"):
            await self._ack(job_id)
            return

        attempt_info = await asyncio.to_thread(_begin_attempt_sync, job_id)
        if not attempt_info:
            await self._ack(job_id)
            return

        attempts = int(attempt_info["attempts"])
        max_attempts = max(1, min(int(attempt_info["max_attempts"]), JOBS_MAX_ATTEMPTS))

        if attempts > max_attempts:
            error_str = f"max attempts exceeded ({attempts}>{max_attempts})"
            await asyncio.to_thread(_set_status_sync, job_id, "dlq", error_str)
            await self._ack(job_id)
            await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                "job_id": job_id, "status": "dlq", "error": error_str, "attempts": attempts,
            })
            logger.error(f"Job {job_id} exceeded max attempts -> DLQ")
            return

        cancelled = False
        self._current_job_id = job_id
        await TRAFFIC_BROADCASTER.broadcast("job_running", {"job_id": job_id, "status": "running"})
        heartbeat = asyncio.create_task(self._heartbeat(job_id))

        payload = job["payload"]
        target = payload.get("alias") or payload.get("model") or "mock"
        messages = payload.get("messages", [{"role": "user", "content": "async batch job"}])
        temperature = payload.get("temperature", 0.7)
        try:
            requested_max_tokens = int(payload.get("max_tokens") or DEFAULT_JOB_MAX_TOKENS)
        except (TypeError, ValueError):
            requested_max_tokens = DEFAULT_JOB_MAX_TOKENS
        max_tokens = max(1, min(requested_max_tokens, GUARDRAILS.max_tokens))

        try:
            # Guardrails and budget run here, inside the worker, so a job can
            # never skip the checks the synchronous /v1/chat path enforces.
            try:
                guard = GUARDRAILS.inspect_input(messages)
            except HTTPException as h_err:
                raise _TerminalJobError(f"guardrail: {h_err.detail}")

            messages = guard["sanitized_messages"]

            budget_limit = await asyncio.to_thread(_get_job_budget_limit, job["key_id"])
            budget_ok, current_spend, _ = await BUDGET_GUARD.check_budget(
                job["key_id"], job["key_name"], budget_limit
            )
            if not budget_ok:
                raise _TerminalJobError(
                    f"budget: daily limit ${budget_limit:.2f} reached (spent ${current_spend:.4f})"
                )

            # Execute through same reliability and concurrency layer
            result = await execute_reliable_completion(
                target=target,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                max_retries_per_provider=1,
                total_time_budget=30.0,
            )

            sanitized_text, output_hits = GUARDRAILS.inspect_output(result.get("text", ""))
            if output_hits:
                result["text"] = sanitized_text

            cost_usd = calculate_request_cost(
                alias=target,
                model=result.get("model", ""),
                token_usage=result.get("token_usage", {}),
                messages=messages,
                completion_text=result.get("text", ""),
            )
            usage = result.get("token_usage", {}) or {}
            prompt_tokens = int(usage.get("prompt_tokens", 5) or 5)
            completion_tokens = int(usage.get("completion_tokens", 5) or 5)
            total_tokens = int(usage.get("total_tokens", prompt_tokens + completion_tokens) or (prompt_tokens + completion_tokens))

            # Increment spend counters at most once per job id before the
            # SQLite commit; the marker makes a retry a no-op.
            await self._apply_cost_counters(job_id, job["key_id"], target, cost_usd)

            finalized = await asyncio.to_thread(
                _finalize_success_sync,
                job_id,
                json.dumps(result),
                cost_usd,
                attempts,
                job["key_id"],
                job["key_name"],
                target,
                result.get("provider", "mock"),
                result.get("model", ""),
                prompt_tokens,
                completion_tokens,
                total_tokens,
            )
            if finalized:
                await TRAFFIC_BROADCASTER.broadcast("job_completed", {
                    "job_id": job_id,
                    "status": "done",
                    "cost_usd": cost_usd,
                })
                logger.info(f"Job {job_id} successfully completed (cost: ${cost_usd:.6f})")
            else:
                logger.info(f"Job {job_id} was already finalized; skipping duplicate side effects.")

        except _TerminalJobError as exc:
            # Guardrail block or budget exhaustion: retrying cannot succeed.
            error_str = str(exc)
            await asyncio.to_thread(_set_status_sync, job_id, "dlq", error_str)
            await self._push_dlq_marker(job_id, error_str, attempts)
            await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                "job_id": job_id,
                "status": "dlq",
                "error": error_str,
                "attempts": attempts,
            })
            logger.warning(f"Job {job_id} rejected without retry (terminal): {error_str}")

        except asyncio.CancelledError:
            cancelled = True
            raise

        except Exception as exc:
            error_str = str(exc)
            logger.warning(f"Job {job_id} attempt {attempts}/{max_attempts} failed: {error_str}")

            if attempts < max_attempts:
                backoff_sec = 0.5 * (2 ** (attempts - 1))
                await asyncio.to_thread(_set_status_sync, job_id, "queued", error_str)
                # Schedule before ack: if the process dies between the two, the
                # retry exists somewhere. A duplicate is harmless (finalize is
                # conditional); a lost job is not.
                await self._schedule_retry(job_id, backoff_sec)
                await self._ack(job_id)
                await TRAFFIC_BROADCASTER.broadcast("job_retried", {
                    "job_id": job_id,
                    "attempts": attempts,
                    "error": error_str,
                })
            else:
                await asyncio.to_thread(_set_status_sync, job_id, "dlq", error_str)
                await self._push_dlq_marker(job_id, error_str, attempts)
                await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                    "job_id": job_id,
                    "status": "dlq",
                    "error": error_str,
                    "attempts": attempts,
                })
                logger.error(f"Job {job_id} reached max attempts ({max_attempts}) -> Moved to DLQ: {error_str}")

        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except (asyncio.CancelledError, Exception):
                pass
            # Keep _current_job_id set on cancellation so stop_worker can hand
            # the job straight back to the queue.
            if self._current_job_id == job_id and not cancelled:
                self._current_job_id = None
            # Ack is idempotent; retries are already in the delayed zset.
            await self._ack(job_id)

    async def _push_dlq_marker(self, job_id: str, error_str: str, attempts: int) -> None:
        payload = json.dumps({"job_id": job_id, "error": error_str, "attempts": attempts})
        self._memory_dlq[job_id] = json.loads(payload)
        r_client = self.get_client()
        if r_client:
            try:
                await r_client.hset(DLQ_KEY, job_id, payload)
                # A terminal failure must not leave a scheduled retry behind.
                await r_client.zrem(DELAYED_KEY, job_id)
            except Exception:
                pass

    async def _schedule_retry(self, job_id: str, delay: float) -> None:
        r_client = self.get_client()
        if r_client:
            try:
                await r_client.zadd(DELAYED_KEY, {job_id: time.time() + delay})
                return
            except Exception as e:
                logger.warning(f"Redis delayed retry failed for {job_id}: {e}")
        asyncio.create_task(self._memory_requeue_later(job_id, delay))

    async def _apply_cost_counters(self, job_id: str, key_id: str, alias: str, cost_usd: float) -> None:
        r_client = self.get_client()
        if not r_client:
            return
        try:
            marker = f"gw:jobs:cost_applied:{job_id}"
            if await r_client.set(marker, "1", nx=True, ex=86400 * 7):
                from datetime import datetime, timezone
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                pipe = r_client.pipeline()
                pipe.incrbyfloat(f"cost:daily:{key_id}:{today}", cost_usd)
                pipe.expire(f"cost:daily:{key_id}:{today}", 86400 * 2)
                pipe.incrbyfloat(f"cost:alias:{alias}", cost_usd)
                pipe.incrbyfloat("cost:total:gateway", cost_usd)
                await pipe.execute()
        except Exception as e:
            logger.warning(f"Redis cost counters failed for job {job_id}: {e}")


JOB_MANAGER = JobQueueManager()
