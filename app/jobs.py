import asyncio
import json
import logging
import os
import random
import socket
from app.db import DATABASE_URL, sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import HTTPException
from prometheus_client import Counter, Histogram

from app.cost import COST_TRACKER, calculate_request_cost, estimate_request_cost, resolve_token_usage
from app.guardrails import GUARDRAILS
from app.reliability import TRAFFIC_BROADCASTER, execute_reliable_completion
from app.tracing import extract_trace_context, span

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
JOB_RECONCILE_INTERVAL = 5.0

JOBS_ENQUEUED = Counter("gateway_jobs_enqueued_total", "Async jobs inserted")
JOBS_CLAIMED = Counter("gateway_jobs_claimed_total", "Async jobs claimed")
JOBS_COMPLETED = Counter("gateway_jobs_completed_total", "Async jobs completed")
JOBS_FAILED = Counter("gateway_jobs_failed_total", "Async job attempts that failed")
JOBS_RETRIED = Counter("gateway_jobs_retried_total", "Async jobs scheduled for retry")
JOBS_DLQ = Counter("gateway_jobs_dlq_total", "Async jobs moved to the DLQ")
JOBS_LEASE_EXPIRATIONS = Counter("gateway_job_lease_expirations_total", "Expired async job leases")
JOBS_RECOVERED = Counter("gateway_jobs_recovered_total", "Due jobs recovered from database state")
JOBS_DUPLICATE_CLAIMS = Counter("gateway_jobs_duplicate_claims_total", "Notifications rejected by database claim")
JOBS_RECONCILIATIONS = Counter("gateway_jobs_reconciliations_total", "Database queue reconciliations")
JOB_EXECUTION_SECONDS = Histogram("gateway_job_execution_duration_seconds", "Async job execution duration")

# Redis lists move notifications atomically. The database owns job state,
# availability, and the lease used to authorize execution and completion.
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

_NOTIFY_JOB_LUA = """
local jid = ARGV[1]
if redis.call('LPOS', KEYS[1], jid) or redis.call('LPOS', KEYS[2], jid) or redis.call('ZSCORE', KEYS[3], jid) then
  return 0
end
redis.call('RPUSH', KEYS[1], jid)
return 1
"""


def _database_now(conn) -> float:
    """Use a shared database clock for leases across hosts and workers."""
    if DATABASE_URL:
        row = conn.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())").fetchone()
    else:
        row = conn.execute("SELECT (julianday('now') - 2440587.5) * 86400.0").fetchone()
    return float(row[0])


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


def _migrate_postgres_idempotency_scope(cursor):
    """Drop legacy global idempotency uniqueness while retaining scoped constraints."""
    cursor.execute(
        """SELECT conname, pg_get_constraintdef(oid)
           FROM pg_constraint
           WHERE conrelid = to_regclass('llm_gateway.async_jobs') AND contype = 'u'"""
    )
    constraints = cursor.fetchall()
    legacy_names = set()
    for name, definition in constraints:
        if definition.strip().lower() == "unique (idempotency_key)":
            quoted = str(name).replace('"', '""')
            cursor.execute(f'ALTER TABLE llm_gateway.async_jobs DROP CONSTRAINT IF EXISTS "{quoted}"')
            legacy_names.add(name)

    cursor.execute(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'llm_gateway' AND tablename = 'async_jobs'"
    )
    for name, definition in cursor.fetchall():
        if name in legacy_names or "CREATE UNIQUE INDEX" not in definition.upper():
            continue
        columns = definition.rsplit("(", 1)[-1].rstrip(") ").replace('"', "").strip()
        if columns.lower() == "idempotency_key":
            quoted = str(name).replace('"', '""')
            cursor.execute(f'DROP INDEX IF EXISTS llm_gateway."{quoted}"')


def init_jobs_db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    if not DATABASE_URL:
        conn.execute("BEGIN IMMEDIATE")
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
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            lease_owner TEXT,
            lease_expires_at DOUBLE PRECISION,
            available_at DOUBLE PRECISION NOT NULL DEFAULT 0,
            replay_count INTEGER NOT NULL DEFAULT 0,
            cost_usd REAL DEFAULT 0.0,
            created_at DOUBLE PRECISION NOT NULL,
            updated_at DOUBLE PRECISION NOT NULL,
            UNIQUE(key_id, idempotency_key)
        )
    """)
    _migrate_jobs_table(cursor)
    if DATABASE_URL:
        cursor.execute("ALTER TABLE async_jobs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0")
        cursor.execute("ALTER TABLE async_jobs ADD COLUMN IF NOT EXISTS lease_owner TEXT")
        cursor.execute("ALTER TABLE async_jobs ADD COLUMN IF NOT EXISTS lease_expires_at DOUBLE PRECISION")
        cursor.execute("ALTER TABLE async_jobs ADD COLUMN IF NOT EXISTS available_at DOUBLE PRECISION NOT NULL DEFAULT 0")
        cursor.execute("ALTER TABLE async_jobs ADD COLUMN IF NOT EXISTS replay_count INTEGER NOT NULL DEFAULT 0")
        for column in ("lease_expires_at", "available_at", "created_at", "updated_at"):
            cursor.execute(
                f"ALTER TABLE async_jobs ALTER COLUMN {column} TYPE DOUBLE PRECISION USING {column}::DOUBLE PRECISION"
            )
        cursor.execute("UPDATE async_jobs SET attempt_count = attempts WHERE attempt_count = 0 AND attempts > 0")
        _migrate_postgres_idempotency_scope(cursor)
    else:
        cursor.execute("PRAGMA table_info(async_jobs)")
        columns = {row[1] for row in cursor.fetchall()}
        for name, definition in (
            ("attempt_count", "INTEGER NOT NULL DEFAULT 0"),
            ("lease_owner", "TEXT"),
            ("lease_expires_at", "REAL"),
            ("available_at", "REAL NOT NULL DEFAULT 0"),
            ("replay_count", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if name not in columns:
                cursor.execute(f"ALTER TABLE async_jobs ADD COLUMN {name} {definition}")
        cursor.execute("UPDATE async_jobs SET attempt_count = attempts WHERE attempt_count = 0 AND attempts > 0")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON async_jobs(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_recovery ON async_jobs(status, available_at, lease_expires_at);")
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
    lease_owner: str,
    budget_reservation_id: Optional[str] = None,
) -> bool:
    """
    Commit the job result and its cost row in one SQLite transaction.

    The status check and both writes share `BEGIN IMMEDIATE`, so a job that is
    retried after a worker crash cannot be finalized or charged twice.
    """
    conn = sqlite3.connect(DB_PATH, timeout=15.0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = _database_now(conn)
        lease_guard_now = "EXTRACT(EPOCH FROM clock_timestamp())" if DATABASE_URL else "?"
        lease_guard_params = () if DATABASE_URL else (now,)
        cursor = conn.execute(
            f"""UPDATE async_jobs
               SET status = 'done', result = ?, attempts = ?, attempt_count = ?, cost_usd = ?,
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
               WHERE id = ? AND status = 'running' AND lease_owner = ? AND lease_expires_at > {lease_guard_now}""",
            (result_json, attempts, attempts, cost_usd, now, job_id, lease_owner, *lease_guard_params),
        )
        if cursor.rowcount != 1:
            conn.execute("ROLLBACK")
            return False
        if budget_reservation_id:
            reservation = conn.execute(
                f"UPDATE budget_reservations SET status = 'settled' WHERE id = ? AND key_id = ? AND status = 'active' AND expires_at > {'EXTRACT(EPOCH FROM clock_timestamp())' if DATABASE_URL else '?'} RETURNING budget_date,amount,created_at",
                (budget_reservation_id, str(key_id), *(() if DATABASE_URL else (now,))),
            ).fetchone()
            if not reservation:
                conn.execute("ROLLBACK")
                return False
            budget_day, reserved_amount, reserved_at = reservation
            conn.execute(
                "UPDATE budget_accounts SET reserved = CASE WHEN reserved > ? THEN reserved-? ELSE 0 END, spent = spent+? WHERE key_id = ? AND budget_date = ?",
                (reserved_amount, reserved_amount, cost_usd, str(key_id), budget_day),
            )
        conn.execute(
            """INSERT INTO cost_records
               (key_id, key_name, alias, provider, model, prompt_tokens, completion_tokens,
                total_tokens, cost_usd, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(key_id), key_name, alias, provider, model, prompt_tokens,
                completion_tokens, total_tokens, cost_usd,
                reserved_at if budget_reservation_id else time.time(),
            ),
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


def _claim_job_sync(job_id: str, lease_owner: str, lease_seconds: float) -> Optional[Dict[str, Any]]:
    """Atomically claim a queued or expired job using the database as authority."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        now = _database_now(conn)
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE async_jobs
               SET status = 'running', attempts = attempts + 1, attempt_count = attempt_count + 1,
                   lease_owner = ?, lease_expires_at = ?, updated_at = ?
               WHERE id = ? AND attempts < max_attempts AND (
                   (status = 'queued' AND available_at <= ?) OR
                   (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
               )""",
            (lease_owner, now + lease_seconds, now, job_id, now, now),
        )
        if cursor.rowcount != 1:
            conn.commit()
            return None
        cursor.execute(
            "SELECT payload, key_id, key_name, attempts, max_attempts FROM async_jobs WHERE id = ? AND lease_owner = ?",
            (job_id, lease_owner),
        )
        row = cursor.fetchone()
        conn.commit()
        if not row:
            return None
        return {
            "payload": json.loads(row[0]) if row[0] else {},
            "key_id": row[1],
            "key_name": row[2],
            "attempts": row[3],
            "attempt_count": row[3],
            "max_attempts": row[4],
            "lease_owner": lease_owner,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _renew_job_lease_sync(job_id: str, lease_owner: str, lease_seconds: float) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        now = _database_now(conn)
        cursor = conn.execute(
            """UPDATE async_jobs SET lease_expires_at = ?, updated_at = ?
               WHERE id = ? AND status = 'running' AND lease_owner = ? AND lease_expires_at > ?""",
            (now + lease_seconds, now, job_id, lease_owner, now),
        )
        conn.commit()
        return cursor.rowcount == 1
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _transition_owned_job_sync(
    job_id: str,
    lease_owner: str,
    status_value: str,
    error: Optional[str],
    delay_seconds: float = 0.0,
) -> bool:
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        now = _database_now(conn)
        cursor = conn.execute(
            """UPDATE async_jobs
               SET status = ?, error = ?, available_at = ?, lease_owner = NULL,
                   lease_expires_at = NULL, updated_at = ?
               WHERE id = ? AND status = 'running' AND lease_owner = ? AND lease_expires_at > ?""",
            (status_value, error, now + max(0.0, delay_seconds), now, job_id, lease_owner, now),
        )
        conn.commit()
        return cursor.rowcount == 1
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _reconcile_jobs_sync(limit: int = 500) -> List[str]:
    """Return due DB jobs and move expired, exhausted jobs to the DLQ."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    try:
        now = _database_now(conn)
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE async_jobs SET status = 'queued', lease_owner = NULL,
                   lease_expires_at = NULL, available_at = ?, updated_at = ?
               WHERE status = 'running' AND attempts < max_attempts
                 AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
               RETURNING id""",
            (now, now, now),
        )
        expired_leases = len(cursor.fetchall())
        cursor.execute(
            """UPDATE async_jobs SET status = 'dlq', error = 'worker lease expired after retry limit',
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
               WHERE attempts >= max_attempts AND (
                   (status = 'queued' AND available_at <= ?) OR
                   (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
               ) RETURNING id""",
            (now, now, now),
        )
        exhausted = len(cursor.fetchall())
        cursor.execute(
            """SELECT id FROM async_jobs WHERE attempts < max_attempts AND (
                   (status = 'queued' AND available_at <= ?) OR
                   (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
               ) ORDER BY created_at LIMIT ?""",
            (now, now, limit),
        )
        ids = [row[0] for row in cursor.fetchall()]
        conn.commit()
        if expired_leases:
            JOBS_LEASE_EXPIRATIONS.inc(expired_leases)
        if exhausted:
            JOBS_DLQ.inc(exhausted)
        if expired_leases or exhausted:
            logger.warning(
                "Reconciled expired job leases expired=%d dlq=%d",
                expired_leases, exhausted,
            )
        return ids
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class JobQueueManager:
    """
    The database owns job state and leases. Redis and memory queues are only
    notifications; workers must win a conditional database claim before work.
    Execution is at-least-once because an upstream call can finish before a
    worker crashes, while persisted completion is lease-checked and idempotent.
    """

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self._redis: Optional[aioredis.Redis] = None
        self._memory_queue: asyncio.Queue = asyncio.Queue()
        self._memory_notified = set()
        self._memory_processing: Dict[str, float] = {}
        self._memory_dlq: Dict[str, Dict[str, Any]] = {}
        self._worker_running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._last_recovery = 0.0
        self._orphan_seen: Dict[str, float] = {}
        self._current_job_id: Optional[str] = None

    async def _notify_job(self, job_id: str) -> bool:
        r_client = self.get_client()
        if r_client:
            try:
                pushed = await r_client.eval(
                    _NOTIFY_JOB_LUA, 3, QUEUE_KEY, PROCESSING_KEY, DELAYED_KEY, job_id
                )
                return bool(pushed)
            except Exception as exc:
                logger.warning("Redis job notification failed for %s (%s)", job_id, type(exc).__name__)
        if job_id not in self._memory_notified:
            self._memory_notified.add(job_id)
            await self._memory_queue.put(job_id)
            return True
        return False

    def get_client(self) -> Optional[aioredis.Redis]:
        if self._redis is None:
            try:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    socket_connect_timeout=2.0,
                    # Must exceed the BLMOVE timeout below, otherwise the client
                    # can time out after Redis already moved the job and lose it.
                    socket_timeout=5.0,
                    health_check_interval=30,
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
        now = _database_now(conn)
        cursor = conn.cursor()

        # 1. Idempotency check, scoped to this caller only.
        if idempotency_key:
            existing_job = _existing_by_idempotency(cursor)
            if existing_job:
                conn.close()
                if existing_job["status"] == "queued":
                    await self._notify_job(existing_job["id"])
                logger.info(
                    "[IDEMPOTENCY] Reusing job %s (scope: %s, status: %s)",
                    existing_job["id"], key_id, existing_job["status"],
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
                INSERT INTO async_jobs (id, idempotency_key, key_id, key_name, status, payload, attempts, attempt_count, max_attempts, available_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'queued', ?, 0, 0, ?, ?, ?, ?)
            """, (job_id, idempotency_key, key_id, key_name, payload_json, max_attempts, now, now, now))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            existing_job = _existing_by_idempotency(cursor) if idempotency_key else None
            conn.close()
            if existing_job:
                if existing_job["status"] == "queued":
                    await self._notify_job(existing_job["id"])
                logger.info(
                    "[IDEMPOTENCY] Concurrent insert reused job %s for tenant %s",
                    existing_job["id"], key_id,
                )
                await TRAFFIC_BROADCASTER.broadcast("idempotency_hit", {
                    "job_id": existing_job["id"],
                    "idempotency_key": idempotency_key,
                })
                return existing_job
            raise
        conn.close()

        # The committed row is authoritative; a worker reconciliation repairs
        # the notification if this process exits before Redis publication.
        JOBS_ENQUEUED.inc()
        await self._notify_job(job_id)

        await TRAFFIC_BROADCASTER.broadcast("job_enqueued", {
            "job_id": job_id,
            "status": "queued",
            "idempotency_key": idempotency_key,
        })
        logger.info("Job enqueued job_id=%s tenant_id=%s", job_id, key_id)

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
            SELECT id, idempotency_key, key_id, key_name, status, payload, result, error, attempts, max_attempts, cost_usd, created_at, updated_at, attempt_count, replay_count
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
            "attempt_count": row[13],
            "replay_count": row[14],
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
        """Replay a DLQ row without changing its job or idempotency identity."""
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        now = _database_now(conn)
        cursor.execute(
            """UPDATE async_jobs SET status = 'queued', error = NULL, attempts = 0,
                   attempt_count = 0, replay_count = replay_count + 1, available_at = ?,
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
               WHERE id = ? AND status = 'dlq'""",
            (now, now, job_id),
        )
        if cursor.rowcount != 1:
            conn.close()
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not in DLQ")
        conn.commit()
        conn.close()

        r_client = self.get_client()
        if r_client:
            try:
                await r_client.hdel(LEASE_KEY, job_id)
                await r_client.hdel(DLQ_KEY, job_id)
                await r_client.zrem(DELAYED_KEY, job_id)
            except Exception as exc:
                logger.warning("Redis replay cleanup failed for %s (%s)", job_id, type(exc).__name__)
        await self._notify_job(job_id)

        await TRAFFIC_BROADCASTER.broadcast("job_replayed", {"job_id": job_id, "status": "queued"})
        JOBS_RETRIED.inc()
        logger.info("Replayed DLQ job %s (replay count incremented)", job_id)
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
        await asyncio.to_thread(
            _transition_owned_job_sync,
            job_id,
            self._owner,
            "queued",
            None,
            delay,
        )
        r_client = self.get_client()
        if r_client:
            try:
                await r_client.lrem(PROCESSING_KEY, 0, job_id)
                await r_client.hdel(LEASE_KEY, job_id)
            except Exception as exc:
                logger.warning("Redis requeue cleanup failed for %s (%s)", job_id, type(exc).__name__)
        self._memory_processing.pop(job_id, None)
        if delay > 0:
            r_client = self.get_client()
            if r_client:
                try:
                    await r_client.zadd(DELAYED_KEY, {job_id: time.time() + delay})
                except Exception:
                    pass
            return
        await self._notify_job(job_id)

    async def _memory_requeue_later(self, job_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        await self._memory_queue.put(job_id)

    async def recover_stale_jobs(self):
        """Republish due work from database state; Redis is only a notification."""
        job_ids = await asyncio.to_thread(_reconcile_jobs_sync)
        recovered = 0
        for job_id in job_ids:
            recovered += int(await self._notify_job(job_id))
        if recovered:
            JOBS_RECOVERED.inc(recovered)
            logger.info("Reconciled %d due job notifications from database state", recovered)
        JOBS_RECONCILIATIONS.inc()

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
        if now - self._last_recovery < JOB_RECONCILE_INTERVAL:
            return
        self._last_recovery = now
        await self._recover_redis()
        await self._recover_memory()
        await self.recover_stale_jobs()

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
        self._memory_notified.discard(job_id)
        self._memory_processing[job_id] = time.time() + JOB_LEASE_SECONDS
        logger.info(f"Picked job {job_id} from memory queue.")
        return job_id

    async def _heartbeat(self, job_id: str) -> None:
        while True:
            await asyncio.sleep(JOB_HEARTBEAT_SECONDS)
            try:
                still_owner = await asyncio.to_thread(
                    _renew_job_lease_sync, job_id, self._owner, JOB_LEASE_SECONDS
                )
            except Exception as exc:
                logger.warning("Database lease renewal failed for %s (%s)", job_id, type(exc).__name__)
                return
            if not still_owner:
                logger.warning("Lease ownership lost job_id=%s worker_id=%s", job_id, self._owner)
                return
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
                    if int(res) == -1:
                        logger.warning("Redis notification lease owner changed for job %s", job_id)
                except Exception as exc:
                    logger.warning("Redis lease renewal failed for %s (%s)", job_id, type(exc).__name__)
            else:
                self._memory_processing[job_id] = time.time() + JOB_LEASE_SECONDS

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

    async def aclose(self) -> None:
        client, self._redis = self._redis, None
        if client is not None:
            await client.aclose()

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
        attempt_info = await asyncio.to_thread(
            _claim_job_sync, job_id, self._owner, JOB_LEASE_SECONDS
        )
        if not attempt_info:
            await self.recover_stale_jobs()
            job_state = await self.get_job(job_id)
            if job_state and job_state["status"] in ("running", "done", "dlq"):
                JOBS_DUPLICATE_CLAIMS.inc()
            if job_state and job_state["status"] == "dlq":
                await self._push_dlq_marker(
                    job_id, job_state.get("error") or "retry limit exhausted", job_state["attempts"]
                )
            await self._ack(job_id)
            return

        job = attempt_info
        attempts = int(attempt_info["attempts"])
        max_attempts = max(1, min(int(attempt_info["max_attempts"]), JOBS_MAX_ATTEMPTS))
        job_started = time.perf_counter()
        JOBS_CLAIMED.inc()
        logger.info(
            "Job claimed job_id=%s tenant_id=%s attempt=%d worker_id=%s",
            job_id, job["key_id"], attempts, self._owner,
        )

        cancelled = False
        self._current_job_id = job_id
        await TRAFFIC_BROADCASTER.broadcast("job_running", {"job_id": job_id, "status": "running"})
        heartbeat = asyncio.create_task(self._heartbeat(job_id))

        payload = job["payload"]
        target = payload.get("alias") or payload.get("model") or "mock"
        messages = payload.get("messages", [{"role": "user", "content": "async batch job"}])
        temperature = payload.get("temperature", 0.7)
        budget_reservation_id = None
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
            estimated_cost = estimate_request_cost(target, target, messages, max_tokens)
            budget_reservation_id, current_spend = await COST_TRACKER.reserve_budget(
                job["key_id"], job["key_name"], budget_limit, estimated_cost
            )
            if not budget_reservation_id:
                raise _TerminalJobError(
                    f"budget: daily limit ${budget_limit:.2f} cannot reserve estimated cost (committed ${current_spend:.4f})"
                )

            # Execute through same reliability and concurrency layer
            with span("async_job.provider_execution", {"job.type": "async"}, extract_trace_context(payload.get("_trace_context"))):
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
            prompt_tokens, completion_tokens, total_tokens = resolve_token_usage(
                result.get("token_usage", {}), messages, result.get("text", "")
            )

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
                self._owner,
                budget_reservation_id,
            )
            if finalized:
                budget_reservation_id = None
                await self._apply_cost_counters(job_id, job["key_id"], target, cost_usd)
                JOBS_COMPLETED.inc()
                await TRAFFIC_BROADCASTER.broadcast("job_completed", {
                    "job_id": job_id,
                    "status": "done",
                    "cost_usd": cost_usd,
                })
                logger.info(
                    "Job completed job_id=%s tenant_id=%s attempt=%d worker_id=%s",
                    job_id, job["key_id"], attempts, self._owner,
                )
            else:
                logger.info("Job %s completion ignored because lease was lost (attempt=%d)", job_id, attempts)

        except _TerminalJobError as exc:
            # Guardrail block or budget exhaustion: retrying cannot succeed.
            error_str = str(exc)
            transitioned = await asyncio.to_thread(
                _transition_owned_job_sync, job_id, self._owner, "dlq", error_str
            )
            if transitioned:
                JOBS_FAILED.inc()
                JOBS_DLQ.inc()
                await self._push_dlq_marker(job_id, error_str, attempts)
                await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                    "job_id": job_id,
                    "status": "dlq",
                    "attempts": attempts,
                })
                logger.warning(
                    "Job rejected job_id=%s tenant_id=%s attempt=%d worker_id=%s",
                    job_id, job["key_id"], attempts, self._owner,
                )

        except asyncio.CancelledError:
            cancelled = True
            raise

        except Exception as exc:
            error_str = str(exc)
            JOBS_FAILED.inc()
            logger.warning(
                "Job attempt failed job_id=%s tenant_id=%s attempt=%d/%d worker_id=%s error_type=%s",
                job_id, job["key_id"], attempts, max_attempts, self._owner, type(exc).__name__,
            )

            if attempts < max_attempts:
                base_delay = 0.5 * (2 ** (attempts - 1))
                backoff_sec = base_delay * random.uniform(0.8, 1.2)
                transitioned = await asyncio.to_thread(
                    _transition_owned_job_sync,
                    job_id,
                    self._owner,
                    "queued",
                    error_str,
                    backoff_sec,
                )
                if not transitioned:
                    logger.info("Retry transition skipped because job %s lease was lost", job_id)
                    return
                JOBS_RETRIED.inc()
                await self._schedule_retry(job_id, backoff_sec)
                logger.info(
                    "Job retry scheduled job_id=%s tenant_id=%s attempt=%d worker_id=%s delay_seconds=%.3f",
                    job_id, job["key_id"], attempts, self._owner, backoff_sec,
                )
                await self._ack(job_id)
                await TRAFFIC_BROADCASTER.broadcast("job_retried", {
                    "job_id": job_id,
                    "attempts": attempts,
                    "error": error_str,
                })
            else:
                transitioned = await asyncio.to_thread(
                    _transition_owned_job_sync, job_id, self._owner, "dlq", error_str
                )
                if not transitioned:
                    logger.info("DLQ transition skipped because job %s lease was lost", job_id)
                    return
                JOBS_DLQ.inc()
                await self._push_dlq_marker(job_id, error_str, attempts)
                await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                    "job_id": job_id,
                    "status": "dlq",
                    "error": error_str,
                    "attempts": attempts,
                })
                logger.error(
                    "Job %s exhausted retries and entered DLQ (attempt=%d, tenant_id=%s, worker_id=%s)",
                    job_id, attempts, job["key_id"], self._owner,
                )

        finally:
            if budget_reservation_id:
                await COST_TRACKER.release_budget(budget_reservation_id, job["key_id"])
            JOB_EXECUTION_SECONDS.observe(time.perf_counter() - job_started)
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
            except Exception as exc:
                logger.warning("Could not publish DLQ marker for %s (%s)", job_id, type(exc).__name__)

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
