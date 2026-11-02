import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from fastapi import HTTPException, Header, Request, status

from app.cost import COST_TRACKER, calculate_request_cost
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


def init_jobs_db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS async_jobs (
            id TEXT PRIMARY KEY,
            idempotency_key TEXT UNIQUE,
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
            updated_at REAL NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON async_jobs(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_idem ON async_jobs(idempotency_key);")
    conn.commit()
    conn.close()


init_jobs_db()


class JobQueueManager:
    """
    Redis-backed queue with automatic in-memory fallback.
    Manages jobs work queue, DLQ, and worker lifecycle.
    """

    def __init__(self, redis_url: str = "redis://127.0.0.1:6379/0"):
        self.redis_url = redis_url
        self._redis: Optional[aioredis.Redis] = None
        self._memory_queue: asyncio.Queue = asyncio.Queue()
        self._memory_dlq: Dict[str, Dict[str, Any]] = {}
        self._worker_running = False
        self._worker_task: Optional[asyncio.Task] = None

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

        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()

        # 1. Idempotency check
        if idempotency_key:
            cursor.execute(
                "SELECT id, status, payload, result, error, attempts, max_attempts, cost_usd, created_at FROM async_jobs WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            row = cursor.fetchone()
            if row:
                conn.close()
                logger.info(f"[IDEMPOTENCY] Reusing existing job {row[0]} for key '{idempotency_key}' (status: {row[1]})")
                await TRAFFIC_BROADCASTER.broadcast("idempotency_hit", {
                    "job_id": row[0],
                    "idempotency_key": idempotency_key,
                })
                return {
                    "id": row[0],
                    "status": row[1],
                    "idempotency_key": idempotency_key,
                    "reused": True,
                    "attempts": row[5],
                    "cost_usd": row[7],
                    "created_at": row[8],
                }

        # 2. Insert new job
        payload_json = json.dumps(payload)
        cursor.execute("""
            INSERT INTO async_jobs (id, idempotency_key, key_id, key_name, status, payload, attempts, max_attempts, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?)
        """, (job_id, idempotency_key, str(key_id), key_name, payload_json, max_attempts, now, now))
        conn.commit()
        conn.close()

        # 3. Push to queue
        r_client = self.get_client()
        enqueued_redis = False
        if r_client:
            try:
                await r_client.rpush("gw:jobs:queue", job_id)
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

        r_client = self.get_client()
        if r_client:
            try:
                r_client.delete("job_queue:dlq")
            except Exception:
                pass
        return count

    async def replay_dlq_job(self, job_id: str) -> Dict[str, Any]:
        """Requeues a dead-lettered job for execution."""
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM async_jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        if not row or row[0] != "dlq":
            conn.close()
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not in DLQ")

        now = time.time()
        cursor.execute("UPDATE async_jobs SET status = 'queued', error = NULL, updated_at = ? WHERE id = ?", (now, job_id))
        conn.commit()
        conn.close()

        # Enqueue back into work queue
        r_client = self.get_client()
        replayed_redis = False
        if r_client:
            try:
                await r_client.rpush("gw:jobs:queue", job_id)
                await r_client.hdel("gw:jobs:dlq", job_id)
                replayed_redis = True
            except Exception:
                pass

        if not replayed_redis:
            await self._memory_queue.put(job_id)

        await TRAFFIC_BROADCASTER.broadcast("job_replayed", {"job_id": job_id, "status": "queued"})
        logger.info(f"Replayed DLQ job {job_id}")
        return {"id": job_id, "status": "queued", "replayed": True}

    async def recover_stale_jobs(self):
        """Crash recovery: re-queues jobs stuck in 'running' state after a crash."""
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        # Any job running for more than 45 seconds is considered orphaned by a dead worker
        stale_threshold = time.time() - 45.0
        cursor.execute("SELECT id FROM async_jobs WHERE status = 'running' AND updated_at < ?", (stale_threshold,))
        stale_jobs = [r[0] for r in cursor.fetchall()]

        for j_id in stale_jobs:
            cursor.execute("UPDATE async_jobs SET status = 'queued', updated_at = ? WHERE id = ?", (time.time(), j_id))
            r_client = self.get_client()
            if r_client:
                try:
                    await r_client.rpush("gw:jobs:queue", j_id)
                    continue
                except Exception:
                    pass
            await self._memory_queue.put(j_id)
            logger.info(f"[CRASH_RECOVERY] Restored orphaned job {j_id} back to queued.")

        conn.commit()
        conn.close()

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

    async def _worker_loop(self):
        logger.info("Async job worker started processing.")
        while self._worker_running:
            job_id = None
            try:
                # 1. Pop from Redis queue or memory queue
                r_client = self.get_client()
                if r_client:
                    try:
                        res = await r_client.blpop("gw:jobs:queue", timeout=1)
                        if res:
                            job_id = res[1]
                    except Exception:
                        pass

                if not job_id:
                    try:
                        job_id = await asyncio.wait_for(self._memory_queue.get(), timeout=1.0)
                    except (asyncio.TimeoutError, asyncio.QueueEmpty):
                        await asyncio.sleep(0.1)
                        continue

                if job_id:
                    await self._process_job(job_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job worker loop: {e}")
                await asyncio.sleep(0.5)

    async def _process_job(self, job_id: str):
        job = await self.get_job(job_id)
        if not job or job["status"] in ("done", "dlq"):
            return

        now = time.time()
        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        cursor.execute("UPDATE async_jobs SET status = 'running', updated_at = ? WHERE id = ?", (now, job_id))
        conn.commit()
        conn.close()

        await TRAFFIC_BROADCASTER.broadcast("job_running", {"job_id": job_id, "status": "running"})

        payload = job["payload"]
        target = payload.get("alias") or payload.get("model") or "mock"
        messages = payload.get("messages", [{"role": "user", "content": "async batch job"}])
        temperature = payload.get("temperature", 0.7)
        attempts = job["attempts"] + 1
        max_attempts = job["max_attempts"]

        try:
            # Execute through same reliability and concurrency layer
            result = await execute_reliable_completion(
                target=target,
                messages=messages,
                temperature=temperature,
                max_tokens=payload.get("max_tokens", 1000),
                max_retries_per_provider=1,
                total_time_budget=30.0,
            )

            # Compute and record cost
            cost_usd = calculate_request_cost(
                alias=target,
                model=result.get("model", ""),
                token_usage=result.get("token_usage", {}),
                messages=messages,
                completion_text=result.get("text", ""),
            )
            await COST_TRACKER.record_cost(
                key_id=job["key_id"],
                key_name=job["key_name"],
                alias=target,
                provider=result.get("provider", "mock"),
                model=result.get("model", ""),
                prompt_tokens=result.get("token_usage", {}).get("prompt_tokens", 5),
                completion_tokens=result.get("token_usage", {}).get("completion_tokens", 5),
                total_tokens=result.get("token_usage", {}).get("total_tokens", 10),
                cost_usd=cost_usd,
            )

            # Mark completed
            conn = sqlite3.connect(DB_PATH, timeout=15.0)
            c = conn.cursor()
            c.execute("""
                UPDATE async_jobs
                SET status = 'done', result = ?, attempts = ?, cost_usd = ?, updated_at = ?
                WHERE id = ?
            """, (json.dumps(result), attempts, cost_usd, time.time(), job_id))
            conn.commit()
            conn.close()

            await TRAFFIC_BROADCASTER.broadcast("job_completed", {
                "job_id": job_id,
                "status": "done",
                "cost_usd": cost_usd,
            })
            logger.info(f"Job {job_id} successfully completed (cost: ${cost_usd:.6f})")

        except Exception as exc:
            error_str = str(exc)
            logger.warning(f"Job {job_id} attempt {attempts}/{max_attempts} failed: {error_str}")

            if attempts < max_attempts:
                # Exponential backoff retry
                backoff_sec = 0.5 * (2 ** (attempts - 1))
                await asyncio.sleep(backoff_sec)

                conn = sqlite3.connect(DB_PATH, timeout=15.0)
                c = conn.cursor()
                c.execute("""
                    UPDATE async_jobs SET status = 'queued', attempts = ?, error = ?, updated_at = ?
                    WHERE id = ?
                """, (attempts, error_str, time.time(), job_id))
                conn.commit()
                conn.close()

                # Re-queue
                r_client = self.get_client()
                if r_client:
                    try:
                        await r_client.rpush("gw:jobs:queue", job_id)
                    except Exception:
                        await self._memory_queue.put(job_id)
                else:
                    await self._memory_queue.put(job_id)

                await TRAFFIC_BROADCASTER.broadcast("job_retried", {
                    "job_id": job_id,
                    "attempts": attempts,
                    "error": error_str,
                })
            else:
                # Dead-lettered (DLQ)
                conn = sqlite3.connect(DB_PATH, timeout=15.0)
                c = conn.cursor()
                c.execute("""
                    UPDATE async_jobs SET status = 'dlq', attempts = ?, error = ?, updated_at = ?
                    WHERE id = ?
                """, (attempts, error_str, time.time(), job_id))
                conn.commit()
                conn.close()

                r_client = self.get_client()
                if r_client:
                    try:
                        await r_client.hset("gw:jobs:dlq", job_id, json.dumps({
                            "job_id": job_id,
                            "error": error_str,
                            "attempts": attempts,
                        }))
                    except Exception:
                        pass

                await TRAFFIC_BROADCASTER.broadcast("job_dlq", {
                    "job_id": job_id,
                    "status": "dlq",
                    "error": error_str,
                    "attempts": attempts,
                })
                logger.error(f"Job {job_id} reached max attempts ({max_attempts}) -> Moved to DLQ: {error_str}")


JOB_MANAGER = JobQueueManager()
