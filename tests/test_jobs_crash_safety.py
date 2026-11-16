import asyncio
import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

import app.jobs as jobs
from app.db import sqlite3


def _database():
    return sqlite3.connect(jobs.DB_PATH, timeout=15.0)


async def _make_job(manager, max_attempts=3, idempotency_key=None):
    manager.get_client = lambda: None
    return await manager.enqueue_job(
        payload={"model": "mock", "messages": [{"role": "user", "content": "job test"}]},
        key_id=f"jobs-test-{uuid.uuid4().hex}",
        key_name="jobs-test",
        idempotency_key=idempotency_key,
        max_attempts=max_attempts,
    )


def _complete_result(**kwargs):
    return {
        "text": "ok",
        "provider": "mock",
        "model": "mock-500ms",
        "token_usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3},
    }


async def _execute_result(**kwargs):
    return _complete_result()


def _expire_lease(job_id):
    conn = _database()
    conn.execute(
        "UPDATE async_jobs SET lease_expires_at = ? WHERE id = ?",
        (time.time() - 1, job_id),
    )
    conn.commit()
    conn.close()


def _complete_claim(claim, job_id):
    return jobs._finalize_success_sync(
        job_id,
        json.dumps(_complete_result()),
        0.001,
        claim["attempts"],
        claim["key_id"],
        claim["key_name"],
        "mock",
        "mock",
        "mock-500ms",
        2,
        1,
        3,
        claim["lease_owner"],
    )


@pytest.mark.asyncio
async def test_database_insert_before_redis_publish_is_reconciled(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    original_notify = manager._notify_job
    async def skip_publish(_job_id):
        return False
    monkeypatch.setattr(manager, "_notify_job", skip_publish)
    created = await _make_job(manager)
    monkeypatch.setattr(manager, "_notify_job", original_notify)

    await manager.recover_stale_jobs()

    queued_ids = [
        await manager._memory_queue.get()
        for _ in range(manager._memory_queue.qsize())
    ]
    assert created["id"] in queued_ids
    claim = jobs._claim_job_sync(created["id"], "reconciler", 30)
    assert claim and claim["attempts"] == 1


@pytest.mark.asyncio
async def test_redis_unavailable_keeps_database_job_executable(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    calls = 0

    async def execute(**kwargs):
        nonlocal calls
        calls += 1
        return _complete_result()

    monkeypatch.setattr(jobs, "execute_reliable_completion", execute)
    created = await _make_job(manager)
    await manager._process_job(created["id"])

    stored = await manager.get_job(created["id"])
    assert stored["status"] == "done"
    assert calls == 1


@pytest.mark.asyncio
async def test_crash_after_claim_recovers_after_database_lease_expiry(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    calls = 0
    async def execute(**kwargs):
        nonlocal calls
        calls += 1
        return _complete_result()
    monkeypatch.setattr(jobs, "execute_reliable_completion", execute)
    created = await _make_job(manager)
    first_claim = jobs._claim_job_sync(created["id"], "crashed-worker", 30)
    assert first_claim
    _expire_lease(created["id"])

    await manager.recover_stale_jobs()
    await manager._process_job(created["id"])

    stored = await manager.get_job(created["id"])
    assert stored["status"] == "done"
    assert stored["attempts"] == 2
    assert stored["attempt_count"] == 2
    assert calls == 1


def test_two_workers_cannot_claim_the_same_job():
    created = asyncio.run(_make_job(jobs.JobQueueManager("redis://127.0.0.1:1/0")))
    workers = 8
    barrier = threading.Barrier(workers)

    def claim(index):
        barrier.wait()
        return jobs._claim_job_sync(created["id"], f"worker-{index}", 30)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        claims = list(pool.map(claim, range(workers)))

    winners = [claim for claim in claims if claim]
    assert len(winners) == 1
    assert asyncio.run(jobs.JOB_MANAGER.get_job(created["id"]))["attempts"] == 1


def test_expired_lease_can_be_reclaimed_but_old_worker_cannot_finalize():
    created = asyncio.run(_make_job(jobs.JobQueueManager("redis://127.0.0.1:1/0")))
    stale = jobs._claim_job_sync(created["id"], "old-worker", 30)
    assert stale
    _expire_lease(created["id"])

    current = jobs._claim_job_sync(created["id"], "new-worker", 30)
    assert current
    assert not _complete_claim(stale, created["id"])
    assert _complete_claim(current, created["id"])

    stored = asyncio.run(jobs.JOB_MANAGER.get_job(created["id"]))
    assert stored["status"] == "done"
    assert json.loads(stored["result"] if isinstance(stored["result"], str) else json.dumps(stored["result"]))["text"] == "ok"


@pytest.mark.asyncio
async def test_execution_before_crash_is_at_least_once_not_exactly_once(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    created = await _make_job(manager)
    first = jobs._claim_job_sync(created["id"], "crashed-worker", 30)
    calls = 0

    async def upstream_call():
        nonlocal calls
        calls += 1

    await upstream_call()  # Upstream completed; process crashed before persistence.
    _expire_lease(created["id"])
    await manager.recover_stale_jobs()
    async def execute_again(**kwargs):
        await upstream_call()
        return _complete_result()
    monkeypatch.setattr(jobs, "execute_reliable_completion", execute_again)
    await manager._process_job(created["id"])

    assert first
    assert calls == 2
    assert (await manager.get_job(created["id"]))["status"] == "done"


@pytest.mark.asyncio
async def test_result_persistence_failure_leaves_job_recoverable(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    monkeypatch.setattr(jobs, "execute_reliable_completion", _execute_result)
    created = await _make_job(manager, max_attempts=2)
    original_finalize = jobs._finalize_success_sync
    failed_once = False

    def fail_first_finalize(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise OSError("simulated persistence failure")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(jobs, "_finalize_success_sync", fail_first_finalize)
    await manager._process_job(created["id"])
    queued = await manager.get_job(created["id"])
    assert queued["status"] == "queued"
    assert queued["result"] is None

    conn = _database()
    conn.execute("UPDATE async_jobs SET available_at = 0 WHERE id = ?", (created["id"],))
    conn.commit()
    conn.close()
    await manager._process_job(created["id"])
    assert (await manager.get_job(created["id"]))["status"] == "done"


def test_result_and_cost_writes_roll_back_together(monkeypatch):
    created = asyncio.run(_make_job(jobs.JobQueueManager("redis://127.0.0.1:1/0")))
    claim = jobs._claim_job_sync(created["id"], "rollback-worker", 30)
    assert claim
    original_connect = jobs.sqlite3.connect

    class FailingConnection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, statement, parameters=()):
            if "INSERT INTO cost_records" in statement:
                raise OSError("simulated cost persistence failure")
            return self.connection.execute(statement, parameters)

        def __getattr__(self, name):
            return getattr(self.connection, name)

    monkeypatch.setattr(
        jobs.sqlite3,
        "connect",
        lambda *args, **kwargs: FailingConnection(original_connect(*args, **kwargs)),
    )
    with pytest.raises(OSError, match="simulated cost persistence failure"):
        _complete_claim(claim, created["id"])
    monkeypatch.setattr(jobs.sqlite3, "connect", original_connect)

    conn = _database()
    status, result, lease_owner = conn.execute(
        "SELECT status, result, lease_owner FROM async_jobs WHERE id = ?",
        (created["id"],),
    ).fetchone()
    conn.close()
    assert status == "running"
    assert result is None
    assert lease_owner == "rollback-worker"


@pytest.mark.asyncio
async def test_duplicate_notifications_do_not_repeat_completed_execution(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    calls = 0

    async def execute(**kwargs):
        nonlocal calls
        calls += 1
        return _complete_result()

    monkeypatch.setattr(jobs, "execute_reliable_completion", execute)
    created = await _make_job(manager)
    await manager._process_job(created["id"])
    await manager._process_job(created["id"])

    assert calls == 1
    assert (await manager.get_job(created["id"]))["status"] == "done"


@pytest.mark.asyncio
async def test_duplicate_redis_notifications_are_claimed_once(monkeypatch):
    base_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/15")
    redis_url = base_url.rsplit("/", 1)[0] + "/14"
    manager = jobs.JobQueueManager(redis_url)
    client = manager.get_client()
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        if os.getenv("GATEWAY_REQUIRE_REDIS_TESTS") == "1":
            pytest.fail(f"Required Redis integration service is unavailable: {type(exc).__name__}")
        pytest.skip("Redis integration service is unavailable")

    created = await manager.enqueue_job(
        {"model": "mock", "messages": [{"role": "user", "content": "duplicate redis notice"}]},
        f"jobs-test-{uuid.uuid4().hex}",
        "jobs-test",
    )
    job_id = created["id"]
    calls = 0

    async def execute(**kwargs):
        nonlocal calls
        calls += 1
        return _complete_result()

    monkeypatch.setattr(jobs, "execute_reliable_completion", execute)
    try:
        await client.rpush(jobs.QUEUE_KEY, job_id)
        assert await manager._next_job() == job_id
        assert await manager._next_job() == job_id
        await manager._process_job(job_id)
        await manager._process_job(job_id)
        assert calls == 1
        assert (await manager.get_job(job_id))["status"] == "done"
    finally:
        await client.lrem(jobs.QUEUE_KEY, 0, job_id)
        await client.lrem(jobs.PROCESSING_KEY, 0, job_id)
        await client.hdel(jobs.LEASE_KEY, job_id)
        await client.zrem(jobs.DELAYED_KEY, job_id)
        await client.hdel(jobs.DLQ_KEY, job_id)
        await client.aclose()


def test_concurrent_same_idempotency_key_returns_one_job():
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    manager.get_client = lambda: None
    key_id = f"jobs-test-{uuid.uuid4().hex}"
    idem = f"idem-{uuid.uuid4().hex}"
    participants = 8
    barrier = threading.Barrier(participants)

    def enqueue(_index):
        async def run():
            barrier.wait()
            return await manager.enqueue_job(
                {"model": "mock", "messages": [{"role": "user", "content": "same request"}]},
                key_id,
                "jobs-test",
                idem,
            )
        return asyncio.run(run())

    with ThreadPoolExecutor(max_workers=participants) as pool:
        results = list(pool.map(enqueue, range(participants)))

    assert len({item["id"] for item in results}) == 1
    conn = _database()
    count = conn.execute(
        "SELECT COUNT(*) FROM async_jobs WHERE key_id = ? AND idempotency_key = ?",
        (key_id, idem),
    ).fetchone()[0]
    conn.close()
    assert count == 1


def test_idempotency_key_is_scoped_to_tenant():
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    idem = f"idem-{uuid.uuid4().hex}"
    first = asyncio.run(_make_job(manager, idempotency_key=idem))
    second = asyncio.run(_make_job(manager, idempotency_key=idem))
    assert first["id"] != second["id"]


@pytest.mark.asyncio
async def test_retry_limit_moves_job_to_dlq(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    async def fail(**kwargs):
        raise RuntimeError("provider unavailable")
    monkeypatch.setattr(jobs, "execute_reliable_completion", fail)
    created = await _make_job(manager, max_attempts=1)

    await manager._process_job(created["id"])

    stored = await manager.get_job(created["id"])
    assert stored["status"] == "dlq"
    assert stored["attempts"] == 1
    assert len([row for row in await manager.list_dlq() if row["id"] == created["id"]]) == 1


@pytest.mark.asyncio
async def test_recovery_retries_expired_lease_once(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    monkeypatch.setattr(jobs, "execute_reliable_completion", _execute_result)
    created = await _make_job(manager, max_attempts=3)
    claim = jobs._claim_job_sync(created["id"], "dead-worker", 30)
    assert claim
    _expire_lease(created["id"])

    await manager.recover_stale_jobs()
    await manager._process_job(created["id"])

    stored = await manager.get_job(created["id"])
    assert stored["status"] == "done"
    assert stored["attempt_count"] == 2


@pytest.mark.asyncio
async def test_dlq_replay_preserves_identity_and_counts_replay(monkeypatch):
    manager = jobs.JobQueueManager("redis://127.0.0.1:1/0")
    monkeypatch.setattr(manager, "get_client", lambda: None)
    created = await _make_job(manager)
    conn = _database()
    conn.execute("UPDATE async_jobs SET status = 'dlq', error = 'failed' WHERE id = ?", (created["id"],))
    conn.commit()
    conn.close()

    replay = await manager.replay_dlq_job(created["id"])
    stored = await manager.get_job(created["id"])
    assert replay["id"] == created["id"]
    assert stored["idempotency_key"] == created["idempotency_key"]
    assert stored["replay_count"] == 1
    assert stored["attempts"] == 0
