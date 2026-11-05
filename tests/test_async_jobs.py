import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.cost import COST_TRACKER
from app.jobs import JOB_MANAGER, init_jobs_db
from app.main import app
from app.reliability import set_failure_injection, INJECTED_FAILURES
from app.security import ADMIN_TOKEN, create_gateway_api_key

init_jobs_db()


@pytest.fixture(autouse=True)
async def ensure_clean_jobs():
    for prov in INJECTED_FAILURES:
        set_failure_injection(prov, enabled=False)
    yield
    for prov in INJECTED_FAILURES:
        set_failure_injection(prov, enabled=False)


def test_job_lease_defaults_are_15s_and_5s():
    from app.jobs import JOB_HEARTBEAT_SECONDS, JOB_LEASE_SECONDS

    assert JOB_LEASE_SECONDS == 15.0
    assert JOB_HEARTBEAT_SECONDS == 5.0


# ---------------------------------------------------------------------------
# 1. Test Idempotency: same key returns same job, never double-enqueues
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_job_idempotency():
    raw_key, info = create_gateway_api_key("Idempotent Tester", quota=100)
    client = TestClient(app)

    idem_key = f"idem_{time.time()}"
    headers = {
        "Authorization": f"Bearer {raw_key}",
        "Idempotency-Key": idem_key,
    }
    payload = {
        "model": "mock",
        "messages": [{"role": "user", "content": "batch message"}],
    }

    # Request 1: Creates job
    resp1 = client.post("/v1/jobs", headers=headers, json=payload)
    assert resp1.status_code == 200
    data1 = resp1.json()
    job_id_1 = data1["id"]
    assert data1["reused"] is False

    # Request 2 with same Idempotency-Key: Must return SAME job id without re-enqueuing
    resp2 = client.post("/v1/jobs", headers=headers, json=payload)
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["id"] == job_id_1
    assert data2["reused"] is True


# ---------------------------------------------------------------------------
# 2. Test Retries with Backoff then Dead-Letter Queue (DLQ)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_job_retries_then_dlq():
    raw_key, info = create_gateway_api_key("DLQ Tester", quota=100)

    # Inject failure on mock provider
    set_failure_injection("mock", enabled=True, mode="503")

    # Enqueue job with max_attempts = 2 for fast test
    job_info = await JOB_MANAGER.enqueue_job(
        payload={"model": "mock", "messages": [{"role": "user", "content": "failing job"}]},
        key_id=str(info["id"]),
        key_name=info["name"],
        idempotency_key=f"fail_{time.time()}",
        max_attempts=2,
    )
    job_id = job_info["id"]

    # Process first attempt (fails -> status queued with attempts = 1)
    await JOB_MANAGER._process_job(job_id)
    j1 = await JOB_MANAGER.get_job(job_id)
    assert j1["status"] == "queued"
    assert j1["attempts"] == 1

    # Process second attempt (fails -> reached max_attempts 2 -> lands in DLQ)
    await JOB_MANAGER._process_job(job_id)
    j2 = await JOB_MANAGER.get_job(job_id)
    assert j2["status"] == "dlq"
    assert j2["attempts"] == 2
    assert "503" in (j2["error"] or "")

    # Listed in DLQ
    dlq_items = await JOB_MANAGER.list_dlq()
    matching = [x for x in dlq_items if x["id"] == job_id]
    assert len(matching) == 1
    assert matching[0]["attempts"] == 2

    # Reset failure injection
    set_failure_injection("mock", enabled=False)


# ---------------------------------------------------------------------------
# 3. Test DLQ Replay Success after Provider Recovery
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_dlq_replay_success():
    raw_key, info = create_gateway_api_key("Replay Tester", quota=100)
    set_failure_injection("mock", enabled=True, mode="503")

    job_info = await JOB_MANAGER.enqueue_job(
        payload={"model": "mock", "messages": [{"role": "user", "content": "dlq replay target"}]},
        key_id=str(info["id"]),
        key_name=info["name"],
        idempotency_key=f"replay_{time.time()}",
        max_attempts=1,
    )
    job_id = job_info["id"]

    # Fails into DLQ immediately (max_attempts = 1)
    await JOB_MANAGER._process_job(job_id)
    j_dlq = await JOB_MANAGER.get_job(job_id)
    assert j_dlq["status"] == "dlq"

    # Restore provider
    set_failure_injection("mock", enabled=False)

    # Replay DLQ job via client endpoint
    client = TestClient(app)
    resp_replay = client.post(f"/v1/dlq/{job_id}/replay", headers={"X-Admin-Token": ADMIN_TOKEN})
    assert resp_replay.status_code == 200
    assert resp_replay.json()["replayed"] is True

    # Job is now queued and attempts reset
    j_requeued = await JOB_MANAGER.get_job(job_id)
    assert j_requeued["status"] == "queued"

    # Process replayed job -> succeeds
    await JOB_MANAGER._process_job(job_id)
    j_done = await JOB_MANAGER.get_job(job_id)
    assert j_done["status"] == "done"
    assert j_done["result"] is not None
    assert j_done["cost_usd"] > 0


# ---------------------------------------------------------------------------
# 4. Test Worker Crash Recovery
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_crash_recovery():
    import sqlite3
    from app.jobs import DB_PATH

    raw_key, info = create_gateway_api_key("Crash Tester", quota=100)
    job_info = await JOB_MANAGER.enqueue_job(
        payload={"model": "mock", "messages": [{"role": "user", "content": "stale crash test"}]},
        key_id=str(info["id"]),
        key_name=info["name"],
    )
    job_id = job_info["id"]

    # Simulate worker crash while job was running 60 seconds ago
    stale_time = time.time() - 60.0
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE async_jobs SET status = 'running', updated_at = ? WHERE id = ?", (stale_time, job_id))
    conn.commit()
    conn.close()

    # Trigger crash recovery
    await JOB_MANAGER.recover_stale_jobs()

    # Verify job was rescued and returned to queued state
    recovered_job = await JOB_MANAGER.get_job(job_id)
    assert recovered_job["status"] == "queued"
