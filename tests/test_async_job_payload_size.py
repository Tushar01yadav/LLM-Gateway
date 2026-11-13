"""OWASP LLM10 byte limits at the async job submission boundary."""

import json
import sqlite3
import uuid

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from app.auth import db_create_session, db_create_user
from app.guardrails import GUARDRAILS
from app.jobs import DB_PATH, JOB_MANAGER
from app.main import app
from app.security import create_gateway_api_key

KEY_PREFIX = "unittest:payload:"


@pytest.fixture(autouse=True)
def offline_jobs(monkeypatch):
    monkeypatch.setattr(JOB_MANAGER, "get_client", lambda: None)
    yield
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DELETE FROM async_jobs WHERE key_id LIKE ?", (KEY_PREFIX + "%",))
        conn.commit()
    finally:
        conn.close()


def _job_exists(idempotency_key: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            "SELECT 1 FROM async_jobs WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
    finally:
        conn.close()
    return row is not None


def _payload_sized_to(limit: int) -> dict:
    payload = {"model": "mock", "messages": [{"role": "user", "content": ""}]}
    base_len = len(json.dumps(payload).encode("utf-8"))
    payload["messages"][0]["content"] = "a" * (limit - base_len)
    assert len(json.dumps(payload).encode("utf-8")) == limit
    return payload


@pytest.mark.asyncio
async def test_enqueue_accepts_payload_at_exact_byte_limit():
    payload = _payload_sized_to(GUARDRAILS.max_body_bytes)

    job = await JOB_MANAGER.enqueue_job(
        payload, KEY_PREFIX + "exact", "exact", f"exact-{uuid.uuid4().hex}"
    )

    assert job["status"] == "queued"
    assert job["reused"] is False


@pytest.mark.asyncio
async def test_enqueue_rejects_payload_one_byte_over_limit():
    limit = GUARDRAILS.max_body_bytes
    payload = _payload_sized_to(limit)
    payload["messages"][0]["content"] += "a"
    idem = f"over-{uuid.uuid4().hex}"

    with pytest.raises(HTTPException) as exc_info:
        await JOB_MANAGER.enqueue_job(payload, KEY_PREFIX + "over", "over", idem)

    assert exc_info.value.status_code == 413
    assert str(limit) in exc_info.value.detail
    assert _job_exists(idem) is False


@pytest.mark.asyncio
async def test_enqueue_measures_utf8_serialized_bytes_not_characters():
    limit = GUARDRAILS.max_body_bytes
    # Each 'é' costs six serialized bytes (\u00e9) but only one character.
    payload = {"model": "mock", "messages": [{"role": "user", "content": "é" * (limit // 2)}]}
    assert len(payload["messages"][0]["content"]) < limit
    assert len(json.dumps(payload).encode("utf-8")) > limit

    with pytest.raises(HTTPException) as exc_info:
        await JOB_MANAGER.enqueue_job(
            payload, KEY_PREFIX + "utf8", "utf8", f"utf8-{uuid.uuid4().hex}"
        )
    assert exc_info.value.status_code == 413


@pytest.mark.asyncio
async def test_enqueue_handles_nested_payloads():
    small_nested = {
        "model": "mock",
        "messages": [{"role": "user", "content": "hello"}],
        "metadata": {"batch": {"items": [1, 2, {"ok": True}]}},
    }
    job = await JOB_MANAGER.enqueue_job(
        small_nested, KEY_PREFIX + "nested", "nested", f"nested-{uuid.uuid4().hex}"
    )
    stored = await JOB_MANAGER.get_job(job["id"])
    assert stored["payload"]["metadata"] == small_nested["metadata"]

    oversized_nested = {
        "model": "mock",
        "messages": [{"role": "user", "content": "hello"}],
        "metadata": {"batch": {"items": ["x" * (GUARDRAILS.max_body_bytes + 1)]}},
    }
    with pytest.raises(HTTPException) as exc_info:
        await JOB_MANAGER.enqueue_job(
            oversized_nested,
            KEY_PREFIX + "nested-over",
            "nested",
            f"nested-over-{uuid.uuid4().hex}",
        )
    assert exc_info.value.status_code == 413


@pytest.mark.asyncio
async def test_http_job_accepts_normal_and_optional_fields():
    raw_key, _ = create_gateway_api_key("Job Payload Normal", quota=10)
    client = TestClient(app)
    idem = f"http-normal-{uuid.uuid4().hex}"

    response = client.post(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {raw_key}", "Idempotency-Key": idem},
        json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
    )

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "queued"
    assert _job_exists(idem) is True


@pytest.mark.asyncio
async def test_http_job_missing_messages_still_validates():
    raw_key, _ = create_gateway_api_key("Job Payload Missing", quota=10)
    client = TestClient(app)

    response = client.post(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {raw_key}"},
        json={"model": "mock"},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_http_job_rejects_oversized_body_without_persisting():
    raw_key, _ = create_gateway_api_key("Job Payload Oversized", quota=10)
    client = TestClient(app)
    idem = f"http-over-{uuid.uuid4().hex}"

    response = client.post(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {raw_key}", "Idempotency-Key": idem},
        json={
            "model": "mock",
            "messages": [
                {"role": "user", "content": "x" * (GUARDRAILS.max_body_bytes + 1024)}
            ],
        },
    )

    assert response.status_code == 413
    assert f"{GUARDRAILS.max_body_bytes} bytes" in response.json()["detail"]
    assert _job_exists(idem) is False


@pytest.mark.asyncio
async def test_http_job_accepts_content_length_at_limit():
    raw_key, _ = create_gateway_api_key("Job Payload At Limit", quota=10)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/jobs",
            headers={
                "Authorization": f"Bearer {raw_key}",
                "Content-Length": str(GUARDRAILS.max_body_bytes),
                "Idempotency-Key": f"http-exact-{uuid.uuid4().hex}",
            },
            json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_http_job_rejects_spoofed_content_length_over_limit():
    raw_key, _ = create_gateway_api_key("Job Payload Spoofed", quota=10)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/jobs",
            headers={
                "Authorization": f"Bearer {raw_key}",
                "Content-Length": str(GUARDRAILS.max_body_bytes + 1),
                "Idempotency-Key": f"http-spoof-{uuid.uuid4().hex}",
            },
            json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_http_job_invalid_key_is_rejected_before_size_check():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/jobs",
            headers={
                "Authorization": "Bearer not-a-real-key",
                "Content-Length": str(GUARDRAILS.max_body_bytes + 1),
            },
            json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_session_authenticated_job_enforces_size_limit():
    user = db_create_user(
        name="Job Payload Session",
        email=f"job-payload-{uuid.uuid4().hex[:10]}@example.com",
        password="secret123",
        auth_provider="local",
        is_verified=True,
    )
    token = db_create_session(user["id"])
    client = TestClient(app)
    headers = {"Cookie": f"llm_gateway_session={token}"}
    idem_ok = f"session-ok-{uuid.uuid4().hex}"
    idem_big = f"session-big-{uuid.uuid4().hex}"

    try:
        small = client.post(
            "/v1/jobs",
            headers={**headers, "Idempotency-Key": idem_ok},
            json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert small.status_code == 200, small.text

        big = client.post(
            "/v1/jobs",
            headers={**headers, "Idempotency-Key": idem_big},
            json={
                "model": "mock",
                "messages": [
                    {"role": "user", "content": "x" * (GUARDRAILS.max_body_bytes + 1)}
                ],
            },
        )
        assert big.status_code == 413
        assert _job_exists(idem_big) is False
    finally:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("DELETE FROM async_jobs WHERE key_id LIKE ?", (f"user_{user['id']}%",))
            conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user["id"],))
            conn.execute("DELETE FROM users WHERE id = ?", (user["id"],))
            conn.commit()
        finally:
            conn.close()


@pytest.mark.asyncio
async def test_normal_job_still_processes_after_enforcement():
    job = await JOB_MANAGER.enqueue_job(
        {"model": "mock", "messages": [{"role": "user", "content": "hello worker"}]},
        KEY_PREFIX + "worker",
        "worker",
        f"worker-{uuid.uuid4().hex}",
    )

    await JOB_MANAGER._process_job(job["id"])
    stored = await JOB_MANAGER.get_job(job["id"])

    assert stored["status"] == "done"
    assert stored["result"] is not None
