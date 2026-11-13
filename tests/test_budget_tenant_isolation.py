import asyncio
import os
import secrets
import sqlite3
import time
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

import app.db as db
import app.cost as cost
import app.main as main_module
from app.auth import db_create_user
from app.cost import COST_TRACKER
from app.main import app
from app.security import (
    GUEST_COOKIE_NAME,
    create_gateway_api_key,
    generate_guest_identity,
    validate_guest_identity,
)


def _email(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}@example.com"


def _create_verified_user(prefix: str) -> tuple[str, str]:
    email = _email(prefix)
    db_create_user(
        name="Tenant User",
        email=email,
        password="secret123",
        auth_provider="local",
        is_verified=True,
    )
    return email, "secret123"


async def _login(client: AsyncClient, email: str, password: str = "secret123") -> str:
    response = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return response.json()["user"]["id"]


@pytest.mark.asyncio
async def test_authenticated_users_have_isolated_budgets():
    """1. Two authenticated users have distinct, isolated budgets and spend pools."""
    email_a, pwd_a = _create_verified_user("user-a")
    email_b, pwd_b = _create_verified_user("user-b")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client_a, \
               AsyncClient(transport=transport, base_url="http://test") as client_b:

        user_a_id = await _login(client_a, email_a, pwd_a)
        user_b_id = await _login(client_b, email_b, pwd_b)
        assert user_a_id != user_b_id

        # User A sends a chat request
        resp_a = await client_a.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "hello from user a"}]},
        )
        assert resp_a.status_code == 200, resp_a.text

        # User B sends a chat request
        resp_b = await client_b.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "hello from user b"}]},
        )
        assert resp_b.status_code == 200, resp_b.text

        # Verify database budget accounts and cost records
        conn = db.sqlite3.connect(main_module.DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("SELECT key_id, spent FROM budget_accounts WHERE key_id IN (?, ?)", (f"user_{user_a_id}", f"user_{user_b_id}"))
            accounts = dict(cur.fetchall())
            assert f"user_{user_a_id}" in accounts
            assert f"user_{user_b_id}" in accounts

            # Each user has their own distinct cost records
            cur.execute("SELECT key_id, cost_usd FROM cost_records WHERE key_id IN (?, ?)", (f"user_{user_a_id}", f"user_{user_b_id}"))
            records = cur.fetchall()
            keys_in_records = {r[0] for r in records}
            assert f"user_{user_a_id}" in keys_in_records
            assert f"user_{user_b_id}" in keys_in_records
        finally:
            conn.close()


@pytest.mark.asyncio
async def test_anonymous_guests_have_isolated_budgets():
    """2. Two anonymous guests have isolated budgets and distinct tenant IDs."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client_a, \
               AsyncClient(transport=transport, base_url="http://test") as client_b:

        resp_a = await client_a.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "guest a message"}]},
        )
        assert resp_a.status_code == 200, resp_a.text
        guest_a_id = resp_a.headers.get("X-Guest-ID")
        assert guest_a_id is not None
        assert GUEST_COOKIE_NAME in client_a.cookies

        resp_b = await client_b.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "guest b message"}]},
        )
        assert resp_b.status_code == 200, resp_b.text
        guest_b_id = resp_b.headers.get("X-Guest-ID")
        assert guest_b_id is not None
        assert GUEST_COOKIE_NAME in client_b.cookies

        # Guest identities must be distinct
        assert guest_a_id != guest_b_id

        conn = db.sqlite3.connect(main_module.DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("SELECT key_id FROM budget_accounts WHERE key_id IN (?, ?)", (guest_a_id, guest_b_id))
            accounts = {r[0] for r in cur.fetchall()}
            assert guest_a_id in accounts
            assert guest_b_id in accounts
        finally:
            conn.close()


@pytest.mark.asyncio
async def test_guest_identity_stable_across_requests():
    """3. A guest's identity remains stable across requests, maintaining budget continuity."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Request 1: Fresh visitor without cookie
        resp1 = await client.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "request 1"}]},
        )
        assert resp1.status_code == 200
        guest_id_1 = resp1.headers.get("X-Guest-ID")
        assert guest_id_1 is not None

        # Request 2: Re-uses cookie from Request 1
        resp2 = await client.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "request 2"}]},
        )
        assert resp2.status_code == 200
        guest_id_2 = resp2.headers.get("X-Guest-ID")
        assert guest_id_2 == guest_id_1

        # Verify both cost records are attributed to the exact same tenant
        conn = db.sqlite3.connect(main_module.DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cost_records WHERE key_id = ?", (guest_id_1,))
            count = cur.fetchone()[0]
            assert count == 2
        finally:
            conn.close()


@pytest.mark.asyncio
async def test_one_tenant_exhausting_budget_does_not_affect_another():
    """4. One tenant exhausting its daily budget gets 429 without affecting other tenants."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client_exhausted, \
               AsyncClient(transport=transport, base_url="http://test") as client_normal:

        # Initialize guest 1
        resp1 = await client_exhausted.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "prime budget"}]},
        )
        assert resp1.status_code == 200
        guest_exhausted_id = resp1.headers["X-Guest-ID"]

        # Artificially set spent to 50.0 (the daily budget limit) for guest 1
        conn = db.sqlite3.connect(main_module.DB_PATH)
        try:
            conn.execute("UPDATE budget_accounts SET spent = 50.0 WHERE key_id = ?", (guest_exhausted_id,))
            conn.commit()
        finally:
            conn.close()

        # Guest 1 should now be rejected with 429 Budget exceeded
        resp_blocked = await client_exhausted.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "try again"}]},
        )
        assert resp_blocked.status_code == 429
        assert "Budget exceeded" in resp_blocked.json()["detail"]

        # Guest 2 should succeed unaffected (200 OK)
        resp_normal = await client_normal.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "i am guest 2"}]},
        )
        assert resp_normal.status_code == 200


@pytest.mark.asyncio
async def test_concurrent_reservations_remain_isolated():
    """5. Concurrent reservations across different tenants remain strictly isolated."""
    tenants = [f"concurrent-tenant-{uuid.uuid4().hex[:8]}" for _ in range(5)]
    workers = [cost.CostTracker(redis_url="") for _ in tenants]

    async def _reserve(tenant, worker):
        res_id, _ = await worker.reserve_budget(tenant, tenant, 10.0, 1.5)
        return res_id

    tasks = [_reserve(t, w) for t, w in zip(tenants, workers)]
    reservation_ids = await asyncio.gather(*tasks)
    assert all(r is not None for r in reservation_ids)
    assert len(set(reservation_ids)) == len(tenants)

    # Settle one and release others
    await workers[0].settle_budget(reservation_ids[0], 1.2, tenants[0], tenants[0], "mock", "mock", "mock", 1, 1, 2)
    for r_id, t_id, w in zip(reservation_ids[1:], tenants[1:], workers[1:]):
        await w.release_budget(r_id, t_id)


@pytest.mark.asyncio
async def test_cost_records_contain_correct_tenant_identity():
    """6. Cost records contain the correct tenant identity (API key, user_{id}, or guest_{token})."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        start_ts = time.time() - 1.0
        # API key
        raw_key, key_info = create_gateway_api_key(name="budget-test-key", quota=60, daily_budget=10.0)
        resp_key = await ac.post(
            "/v1/chat",
            headers={"Authorization": f"Bearer {raw_key}"},
            json={"model": "mock", "messages": [{"role": "user", "content": "api key call"}]},
        )
        assert resp_key.status_code == 200

        # User
        email, pwd = _create_verified_user("user-cost")
        user_id = await _login(ac, email, pwd)
        resp_user = await ac.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "user call"}]},
        )
        assert resp_user.status_code == 200

    # Guest (clean client without user session)
    async with AsyncClient(transport=transport, base_url="http://test") as guest_ac:
        resp_guest = await guest_ac.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "guest call"}]},
        )
        assert resp_guest.status_code == 200
        guest_id = resp_guest.headers["X-Guest-ID"]

    conn = db.sqlite3.connect(main_module.DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT key_id, key_name FROM cost_records WHERE key_id = ?", (str(key_info["id"]),))
        key_record = cur.fetchone()
        assert key_record is not None and key_record[0] == str(key_info["id"])

        cur.execute("SELECT key_id FROM cost_records WHERE key_id = ?", (f"user_{user_id}",))
        user_record = cur.fetchone()
        assert user_record is not None and user_record[0] == f"user_{user_id}"

        cur.execute("SELECT key_id FROM cost_records WHERE key_id = ?", (guest_id,))
        guest_record = cur.fetchone()
        assert guest_record is not None and guest_record[0] == guest_id

        # Verify no record was created under key_id="0" during this test
        cur.execute("SELECT COUNT(*) FROM cost_records WHERE key_id = '0' AND timestamp >= ?", (start_ts,))
        count_zero = cur.fetchone()[0]
        assert count_zero == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_cache_hits_and_async_jobs_preserve_attribution():
    """7. Cache hits and async jobs preserve intended tenant attribution."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client_a, \
               AsyncClient(transport=transport, base_url="http://test") as client_b:

        unique_query = f"cache-test-prompt-{uuid.uuid4().hex}"
        payload = {"model": "mock", "messages": [{"role": "user", "content": unique_query}]}

        # Guest A first call -> cache miss
        r1 = await client_a.post("/v1/chat", json=payload)
        assert r1.status_code == 200
        assert r1.json().get("cached", False) is False
        guest_a_id = r1.headers["X-Guest-ID"]

        # Guest A second call -> cache hit
        r2 = await client_a.post("/v1/chat", json=payload)
        assert r2.status_code == 200
        assert r2.json().get("cached") is True

        # Guest B call with SAME payload -> must be cache MISS because of tenant scoping
        r3 = await client_b.post("/v1/chat", json=payload)
        assert r3.status_code == 200
        assert r3.json().get("cached", False) is False
        guest_b_id = r3.headers["X-Guest-ID"]
        assert guest_a_id != guest_b_id

    # Async Jobs attribution: user A enqueues job
    email_a, pwd_a = _create_verified_user("async-a")
    email_b, pwd_b = _create_verified_user("async-b")
    async with AsyncClient(transport=transport, base_url="http://test") as user_a_client, \
               AsyncClient(transport=transport, base_url="http://test") as user_b_client:
        user_a_id = await _login(user_a_client, email_a, pwd_a)
        user_b_id = await _login(user_b_client, email_b, pwd_b)

        job_res = await user_a_client.post(
            "/v1/jobs",
            json={"model": "mock", "messages": [{"role": "user", "content": "async job query"}]},
        )
        assert job_res.status_code == 200
        job_id = job_res.json()["id"]

        # User B attempting to view User A's job -> 403 Forbidden
        cross_job_res = await user_b_client.get(f"/v1/jobs/{job_id}")
        assert cross_job_res.status_code == 403


@pytest.mark.asyncio
async def test_existing_api_key_budget_behavior():
    """8. Existing API-key budget enforcement remains 100% compatible."""
    raw_key, info = create_gateway_api_key(name="strict-api-key", quota=60, daily_budget=0.000001)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # First request uses up the budget
        r1 = await client.post(
            "/v1/chat",
            headers={"Authorization": f"Bearer {raw_key}"},
            json={"model": "mock", "messages": [{"role": "user", "content": "spend budget"}]},
        )
        # Subsequent request fails closed with 429
        r2 = await client.post(
            "/v1/chat",
            headers={"Authorization": f"Bearer {raw_key}"},
            json={"model": "mock", "messages": [{"role": "user", "content": "exceed budget"}]},
        )
        assert r2.status_code == 429
        assert "Budget exceeded" in r2.json()["detail"]


@pytest.mark.asyncio
async def test_legacy_data_and_migration_safety():
    """9. Legacy records and pre-existing data with key_id='0' are preserved safely."""
    conn = db.sqlite3.connect(main_module.DB_PATH)
    legacy_res_id = f"legacy-res-{uuid.uuid4().hex}"
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM budget_reservations WHERE key_id = '0'")
        cur.execute("DELETE FROM budget_accounts WHERE key_id = '0'")
        cur.execute(
            "INSERT INTO budget_accounts (key_id, budget_date, daily_budget, spent, reserved) VALUES ('0', '2026-01-01', 50.0, 10.0, 0.0)"
        )
        cur.execute(
            "INSERT INTO cost_records (key_id, key_name, alias, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, timestamp) VALUES ('0', 'legacy', 'mock', 'mock', 'mock', 1, 1, 2, 0.05, 1700000000.0)"
        )
        cur.execute(
            "INSERT INTO budget_reservations (id, key_id, key_name, budget_date, amount, status, expires_at, created_at) VALUES (?, '0', 'legacy', '2026-01-01', 0.1, 'active', 1000.0, 900.0)",
            (legacy_res_id,),
        )
        conn.commit()
    finally:
        conn.close()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "post-migration check"}]},
        )
        assert resp.status_code == 200
        guest_id = resp.headers["X-Guest-ID"]
        assert guest_id != "0"

    # Verify legacy records remain intact and separate
    conn = db.sqlite3.connect(main_module.DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT spent FROM budget_accounts WHERE key_id = '0'")
        row = cur.fetchone()
        assert row is not None and float(row[0]) == pytest.approx(10.0)

        cur.execute("SELECT key_id FROM budget_reservations WHERE id = ?", (legacy_res_id,))
        assert cur.fetchone()[0] == "0"
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_negative_guest_spoofing_rejected():
    """10. Negative tests: Forged/tampered guest cookies and headers are rejected with 403."""
    transport = ASGITransport(app=app)

    # A: Forged cookie with invalid signature
    async with AsyncClient(transport=transport, base_url="http://test", cookies={GUEST_COOKIE_NAME: "guest_0123456789abcdef0123456789abcdef_deadbeefcafebabe"}) as client:
        resp_spoofed = await client.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "forged token"}]},
        )
        assert resp_spoofed.status_code == 403
        assert "Invalid or forged guest identity" in resp_spoofed.json()["detail"]

    # B: Malformed guest cookie
    async with AsyncClient(transport=transport, base_url="http://test", cookies={GUEST_COOKIE_NAME: "guest_fake_token_123"}) as client:
        resp_malformed = await client.post(
            "/v1/chat",
            json={"model": "mock", "messages": [{"role": "user", "content": "malformed token"}]},
        )
        assert resp_malformed.status_code == 403

    # C: Forged X-Guest-ID header without cookie
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_header_forged = await client.post(
            "/v1/chat",
            headers={"X-Guest-ID": "guest_spoofed_victim"},
            json={"model": "mock", "messages": [{"role": "user", "content": "spoofed header"}]},
        )
        assert resp_header_forged.status_code == 403


@pytest.mark.asyncio
async def test_negative_cross_tenant_session_hijacking():
    """11. Negative tests: Chat session hijacking across users and guests is rejected with 403."""
    transport = ASGITransport(app=app)

    # Guest A creates session
    session_id = f"hijack-test-session-{uuid.uuid4().hex}"
    async with AsyncClient(transport=transport, base_url="http://test") as client_a:
        r1 = await client_a.post(
            "/v1/chat",
            json={"model": "mock", "session_id": session_id, "messages": [{"role": "user", "content": "guest A secret conversation"}]},
        )
        assert r1.status_code == 200

    # Guest B tries to append to Guest A's session
    async with AsyncClient(transport=transport, base_url="http://test") as client_b:
        r2 = await client_b.post(
            "/v1/chat",
            json={"model": "mock", "session_id": session_id, "messages": [{"role": "user", "content": "guest B intruding"}]},
        )
        assert r2.status_code == 403
        assert "belongs to another user" in r2.json()["detail"]

    # Logged in User tries to append to Guest A's session
    email, pwd = _create_verified_user("hijack-user")
    async with AsyncClient(transport=transport, base_url="http://test") as client_user:
        await _login(client_user, email, pwd)
        r3 = await client_user.post(
            "/v1/chat",
            json={"model": "mock", "session_id": session_id, "messages": [{"role": "user", "content": "user intruding"}]},
        )
        assert r3.status_code == 403
