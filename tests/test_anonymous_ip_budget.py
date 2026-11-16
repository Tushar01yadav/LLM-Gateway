"""IP-scoped daily spending cap for anonymous callers.

A guest identity is cheap to mint: discard the cookie and the next request gets
a fresh per-guest budget account. Every anonymous request therefore also
reserves against a durable daily account keyed by the trusted-proxy client IP,
so rotating identities cannot reset the spend ceiling. These tests pin the
ledger semantics and the request wiring.
"""

import asyncio
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from httpx import ASGITransport, AsyncClient

import app.cost as cost
import app.db as db
import app.main as main_module
from app.auth import db_create_user
from app.cost import (
    ANONYMOUS_IP_BUDGET_PREFIX,
    CostTracker,
    anonymous_ip_budget_key,
    get_anonymous_ip_daily_budget,
)
from app.main import app
from app.security import create_gateway_api_key

PAYLOAD = {"model": "mock", "messages": [{"role": "user", "content": "anonymous spend probe"}]}


def _unique_ip() -> str:
    return f"2001:db8::{uuid.uuid4().hex[:8]}"


def _ip_ledger(client_ip: str) -> str:
    return anonymous_ip_budget_key(client_ip)


@pytest.fixture
def ip_ledger_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DATABASE_URL", "")
    monkeypatch.setattr(cost, "DATABASE_URL", "")
    monkeypatch.setattr(db, "sqlite3", sqlite3)
    monkeypatch.setattr(cost, "sqlite3", sqlite3)
    monkeypatch.setattr(cost, "DB_PATH", str(tmp_path / "ip-budget.sqlite3"))
    conn = sqlite3.connect(str(tmp_path / "ip-budget.sqlite3"))
    conn.execute("CREATE TABLE gateway_api_keys (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    cost.init_cost_db()
    return CostTracker(redis_url="")


def _reserve_concurrently(workers, client_ip, cap, amount):
    barrier = threading.Barrier(len(workers))

    def reserve(worker):
        barrier.wait(timeout=10)
        return asyncio.run(worker.reserve_anonymous_ip_budget(client_ip, amount, cap))

    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        return list(pool.map(reserve, workers))


def _account(client_ip):
    conn = sqlite3.connect(str(cost.DB_PATH))
    try:
        return conn.execute(
            "SELECT spent,reserved FROM budget_accounts WHERE key_id = ?",
            (_ip_ledger(client_ip),),
        ).fetchone()
    finally:
        conn.close()


def _cost_record_count(client_ip):
    conn = sqlite3.connect(str(cost.DB_PATH))
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM cost_records WHERE key_id = ?", (_ip_ledger(client_ip),)
        ).fetchone()[0]
    finally:
        conn.close()


def test_ip_budget_key_is_canonical():
    assert anonymous_ip_budget_key("203.0.113.9") == f"{ANONYMOUS_IP_BUDGET_PREFIX}203.0.113.9"
    # IPv4-mapped IPv6 must share the IPv4 account.
    assert anonymous_ip_budget_key("::ffff:203.0.113.9") == anonymous_ip_budget_key("203.0.113.9")
    # Different textual forms of the same IPv6 address must share one account.
    assert anonymous_ip_budget_key("2001:0db8:0:0:0:0:0:1") == anonymous_ip_budget_key("2001:db8::1")
    assert anonymous_ip_budget_key("not-an-ip") == f"{ANONYMOUS_IP_BUDGET_PREFIX}not-an-ip"


def test_anonymous_ip_cap_configuration_precedence(monkeypatch):
    cfg = cost.get_models_config()["cost_control"]
    monkeypatch.delenv("GATEWAY_ANONYMOUS_IP_DAILY_BUDGET_USD", raising=False)
    assert get_anonymous_ip_daily_budget() == pytest.approx(cfg["anonymous_ip_daily_budget_usd"])

    monkeypatch.setenv("GATEWAY_ANONYMOUS_IP_DAILY_BUDGET_USD", "1.25")
    assert get_anonymous_ip_daily_budget() == pytest.approx(1.25)

    # Invalid or non-finite overrides fall back to models.json.
    for bad in ("not-a-number", "nan", "-1"):
        monkeypatch.setenv("GATEWAY_ANONYMOUS_IP_DAILY_BUDGET_USD", bad)
        assert get_anonymous_ip_daily_budget() == pytest.approx(cfg["anonymous_ip_daily_budget_usd"])


@pytest.mark.asyncio
async def test_concurrent_ip_reservations_cannot_exceed_cap(ip_ledger_db):
    client_ip = "203.0.113.9"
    workers = [CostTracker(redis_url="") for _ in range(20)]
    results = await asyncio.to_thread(_reserve_concurrently, workers, client_ip, 1.0, 0.2)

    accepted = [reservation_id for reservation_id, _ in results if reservation_id]
    assert len(accepted) == 5
    assert len(set(accepted)) == len(accepted)

    spent, reserved = _account(client_ip)
    assert spent + reserved == pytest.approx(1.0)
    assert spent + reserved <= 1.0

    for reservation_id in accepted:
        await workers[0].release_anonymous_ip_budget(reservation_id, client_ip)


@pytest.mark.asyncio
async def test_concurrent_settlement_and_release_keep_ledger_consistent(ip_ledger_db):
    client_ip = "203.0.113.10"
    workers = [CostTracker(redis_url="") for _ in range(5)]
    reservations = await asyncio.gather(
        *(worker.reserve_anonymous_ip_budget(client_ip, 0.2, 1.0) for worker in workers)
    )
    reservation_ids = [reservation_id for reservation_id, _ in reservations]
    assert all(reservation_ids)

    settled, released = reservation_ids[:3], reservation_ids[3:]
    results = await asyncio.gather(
        *(workers[i].settle_anonymous_ip_budget(rid, client_ip, 0.1) for i, rid in enumerate(settled)),
        *(workers[i + 3].release_anonymous_ip_budget(rid, client_ip) for i, rid in enumerate(released)),
    )
    assert all(results)

    spent, reserved = _account(client_ip)
    assert spent == pytest.approx(0.3)
    assert reserved == pytest.approx(0.0)
    assert _cost_record_count(client_ip) == 0

    # Settled spend still counts against the cap: 0.3 + 0.5 fits, the next
    # 0.3 no longer does.
    first, _ = await workers[0].reserve_anonymous_ip_budget(client_ip, 0.5, 1.0)
    assert first
    second, committed = await workers[0].reserve_anonymous_ip_budget(client_ip, 0.3, 1.0)
    assert second is None
    assert committed == pytest.approx(0.8)
    await workers[0].release_anonymous_ip_budget(first, client_ip)


@pytest.mark.asyncio
async def test_ip_settlement_does_not_emit_second_cost_record(ip_ledger_db):
    tracker = ip_ledger_db
    client_ip = "203.0.113.11"
    reservation_id, _ = await tracker.reserve_anonymous_ip_budget(client_ip, 0.5, 10.0)
    assert reservation_id

    assert await tracker.settle_anonymous_ip_budget(reservation_id, client_ip, 0.25) is True
    assert await tracker.settle_anonymous_ip_budget(reservation_id, client_ip, 0.25) is False

    spent, reserved = _account(client_ip)
    assert spent == pytest.approx(0.25)
    assert reserved == pytest.approx(0.0)
    assert _cost_record_count(client_ip) == 0
    assert tracker._total_cost_memory == 0.0


@pytest.mark.asyncio
async def test_released_ip_reservation_frees_capacity(ip_ledger_db):
    tracker = ip_ledger_db
    client_ip = "203.0.113.12"
    reservation_id, _ = await tracker.reserve_anonymous_ip_budget(client_ip, 0.9, 1.0)
    assert reservation_id
    rejected, _ = await tracker.reserve_anonymous_ip_budget(client_ip, 0.2, 1.0)
    assert rejected is None

    assert await tracker.release_anonymous_ip_budget(reservation_id, client_ip) is True
    retried, _ = await tracker.reserve_anonymous_ip_budget(client_ip, 0.2, 1.0)
    assert retried
    await tracker.release_anonymous_ip_budget(retried, client_ip)


@pytest.mark.asyncio
async def test_rotating_guest_identities_share_one_ip_budget(monkeypatch):
    client_ip = _unique_ip()
    monkeypatch.setattr(main_module, "get_anonymous_ip_daily_budget", lambda: 1.0)
    # Pin estimated and settled cost so the arithmetic is deterministic.
    monkeypatch.setattr(main_module, "estimate_request_cost", lambda *args, **kwargs: 0.6)
    monkeypatch.setattr(main_module, "calculate_request_cost", lambda *args, **kwargs: 0.6)

    transport = ASGITransport(app=app, client=(client_ip, 12345))
    async with AsyncClient(transport=transport, base_url="http://test") as first_client:
        first = await first_client.post("/v1/chat", json=PAYLOAD)
        assert first.status_code == 200, first.text
        first_guest = first.headers["X-Guest-ID"]

    # Fresh cookie jar: the caller drops the guest identity and gets a new one.
    async with AsyncClient(transport=transport, base_url="http://test") as second_client:
        second = await second_client.post("/v1/chat", json=PAYLOAD)
        assert second.status_code == 429, second.text
        assert "Anonymous client IP" in second.json()["detail"]
        assert second.headers.get("X-Guest-ID") != first_guest

    spent, reserved = _account(client_ip)
    assert spent + reserved <= 1.0
    assert spent == pytest.approx(0.6)


@pytest.mark.asyncio
async def test_ip_cap_rejection_releases_guest_reservation(monkeypatch):
    client_ip = _unique_ip()
    monkeypatch.setattr(main_module, "get_anonymous_ip_daily_budget", lambda: 0.1)
    monkeypatch.setattr(main_module, "estimate_request_cost", lambda *args, **kwargs: 0.6)
    monkeypatch.setattr(main_module, "calculate_request_cost", lambda *args, **kwargs: 0.6)

    transport = ASGITransport(app=app, client=(client_ip, 12345))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/chat", json=PAYLOAD)
        assert response.status_code == 429
        assert "Anonymous client IP" in response.json()["detail"]
        guest_id = response.headers["X-Guest-ID"]

    conn = sqlite3.connect(main_module.DB_PATH)
    try:
        guest_account = conn.execute(
            "SELECT reserved FROM budget_accounts WHERE key_id = ?", (guest_id,)
        ).fetchone()
        ip_account = conn.execute(
            "SELECT spent,reserved FROM budget_accounts WHERE key_id = ?", (_ip_ledger(client_ip),)
        ).fetchone()
    finally:
        conn.close()
    assert guest_account is None or guest_account[0] == pytest.approx(0.0)
    # The rejected IP reservation leaves an empty account row but commits no spend.
    assert ip_account is None or ip_account == pytest.approx((0.0, 0.0))


@pytest.mark.asyncio
async def test_provider_failure_releases_both_reservations(monkeypatch):
    client_ip = _unique_ip()

    async def _explode(**kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(main_module, "get_anonymous_ip_daily_budget", lambda: 1.0)
    monkeypatch.setattr(main_module, "estimate_request_cost", lambda *args, **kwargs: 0.6)
    monkeypatch.setattr(main_module, "execute_reliable_completion", _explode)

    transport = ASGITransport(app=app, client=(client_ip, 12345))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/chat", json=PAYLOAD)
        assert response.status_code == 502, response.text

    spent, reserved = _account(client_ip)
    assert spent == pytest.approx(0.0)
    assert reserved == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_api_key_and_session_requests_skip_ip_cap(monkeypatch):
    client_ip = _unique_ip()
    # Any IP reservation would be rejected; authenticated traffic must not make one.
    monkeypatch.setattr(main_module, "get_anonymous_ip_daily_budget", lambda: 0.0)

    raw_key, _ = create_gateway_api_key(name="ip-cap-unaffected", quota=60, daily_budget=5.0)
    transport = ASGITransport(app=app, client=(client_ip, 12345))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        key_response = await client.post(
            "/v1/chat",
            headers={"Authorization": f"Bearer {raw_key}"},
            json=PAYLOAD,
        )
        assert key_response.status_code == 200, key_response.text

        email = f"ip-cap-{uuid.uuid4().hex[:10]}@example.com"
        db_create_user(
            name="IP Cap User",
            email=email,
            password="secret123",
            auth_provider="local",
            is_verified=True,
        )
        login = await client.post("/api/auth/login", json={"email": email, "password": "secret123"})
        assert login.status_code == 200, login.text
        user_response = await client.post("/v1/chat", json=PAYLOAD)
        assert user_response.status_code == 200, user_response.text

    conn = sqlite3.connect(main_module.DB_PATH)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM budget_accounts WHERE key_id = ?", (_ip_ledger(client_ip),)
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0
