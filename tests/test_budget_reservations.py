import asyncio
import os
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

import app.db as db
import app.cost as cost


@pytest.fixture
def isolated_budget_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DATABASE_URL", "")
    monkeypatch.setattr(cost, "DATABASE_URL", "")
    monkeypatch.setattr(db, "sqlite3", sqlite3)
    monkeypatch.setattr(cost, "sqlite3", sqlite3)
    monkeypatch.setattr(cost, "DB_PATH", str(tmp_path / "budget.sqlite3"))
    conn = db.sqlite3.connect(str(tmp_path / "budget.sqlite3"))
    conn.execute("CREATE TABLE gateway_api_keys (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    cost.init_cost_db()
    tracker = cost.CostTracker()
    tracker.redis_url = ""
    return tracker


def _seed_current_day_rollover(key_id):
    conn = db.sqlite3.connect(str(cost.DB_PATH))
    try:
        now = cost._database_epoch(conn.cursor())
        current_day = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d")
        prior_day = (datetime.fromtimestamp(now, timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        today_start = datetime.fromtimestamp(now, timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp()
        conn.execute(
            "INSERT INTO cost_records (key_id,key_name,alias,provider,model,prompt_tokens,completion_tokens,total_tokens,cost_usd,timestamp) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (key_id, "rollover", "mock", "mock", "mock", 1, 1, 2, 0.7, today_start + 1),
        )
        conn.execute(
            "INSERT INTO budget_accounts (key_id,budget_date,daily_budget,spent,reserved) VALUES (?,?,?,?,?)",
            (key_id, prior_day, 1.0, 0.7, 0.2),
        )
        conn.execute(
            "INSERT INTO budget_reservations (id,key_id,key_name,budget_date,amount,status,expires_at,created_at) VALUES (?,?,?,?,?,'active',?,?)",
            (f"old-{key_id}", key_id, "rollover", prior_day, 0.2, now + 3600, now - 86400),
        )
        conn.commit()
        return now, current_day
    finally:
        conn.close()


def _reserve_concurrently(workers, key_id, key_name, daily_budget, amount):
    barrier = threading.Barrier(len(workers))

    def reserve(worker):
        barrier.wait(timeout=10)
        return asyncio.run(worker.reserve_budget(key_id, key_name, daily_budget, amount))

    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        return list(pool.map(reserve, workers))


@pytest.mark.asyncio
async def test_concurrent_reservations_cannot_exceed_daily_budget(isolated_budget_db):
    key_id = f"budget-{uuid.uuid4().hex}"
    workers = [cost.CostTracker(redis_url="") for _ in range(20)]
    reservations = await asyncio.to_thread(
        _reserve_concurrently, workers, key_id, "concurrent", 1.0, 0.2
    )
    accepted = [reservation_id for reservation_id, _ in reservations if reservation_id]
    assert len(accepted) == 5
    assert len(set(accepted)) == len(accepted)
    conn = db.sqlite3.connect(str(cost.DB_PATH))
    try:
        account = conn.execute("SELECT spent,reserved FROM budget_accounts WHERE key_id = ?", (key_id,)).fetchone()
        assert account[0] + account[1] == pytest.approx(1.0)
        assert account[0] + account[1] <= 1.0
    finally:
        conn.close()
    await asyncio.gather(*(workers[0].release_budget(reservation_id, key_id) for reservation_id in accepted))


@pytest.mark.asyncio
async def test_concurrent_daily_rollover_preserves_current_spend_and_old_reservation(isolated_budget_db):
    key_id = f"rollover-{uuid.uuid4().hex}"
    _, current_day = _seed_current_day_rollover(key_id)
    workers = [cost.CostTracker(redis_url="") for _ in range(20)]
    results = await asyncio.to_thread(
        _reserve_concurrently, workers, key_id, "rollover", 1.0, 0.15
    )
    accepted = [reservation_id for reservation_id, _ in results if reservation_id]
    assert len(accepted) == 2
    conn = db.sqlite3.connect(str(cost.DB_PATH))
    try:
        account = conn.execute(
            "SELECT budget_date,spent,reserved FROM budget_accounts WHERE key_id = ?", (key_id,)
        ).fetchone()
        old_reservation = conn.execute(
            "SELECT status FROM budget_reservations WHERE id = ?", (f"old-{key_id}",)
        ).fetchone()
        assert account[0] == current_day
        assert account[1] + account[2] == pytest.approx(1.0)
        assert old_reservation == ("active",)
    finally:
        conn.close()
    for reservation_id in accepted:
        await workers[0].release_budget(reservation_id, key_id)
    await workers[0].release_budget(f"old-{key_id}", key_id)


@pytest.mark.asyncio
async def test_settlement_is_idempotent_and_releases_unused_reservation(isolated_budget_db):
    tracker = isolated_budget_db
    key_id = f"budget-{uuid.uuid4().hex}"
    reservation_id, _ = await tracker.reserve_budget(key_id, "settlement", 1.0, 0.8)
    assert reservation_id
    args = dict(reservation_id=reservation_id, key_id=key_id, key_name="settlement", alias="mock",
                provider="mock", model="mock", prompt_tokens=10, completion_tokens=5,
                total_tokens=15, actual_cost=0.3)
    other_tenant = f"other-{uuid.uuid4().hex}"
    assert await tracker.release_budget(reservation_id, other_tenant) is False
    assert await tracker.settle_budget(**{**args, "key_id": other_tenant}) is False
    assert await tracker.settle_budget(**args) is True
    assert await cost.CostTracker(redis_url="").settle_budget(**args) is False
    conn = db.sqlite3.connect(str(cost.DB_PATH))
    try:
        reservation_time = conn.execute(
            "SELECT created_at FROM budget_reservations WHERE id = ?", (reservation_id,)
        ).fetchone()[0]
        recorded_time = conn.execute(
            "SELECT timestamp FROM cost_records WHERE key_id = ?", (key_id,)
        ).fetchone()[0]
        assert recorded_time == reservation_time
    finally:
        conn.close()
    next_reservation, committed = await tracker.reserve_budget(key_id, "settlement", 1.0, 0.7)
    assert next_reservation
    assert committed == pytest.approx(1.0)
    await tracker.release_budget(next_reservation, key_id)


def test_missing_provider_usage_uses_consistent_text_estimates():
    assert cost.resolve_token_usage(
        {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        [{"role": "user", "content": "abcd"}],
        "abcdefgh",
    ) == (1, 2, 3)


@pytest.mark.asyncio
async def test_expired_reservation_is_recovered_and_stale_settlement_rejected(isolated_budget_db):
    tracker = isolated_budget_db
    key_id = f"budget-{uuid.uuid4().hex}"
    expired_id, _ = await tracker.reserve_budget(key_id, "expiry", 1.0, 0.8, lease_seconds=0)
    assert expired_id
    assert await tracker.settle_budget(
        expired_id, key_id, "expiry", "mock", "mock", "mock", 1, 1, 2, 0.1
    ) is False
    recovered_id, committed = await tracker.reserve_budget(key_id, "expiry", 1.0, 0.8)
    assert recovered_id
    assert committed == pytest.approx(0.8)
    await tracker.release_budget(recovered_id, key_id)


@pytest.mark.asyncio
async def test_stale_worker_id_cannot_release_or_settle_another_reservation(isolated_budget_db):
    tracker = isolated_budget_db
    key_id = f"budget-{uuid.uuid4().hex}"
    active_id, _ = await tracker.reserve_budget(key_id, "worker-b", 1.0, 0.5)
    assert active_id
    stale_worker_id = uuid.uuid4().hex
    assert await tracker.release_budget(stale_worker_id, key_id) is False
    assert await tracker.settle_budget(
        stale_worker_id, key_id, "worker-a", "mock", "mock", "mock", 1, 1, 2, 0.1
    ) is False
    conn = db.sqlite3.connect(str(cost.DB_PATH))
    try:
        assert conn.execute(
            "SELECT reserved FROM budget_accounts WHERE key_id = ?", (key_id,)
        ).fetchone()[0] == pytest.approx(0.5)
    finally:
        conn.close()
    await tracker.release_budget(active_id, key_id)
    assert await tracker.release_budget(active_id, key_id) is False


@pytest.mark.asyncio
async def test_reservation_insert_failure_rolls_back_account_increment(isolated_budget_db):
    tracker = isolated_budget_db
    reservation_id = uuid.uuid4().hex
    first_key = f"first-{uuid.uuid4().hex}"
    second_key = f"second-{uuid.uuid4().hex}"
    first_allowed, _ = await asyncio.to_thread(
        cost.reserve_budget_sync, first_key, "first", 1.0, 0.4, reservation_id, 120.0
    )
    assert first_allowed
    with pytest.raises(db.sqlite3.IntegrityError):
        await asyncio.to_thread(
            cost.reserve_budget_sync, second_key, "second", 1.0, 0.4,
            reservation_id, 120.0,
        )
    conn = db.sqlite3.connect(str(cost.DB_PATH))
    try:
        account = conn.execute(
            "SELECT spent,reserved FROM budget_accounts WHERE key_id = ?", (second_key,)
        ).fetchone()
        assert account is None
    finally:
        conn.close()
    await tracker.release_budget(reservation_id, first_key)


@pytest.mark.asyncio
async def test_postgres_reservations_are_atomic_across_independent_trackers():
    if not db.DATABASE_URL or not os.getenv("GATEWAY_TEST_DATABASE_URL"):
        if os.getenv("GATEWAY_REQUIRE_POSTGRES_TESTS") == "1":
            pytest.fail("PostgreSQL integration is required but DATABASE_URL/GATEWAY_TEST_DATABASE_URL is missing")
        pytest.skip("dedicated PostgreSQL integration service is not configured")
    if db.DATABASE_URL != os.getenv("GATEWAY_TEST_DATABASE_URL"):
        pytest.fail("DATABASE_URL must match the dedicated GATEWAY_TEST_DATABASE_URL")
    tracker_a, tracker_b = cost.CostTracker(), cost.CostTracker()
    tracker_a.redis_url = tracker_b.redis_url = ""
    key_id = f"pg-budget-{uuid.uuid4().hex}"
    workers = [tracker_a, tracker_b] + [cost.CostTracker(redis_url="") for _ in range(18)]
    rollover_key = f"pg-rollover-{uuid.uuid4().hex}"
    rollover_accepted = []
    reservations = await asyncio.to_thread(
        _reserve_concurrently, workers, key_id, "postgres-race", 1.0, 0.2
    )
    accepted = [reservation_id for reservation_id, _ in reservations if reservation_id]
    try:
        assert len(accepted) == 5
        assert len(set(accepted)) == len(accepted)
        conn = db.sqlite3.connect()
        try:
            account = conn.execute(
                "SELECT spent,reserved FROM budget_accounts WHERE key_id = ?", (key_id,)
            ).fetchone()
            assert account[0] + account[1] == pytest.approx(1.0)
            assert account[0] + account[1] <= 1.0
        finally:
            conn.close()

        _, current_day = _seed_current_day_rollover(rollover_key)
        rollover_results = await asyncio.to_thread(
            _reserve_concurrently, workers, rollover_key, "postgres-rollover", 1.0, 0.15
        )
        rollover_accepted = [reservation_id for reservation_id, _ in rollover_results if reservation_id]
        assert len(rollover_accepted) == 2
        conn = db.sqlite3.connect()
        try:
            account = conn.execute(
                "SELECT budget_date,spent,reserved FROM budget_accounts WHERE key_id = ?",
                (rollover_key,),
            ).fetchone()
            old_status = conn.execute(
                "SELECT status FROM budget_reservations WHERE id = ?",
                (f"old-{rollover_key}",),
            ).fetchone()
            assert account[0] == current_day
            assert account[1] + account[2] == pytest.approx(1.0)
            assert old_status == ("active",)
        finally:
            conn.close()
    finally:
        for reservation_id in accepted:
            await tracker_a.release_budget(reservation_id, key_id)
        for reservation_id in rollover_accepted:
            await tracker_a.release_budget(reservation_id, rollover_key)
        await tracker_a.release_budget(f"old-{rollover_key}", rollover_key)
        conn = db.sqlite3.connect()
        try:
            for cleanup_key in (key_id, rollover_key):
                conn.execute("DELETE FROM budget_reservations WHERE key_id = ?", (cleanup_key,))
                conn.execute("DELETE FROM budget_accounts WHERE key_id = ?", (cleanup_key,))
                conn.execute("DELETE FROM cost_records WHERE key_id = ?", (cleanup_key,))
            conn.commit()
        finally:
            conn.close()
