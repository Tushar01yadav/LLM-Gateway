import os
import sqlite3
import uuid
import pytest
from app import db, cost, security
from app.db import _translate_sql


@pytest.fixture
def sqlite_test_db(tmp_path, monkeypatch):
    """Fixture that isolates a SQLite database file and clears DATABASE_URL."""
    db_file = str(tmp_path / "test_gateway.db")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setattr(db, "DATABASE_URL", "")
    monkeypatch.setattr(cost, "DATABASE_URL", "")
    monkeypatch.setattr(security, "DATABASE_URL", "")
    monkeypatch.setattr(security, "DB_PATH", db_file)
    monkeypatch.setattr(cost, "DB_PATH", db_file)
    return db_file


def test_fresh_sqlite_db_initialization(sqlite_test_db):
    """Verify that a fresh SQLite database creates gateway_api_keys with daily_budget."""
    security.init_security_db()
    cost.init_cost_db()

    conn = sqlite3.connect(sqlite_test_db)
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(gateway_api_keys)")
        cols = {row[1]: row[2] for row in cursor.fetchall()}
        assert "daily_budget" in cols
        assert "REAL" in cols["daily_budget"].upper()
    finally:
        conn.close()

    # Create key and verify daily_budget round-trips
    raw_key, info = security.create_gateway_api_key("Fresh Key", quota=120, daily_budget=15.0)
    assert info["daily_budget"] == 15.0

    validated = security.validate_gateway_api_key(raw_key)
    assert validated is not None
    assert validated["name"] == "Fresh Key"
    assert validated["quota"] == 120
    assert validated["daily_budget"] == 15.0

    all_keys = security.list_gateway_api_keys()
    assert any(k["id"] == info["id"] and k["daily_budget"] == 15.0 for k in all_keys)


def test_legacy_sqlite_db_migration_preserves_keys_and_enables_daily_budget(sqlite_test_db):
    """
    Verify that an existing SQLite database with the legacy schema (lacking daily_budget)
    is properly migrated, preserving all existing keys and data, and allowing authentication
    and daily budget enforcement.
    """
    # 1. Create legacy table without daily_budget
    conn = sqlite3.connect(sqlite_test_db)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE gateway_api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            key_hash TEXT UNIQUE NOT NULL,
            prefix TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            quota INTEGER NOT NULL DEFAULT 60,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX idx_gw_key_hash ON gateway_api_keys(key_hash);")

    # 2. Populate legacy active key and revoked key
    raw_key_active = f"gw_live_legacyactive_{os.urandom(16).hex()}"
    hash_active = security.hash_api_key(raw_key_active)
    prefix_active = f"{raw_key_active[:12]}...{raw_key_active[-4:]}"

    raw_key_revoked = f"gw_live_legacyrevoked_{os.urandom(16).hex()}"
    hash_revoked = security.hash_api_key(raw_key_revoked)
    prefix_revoked = f"{raw_key_revoked[:12]}...{raw_key_revoked[-4:]}"

    cursor.execute(
        "INSERT INTO gateway_api_keys (name, key_hash, prefix, status, quota) VALUES (?, ?, ?, 'active', 90)",
        ("Legacy Active Client", hash_active, prefix_active),
    )
    active_key_id = cursor.lastrowid

    cursor.execute(
        "INSERT INTO gateway_api_keys (name, key_hash, prefix, status, quota) VALUES (?, ?, ?, 'revoked', 30)",
        ("Legacy Revoked Client", hash_revoked, prefix_revoked),
    )
    revoked_key_id = cursor.lastrowid
    conn.commit()

    # Verify that prior to migration, daily_budget column does NOT exist
    cursor.execute("PRAGMA table_info(gateway_api_keys)")
    cols_pre = {row[1] for row in cursor.fetchall()}
    assert "daily_budget" not in cols_pre

    # Prior to migration, querying daily_budget fails
    with pytest.raises(sqlite3.OperationalError, match="no such column"):
        cursor.execute("SELECT daily_budget FROM gateway_api_keys WHERE id = ?", (active_key_id,))

    conn.close()

    # 3. Run initialization / migration
    security.init_security_db()
    cost.init_cost_db()

    # 4. Verify schema now includes daily_budget
    conn = sqlite3.connect(sqlite_test_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(gateway_api_keys)")
    cols_post = {row[1] for row in cursor.fetchall()}
    assert "daily_budget" in cols_post
    conn.close()

    # 5. Verify existing keys authenticate and have default daily_budget (5.0)
    key_info = security.validate_gateway_api_key(raw_key_active)
    assert key_info is not None
    assert key_info["id"] == active_key_id
    assert key_info["name"] == "Legacy Active Client"
    assert key_info["status"] == "active"
    assert key_info["quota"] == 90
    assert key_info["prefix"] == prefix_active
    assert key_info["daily_budget"] == 5.0

    # Revoked key remains revoked
    assert security.validate_gateway_api_key(raw_key_revoked) is None

    # List keys preserves all records with daily_budget
    keys = {k["id"]: k for k in security.list_gateway_api_keys()}
    assert active_key_id in keys
    assert keys[active_key_id]["daily_budget"] == 5.0
    assert keys[active_key_id]["name"] == "Legacy Active Client"
    assert revoked_key_id in keys
    assert keys[revoked_key_id]["daily_budget"] == 5.0
    assert keys[revoked_key_id]["status"] == "revoked"

    # 6. Verify daily_budget works for budget reservation
    allowed, spend = cost.reserve_budget_sync(
        str(active_key_id),
        key_info["name"],
        key_info["daily_budget"],
        2.0,
        "res-1",
        60.0,
    )
    assert allowed is True
    assert spend == 2.0

    # Reserving amount that exceeds daily_budget (2.0 spent/reserved + 4.0 = 6.0 > 5.0) fails
    allowed2, spend2 = cost.reserve_budget_sync(
        str(active_key_id),
        key_info["name"],
        key_info["daily_budget"],
        4.0,
        "res-2",
        60.0,
    )
    assert allowed2 is False


def test_migration_is_idempotent(sqlite_test_db):
    """Verify running migrations repeatedly does not error or duplicate columns."""
    # Run multiple times consecutively
    for _ in range(4):
        security.init_security_db()
        cost.init_cost_db()

    conn = sqlite3.connect(sqlite_test_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(gateway_api_keys)")
    col_names = [row[1] for row in cursor.fetchall()]
    conn.close()

    assert col_names.count("daily_budget") == 1


def test_cost_db_init_handles_missing_gateway_api_keys_table(sqlite_test_db):
    """Verify init_cost_db succeeds even if gateway_api_keys table does not yet exist."""
    # Should not raise any OperationalError
    cost.init_cost_db()

    conn = sqlite3.connect(sqlite_test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cost_records'")
    assert cursor.fetchone() is not None
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gateway_api_keys'")
    assert cursor.fetchone() is None
    conn.close()


def test_postgres_sql_translation_and_behavior(monkeypatch):
    """Verify PostgreSQL translation behavior for plain ALTER TABLE ADD COLUMN."""
    monkeypatch.setattr("app.db.DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")

    # Plain ALTER TABLE ADD COLUMN is translated to ADD COLUMN IF NOT EXISTS
    sql_plain = "ALTER TABLE gateway_api_keys ADD COLUMN daily_budget REAL DEFAULT 5.0"
    translated = _translate_sql(sql_plain)
    assert "ALTER TABLE llm_gateway.gateway_api_keys ADD COLUMN IF NOT EXISTS daily_budget REAL DEFAULT 5.0" == translated

    # Already containing IF NOT EXISTS is also correctly rewritten to target schema
    sql_if_not_exists = "ALTER TABLE gateway_api_keys ADD COLUMN IF NOT EXISTS daily_budget REAL DEFAULT 5.0"
    translated_if = _translate_sql(sql_if_not_exists)
    assert "ALTER TABLE llm_gateway.gateway_api_keys ADD COLUMN IF NOT EXISTS daily_budget REAL DEFAULT 5.0" == translated_if

    # PRAGMA is filtered out on PostgreSQL
    assert _translate_sql("PRAGMA table_info(gateway_api_keys)") is None


def test_postgres_legacy_daily_budget_migration_and_preservation():
    """
    Verify PostgreSQL path migrates legacy gateway_api_keys lacking daily_budget,
    preserves existing keys, and ensures validate_gateway_api_key returns daily_budget.
    """
    if not db.DATABASE_URL or not os.getenv("GATEWAY_TEST_DATABASE_URL"):
        if os.getenv("GATEWAY_REQUIRE_POSTGRES_TESTS") == "1":
            pytest.fail("PostgreSQL integration is required but DATABASE_URL/GATEWAY_TEST_DATABASE_URL is missing")
        pytest.skip("dedicated PostgreSQL integration service is not configured")
    if db.DATABASE_URL != os.getenv("GATEWAY_TEST_DATABASE_URL"):
        pytest.fail("DATABASE_URL must match the dedicated GATEWAY_TEST_DATABASE_URL")

    # Connect to PostgreSQL and create legacy table without daily_budget
    conn = db.sqlite3.connect()
    try:
        conn.execute("DROP TABLE IF EXISTS llm_gateway.gateway_api_keys CASCADE")
        conn.execute("""
            CREATE TABLE llm_gateway.gateway_api_keys (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                key_hash TEXT UNIQUE NOT NULL,
                prefix TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                quota INTEGER NOT NULL DEFAULT 60,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gw_key_hash ON llm_gateway.gateway_api_keys(key_hash)")

        # Seed legacy active key
        raw_key = f"gw_live_pglegacy_{uuid.uuid4().hex}"
        key_hash = security.hash_api_key(raw_key)
        prefix = f"{raw_key[:12]}...{raw_key[-4:]}"
        conn.execute(
            "INSERT INTO llm_gateway.gateway_api_keys (name, key_hash, prefix, status, quota) VALUES (%s, %s, %s, 'active', 75)",
            ("PG Legacy Client", key_hash, prefix),
        )
        conn.commit()

        # Verify daily_budget is not yet present
        cols_pre = [
            row[0] for row in conn.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'llm_gateway' AND table_name = 'gateway_api_keys'"""
            ).fetchall()
        ]
        assert "daily_budget" not in cols_pre
    finally:
        conn.close()

    # Run initialization / migration on PostgreSQL
    security.init_security_db()
    cost.init_cost_db()

    # Verify column daily_budget is present
    conn = db.sqlite3.connect()
    try:
        cols_post = [
            row[0] for row in conn.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'llm_gateway' AND table_name = 'gateway_api_keys'"""
            ).fetchall()
        ]
        assert "daily_budget" in cols_post
    finally:
        conn.close()

    # Verify legacy key authenticates and has default daily_budget = 5.0
    key_info = security.validate_gateway_api_key(raw_key)
    assert key_info is not None
    assert key_info["name"] == "PG Legacy Client"
    assert key_info["quota"] == 75
    assert key_info["status"] == "active"
    assert key_info["daily_budget"] == 5.0

    # Verify creating key with custom daily_budget on PostgreSQL works
    raw_key2, info2 = security.create_gateway_api_key("PG Fresh Key", quota=150, daily_budget=20.0)
    assert info2["daily_budget"] == 20.0
    val2 = security.validate_gateway_api_key(raw_key2)
    assert val2 is not None
    assert val2["daily_budget"] == 20.0

    # Verify idempotency on PostgreSQL
    security.init_security_db()
    cost.init_cost_db()
