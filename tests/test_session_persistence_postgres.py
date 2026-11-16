import os
import uuid

import pytest

import app.main as main_module
from app import db


def _require_postgres():
    if not db.DATABASE_URL or not os.getenv("GATEWAY_TEST_DATABASE_URL"):
        if os.getenv("GATEWAY_REQUIRE_POSTGRES_TESTS") == "1":
            pytest.fail(
                "PostgreSQL integration is required but DATABASE_URL/GATEWAY_TEST_DATABASE_URL is missing"
            )
        pytest.skip("dedicated PostgreSQL integration service is not configured")
    if db.DATABASE_URL != os.getenv("GATEWAY_TEST_DATABASE_URL"):
        pytest.fail("DATABASE_URL must match the dedicated GATEWAY_TEST_DATABASE_URL")


def test_postgres_integer_user_id_is_migrated_to_text():
    _require_postgres()
    main_module.init_db()

    conn = db.sqlite3.connect()
    try:
        existing = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    finally:
        conn.close()
    if existing:
        pytest.skip("sessions table already holds rows; refusing to rewrite a shared column type")

    conn = db.sqlite3.connect()
    try:
        conn.execute(
            "ALTER TABLE sessions ALTER COLUMN user_id TYPE INTEGER USING user_id::INTEGER"
        )
        conn.execute(
            "INSERT INTO sessions (id, title, model, user_id) VALUES "
            "('legacy-pg-session', 'Legacy', 'mock', 42)"
        )
        conn.commit()
        legacy_type = conn.execute(
            """SELECT data_type FROM information_schema.columns
               WHERE table_schema = 'llm_gateway'
                 AND table_name = 'sessions'
                 AND column_name = 'user_id'"""
        ).fetchone()
    finally:
        conn.close()
    assert legacy_type == ("integer",)

    main_module.init_db()

    conn = db.sqlite3.connect()
    try:
        migrated_type = conn.execute(
            """SELECT data_type FROM information_schema.columns
               WHERE table_schema = 'llm_gateway'
                 AND table_name = 'sessions'
                 AND column_name = 'user_id'"""
        ).fetchone()
        migrated_row = conn.execute(
            "SELECT user_id FROM sessions WHERE id = 'legacy-pg-session'"
        ).fetchone()
        conn.execute("DELETE FROM sessions WHERE id = 'legacy-pg-session'")
        conn.commit()
    finally:
        conn.close()

    assert migrated_type == ("text",)
    assert migrated_row == ("42",)


def test_postgres_sessions_accept_uuid_user_ids():
    _require_postgres()

    # init_db is idempotent: the second call exercises ALTER ... TYPE on an
    # already-text column.
    main_module.init_db()
    main_module.init_db()

    conn = db.sqlite3.connect()
    try:
        column = conn.execute(
            """SELECT data_type FROM information_schema.columns
               WHERE table_schema = 'llm_gateway'
                 AND table_name = 'sessions'
                 AND column_name = 'user_id'"""
        ).fetchone()
    finally:
        conn.close()
    assert column == ("text",)

    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    try:
        main_module.db_create_session(
            session_id=session_id,
            title="postgres uuid session",
            model="mock",
            user_id=user_id,
            user_name="PG User",
            user_email="pg-session@example.com",
        )

        sessions = main_module.db_get_sessions(user_id)
        assert [s["id"] for s in sessions] == [session_id]
        assert sessions[0]["user_id"] == user_id

        owner = main_module.db_get_session_owner(session_id)
        assert owner == {"id": session_id, "user_id": user_id}

        main_module._assert_session_owner(session_id, user_id)

        with pytest.raises(main_module.HTTPException) as exc:
            main_module._assert_session_owner(session_id, str(uuid.uuid4()))
        assert exc.value.status_code == 403
    finally:
        conn = db.sqlite3.connect()
        try:
            conn.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
            conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            conn.commit()
        finally:
            conn.close()
