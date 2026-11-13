import logging
import os
import sqlite3
import uuid

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

import app.main as main_module
from app.auth import db_create_user
from app.db import sqlite3 as db_connect
from app.main import app


def _email(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}@example.com"


def _create_verified_user(prefix: str) -> tuple[str, str]:
    email = _email(prefix)
    db_create_user(
        name="Session Tester",
        email=email,
        password="secret123",
        auth_provider="local",
        is_verified=True,
    )
    return email, "secret123"


def _login(client: TestClient, email: str, password: str = "secret123") -> str:
    response = client.post("/api/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return response.json()["user"]["id"]


def _cleanup(user_id: str, session_id: str) -> None:
    main_module.db_delete_session(session_id)
    conn = db_connect.connect(main_module.DB_PATH)
    try:
        conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


def test_uuid_user_chat_history_persists_and_is_owned():
    email, password = _create_verified_user("session-owner")
    session_id = str(uuid.uuid4())
    user_id = None

    try:
        with TestClient(app) as client:
            user_id = _login(client, email, password)
            assert uuid.UUID(user_id)

            response = client.post(
                "/v1/chat",
                json={
                    "model": "mock",
                    "session_id": session_id,
                    "messages": [{"role": "user", "content": f"hello {uuid.uuid4().hex}"}],
                },
            )
            assert response.status_code == 200, response.text
            assert response.json()["provider"] == "mock"

            conn = db_connect.connect(main_module.DB_PATH)
            try:
                row = conn.execute(
                    "SELECT user_id FROM sessions WHERE id = ?", (session_id,)
                ).fetchone()
            finally:
                conn.close()
            assert row is not None and row[0] == user_id

            chats = client.get("/api/chats")
            assert chats.status_code == 200
            assert any(
                c["id"] == session_id and c["user_id"] == user_id for c in chats.json()
            )

            history = client.get(f"/api/chats/{session_id}")
            assert history.status_code == 200
            assert [m["role"] for m in history.json()["messages"]] == ["user", "assistant"]

        other_email, other_password = _create_verified_user("session-other")
        other_id = None
        try:
            with TestClient(app) as other_client:
                other_id = _login(other_client, other_email, other_password)

                assert all(
                    c["id"] != session_id for c in other_client.get("/api/chats").json()
                )
                assert other_client.get(f"/api/chats/{session_id}").status_code == 403
                assert other_client.delete(f"/api/chats/{session_id}").status_code == 403
                assert other_client.patch(
                    f"/api/chats/{session_id}", json={"title": "stolen"}
                ).status_code == 403
                assert other_client.delete("/api/chats").status_code == 200
                assert other_client.get(f"/api/chats/{session_id}").status_code == 403
        finally:
            if other_id:
                _cleanup(other_id, str(uuid.uuid4()))
    finally:
        if user_id:
            _cleanup(user_id, session_id)


def test_persistence_failure_is_logged_and_counted(caplog, monkeypatch):
    email, password = _create_verified_user("session-failure")
    user_id = None

    def failing_create_session(**kwargs):
        raise RuntimeError("simulated session write failure")

    monkeypatch.setattr(main_module, "db_create_session", failing_create_session)

    try:
        with TestClient(app) as client:
            user_id = _login(client, email, password)
            before = REGISTRY.get_sample_value(
                "gateway_persistence_failures_total", {"path": "chat"}
            ) or 0
            with caplog.at_level(logging.ERROR):
                response = client.post(
                    "/v1/chat",
                    json={
                        "model": "mock",
                        "session_id": str(uuid.uuid4()),
                        "messages": [
                            {"role": "user", "content": f"failure {uuid.uuid4().hex}"}
                        ],
                    },
                )
            after = REGISTRY.get_sample_value(
                "gateway_persistence_failures_total", {"path": "chat"}
            ) or 0

        assert response.status_code == 200
        assert after == before + 1
        assert any(
            "Chat history persistence failed" in record.getMessage()
            and "simulated session write failure" in record.getMessage()
            for record in caplog.records
        )
    finally:
        if user_id:
            _cleanup(user_id, str(uuid.uuid4()))


@pytest.mark.skipif(bool(os.getenv("DATABASE_URL")), reason="SQLite-only migration path")
def test_legacy_integer_user_id_is_migrated_without_data_loss(tmp_path, monkeypatch):
    legacy_db = tmp_path / "legacy-gateway.db"
    conn = sqlite3.connect(legacy_db)
    try:
        conn.execute(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                title TEXT,
                model TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO sessions (id, title, model, user_id) VALUES (?, ?, ?, ?)",
            ("legacy-session", "Legacy", "mock", 42),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(main_module, "DB_PATH", str(legacy_db))
    main_module.init_db()

    conn = sqlite3.connect(legacy_db)
    try:
        declared = {
            row[1]: (row[2] or "").upper() for row in conn.execute("PRAGMA table_info(sessions)")
        }
        row = conn.execute(
            "SELECT user_id, title FROM sessions WHERE id = 'legacy-session'"
        ).fetchone()
    finally:
        conn.close()

    assert declared["user_id"] == "TEXT"
    assert row == ("42", "Legacy")
