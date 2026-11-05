import sqlite3
import time
import uuid

from fastapi.testclient import TestClient

from app.auth import (
    DB_PATH,
    db_consume_oauth_state,
    db_create_oauth_state,
    db_create_user,
    db_get_oauth_tokens,
    db_save_oauth_tokens,
    decrypt_oauth_secret,
    encrypt_oauth_secret,
)
from app.main import GOOGLE_CONFIG, app


def _email(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}@example.com"


def test_oauth_state_is_single_use():
    state = f"state-{uuid.uuid4().hex}"

    db_create_oauth_state(state, "verifier-value")

    assert db_consume_oauth_state(state) == "verifier-value"
    assert db_consume_oauth_state(state) is None


def test_oauth_state_rejects_unknown_and_expired():
    assert db_consume_oauth_state("never-issued") is None

    expired = f"state-{uuid.uuid4().hex}"
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO oauth_states (state, code_verifier, expires_at) VALUES (?, ?, ?)",
        (expired, "verifier", time.time() - 1),
    )
    conn.commit()
    conn.close()

    assert db_consume_oauth_state(expired) is None


def test_oauth_tokens_are_encrypted_at_rest():
    key = f"test-key-{uuid.uuid4().hex}"
    plain_access = "ya29.plain-access-token-value"
    plain_refresh = "1//plain-refresh-token-value"

    db_save_oauth_tokens(key, plain_access, plain_refresh, expires_in=60)

    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT access_token, refresh_token FROM oauth_tokens WHERE key = ?", (key,)
    ).fetchone()
    conn.close()

    assert row[0].startswith("enc:v1:")
    assert row[1].startswith("enc:v1:")
    assert plain_access not in row[0]
    assert plain_refresh not in row[1]

    tokens = db_get_oauth_tokens(key)
    assert tokens["access_token"] == plain_access
    assert tokens["refresh_token"] == plain_refresh


def test_encrypt_decrypt_round_trip_and_legacy_plaintext():
    ciphertext = encrypt_oauth_secret("secret-value")
    assert ciphertext.startswith("enc:v1:")
    assert decrypt_oauth_secret(ciphertext) == "secret-value"
    # Rows written before encryption was added stay readable.
    assert decrypt_oauth_secret("legacy-plaintext") == "legacy-plaintext"


def test_session_cookie_is_secure_in_production_mode(monkeypatch):
    import app.main as main_module

    email = _email("cookie")
    db_create_user(
        name="Cookie Tester",
        email=email,
        password="secret123",
        auth_provider="local",
        is_verified=True,
    )
    monkeypatch.setattr(main_module, "COOKIE_SECURE", True)

    with TestClient(app) as client:
        response = client.post("/api/auth/login", json={"email": email, "password": "secret123"})

    assert response.status_code == 200
    assert "secure" in response.headers["set-cookie"].lower()


def test_google_auth_url_has_state_and_pkce():
    if not GOOGLE_CONFIG.get("client_id"):
        import pytest

        pytest.skip("Google OAuth client is not configured")

    with TestClient(app) as client:
        response = client.get("/api/auth/google/url")

    assert response.status_code == 200
    url = response.json()["url"]
    assert "state=" in url
    assert "code_challenge=" in url
    assert "code_challenge_method=S256" in url


def test_google_callback_rejects_unknown_state():
    with TestClient(app) as client:
        response = client.post("/api/auth/google/callback", json={"code": "fake-code", "state": "not-issued"})

    assert response.status_code == 400
    assert "OAuth state" in response.json()["detail"]
