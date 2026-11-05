import time

from fastapi.testclient import TestClient

from app.main import HTML_CONTENT, app
from app.reliability import INJECTED_LATENCY_MS
from app.security import ADMIN_TOKEN


ADMIN = {"X-Admin-Token": ADMIN_TOKEN}


def test_chat_response_headers_expose_routing_metadata():
    with TestClient(app) as client:
        response = client.post(
            "/v1/chat",
            json={"alias": "mock", "messages": [{"role": "user", "content": "header check"}]},
        )

    assert response.status_code == 200
    headers = response.headers
    assert headers["X-Request-Id"].startswith("req_")
    assert headers["X-Provider"] == "mock"
    assert headers["X-Model"]
    assert headers["X-Fallback-Used"] in ("true", "false")
    assert float(headers["X-Latency-Ms"]) >= 0
    assert float(headers["X-Cost-USD"]) >= 0


def test_injected_latency_changes_mock_latency():
    with TestClient(app) as client:
        try:
            set_response = client.post(
                "/api/reliability/inject-latency",
                headers=ADMIN,
                json={"provider": "mock", "latency_ms": 1200},
            )
            assert set_response.status_code == 200
            assert set_response.json()["latency_ms"] == 1200

            started = time.perf_counter()
            chat = client.post(
                "/v1/chat",
                json={"alias": "mock", "messages": [{"role": "user", "content": "latency check"}]},
            )
            elapsed = time.perf_counter() - started

            assert chat.status_code == 200
            assert elapsed >= 1.0
        finally:
            reset = client.post(
                "/api/reliability/inject-latency",
                headers=ADMIN,
                json={"provider": "mock", "latency_ms": 0},
            )
            assert reset.status_code == 200

    assert INJECTED_LATENCY_MS["mock"] == 0


def test_inject_latency_requires_admin_and_known_provider():
    with TestClient(app) as client:
        assert client.post("/api/reliability/inject-latency", json={"provider": "mock", "latency_ms": 100}).status_code == 403
        unknown = client.post(
            "/api/reliability/inject-latency",
            headers=ADMIN,
            json={"provider": "nope", "latency_ms": 100},
        )
        assert unknown.status_code == 400


def test_redis_chaos_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("GATEWAY_ALLOW_CHAOS", raising=False)
    with TestClient(app) as client:
        response = client.post("/api/reliability/redis", headers=ADMIN, json={"action": "down"})

    assert response.status_code == 403
    assert "GATEWAY_ALLOW_CHAOS" in response.json()["detail"]


def test_chaos_panel_is_rendered_in_visualizer():
    assert 'id="vizChaosBar"' in HTML_CONTENT
    assert "chaosToggleProvider()" in HTML_CONTENT
    assert "chaosInjectLatency()" in HTML_CONTENT
    assert "chaosRedis('down')" in HTML_CONTENT
    assert "chaosRedis('up')" in HTML_CONTENT
