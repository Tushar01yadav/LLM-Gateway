import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.security import create_gateway_api_key


@pytest.mark.asyncio
async def test_stats_exposes_real_latency_percentiles():
    raw_key, _ = create_gateway_api_key("percentile_api_client", quota=200, daily_budget=50.0)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", timeout=30.0) as ac:
        headers = {"Authorization": f"Bearer {raw_key}"}
        for i in range(5):
            res = await ac.post(
                "/v1/chat",
                json={"alias": "mock", "messages": [{"role": "user", "content": f"percentile sample {i}"}]},
                headers=headers,
            )
            assert res.status_code == 200

        stats = await ac.get("/api/stats")
        assert stats.status_code == 200
        body = stats.json()
        assert "latency_percentiles" in body
        snapshot = body["latency_percentiles"]
        assert snapshot["sample_size"] >= 5
        assert snapshot["window_seconds"] == 3600

        mock = snapshot["by_provider"].get("mock")
        assert mock is not None, snapshot["by_provider"]
        assert mock["count"] >= 5
        assert 0 < mock["p50"] <= mock["p95"] <= mock["p99"] <= mock["max"]
        assert mock["p99"] >= 400  # the mock adapter sleeps 500 ms

        assert "/v1/chat" in snapshot["by_endpoint"]
        assert snapshot["by_endpoint"]["/v1/chat"]["count"] >= 5

        listed = await ac.get("/api/reliability/provider-nodes", headers={"X-Admin-Token": "test-admin-token"})
        assert listed.status_code == 200
        nodes = {n["provider"]: n for n in listed.json()}
        # provider-nodes only lists real providers; percentiles must still be a dict when absent.
        assert all(isinstance(n.get("percentiles"), dict) for n in nodes.values())
