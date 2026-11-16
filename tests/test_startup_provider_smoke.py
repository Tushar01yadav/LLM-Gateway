import asyncio
from unittest.mock import AsyncMock

import pytest

import app.main as gateway_main


@pytest.mark.asyncio
async def test_ci_startup_does_not_require_or_call_external_providers(monkeypatch):
    for name in ("GROQ_API_KEY", "OPENROUTER_API_KEY", "GEMINI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GATEWAY_STARTUP_PROVIDER_SMOKE_TEST", "false")

    monkeypatch.setattr(gateway_main, "check_deploy_config", lambda: None)
    monkeypatch.setattr(gateway_main, "check_startup_keys", lambda: None)
    monkeypatch.setattr(gateway_main, "init_db", lambda: None)
    monkeypatch.setattr(gateway_main, "init_node_metrics_from_db", lambda: None)
    monkeypatch.setattr(gateway_main.JOB_MANAGER, "start_worker", AsyncMock())
    monkeypatch.setattr(gateway_main.JOB_MANAGER, "stop_worker", AsyncMock())
    smoke_test = AsyncMock()
    monkeypatch.setattr(gateway_main, "run_startup_smoke_test", smoke_test)
    create_task = AsyncMock()
    monkeypatch.setattr(asyncio, "create_task", create_task)

    async with gateway_main.lifespan(gateway_main.app):
        assert gateway_main.startup_provider_smoke_test_enabled() is False
        smoke_test.assert_not_awaited()
        create_task.assert_not_awaited()

    smoke_test.assert_not_awaited()
