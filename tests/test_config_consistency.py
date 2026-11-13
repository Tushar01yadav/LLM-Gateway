"""Tests for production documentation and configuration consistency (M6)."""

import json
import os
from pathlib import Path

import pytest

from app.cost import calculate_request_cost, get_models_config
from app.main import startup_provider_smoke_test_enabled
from app.reliability import (
    ADAPTER_MAP,
    load_models_config,
    resolve_fallback_chain,
    should_count_circuit_failure,
)
from app.security import rate_limit_fail_closed

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_models_json_schema_and_adapter_coverage():
    """Verify models.json structure, pricing bounds, and adapter mappings."""
    models_path = REPO_ROOT / "models.json"
    assert models_path.exists(), "models.json must exist in repository root"

    with open(models_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Required top-level configuration blocks
    for block in ("circuit_breakers", "guardrails", "rate_limiting", "cost_control", "concurrency"):
        assert block in cfg, f"Missing required configuration block: {block}"

    # Required active aliases
    for alias in ("fast", "smart", "cheap"):
        assert alias in cfg, f"Primary alias '{alias}' must be defined in models.json"
        data = cfg[alias]
        assert "provider" in data, f"Alias '{alias}' missing provider"
        assert "model" in data, f"Alias '{alias}' missing model"
        assert data["provider"] in ADAPTER_MAP, f"Provider '{data['provider']}' not in ADAPTER_MAP"

        assert float(data.get("input_price_per_token", 0)) > 0, f"Alias '{alias}' must have positive input price"
        assert float(data.get("output_price_per_token", 0)) > 0, f"Alias '{alias}' must have positive output price"
        assert int(data.get("context_limit", 0)) > 0, f"Alias '{alias}' must have positive context limit"

        # Check fallback targets
        fallback = data.get("fallback", [])
        assert isinstance(fallback, list) and len(fallback) > 0, f"Alias '{alias}' must have fallback targets"
        for fb in fallback:
            assert "provider" in fb and fb["provider"] in ADAPTER_MAP, f"Fallback provider '{fb.get('provider')}' invalid"
            assert "model" in fb and fb["model"], f"Fallback target in '{alias}' missing model name"

    # Deprecated alias 'complex' must NOT be present
    assert "complex" not in cfg, "'complex' alias was deprecated and must not appear in models.json"


def test_circuit_breaker_429_configuration_consistency():
    """Verify circuit breaker 429 counting matches models.json configuration."""
    cfg = load_models_config()
    cb_cfg = cfg.get("circuit_breakers", {})
    assert "count_429" in cb_cfg, "circuit_breakers must define count_429"
    assert isinstance(cb_cfg["count_429"], bool), "count_429 must be boolean"

    # Upstream 429 is retryable and should be counted per models.json config
    should_count = should_count_circuit_failure(429, True, "groq", cfg)
    assert should_count is cb_cfg["count_429"]


def test_rate_limit_fail_mode_precedence(monkeypatch):
    """Verify rate_limit_fail_closed precedence: env override > models.json."""
    cfg = get_models_config()
    default_fail_closed = cfg.get("rate_limiting", {}).get("redis_fail_closed", False)

    # When env var is unset, fallback to models.json
    monkeypatch.delenv("RATE_LIMIT_FAIL_MODE", raising=False)
    assert rate_limit_fail_closed() == default_fail_closed

    # When RATE_LIMIT_FAIL_MODE=closed, override to True
    monkeypatch.setenv("RATE_LIMIT_FAIL_MODE", "closed")
    assert rate_limit_fail_closed() is True

    # When RATE_LIMIT_FAIL_MODE=open, override to False
    monkeypatch.setenv("RATE_LIMIT_FAIL_MODE", "open")
    assert rate_limit_fail_closed() is False


def test_fallback_cost_calculation_uses_alias_rate():
    """Verify that requests routed via alias are charged the alias rate on fallback."""
    cfg = get_models_config()
    fast_cfg = cfg["fast"]
    fast_input_price = fast_cfg["input_price_per_token"]
    fast_output_price = fast_cfg["output_price_per_token"]

    chain = resolve_fallback_chain("fast")
    assert len(chain) >= 2
    fallback_target = chain[1]

    # Compute cost with alias='fast' but response model=fallback_target['model']
    cost = calculate_request_cost(
        alias="fast",
        model=fallback_target["model"],
        token_usage={"prompt_tokens": 1000, "completion_tokens": 500},
    )

    expected = (1000 * fast_input_price) + (500 * fast_output_price)
    assert abs(cost - expected) < 1e-9


def test_startup_provider_smoke_test_defaults_and_env(monkeypatch):
    """Verify startup provider smoke test defaults to False and respects env flag."""
    monkeypatch.delenv("GATEWAY_STARTUP_PROVIDER_SMOKE_TEST", raising=False)
    assert startup_provider_smoke_test_enabled() is False

    for truthy in ("true", "1", "yes", "on", "True", "YES"):
        monkeypatch.setenv("GATEWAY_STARTUP_PROVIDER_SMOKE_TEST", truthy)
        assert startup_provider_smoke_test_enabled() is True

    for falsy in ("false", "0", "no", "off", "anything_else"):
        monkeypatch.setenv("GATEWAY_STARTUP_PROVIDER_SMOKE_TEST", falsy)
        assert startup_provider_smoke_test_enabled() is False
