import sys

import pytest

from app.security import check_deploy_config


def _set(monkeypatch, env, argv):
    monkeypatch.setenv("APP_ENV", env)
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.delenv("GATEWAY_PROXY_HEADERS_DISABLED", raising=False)


def test_prod_rejects_trusted_proxies_with_enabled_proxy_headers(monkeypatch):
    _set(monkeypatch, "production", ["uvicorn", "app.main:app", "--proxy-headers"])
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "1")

    with pytest.raises(RuntimeError, match="--no-proxy-headers"):
        check_deploy_config()


def test_prod_rejects_trusted_proxies_when_proxy_header_mode_unknown(monkeypatch):
    _set(monkeypatch, "production", ["gunicorn", "app.main:app"])
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "2")

    with pytest.raises(RuntimeError):
        check_deploy_config()


def test_prod_accepts_trusted_proxies_with_no_proxy_headers(monkeypatch):
    _set(monkeypatch, "production", ["uvicorn", "app.main:app", "--no-proxy-headers"])
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "1")

    result = check_deploy_config()

    assert result["errors"] == []
    assert result["trusted_proxy_count"] == 1
    assert result["proxy_headers"] is False


def test_non_cli_launcher_can_acknowledge_disabled_proxy_headers(monkeypatch):
    _set(monkeypatch, "production", ["my-launcher"])
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "1")
    monkeypatch.setenv("GATEWAY_PROXY_HEADERS_DISABLED", "1")

    result = check_deploy_config()

    assert result["errors"] == []
    assert result["proxy_headers"] is False


def test_dev_warns_instead_of_failing(monkeypatch):
    _set(monkeypatch, "development", ["uvicorn", "app.main:app"])
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "1")

    result = check_deploy_config()

    assert result["is_production"] is False
    assert len(result["errors"]) == 1


def test_prod_without_trusted_proxies_warns_about_forwarded_for(monkeypatch):
    _set(monkeypatch, "production", ["uvicorn", "app.main:app", "--no-proxy-headers"])
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "0")

    result = check_deploy_config()

    assert result["errors"] == []
    assert any("TRUSTED_PROXY_COUNT=0" in warning for warning in result["warnings"])
