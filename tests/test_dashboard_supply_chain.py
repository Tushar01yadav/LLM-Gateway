"""The dashboard must load vendored, hash-pinned scripts only."""
import base64
import hashlib
import re
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import HTML_CONTENT, app

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "app" / "static" / "vendor"

SCRIPT_RE = re.compile(r'<script\s+src="([^"]+)"([^>]*)></script>')


def _tags():
    return [(src, attrs) for src, attrs in SCRIPT_RE.findall(HTML_CONTENT)]


def _integrity(attrs: str):
    match = re.search(r'integrity="([^"]+)"', attrs)
    return match.group(1) if match else None


def test_dashboard_has_no_remote_script_sources():
    remote = [src for src, _ in _tags() if src.startswith(("http://", "https://", "//"))]
    assert remote == []


def test_every_vendored_script_matches_its_sri_hash():
    vendored = [(src, attrs) for src, attrs in _tags() if src.startswith("/static/vendor/")]
    assert {src for src, _ in vendored} == {
        "/static/vendor/tailwindcss-3.4.16.js",
        "/static/vendor/marked-12.0.2.min.js",
        "/static/vendor/dompurify-3.1.6.min.js",
    }
    for src, attrs in vendored:
        path = ROOT / "app" / src.lstrip("/")
        assert path.is_file(), src
        digest = base64.b64encode(hashlib.sha384(path.read_bytes()).digest()).decode()
        assert _integrity(attrs) == f"sha384-{digest}", src


@pytest.mark.asyncio
async def test_vendored_scripts_are_served():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for name in ("tailwindcss-3.4.16.js", "marked-12.0.2.min.js", "dompurify-3.1.6.min.js"):
            res = await ac.get(f"/static/vendor/{name}")
            assert res.status_code == 200
            assert res.content == (VENDOR / name).read_bytes()
