"""Guards the dashboard's inline JavaScript against the Python-string newline bug.

The dashboard ships inside a Python triple-quoted string. A literal ``\n`` written
as ``\n`` in that string becomes a real newline in the browser, which once broke
every dashboard function without any Python test noticing. ``node --check`` on the
extracted scripts catches that class of regression.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from app.main import HTML_CONTENT

INLINE_SCRIPT_RE = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)


def _inline_scripts() -> list[str]:
    return [s for s in INLINE_SCRIPT_RE.findall(HTML_CONTENT) if s.strip()]


def test_dashboard_has_inline_scripts():
    scripts = _inline_scripts()
    assert scripts, "dashboard HTML should contain at least one inline <script> block"
    assert max(len(s) for s in scripts) > 100_000, "main dashboard script looks truncated"


def test_dashboard_inline_js_syntax(tmp_path: Path):
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed; cannot run --check")

    scripts = _inline_scripts()
    assert scripts

    failures = []
    for index, source in enumerate(scripts):
        script_path = tmp_path / f"dashboard_{index}.js"
        script_path.write_text(source, encoding="utf-8")
        result = subprocess.run(
            [node, "--check", str(script_path)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            failures.append(f"script #{index}: {result.stderr.strip()[:800]}")

    assert not failures, "dashboard JS syntax errors:\n" + "\n".join(failures)


def test_dashboard_markdown_uses_sanitizer():
    """No raw marked.parse() output may reach innerHTML directly."""
    assert "DOMPurify.sanitize" in HTML_CONTENT
    assert "function renderMarkdown(" in HTML_CONTENT
    assert "marked.parse" in HTML_CONTENT
    # Only the helper is allowed to call marked.parse.
    assert HTML_CONTENT.count("marked.parse") == 1
