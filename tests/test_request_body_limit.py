import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from app.body_limit import RequestBodyLimitMiddleware
from app.main import app
from app.security import ADMIN_TOKEN, create_gateway_api_key, revoke_gateway_api_key

LIMIT = 1024


async def _echo_app(scope, receive, send):
    """Reads the whole body and reports how many bytes arrived."""
    total = 0
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            break
        total += len(message.get("body", b"") or b"")
        if not message.get("more_body", False):
            break
    payload = json.dumps({"bytes": total}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": payload, "more_body": False})


def _limited_app(limit=LIMIT):
    return RequestBodyLimitMiddleware(_echo_app, max_body_bytes=limit)


def _direct_client(limit=LIMIT):
    return AsyncClient(transport=ASGITransport(app=_limited_app(limit)), base_url="http://test")


def _http_scope(headers=None):
    return {
        "type": "http",
        "method": "POST",
        "path": "/echo",
        "raw_path": b"/echo",
        "query_string": b"",
        "headers": headers or [],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
        "scheme": "http",
        "http_version": "1.1",
    }


async def _drive_asgi(scope, messages):
    sent = []
    calls = {"receive": 0}
    queue = list(messages)

    async def send(message):
        sent.append(message)

    async def receive():
        calls["receive"] += 1
        if queue:
            return queue.pop(0)
        return {"type": "http.disconnect"}

    await RequestBodyLimitMiddleware(_echo_app, max_body_bytes=LIMIT)(scope, receive, send)
    return sent, calls


async def _chunks(chunk: bytes, count: int):
    for _ in range(count):
        yield chunk


async def _split_chunks(payload: bytes, size: int):
    for offset in range(0, len(payload), size):
        yield payload[offset:offset + size]


def _status(sent):
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


def _body(sent):
    return b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")


@pytest.mark.asyncio
async def test_content_length_over_limit_is_rejected_before_body_read():
    async with _direct_client() as ac:
        res = await ac.post("/echo", content=b"x" * (LIMIT + 1))
    assert res.status_code == 413
    assert res.json()["detail"] == (
        f"Payload size {LIMIT + 1} bytes exceeds maximum allowed limit of 1KB (OWASP LLM10)."
    )


@pytest.mark.asyncio
async def test_content_length_within_limit_passes():
    async with _direct_client() as ac:
        res = await ac.post("/echo", content=b"x" * LIMIT)
    assert res.status_code == 200
    assert res.json() == {"bytes": LIMIT}


@pytest.mark.asyncio
async def test_chunked_body_over_limit_is_rejected():
    async with _direct_client() as ac:
        res = await ac.post("/echo", content=_chunks(b"x" * 512, 3))
    assert res.status_code == 413
    assert "exceeds maximum allowed limit" in res.json()["detail"]


@pytest.mark.asyncio
async def test_chunked_body_within_limit_passes():
    async with _direct_client() as ac:
        res = await ac.post("/echo", content=_chunks(b"x" * 400, 2))
    assert res.status_code == 200
    assert res.json() == {"bytes": 800}


@pytest.mark.asyncio
async def test_missing_content_length_is_enforced_while_streaming():
    scope = _http_scope()
    sent, _ = await _drive_asgi(
        scope,
        [
            {"type": "http.request", "body": b"x" * 600, "more_body": True},
            {"type": "http.request", "body": b"x" * 600, "more_body": False},
        ],
    )
    assert _status(sent) == 413


@pytest.mark.asyncio
async def test_malformed_content_length_is_enforced_while_streaming():
    scope = _http_scope([(b"content-length", b"not-a-number")])
    sent, _ = await _drive_asgi(
        scope,
        [
            {"type": "http.request", "body": b"x" * 600, "more_body": True},
            {"type": "http.request", "body": b"x" * 600, "more_body": False},
        ],
    )
    assert _status(sent) == 413


@pytest.mark.asyncio
async def test_malformed_content_length_small_body_passes():
    scope = _http_scope([(b"content-length", b"not-a-number")])
    sent, _ = await _drive_asgi(
        scope,
        [{"type": "http.request", "body": b"x" * 10, "more_body": False}],
    )
    assert _status(sent) == 200
    assert json.loads(_body(sent)) == {"bytes": 10}


@pytest.mark.asyncio
async def test_stops_consuming_body_after_limit():
    scope = _http_scope()
    sent, calls = await _drive_asgi(
        scope,
        [
            {"type": "http.request", "body": b"x" * 512, "more_body": True},
            {"type": "http.request", "body": b"x" * 512, "more_body": True},
            {"type": "http.request", "body": b"x" * 512, "more_body": True},
            {"type": "http.request", "body": b"x" * 512, "more_body": False},
        ],
    )
    assert _status(sent) == 413
    assert calls["receive"] == 3  # the crossing chunk, and nothing after it


@pytest.mark.asyncio
async def test_http2_rejection_omits_connection_header():
    scope = _http_scope([(b"content-length", str(LIMIT + 1).encode())])
    scope["http_version"] = "2"
    sent, _ = await _drive_asgi(scope, [])
    start = next(m for m in sent if m["type"] == "http.response.start")
    assert start["status"] == 413
    assert all(name != b"connection" for name, _ in start["headers"])


@pytest.mark.asyncio
async def test_non_http_scopes_pass_through():
    calls = []

    async def lifespan_app(scope, receive, send):
        calls.append(scope["type"])

    async def receive():
        return {"type": "lifespan.startup"}

    async def send(message):
        return None

    middleware = RequestBodyLimitMiddleware(lifespan_app, max_body_bytes=LIMIT)
    await middleware({"type": "lifespan"}, receive, send)
    assert calls == ["lifespan"]


# ---------------------------------------------------------------------------
# Real application behaviour
# ---------------------------------------------------------------------------
@pytest.fixture
def api_key():
    raw, info = create_gateway_api_key(name="body_limit_client", quota=120, daily_budget=20.0)
    yield raw
    revoke_gateway_api_key(info["id"])


@pytest.mark.asyncio
async def test_oversized_unauthenticated_request_rejected_before_route_processing():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.post(
            "/v1/chat",
            content=b'{"alias": "mock", "messages": [{"role": "user", "content": "hi"}]}',
            headers={"Content-Type": "application/json", "Content-Length": "2097152"},
        )
    assert res.status_code == 413
    assert "exceeds maximum allowed limit of 1MB" in res.json()["detail"]
    assert "x-guest-id" not in res.headers  # route never processed the request


@pytest.mark.asyncio
async def test_oversized_chunked_unauthenticated_request_rejected():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.post(
            "/v1/chat",
            content=_chunks(b"x" * 524288, 3),
            headers={"Content-Type": "application/json"},
        )
    assert res.status_code == 413


@pytest.mark.asyncio
async def test_normal_json_request_still_succeeds(api_key):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.post(
            "/v1/chat",
            json={"alias": "mock", "messages": [{"role": "user", "content": "hello"}]},
            headers={"Authorization": f"Bearer {api_key}"},
        )
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_chunked_json_request_within_limit_still_succeeds(api_key):
    payload = json.dumps(
        {"alias": "mock", "messages": [{"role": "user", "content": "hello"}]}
    ).encode()
    third = len(payload) // 3 + 1
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.post(
            "/v1/chat",
            content=_split_chunks(payload, third),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_sse_stream_still_works():
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/reliability/traffic-stream",
        "raw_path": b"/api/reliability/traffic-stream",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"x-admin-token", ADMIN_TOKEN.encode())],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    }
    sent = []
    first_event = asyncio.Event()
    request_complete = False

    async def receive():
        nonlocal request_complete
        if not request_complete:
            request_complete = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        sent.append(message)
        if b"event: connected" in (message.get("body") or b""):
            first_event.set()

    task = asyncio.create_task(app(scope, receive, send))
    try:
        await asyncio.wait_for(first_event.wait(), timeout=5.0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    start = next(m for m in sent if m["type"] == "http.response.start")
    assert start["status"] == 200
    assert (b"content-type", b"text/event-stream; charset=utf-8") in start["headers"]
    assert b"event: connected" in b"".join(m.get("body", b"") for m in sent)
