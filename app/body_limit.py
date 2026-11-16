"""ASGI middleware that caps request body size before a route can buffer it."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

ASGIReceive = Callable[[], Awaitable[dict]]
ASGISend = Callable[[dict], Awaitable[None]]
ASGIApp = Callable[[dict, ASGIReceive, ASGISend], Awaitable[None]]


class _BodyTooLarge(Exception):
    """Raised through the app when a streamed request body crosses the limit."""


def _human_limit(limit: int) -> str:
    if limit % (1024 * 1024) == 0:
        return f"{limit // (1024 * 1024)}MB"
    if limit % 1024 == 0:
        return f"{limit // 1024}KB"
    return f"{limit} bytes"


def payload_too_large_detail(byte_count: int, limit: int) -> str:
    return (
        f"Payload size {byte_count} bytes exceeds maximum allowed limit of "
        f"{_human_limit(limit)} (OWASP LLM10)."
    )


def _declared_content_length(scope: dict) -> Optional[int]:
    raw: Optional[bytes] = None
    for name, value in scope.get("headers") or []:
        if name.lower() != b"content-length":
            continue
        if raw is not None:
            return None  # ambiguous duplicate header: enforce while streaming
        raw = value
    if raw is None:
        return None
    try:
        declared = int(raw.strip())
    except (TypeError, ValueError):
        return None  # malformed header: enforce while streaming
    return declared if declared >= 0 else None


async def _send_too_large(send: ASGISend, byte_count: int, limit: int, http_version: str = "1.1") -> None:
    body = json.dumps({"detail": payload_too_large_detail(byte_count, limit)}).encode()
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ]
    if http_version != "2":
        # The unread body is abandoned; do not reuse this connection.
        # Connection-specific headers are forbidden in HTTP/2.
        headers.append((b"connection", b"close"))
    await send({"type": "http.response.start", "status": 413, "headers": headers})
    await send({"type": "http.response.body", "body": body, "more_body": False})


class RequestBodyLimitMiddleware:
    """Reject oversized HTTP bodies without buffering them.

    Content-Length is checked before the app runs; http.request chunks are
    counted as they stream, so chunked and malformed-header requests are
    bounded too. Once the limit is crossed the remaining body is never read.
    """

    def __init__(self, app: ASGIApp, max_body_bytes: Optional[int] = None) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    def _limit(self) -> Optional[int]:
        if self.max_body_bytes is None:
            return None
        try:
            limit = int(self.max_body_bytes)
        except (TypeError, ValueError):
            return None
        return limit if limit > 0 else None

    async def __call__(self, scope: dict, receive: ASGIReceive, send: ASGISend) -> None:
        limit = self._limit()
        if scope.get("type") != "http" or limit is None:
            await self.app(scope, receive, send)
            return

        declared = _declared_content_length(scope)
        if declared is not None and declared > limit:
            await _send_too_large(send, declared, limit, scope.get("http_version", "1.1"))
            return

        received = 0
        exceeded = False
        response_started = False

        async def limited_receive() -> dict:
            nonlocal received, exceeded
            if exceeded:
                return {"type": "http.disconnect"}
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b"") or b"")
                if received > limit:
                    exceeded = True
                    raise _BodyTooLarge()
            return message

        async def tracked_send(message: dict) -> None:
            nonlocal response_started
            if exceeded:
                # Frameworks may convert the receive error into their own
                # response (FastAPI turns it into a 400); drop it so the 413 wins.
                if response_started:
                    await send(message)
                return
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _BodyTooLarge:
            pass
        except asyncio.CancelledError:
            raise
        except BaseException:
            # Keep an overflow rejection even if a framework wraps the receive
            # error in another exception type (for example an ExceptionGroup).
            if not exceeded:
                raise

        if not exceeded:
            return
        if not response_started:
            await _send_too_large(send, received, limit, scope.get("http_version", "1.1"))
        else:
            logger.warning(
                "Request body exceeded %s bytes after the response started; "
                "leaving the connection for the server to close",
                limit,
            )
