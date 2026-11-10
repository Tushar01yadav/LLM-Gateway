import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import app.tracing as tracing
import app.main as gateway_main


class UnavailableCollectorExporter(InMemorySpanExporter):
    def __init__(self):
        super().__init__()
        self.attempts = 0

    def export(self, spans):
        self.attempts += 1
        super().export(spans)
        return SpanExportResult.FAILURE


@pytest.mark.asyncio
async def test_tracing_disabled_and_enabled_propagation_are_safe(monkeypatch):
    app = FastAPI()
    monkeypatch.setenv("OTEL_ENABLED", "false")
    assert tracing.configure_tracing(app) is False
    assert tracing.inject_trace_context() == {}
    with tracing.span("disabled") as current:
        assert current is None

    exporter = UnavailableCollectorExporter()
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://collector.invalid/v1/traces")
    monkeypatch.setenv("OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST", "Authorization")
    assert tracing.configure_tracing(app, span_exporter=exporter) is True

    @app.get("/trace-check")
    async def trace_check():
        return {"ok": True}

    @app.get("/trace-failure")
    async def trace_failure():
        raise RuntimeError("private provider response body")

    try:
        enqueued = {}

        async def capture_job(**kwargs):
            enqueued.update(kwargs)
            return {"job_id": "trace-job"}

        monkeypatch.setattr(
            gateway_main, "validate_gateway_api_key",
            lambda key: {"id": "trace-tenant", "name": "trace-tenant"},
        )
        monkeypatch.setattr(gateway_main.JOB_MANAGER, "enqueue_job", capture_job)
        request_http = Request({
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/v1/jobs",
            "raw_path": b"/v1/jobs",
            "query_string": b"",
            "headers": [(b"authorization", b"Bearer test-api-key")],
            "server": ("test", 80),
            "client": ("test", 1000),
            "root_path": "",
            "app": app,
        })
        with tracing.span("request.root"):
            job_response = await gateway_main.create_async_job(
                gateway_main.CreateJobPayload(messages=[{"role": "user", "content": "question"}]),
                request_http,
            )
            carrier = enqueued["payload"]["_trace_context"]
            assert "traceparent" in carrier
        assert job_response == {"job_id": "trace-job"}
        with tracing.span("async_job.provider_execution", context=tracing.extract_trace_context(carrier)):
            with pytest.raises(RuntimeError):
                with tracing.span("provider.request", {"provider": "test"}):
                    raise RuntimeError("sensitive prompt must not be recorded")

        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
        ) as client:
            response = await client.get("/trace-check?access_token=private", headers={"Authorization": "Bearer private"})
            failure_response = await client.get("/trace-failure")
        assert response.status_code == 200
        assert failure_response.status_code == 500
        assert tracing._provider.force_flush(timeout_millis=3000) is not False
        assert exporter.attempts > 0

        spans = exporter.get_finished_spans()
        root = next(item for item in spans if item.name == "request.root")
        job = next(item for item in spans if item.name == "async_job.provider_execution")
        provider = next(item for item in spans if item.name == "provider.request")
        assert job.context.trace_id == root.context.trace_id
        assert job.parent.span_id == root.context.span_id
        assert provider.parent.span_id == job.context.span_id
        exception_event = next(event for event in provider.events if event.name == "exception")
        assert exception_event.attributes == {"exception.type": "RuntimeError"}
        assert all("private" not in str(span.attributes) for span in spans)
        assert any(item.name == "GET /trace-check" for item in spans)
        server_span = next(item for item in spans if item.name == "GET /trace-check")
        assert server_span.attributes.get("http.url") == "[REDACTED]"
        assert server_span.attributes.get("http.target") == "[REDACTED]"
        assert server_span.attributes.get("http.request.header.authorization") == ("[REDACTED]",)
        assert all("private" not in str(item.attributes) for item in spans)
        assert all(
            "private" not in str(event.attributes)
            for item in spans for event in item.events
        )
    finally:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.uninstrument_app(app)
        tracing.shutdown_tracing()
