"""Optional OpenTelemetry tracing with W3C context propagation."""

import logging
import os
from contextlib import contextmanager, nullcontext
from typing import Any, Dict, Optional

logger = logging.getLogger("gateway.tracing")
_tracer = None
_provider = None
_enabled = False


def _sanitize_server_request_span(span, scope) -> None:
    # Keep route names but do not export raw paths, query strings, or URLs,
    # which may contain account identifiers or credentials.
    for attribute in ("http.target", "http.url", "url.full", "url.path", "url.query"):
        span.set_attribute(attribute, "[REDACTED]")


def configure_tracing(app, span_exporter=None) -> bool:
    global _tracer, _provider, _enabled
    if os.getenv("OTEL_ENABLED", "false").strip().lower() not in ("1", "true", "yes", "on"):
        return False
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    if not endpoint:
        base_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        endpoint = f"{base_endpoint.rstrip('/')}/v1/traces" if base_endpoint else None
    if not endpoint:
        logger.warning("OpenTelemetry is enabled but no OTLP endpoint is configured; tracing remains disabled")
        return False
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.propagate import set_global_textmap
        from opentelemetry.trace import Status, StatusCode
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace import Event as ReadableEvent
        from opentelemetry.sdk.trace import ReadableSpan
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.export import SpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        _provider = TracerProvider(resource=Resource.create({
            "service.name": os.getenv("OTEL_SERVICE_NAME", "llm-gateway"),
        }))
        exporter = span_exporter or OTLPSpanExporter(endpoint=endpoint)

        class RedactingSpanExporter(SpanExporter):
            def export(self, spans):
                safe_spans = []
                for item in spans:
                    safe_events = []
                    for event in item.events:
                        if event.name == "exception":
                            event_attributes = {
                                key: value for key, value in event.attributes.items()
                                if key == "exception.type"
                            }
                            safe_events.append(ReadableEvent(event.name, event_attributes, event.timestamp))
                        else:
                            safe_events.append(event)
                    status = item.status
                    if status.status_code == StatusCode.ERROR and status.description:
                        status = Status(status.status_code)
                    safe_spans.append(ReadableSpan(
                        name=item.name,
                        context=item.context,
                        parent=item.parent,
                        resource=item.resource,
                        attributes=dict(item.attributes),
                        events=safe_events,
                        links=item.links,
                        kind=item.kind,
                        instrumentation_info=item.instrumentation_info,
                        instrumentation_scope=item.instrumentation_scope,
                        status=status,
                        start_time=item.start_time,
                        end_time=item.end_time,
                    ))
                return exporter.export(safe_spans)

            def shutdown(self):
                exporter.shutdown()

            def force_flush(self, timeout_millis=30000):
                force_flush = getattr(exporter, "force_flush", None)
                return force_flush(timeout_millis) if force_flush else True

        _provider.add_span_processor(BatchSpanProcessor(RedactingSpanExporter()))
        trace.set_tracer_provider(_provider)
        _tracer = _provider.get_tracer("llm_gateway")
        set_global_textmap(TraceContextTextMapPropagator())
        FastAPIInstrumentor.instrument_app(
            app,
            excluded_urls="/metrics",
            server_request_hook=_sanitize_server_request_span,
            tracer_provider=_provider,
            http_capture_headers_sanitize_fields=[".*"],
        )
        _enabled = True
        logger.info("OpenTelemetry tracing enabled service=%s", os.getenv("OTEL_SERVICE_NAME", "llm-gateway"))
        return True
    except Exception as exc:
        if _provider is not None:
            try:
                _provider.shutdown()
            except Exception:
                pass
        _provider = None
        _tracer = None
        _enabled = False
        logger.error("OpenTelemetry tracing could not be configured (%s)", type(exc).__name__)
        return False


@contextmanager
def span(name: str, attributes: Optional[Dict[str, Any]] = None, context=None):
    if not _enabled or _tracer is None:
        with nullcontext():
            yield None
        return
    from opentelemetry.trace import Status, StatusCode

    with _tracer.start_as_current_span(
        name, context=context, record_exception=False, set_status_on_exception=False
    ) as current:
        for key, value in (attributes or {}).items():
            if value is not None and isinstance(value, (str, bool, int, float)):
                current.set_attribute(key, value)
        try:
            yield current
        except Exception as exc:
            current.add_event("exception", {"exception.type": type(exc).__name__})
            current.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            raise


def inject_trace_context() -> Dict[str, str]:
    if not _enabled:
        return {}
    from opentelemetry import propagate

    carrier: Dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def extract_trace_context(carrier: Optional[Dict[str, str]] = None):
    if not _enabled or not carrier:
        return None
    from opentelemetry import propagate

    return propagate.extract(carrier)


def shutdown_tracing() -> None:
    global _tracer, _provider, _enabled
    if _provider is not None:
        try:
            _provider.shutdown()
        except Exception as exc:
            logger.warning("OpenTelemetry shutdown failed (%s)", type(exc).__name__)
    _tracer = None
    _provider = None
    _enabled = False
