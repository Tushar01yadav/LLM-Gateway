# OpenTelemetry tracing

Tracing is opt-in. Set `OTEL_ENABLED=true` and configure
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) for an
OTLP HTTP/protobuf collector. With no endpoint, tracing remains disabled.
FastAPI request spans exclude `/metrics`; manual spans cover async job provider
execution and provider requests. Async jobs carry W3C `traceparent` context in
their persisted payload, allowing the worker span to continue the submitting
request trace.

Provider and job spans contain only operation/provider/model attributes. URL
attributes and captured HTTP headers are redacted. Exception events retain the
exception type but remove messages and stack traces before export. The OTLP
batch processor exports asynchronously; collector failures are logged and do
not synchronously fail request handling. Its queue is bounded by the OpenTelemetry
SDK. Spans can be dropped while the collector is unavailable.

Redis-backed rate-limit/cache behavior and local degraded fallbacks are
documented in `degraded-mode.md`. Tracing is not a substitute for distributed
rate-limit or concurrency coordination.
