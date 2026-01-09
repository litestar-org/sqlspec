# Telemetry

SQLSpec exposes structured logs, optional OpenTelemetry trace context, and a
lightweight runtime telemetry snapshot API.

## Structured Logging

All logging events use static event names and structured fields. See
[Logging Format](logging-format.md) for the schema and examples.

## Traces and Correlation

When OpenTelemetry is configured, statement logging enriches records with
`trace_id` and `span_id`. The Litestar correlation middleware adds a
`correlation_id` for request-level grouping.

Enable trace context capture:

```python
from sqlspec.observability import ObservabilityConfig
from sqlspec.observability import LoggingConfig

ObservabilityConfig(logging=LoggingConfig(include_trace_context=True))
```

## Telemetry Snapshot

`SQLSpec.telemetry_snapshot()` returns counters and recent storage job metrics.
Use it for quick diagnostics or to export lightweight health checks.
