# Observability

```python
from sqlspec.observability import (
    AWSLogFormatter,
    AzureLogFormatter,
    CloudLogFormatter,
    GCPLogFormatter,
    LifecycleContext,
    LifecycleDispatcher,
    LoggingConfig,
    ObservabilityConfig,
    ObservabilityRuntime,
    OTelConsoleFormatter,
    OTelJSONFormatter,
    RedactionConfig,
    SamplingConfig,
    SpanManager,
    StatementEvent,
    TelemetryConfig,
    TelemetryDiagnostics,
    create_statement_observer,
    default_statement_observer,
    format_statement_event,
)
```

Structured statement observers, parameter redaction, OpenTelemetry span management, lifecycle hooks, and cloud-native log formatters (`GCP`, `AWS`, `Azure`, `OTel`). See [Observability](../learn/observability.md) for setup guides.

## Configuration & Runtime

::: sqlspec.observability._config.ObservabilityConfig
    options:
      heading_level: 3
      members:
        - copy
        - merge

::: sqlspec.observability._config.TelemetryConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.observability._config.LoggingConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.observability._config.RedactionConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.observability._sampling.SamplingConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.observability._runtime.ObservabilityRuntime
    options:
      heading_level: 3
      members:
        - emit_statement_event
        - start_span
        - increment_metric
        - diagnostics_snapshot

## Statement Events & Observers

::: sqlspec.observability._observer.StatementEvent
    options:
      heading_level: 3
      members: false

::: sqlspec.observability._observer.create_event
    options:
      heading_level: 4

::: sqlspec.observability._observer.create_statement_observer
    options:
      heading_level: 4

::: sqlspec.observability._observer.default_statement_observer
    options:
      heading_level: 4

::: sqlspec.observability._observer.format_statement_event
    options:
      heading_level: 4

## Spans, Lifecycle & Diagnostics

::: sqlspec.observability._spans.SpanManager
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._dispatcher.LifecycleDispatcher
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._dispatcher.LifecycleContext
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._diagnostics.TelemetryDiagnostics
    options:
      heading_level: 4
      members: false

## Cloud & OpenTelemetry Log Formatters

::: sqlspec.observability._formatters.CloudLogFormatter
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._formatters.GCPLogFormatter
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._formatters.AWSLogFormatter
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._formatters.AzureLogFormatter
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._formatting.OTelJSONFormatter
    options:
      heading_level: 4
      members: false

::: sqlspec.observability._formatting.OTelConsoleFormatter
    options:
      heading_level: 4
      members: false
