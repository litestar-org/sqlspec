# Logging Format

SQLSpec emits structured log records with a static event name (the log
message) plus a stable set of context fields. The default statement logger
follows OpenTelemetry semantic conventions where possible, while other modules
use SQLSpec-specific names.

## Common Fields

These fields are always present in structured JSON output:

- `timestamp`: formatted timestamp string.
- `level`: log level name.
- `logger`: fully-qualified logger name (for example, `sqlspec.observability`).
- `message`: static event name, such as `db.query` or `migration.apply`.
- `module`, `function`, `line`: source location.

Additional fields are merged from the event context (`log_with_context` or
`default_statement_observer`).

## Statement Logging (db.query)

Statement events (`db.query`) include:

- `db.system`: database system name (for example, `postgresql`, `sqlite`).
- `db.operation`: SQL operation (SELECT, INSERT, etc.).
- `db.statement`: SQL preview (may be truncated).
- `db.statement.truncated`: `true` when truncation occurred.
- `db.statement.length`: original SQL length.
- `db.statement.preview_length`: preview length.
- `db.statement.hash`: stable 16-char hash when enabled.
- `duration_ms`: elapsed time in milliseconds.
- `rows_affected`: row count, when available.
- `sqlspec.driver`, `sqlspec.bind_key`, `sqlspec.transaction_state`,
  `sqlspec.prepared_statement`, `execution_mode`, `is_many`, `is_script`.

When parameters are summarized:

- `parameters_type`: `dict`, `list`, `tuple`, or concrete type name.
- `parameters_size`: count or `null`.

When debug logging is enabled:

- `parameters`: truncated parameter payload.
- `parameters_truncated`: `true` when truncation occurred.
- `batch_size`: number of executions for `execute_many`.

## Trace + Correlation

When trace context is available:

- `trace_id`: OpenTelemetry trace ID.
- `span_id`: OpenTelemetry span ID.

When correlation middleware is enabled:

- `correlation_id`: request-scoped correlation identifier.

## Configuration

Configure structured logging through `ObservabilityConfig`:

```python
from sqlspec.observability import ObservabilityConfig
from sqlspec.observability import LoggingConfig

ObservabilityConfig(
    logging=LoggingConfig(
        include_sql_hash=True,
        sql_truncation_length=2000,
        parameter_truncation_count=100,
        include_trace_context=True,
    )
)
```

## Console vs JSON Output

`OTelConsoleFormatter` produces a compact, human-readable line:

```
db.query db.system=postgresql db.operation=SELECT duration_ms=1.23 db.statement=SELECT 1
```

`StructuredFormatter` emits JSON:

```json
{
  "timestamp": "2026-01-09T12:00:00",
  "level": "INFO",
  "logger": "sqlspec.observability",
  "message": "db.query",
  "db.system": "postgresql",
  "db.operation": "SELECT",
  "db.statement": "SELECT 1",
  "duration_ms": 1.23,
  "correlation_id": "req-123",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7"
}
```

## Observability Stack Examples

### Datadog

- Parse `message` as the event name.
- Map `db.system`, `db.operation`, and `duration_ms` to tags/metrics.
- Correlate APM traces via `trace_id` and `span_id`.

### Grafana Loki

- Ingest JSON logs and query by `message="db.query"`.
- Group by `db.system` and `db.operation`.
- Filter slow queries with `duration_ms > 500`.
