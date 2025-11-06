# SQLSpec Observability Runtime

This guide explains how the consolidated observability stack works after the Lifecycle Dispatcher + Statement Observer integration. Use it as the single source of truth when wiring new adapters, features, or docs.

## Goals

1. **Unified Hooks** – every pool, connection, session, and query event is emitted through one dispatcher with zero work when no listeners exist.
2. **Structured Statement Events** – observers receive normalized payloads (`StatementEvent`) for printing, logging, or exporting to tracing systems.
3. **Optional OpenTelemetry Spans** – span creation is lazy and never imports `opentelemetry` unless spans are enabled.
4. **Diagnostics** – storage bridge + serializer metrics + lifecycle counters roll up under `SQLSpec.telemetry_snapshot()` (Phase 5).

## Configuration Sources

There are three ways to enable observability today:

1. **Registry-Level** – pass `observability_config=ObservabilityConfig(...)` to `SQLSpec()`.
2. **Adapter Override** – each config constructor accepts `observability_config=` for adapter-specific knobs.
3. **`driver_features` Compatibility** – existing keys such as `"on_connection_create"`, `"on_pool_destroy"`, and `"on_session_start"` are automatically promoted into lifecycle observers, so user-facing APIs do **not** change.

```python
from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

def ensure_extensions(connection):
    connection.execute("INSTALL http_client; LOAD http_client;")

config = DuckDBConfig(
    pool_config={"database": ":memory:"},
    driver_features={
        "extensions": [{"name": "http_client"}],
        "on_connection_create": ensure_extensions,  # promoted to observability runtime
    },
)

sql = SQLSpec(observability_config=ObservabilityConfig(print_sql=True))
sql.add_config(config)
```

> **Implementation note:** During config initialization we inspect `driver_features` for known hook keys and wrap them into `ObservabilityConfig` callbacks. Hooks that accepted a raw resource (e.g., connection) continue to do so without additional adapter plumbing.

## Lifecycle Events

The dispatcher exposes the following events (all opt-in and guard-checked):

| Event | Context contents |
| --- | --- |
| `on_pool_create` / `on_pool_destroy` | `pool`, `config`, `bind_key`, `correlation_id` |
| `on_connection_create` / `on_connection_destroy` | `connection`, plus base context |
| `on_session_start` / `on_session_end` | `session` / driver instance |
| `on_query_start` / `on_query_complete` | SQL text, parameters, metadata |
| `on_error` | `exception` plus last query context |

`SQLSpec.provide_connection()` and `SQLSpec.provide_session()` now emit these events automatically, regardless of whether the caller uses registry helpers or adapter helpers directly.

## Statement Observers & Print SQL

Statement observers receive `StatementEvent` objects. Typical uses:

* enable `print_sql=True` to attach the built-in logger.
* add custom redaction rules via `RedactionConfig` (mask parameters, mask literals, allow-list names).
* forward events to bespoke loggers or telemetry exporters.

```python
def log_statement(event: StatementEvent) -> None:
    logger.info("%s (%s) -> %ss", event.operation, event.driver, event.duration_s)

ObservabilityConfig(
    print_sql=False,
    statement_observers=(log_statement,),
    redaction=RedactionConfig(mask_parameters=True, parameter_allow_list=("tenant_id",)),
)
```

### Optional Exporters (OpenTelemetry & Prometheus)

Two helper modules wire optional dependencies into the runtime without forcing unconditional imports:

* `sqlspec.extensions.otel.enable_tracing()` ensures `opentelemetry-api` is installed, then returns an `ObservabilityConfig` whose `TelemetryConfig` enables spans and (optionally) injects a tracer provider factory.
* `sqlspec.extensions.prometheus.enable_metrics()` ensures `prometheus-client` is installed and appends a `PrometheusStatementObserver` that emits counters and histograms for every `StatementEvent`.

Both helpers rely on the conditional stubs defined in `sqlspec/typing.py`, so they remain safe to import even when the extras are absent.

```python
from sqlspec.extensions import otel, prometheus

config = otel.enable_tracing(resource_attributes={"service.name": "orders-api"})
config = prometheus.enable_metrics(base_config=config, label_names=("driver", "operation", "adapter"))
sql = SQLSpec(observability_config=config)
```

You can also opt in per adapter by passing `extension_config["otel"]` or `extension_config["prometheus"]` when constructing a config; the helpers above are invoked automatically during initialization.

## Span Manager & Diagnostics (Roadmap)

* **Span Manager:** Query spans ship today, lifecycle events emit `sqlspec.lifecycle.*` spans, and storage bridge helpers now wrap reads/writes with `sqlspec.storage.*` spans (see `StorageDriverMixin`). Mocked span tests live in `tests/unit/test_observability.py`.
* **Diagnostics:** `TelemetryDiagnostics` aggregates lifecycle counters plus storage bridge metrics. Storage telemetry now carries backend IDs, bind key, and correlation IDs so snapshots/spans inherit the same context, and `SQLSpec.telemetry_snapshot()` exposes that data via flat counters (e.g., `storage_bridge.bytes_written`, `serializer.hits`) plus a `storage_bridge.recent_jobs` list detailing the last 25 storage jobs.

Example snapshot payload:

```
{
  "storage_bridge.bytes_written": 2048,
  "storage_bridge.recent_jobs": [
    {
      "destination": "alias://warehouse/users.parquet",
      "backend": "s3",
      "bytes_processed": 2048,
      "rows_processed": 16,
      "config": "AsyncpgConfig",
      "bind_key": "analytics",
      "correlation_id": "8f64c0f6",
      "format": "parquet"
    }
  ],
  "serializer.hits": 12,
  "serializer.misses": 2,
  "AsyncpgConfig.lifecycle.query_start": 4
}
```

## Next Steps (2025 Q4)

1. **Docs & Samples:** Expand runnable examples covering span enablement, Litestar correlation middleware, and storage telemetry (`storage_bridge.recent_jobs`) once diagnostics are finalized.
2. **Adapter Migration:** Audit remaining adapters for bespoke callbacks (e.g., `on_statement_execute`) and move them to ObservabilityConfig overrides.
3. **Performance Budgets:** Add guard-path benchmarks/tests to ensure disabled observability remains near-zero overhead after diagnostics wiring.
