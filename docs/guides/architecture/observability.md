# SQLSpec Observability Runtime

This guide explains how the consolidated observability stack works after the Lifecycle Dispatcher + Statement Observer integration. Use it as the single source of truth when wiring new adapters, features, or docs.

## Goals

1. **Unified Hooks** – every pool, connection, session, and query event is emitted through one dispatcher with zero work when no listeners exist.
2. **Structured Statement Events** – observers receive normalized payloads (`StatementEvent`) for printing, logging, or exporting to tracing systems.
3. **Optional OpenTelemetry Spans** – span creation is lazy and never imports `opentelemetry` unless spans are enabled.
4. **Diagnostics** – storage bridge + serializer metrics + lifecycle counters roll up under `SQLSpec.telemetry_snapshot()` (Phase 5).
5. **Loader & Migration Telemetry** – SQL file loader, caching, and migration runners emit metrics/spans without additional plumbing (Phase 7).

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
    connection_config={"database": ":memory:"},
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

## Loader & Migration Telemetry

`SQLSpec` instantiates a dedicated `ObservabilityRuntime` for the SQL file loader and shares it with every migration command/runner. Instrumentation highlights:

- Loader metrics such as `SQLFileLoader.loader.load.invocations`, `.cache.hit`, `.files.loaded`, `.statements.loaded`, and `.directories.scanned` fire automatically when queries are loaded or cache state is inspected.
- Migration runners publish cache stats (`{Config}.migrations.listing.cache_hit`, `.cache_miss`, `.metadata.cache_hit`), command metrics (`{Config}.migrations.command.upgrade.invocations`, `.downgrade.errors`), and per-migration execution metrics (`{Config}.migrations.upgrade.duration_ms`, `.downgrade.applied`).
- Command and migration spans (`sqlspec.migration.command.upgrade`, `sqlspec.migration.upgrade`) include version numbers, bind keys, and correlation IDs; they end with duration attributes even when exceptions occur.

All metrics surface through `SQLSpec.telemetry_snapshot()` under the adapter key, so exporters observe a flat counter space regardless of which subsystem produced the events.

## Span Manager & Diagnostics

* **Span Manager:** Query spans ship today, lifecycle events emit `sqlspec.lifecycle.*` spans, storage bridge helpers wrap reads/writes with `sqlspec.storage.*` spans, and migration runners create `sqlspec.migration.*` spans for both commands and individual revisions. Mocked span tests live in `tests/unit/test_observability.py`.
* **Diagnostics:** `TelemetryDiagnostics` aggregates lifecycle counters, loader/migration metrics, storage bridge telemetry, and serializer cache stats. Storage telemetry carries backend IDs, bind key, and correlation IDs so snapshots/spans inherit the same context, and `SQLSpec.telemetry_snapshot()` exposes that data via flat counters plus a `storage_bridge.recent_jobs` list detailing the last 25 operations.

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

1. **Exporter Validation:** Exercise the OpenTelemetry/Prometheus helpers against the new loader + migration metrics and document recommended dashboards.
2. **Adapter Audit:** Confirm every adapter’s migration tracker benefits from the instrumentation (especially Oracle/BigQuery fixtures) and extend coverage where needed.
3. **Performance Budgets:** Add guard-path benchmarks/tests to ensure disabled observability remains near-zero overhead now that migration/loader events emit metrics by default.
