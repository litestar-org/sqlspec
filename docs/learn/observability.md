# Observability

SQLSpec ships an opt-in observability pipeline (`sqlspec.observability`) with zero hot-path overhead when idle and fast-path preservation when active. Instrument queries with custom `StatementObserver` callbacks, deterministic sampling, OpenTelemetry spans, Prometheus metrics, structured cloud log formatters, correlation IDs, wire-level `sqlcommenter` tags, and DuckDB statement profiling.

## Custom Statement Observers

Register one or more `StatementObserver` functions on `ObservabilityConfig` to inspect `StatementEvent` records (`sql`, `operation`, `driver`, `duration_s`, `rows_affected`, `correlation_id`, `profile`):

```{ .python title="docs/examples/patterns/observability/custom_observer.py" hl_lines="14 17 18" }
--8<-- "docs/examples/patterns/observability/custom_observer.py"
```

### `StatementEvent` Fields

| Field | Type | Description |
| --- | --- | --- |
| `sql` | `str` | Executed SQL text (with literals redacted when `RedactionConfig.mask_literals` is enabled). |
| `parameters` | `Any` | Bound parameters (masked automatically when `RedactionConfig.mask_parameters` is enabled). |
| `driver` | `str` | Adapter driver class name (such as `"SqliteDriver"` or `"AsyncpgDriver"`). |
| `adapter` | `str` | Adapter identifier (such as `"sqlite"`, `"asyncpg"`, or `"oracledb"`). |
| `bind_key` | `str \| None` | Optional `bind_key` assigned on the owning database configuration. |
| `operation` | `str` | Normalized SQL operation label (`"SELECT"`, `"INSERT"`, `"UPDATE"`, `"DELETE"`, `"DDL"`). |
| `execution_mode` | `str` | Driver execution path (such as `"single"`, `"many"`, `"script"`, or `"stream"`). |
| `is_many` / `is_script` | `bool` | Flags indicating batched `execute_many` or multi-statement `execute_script` execution. |
| `rows_affected` | `int \| None` | Driver-reported affected or fetched row count when available. |
| `duration_s` | `float` | Wall-clock driver execution latency in seconds. |
| `correlation_id` | `str \| None` | Active `CorrelationContext` identifier bound for the current request or task. |
| `storage_backend` | `str \| None` | Storage backend name when emitted during `select_to_storage` or `load_from_storage` jobs. |
| `started_at` | `float` | Epoch timestamp when statement execution began. |
| `profile` | `dict[str, Any] \| None` | Parsed execution plan payload when DuckDB `enable_statement_profiling` is enabled. |
| `script_ordinal` / `script_statement_count` | `int \| None` | Statement index and total count when profiling multi-statement scripts. |

## Sampling and Parameter Redaction

High-throughput services can sample routine queries deterministically while always capturing failed statements (`force_sample_on_error=True`) and masking sensitive bind parameters via `RedactionConfig`:

```{ .python title="docs/examples/patterns/observability/sampling_config.py" hl_lines="10 11" }
--8<-- "docs/examples/patterns/observability/sampling_config.py"
```

```python
from sqlspec.observability import ObservabilityConfig, RedactionConfig

observability = ObservabilityConfig(
    redaction=RedactionConfig(mask_parameters=True, mask_literals=False, parameter_allow_list=("tenant_id",))
)
```

## OpenTelemetry, Prometheus, and Cloud Log Formatters

Enable OpenTelemetry spans (`uv add "sqlspec[opentelemetry]"`) and Prometheus counters/histograms (`uv add "sqlspec[prometheus]"`) declaratively through `extension_config` on any adapter config, or format structured logs for Google Cloud Logging and AWS CloudWatch:

```python
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    connection_config={"database": ":memory:"},
    extension_config={
        "otel": {"enable_spans": True},
        "prometheus": {"enabled": True, "namespace": "app", "subsystem": "db"},
    },
)
```

```{ .python title="docs/examples/patterns/observability/cloud_formatters.py" hl_lines="10 11" }
--8<-- "docs/examples/patterns/observability/cloud_formatters.py"
```

## Request Correlation and Wire-Level `sqlcommenter`

Bind request IDs with `CorrelationContext` (or enable `enable_correlation_middleware` in Litestar) so logs, spans, and database slow-query logs share the same trace context:

```{ .python title="docs/examples/patterns/observability/correlation_middleware.py" hl_lines="16 20 21" }
--8<-- "docs/examples/patterns/observability/correlation_middleware.py"
```

Setting `StatementConfig(enable_sqlcommenter=True)` appends W3C `traceparent` and `CorrelationContext` tags at wire dispatch time (`append_wire_sqlcomment`), keeping compiled SQL cache keys stable across requests so statement caching stays active. On Oracle (`python-oracledb`), `driver_features={"enable_session_tracing": True}` piggybacks `module`, `action`, `client_identifier`, and `econtext_id` onto the next TTC packet with zero extra round-trips.

## DuckDB Statement Profiling

On `DuckDBConfig` and DuckDB-backed `AdbcConfig`, setting `driver_features={"enable_statement_profiling": True}` captures per-statement JSON execution profiles onto `StatementEvent.profile` (along with `script_ordinal` and `script_statement_count` for multi-statement scripts).

## Recap

- Attach `ObservabilityConfig` to `SQLSpec(...)` or individual adapter configs for uniform telemetry across sync and async drivers.
- Combine `SamplingConfig(force_sample_on_error=True)` with `RedactionConfig(mask_parameters=True)` in production.
- Enable `enable_sqlcommenter=True` to correlate slow database queries back to application traces without sacrificing statement cache hit rates.
