# Observability

SQLSpec ships an opt-in observability pipeline (`sqlspec.observability`) with zero hot-path overhead when idle and fast-path preservation when active. Instrument queries with custom `StatementObserver` callbacks, deterministic sampling, OpenTelemetry spans, Prometheus metrics, structured cloud log formatters, correlation IDs, wire-level `sqlcommenter` tags, and DuckDB statement profiling.

## Custom Statement Observers

Register one or more `StatementObserver` functions on `ObservabilityConfig` to inspect `StatementEvent` records (`sql`, `operation`, `driver`, `duration_s`, `rows_affected`, `correlation_id`, `profile`):

```{ .python title="docs/examples/patterns/observability/custom_observer.py" }
--8<-- "docs/examples/patterns/observability/custom_observer.py:example"
```

### StatementEvent Fields

| Field | Type | Description |
| --- | --- | --- |
| `sql` | `str` | Executed SQL text (with literals redacted when `RedactionConfig.mask_literals` is enabled). |
| `parameters` | `Any` | Bound parameters (masked automatically when `RedactionConfig.mask_parameters` is enabled). |
| `driver` | `str` | Adapter driver class name (such as `"SqliteDriver"` or `"AsyncpgDriver"`). |
| `adapter` | `str` | Adapter identifier (such as `"sqlite"`, `"asyncpg"`, or `"oracledb"`). |
| `bind_key` | `str or None` | Optional `bind_key` assigned on the owning database configuration. |
| `operation` | `str` | Normalized SQL operation label (`"SELECT"`, `"INSERT"`, `"UPDATE"`, `"DELETE"`, `"DDL"`). |
| `execution_mode` | `str` | Driver execution path (such as `"single"`, `"many"`, `"script"`, or `"stream"`). |
| `is_many` / `is_script` | `bool` | Flags indicating batched `execute_many` or multi-statement `execute_script` execution. |
| `rows_affected` | `int or None` | Driver-reported affected or fetched row count when available. |
| `duration_s` | `float` | Wall-clock driver execution latency in seconds. |
| `correlation_id` | `str or None` | Active `CorrelationContext` identifier bound for the current request or task. |
| `storage_backend` | `str or None` | Storage backend name when emitted during `select_to_storage` or `load_from_storage` jobs. |
| `started_at` | `float` | Epoch timestamp when statement execution began. |
| `profile` | `dict[str, Any] or None` | Parsed execution plan payload when DuckDB `enable_statement_profiling` is enabled. |
| `script_ordinal` / `script_statement_count` | `int or None` | Statement index and total count when profiling multi-statement scripts. |

## Sampling and Parameter Redaction

High-throughput services can sample routine queries deterministically while always capturing failed statements (`force_sample_on_error=True`) and masking sensitive bind parameters via `RedactionConfig`:

```{ .python title="docs/examples/patterns/observability/sampling_config.py" }
--8<-- "docs/examples/patterns/observability/sampling_config.py:example"
```

```python
from sqlspec.observability import ObservabilityConfig, RedactionConfig

observability = ObservabilityConfig(
    redaction=RedactionConfig(
        mask_parameters=True,
        mask_literals=False,
        parameter_allow_list=("tenant_id",),
    )
)
```

## OpenTelemetry, Prometheus, and Cloud Log Formatters

Enable OpenTelemetry spans (`opentelemetry` extra) and Prometheus counters/histograms (`prometheus` extra) declaratively through `extension_config` on any adapter config, or format structured logs for Google Cloud Logging and AWS CloudWatch:

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

```{ .python title="docs/examples/patterns/observability/cloud_formatters.py" }
--8<-- "docs/examples/patterns/observability/cloud_formatters.py:example"
```

## Request Correlation and Wire-Level SQLCommenter

Bind request IDs with `CorrelationContext` (or enable `enable_correlation_middleware` in Litestar) so logs, spans, and database slow-query logs share the same trace context:

```{ .python title="docs/examples/patterns/observability/correlation_middleware.py" }
--8<-- "docs/examples/patterns/observability/correlation_middleware.py:example"
```

Setting `StatementConfig(enable_sqlcommenter=True)` appends W3C `traceparent` and `CorrelationContext` tags at wire dispatch time (`append_wire_sqlcomment`), keeping compiled SQL cache keys stable across requests so statement caching stays active. On Oracle (`python-oracledb`), `driver_features={"enable_session_tracing": True}` piggybacks `module`, `action`, `client_identifier`, and `econtext_id` onto the next TTC packet with zero extra round-trips.

`get_w3c_trace_context()` returns `(traceparent, tracestate)` from the active span. It works with both recorded and propagated spans. With no valid span, it reads `SQLCommenterContext`. With no trace at all, it returns `(None, "")`.

```python
from sqlspec.observability import get_w3c_trace_context

traceparent, tracestate = get_w3c_trace_context()
```

On Oracle 26ai, SQLSpec sends these headers through the native API when spans are enabled. It first checks `supports_opentelemetry_tracing`. The headers ride with the next request and add no trace-only call. Pool release clears them. Set `driver_features["enable_otel_trace_context"]=False` to opt out; the flag defaults to `True`. Older servers and Thick connections keep the existing session metadata path. When the native path sends W3C headers, SQLSpec omits the SQLCommenter suffix for that request. This avoids duplicate trace data. If you opt out, you can still enable SQLCommenter.

## DuckDB Statement Profiling

On `DuckDBConfig` and DuckDB-backed `AdbcConfig`, setting `driver_features={"enable_statement_profiling": True}` captures per-statement JSON execution profiles onto `StatementEvent.profile` (along with `script_ordinal` and `script_statement_count` for multi-statement scripts).

## Recap

- Attach `ObservabilityConfig` to `SQLSpec(...)` or individual adapter configs for uniform telemetry across sync and async drivers.
- Combine `SamplingConfig(force_sample_on_error=True)` with `RedactionConfig(mask_parameters=True)` in production.
- Enable `enable_sqlcommenter=True` to correlate slow database queries back to application traces without sacrificing statement cache hit rates.
