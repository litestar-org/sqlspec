# Google Cloud Spanner

`sqlspec.adapters.spanner` provides symmetric synchronous (`SpannerSyncConfig`) and native `asyncio` (`SpannerAsyncConfig` over `google.cloud.spanner_v1._async`) drivers for Google Cloud Spanner (`>= 3.64.0`), supporting both **GoogleSQL** (`dialect="spanner"`) and **PostgreSQL** (`dialect="spangres"`) interfaces.

## Overview & Capabilities

| Adapter Config | Driver Extra | Execution | Default Session Pool | Best For |
| --- | --- | --- | --- | --- |
| `SpannerAsyncConfig` | `sqlspec[spanner]` | Async (`asyncio`) | `AsyncFixedSizePool` (`size`, `target_size`, `max_sessions`, `ping_interval`) | Native `asyncio` microservices, lock-free snapshot reads, and ADK vector/artifact stores. |
| `SpannerSyncConfig` | `sqlspec[spanner]` | Sync | `FixedSizePool` (`size`, `target_size`, `max_sessions`, `ping_interval`) | Synchronous services, Partitioned DML batch jobs, and CLI schema migrations. |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[spanner]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[spanner]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[spanner]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[spanner]"
    ```

=== "Async (SpannerAsyncConfig)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.spanner import SpannerAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        SpannerAsyncConfig(
            connection_config={
                "project": "my-gcp-project",
                "instance_id": "prod-instance",
                "database_id": "app_db",
                "size": 25,
                "target_size": 25,
                "max_sessions": 100,
                "ping_interval": 1800,
            }
        )
    )
    ```

=== "PostgreSQL Interface (spangres)"

    ```python
    from sqlspec import SQLSpec, StatementConfig
    from sqlspec.adapters.spanner import SpannerAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        SpannerAsyncConfig(
            connection_config={
                "project": "my-gcp-project",
                "instance_id": "prod-instance",
                "database_id": "pg_app_db",
                "size": 25,
                "max_sessions": 100,
            },
            statement_config=StatementConfig(dialect="spangres"),
        )
    )
    ```

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `SpannerConnectionParams` and `SpannerPoolParams` keys inside `connection_config={...}` on `SpannerSyncConfig` or `SpannerAsyncConfig` (defaulting to `FixedSizePool` on sync and `AsyncFixedSizePool` on async):

<!-- GENERATED:START config-table:sqlspec.adapters.spanner.config.SpannerConnectionParams,sqlspec.adapters.spanner.config.SpannerPoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `project` | `str` | - | Google Cloud project ID owning the Spanner instance. |
| `project_id` | `str` | - | Alias normalized to `project` when `project` is unset. |
| `credentials` | `Credentials` | - | Explicit `google.auth.credentials.Credentials` instance overriding ADC. |
| `client_info` | `ClientInfo` | - | Custom `google.api_core.gapic_v1.client_info.ClientInfo` user-agent metadata. |
| `client_options` | `ClientOptions or dict[str, Any]` | - | GAPIC `ClientOptions` or mapping configuring API endpoint, universe domain, or quota project. |
| `query_options` | `ExecuteSqlRequest.QueryOptions` | - | Client-wide default `ExecuteSqlRequest.QueryOptions` (such as `optimizer_version`). |
| `route_to_leader_enabled` | `bool` | - | Route read-write transactions and Partitioned DML directly to Paxos leader replicas. |
| `directed_read_options` | `DirectedReadOptions` | - | Client-wide replica routing rules for read-only snapshot requests. |
| `observability_options` | `Any` | - | Google Cloud Spanner client-side OpenTelemetry tracing configuration. |
| `default_transaction_options` | `DefaultTransactionOptions` | - | Default isolation level or read-lock mode applied to read-write transactions. |
| `experimental_host` | `str` | - | Direct single-endpoint Spanner or emulator host address. |
| `disable_builtin_metrics` | `bool` | - | Disable built-in Cloud Monitoring client-side metric export. |
| `client_context` | `dict[str, str]` | - | Key-value client context attributes attached to Spanner RPC metadata. |
| `use_plain_text` | `bool` | - | Connect over unencrypted plaintext gRPC when targeting local emulators. |
| `ca_certificate` | `str` | - | PEM root CA certificate bundle path for custom TLS verification. |
| `client_certificate` | `str` | - | PEM client certificate path for mTLS authentication. |
| `client_key` | `str` | - | PEM client private key path paired with `client_certificate`. |
| `instance_type` | `str` | - | Spanner instance edition or provisioning type forwarded to the client. |
| `instance_id` | `str` | - | Target Spanner instance identifier. |
| `instance` | `str` | - | Alias normalized to `instance_id` when `instance_id` is unset. |
| `configuration_name` | `str` | - | Regional or multi-region Spanner instance configuration resource name. |
| `display_name` | `str` | - | Human-readable display name used when referencing or provisioning an instance. |
| `node_count` | `int` | - | Allocated node count for the Spanner instance descriptor. |
| `processing_units` | `int` | - | Allocated processing units for sub-node or granular Spanner instances. |
| `instance_labels` | `dict[str, str]` | - | Resource labels associated with the Spanner instance. |
| `database_id` | `str` | - | Target Spanner database identifier within `instance_id`. |
| `database` | `str` | - | Alias normalized to `database_id` when `database_id` is unset. |
| `db` | `str` | - | Short alias normalized to `database_id` when `database_id` is unset. |
| `ddl_statements` | `tuple[str, ...] or list[str]` | - | Initial DDL statements associated with the `Database` handle. |
| `logger` | `Logger` | - | Custom Python `logging.Logger` attached to the Spanner `Database` object. |
| `encryption_config` | `EncryptionConfig or dict[str, Any]` | - | Customer-managed Cloud KMS encryption configuration for the database. |
| `database_dialect` | `DatabaseDialect` | - | Spanner SQL dialect enum (`GOOGLE_STANDARD_SQL` or `POSTGRESQL`). |
| `database_role` | `str` | - | Fine-grained access control database role assumed by sessions. |
| `enable_drop_protection` | `bool` | - | Flag indicating whether database drop protection is enabled on the handle. |
| `enable_interceptors_in_tests` | `bool` | - | Enable gRPC client interceptors when running under test harnesses. |
| `proto_descriptors` | `bytes` | - | Serialized protobuf `FileDescriptorSet` bytes associated with the database. |
| `extra` | `dict[str, Any]` | - | Additional keyword arguments retained in normalized connection configuration. |
| `pool_type` | `type[AbstractSessionPool or AsyncAbstractSessionPool]` | - | Session pool class (`FixedSizePool`, `BurstyPool`, `PingingPool`, or async equivalents). |
| `size` | `int` | - | Fixed session count pre-allocated by `FixedSizePool` or `PingingPool`. |
| `target_size` | `int` | - | Target warm session count maintained by `BurstyPool`. |
| `max_sessions` | `int` | - | Upper cap on sessions opened concurrently by custom or bursty pools. |
| `default_timeout` | `int or float` | - | Seconds to wait when borrowing a session from the pool before timing out. |
| `session_labels` | `dict[str, str]` | - | Key-value labels attached to sessions created by the pool. |
| `labels` | `dict[str, str]` | - | Alias for `session_labels` forwarded to pool session creation. |
| `ping_interval` | `int` | - | Background keepalive ping interval in seconds for `PingingPool`. |
| `max_age_minutes` | `int` | - | Maximum session age in minutes before pool rotation. |
<!-- GENERATED:END config-table:sqlspec.adapters.spanner.config.SpannerConnectionParams,sqlspec.adapters.spanner.config.SpannerPoolParams -->

### Driver Features

Pass `SpannerDriverFeatures` keys inside `driver_features={...}` on `SpannerSyncConfig` or `SpannerAsyncConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.spanner.config.SpannerDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enable_uuid_conversion` | `bool` | `True` | Automatically coerce `UUID` parameters to strings (defaults to `True`). |
| `json_serializer` | `Callable[[Any], str]` | - | Custom JSON serializer for `JSON` parameter payloads. |
| `json_deserializer` | `Callable[[str], Any]` | - | Custom JSON deserializer for `JSON` column values. |
| `retry` | `Retry or None` | - | Default `google.api_core.retry.Retry` policy passed to `execute_sql`, `execute_update`, and `batch_update`. |
| `timeout` | `float or None` | - | Default per-RPC timeout in seconds passed to `execute_sql`, `execute_update`, and `batch_update`. |
| `query_options` | `ExecuteSqlRequest.QueryOptions or dict[str, Any] or None` | - | Default `ExecuteSqlRequest.QueryOptions` forwarded to SQL execution calls. |
| `request_options` | `RequestOptions or dict[str, Any] or None` | - | Default `RequestOptions` (priority, request tag, transaction tag) forwarded to statement RPCs. |
| `directed_read_options` | `DirectedReadOptions or None` | - | Default `DirectedReadOptions` forwarded to snapshot `execute_sql` reads. |
| `session_labels` | `dict[str, str]` | - | Deprecated compatibility alias for `connection_config['session_labels']`. |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channel support. |
| `events_backend` | `Literal['poll_queue']` | - | Event queue backend name (`'poll_queue'`). |
| `enable_batch_write_api` | `bool` | `False` | Route `load_from_arrow` through Spanner `batch_write` mutation groups instead of single-transaction mutations (defaults to `False`). |
| `on_connection_create` | `Callable[[Any], Any or None] or None` | - | Callback invoked after a new Spanner connection (Snapshot or Transaction) is created. |
<!-- GENERATED:END config-table:sqlspec.adapters.spanner.config.SpannerDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `INT64` | `int` | Spanner 64-bit signed integer (`param_types.INT64`). |
| `FLOAT64` / `FLOAT32` / `NUMERIC` | `float` / `decimal.Decimal` | 64-bit/32-bit floats and exact `NUMERIC` (`pg.numeric` on `spangres`). |
| `STRING` / `BYTES` | `str` / `bytes` | UTF-8 strings and Base64/raw binary columns. |
| `JSON` / `PG.JSONB` | `dict` / `list` | Encoded and decoded via `json_serializer` and `json_deserializer`. |
| `ARRAY<FLOAT32>` / `ARRAY<T>` | `list[float]` / `list[T]` | Native vector embedding and typed array binding. |
| `TIMESTAMP` / `DATE` | `datetime.datetime` / `datetime.date` | Externally consistent TrueTime commit timestamps and dates. |
| `UUID` / `STRING(36)` | `uuid.UUID` / `str` | Converted automatically when `enable_uuid_conversion=True` (default). |

- **Read Sessions, Write Sessions, and Transaction Retries**: Call `config.provide_read_session()` (or `config.provide_session(transaction=False)`) for lock-free single-use or bounded-staleness `Snapshot` reads with zero `Commit` RPC overhead. Call `config.provide_write_session()` for read-write sessions, or `config.run_in_transaction(fn)` / `session.run_in_transaction(fn)` to automatically retry `Aborted` (`DeadlockError`) transactions using Spanner's native backoff delay.
- **Direct Mutation Batching and Partitioned DML**: For high-throughput key-value writes that bypass SQL parsing, call `session.write_insert(table, data)`, `session.write_update(table, data)`, `session.write_upsert(table, data)`, or `session.write_delete(table, keys)` (automatically chunked to stay under Spanner's per-commit mutation limit). For large bulk updates or deletes across an entire table, call `session.execute_partitioned_dml(sql, params)` or `config.execute_partitioned_dml(sql, params)`.
- **THEN RETURN DML and Dual Dialects**: Both sync and async drivers return projected rows and `rows_affected` on `INSERT ... THEN RETURN`, `UPDATE ... THEN RETURN`, and `DELETE ... THEN RETURN` statements across both GoogleSQL (`dialect="spanner"`, supporting `INTERLEAVE IN PARENT`, `ROW DELETION POLICY`, and `GRAPH_TABLE`) and PostgreSQL (`dialect="spangres"`, supporting `TTL INTERVAL` and `$1` placeholders).
- **ADK Vector, Hybrid RRF, and Artifact Stores**: `SpannerSyncADKMemoryStore` and `SpannerAsyncADKMemoryStore` support `ARRAY<FLOAT32>` embeddings with `APPROX_COSINE_DISTANCE`, FTS `SCORE`, and single-round-trip Hybrid RRF (`k=60`), alongside `SpannerSyncADKArtifactStore` and `SpannerAsyncADKArtifactStore`.

## See Also

- [Spanner API Reference](/reference/adapters/spanner/)
- [Google Cloud Spanner Dialect Reference](/reference/data-dictionary/dialects/spanner/)
