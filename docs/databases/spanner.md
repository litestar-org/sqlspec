# Google Cloud Spanner

`sqlspec.adapters.spanner` provides symmetric synchronous (`SpannerSyncConfig`) and native `asyncio` (`SpannerAsyncConfig` over `google.cloud.spanner_v1._async`) drivers for Google Cloud Spanner (`>= 3.64.0`), supporting both **GoogleSQL** (`dialect="spanner"`) and **PostgreSQL** (`dialect="spangres"`) interfaces.

## Install and When to Choose

```bash
uv add "sqlspec[spanner]"
```

Choose `SpannerAsyncConfig` or `SpannerSyncConfig` for globally distributed, externally consistent OLTP databases, multi-region scale-out workloads, property-graph and `ARRAY<FLOAT32>` vector search, and agentic Google ADK session/memory/artifact persistence.

## Minimal Connection and Pool Setup

=== "Async (`SpannerAsyncConfig`)"

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
                "max_sessions": 100,
                "ping_interval": 1800,
            }
        )
    )
    ```

=== "PostgreSQL Interface (`spangres`)"

    ```python
    from sqlspec import SQLSpec, StatementConfig
    from sqlspec.adapters.spanner import SpannerAsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        SpannerAsyncConfig(
            connection_config={"project": "my-gcp-project", "instance_id": "prod-instance", "database_id": "pg_app_db"},
            statement_config=StatementConfig(dialect="spangres"),
        )
    )
    ```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `project` / `instance_id` / `database_id` | `connection_config` | Target Spanner project, instance, and database identifiers. |
| `size` / `max_sessions` / `ping_interval` | `connection_config` | Session pool sizing on `PingingPool` / `TransactionPingingPool` (note: Spanner pools use `size` and `max_sessions` rather than `min_sessions`). |
| `database_role` | `connection_config` | Fine-grained access control database role assumed by pooled sessions. |
| `enable_batch_write_api` | `driver_features` | Enables Spanner's low-latency mutation batch-write RPCs where applicable. |
| `directed_read_options` | `driver_features` | Routes read-only snapshot queries to specific replica regions or types (`READ_ONLY` / `READ_WRITE`). |
| `query_options` / `request_options` | `driver_features` | Sets Spanner optimizer version, statistics package, request priority, and transaction tags. |
| `timeout` / `retry` | `driver_features` | Customizes gRPC deadline and retry policies per statement. |
| `enable_uuid_conversion` | `driver_features` | Converts Spanner `UUID` / string columns to Python `uuid.UUID`. |

See [Spanner API Reference](/reference/adapters/spanner/) for full `SpannerConnectionParams`, `SpannerPoolParams`, and `SpannerDriverFeatures` TypedDict tables.

## Type Mapping and Dialect Notes

- **Snapshot Reads vs Read-Write Retries (`run_in_transaction`)**: Use `provide_session(transaction=False)` for lock-free single-use or multi-use `Snapshot` reads (zero `Commit` RPC cost). For contended read-write transactions, call `config.run_in_transaction(...)` or `session.run_in_transaction(...)`, which automatically unwraps `DeadlockError` (`Aborted`) and re-executes the closure with Spanner's native backoff hint.
- **`THEN RETURN` DML Support**: Both sync and async drivers return projected rows and `rows_affected` on `INSERT ... THEN RETURN`, `UPDATE ... THEN RETURN`, and `DELETE ... THEN RETURN` statements.
- **GoogleSQL (`spanner`) vs PostgreSQL (`spangres`) Dialects**: SQLSpec includes dedicated custom SQLGlot dialects for both `spanner` (`INTERLEAVE IN PARENT`, `ROW DELETION POLICY`, `GRAPH_TABLE`) and `spangres` (`TTL INTERVAL`, `$1` placeholders).
- **ADK Vector, Hybrid RRF, and Artifact Stores**: `Spanner*ADKMemoryStore` supports `ARRAY<FLOAT32>` embeddings with `APPROX_COSINE_DISTANCE`, FTS `SCORE`, and single-round-trip Hybrid RRF (`k=60`), plus `SpannerSyncADKArtifactStore` / `SpannerAsyncADKArtifactStore` in `sqlspec.adapters.spanner.adk`.
- **Spanner Emulator**: Set `SPANNER_EMULATOR_HOST="localhost:9010"` (or use `pytest-databases` `spanner_service`) to run full sync and async tests locally without GCP credentials.
