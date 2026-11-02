# Google Cloud BigQuery

`sqlspec.adapters.bigquery` connects SQLSpec to Google Cloud BigQuery (`google-cloud-bigquery`, `google-cloud-bigquery-storage`, and `google-cloud-storage`), pairing low-latency `query_and_wait` interactive execution with high-throughput Storage Read and Storage Write Arrow APIs.

## Install and When to Choose

=== "uv"

    ```bash
    uv add "sqlspec[bigquery]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[bigquery]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[bigquery]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[bigquery]"
    ```

Choose `BigQueryConfig` for serverless warehouse queries, multi-statement GoogleSQL procedural scripts, cost-governed analytical APIs (`maximum_bytes_billed`, `reservation`), and streaming Arrow/Parquet lakehouse pipelines.

## Minimal Connection and Pool Setup

`BigQueryConfig` extends `NoPoolSyncConfig` because `google.cloud.bigquery.Client` multiplexes stateless HTTPS and gRPC requests internally using Application Default Credentials (ADC) or an explicit service account:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.bigquery import BigQueryConfig

spec = SQLSpec()
config = spec.add_config(
    BigQueryConfig(
        connection_config={
            "project": "my-gcp-project",
            "dataset_id": "analytics",
            "location": "US",
            "maximum_bytes_billed": 10 * 1024**3,
        },
        driver_features={
            "enable_query_and_wait": True,
            "enable_storage_write_api": True,
            "storage_write_stream_type": "PENDING",
        },
    )
)
```

## Key Driver Features and Pool Tuning

| Setting | Location | Description |
| --- | --- | --- |
| `project` / `dataset_id` / `location` | `connection_config` | Billing project, default dataset for unqualified tables, and geographic job region. |
| `credentials` / `credentials_path` | `connection_config` | Explicit `google.auth.credentials.Credentials` instance or service-account JSON path (uses ADC when omitted). |
| `maximum_bytes_billed` / `priority` / `reservation` | `connection_config` | Cost guardrails (`INTERACTIVE` vs `BATCH` priority and BigQuery Editions reservation assignment). |
| `labels` | `connection_config` | Billing and observability key-value labels attached to submitted jobs. |
| `enable_query_and_wait` | `driver_features` | Uses low-latency `client.query_and_wait()` for interactive queries and falls back to `QueryJob` when destination tables, transactions, or `EXPORT DATA` require job handles. |
| `enable_storage_write_api` / `storage_write_stream_type` | `driver_features` | Streams `load_from_arrow` batches via BigQuery Storage Write API (`"PENDING"` atomic commit default or `"COMMITTED"` immediate visibility). |
| `enable_native_storage` | `driver_features` | Accelerates GCS `gs://` Parquet/CSV/JSON exports and loads using server-side BigQuery jobs. |
| `request_timeout` / `job_result_timeout` | `driver_features` | Per-RPC and overall job completion timeout bounds in seconds. |

See [BigQuery API Reference](/reference/adapters/bigquery/) for full `BigQueryConnectionParams` and `BigQueryDriverFeatures` TypedDict documentation.

## Type Mapping and Dialect Notes

- **Named `@param` Compilation**: Write `:param` or `@param` in SQLSpec queries; the compiler emits BigQuery `@param` syntax with typed `ScalarQueryParameter`, `ArrayQueryParameter`, and explicit `StructQueryParameter` bindings.
- **Preserving Verbatim Alias Casing in `QueryBuilder`**: SQLGlot's default optimizer (`enable_optimization=True`) qualifies and lowercases BigQuery column aliases. If downstream consumers require exact mixed-case aliases from `sql.select(...)`, configure `StatementConfig(dialect="bigquery", enable_optimization=False)` on `BigQueryConfig`.
- **Multi-Statement Procedural Scripts**: `session.execute_script(sql)` submits `DECLARE`, `BEGIN ... END`, temporary tables, and script-level transactions as a single un-split BigQuery script job rather than splitting on semicolons client-side.
- **Local Emulator Testing**: Point `api_endpoint` in `connection_config` at a local BigQuery emulator (`pytest-databases` `bigquery_service`) for offline CI integration tests.
