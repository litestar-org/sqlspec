# Google Cloud BigQuery

`sqlspec.adapters.bigquery` connects SQLSpec to Google Cloud BigQuery (`google-cloud-bigquery`, `google-cloud-bigquery-storage`, and `google-cloud-storage`), pairing low-latency `query_and_wait` interactive execution with high-throughput Storage Read and Storage Write Arrow APIs.

## Overview & Capabilities

`BigQueryConfig` extends `NoPoolSyncConfig` because `google.cloud.bigquery.Client` multiplexes stateless HTTPS and gRPC requests internally using Application Default Credentials (ADC) or an explicit `google.auth.credentials.Credentials` instance.

| Capability | Details |
| --- | --- |
| **Adapter Config** | `BigQueryConfig` (`sqlspec.adapters.bigquery`) |
| **Driver Extra** | `sqlspec[bigquery]` (`google-cloud-bigquery`, `google-cloud-bigquery-storage`, `pyarrow`) |
| **Execution Mode** | Synchronous (`query_and_wait` fast path + `QueryJob` fallback) |
| **Arrow & Storage** | BigQuery Storage Read API (`select_to_arrow`) and Storage Write API (`load_from_arrow`) |
| **Extensions** | Litestar store, Events table queue, Google ADK session + event store (`BigQueryADKStore`) |

## Installation & Quickstart

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

For local BigQuery emulator or custom VPC-SC endpoints, pass `client_options={"api_endpoint": "http://localhost:9050"}` inside `connection_config` and an explicit `credentials` object:

```python
from google.auth.credentials import AnonymousCredentials
from sqlspec.adapters.bigquery import BigQueryConfig

emulator_config = BigQueryConfig(
    connection_config={
        "project": "test-project",
        "dataset_id": "test_dataset",
        "credentials": AnonymousCredentials(),
        "client_options": {"api_endpoint": "http://localhost:9050"},
    }
)
```

## Connection & Pool Configuration

### Connection Parameters

Pass `BigQueryConnectionParams` keys inside `connection_config={...}` on `BigQueryConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.bigquery.config.BigQueryConnectionParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `project` | `str` | - | Billing and default job execution Google Cloud project ID. |
| `project_id` | `str` | - | Alias normalized to `project` when `project` is unset. |
| `location` | `str` | - | Default geographic region or multi-region (such as `'US'` or `'EU'`) for jobs and datasets. |
| `credentials` | `Credentials` | - | Explicit `google.auth.credentials.Credentials` instance overriding ADC. |
| `client_options` | `ClientOptions` | - | GAPIC `ClientOptions` configuring custom endpoints, quota project, or scopes. |
| `client_info` | `ClientInfo` | - | Custom `ClientInfo` user-agent metadata attached to BigQuery API requests. |
| `default_query_job_config` | `QueryJobConfig` | - | Base `QueryJobConfig` cloned and overlaid with top-level query governance fields. |
| `default_load_job_config` | `LoadJobConfig` | - | Base `LoadJobConfig` passed to the BigQuery `Client` for load jobs. |
| `default_job_creation_mode` | `str` | - | Client job creation mode (such as `'JOB_CREATION_OPTIONAL'` for stateless short queries). |
| `dataset_id` | `str` | - | Default dataset (`dataset` or `project.dataset`) used to resolve unqualified table names. |
| `dataset` | `str` | - | Alias normalized to `dataset_id` when `dataset_id` is unset. |
| `database` | `str` | - | Alias normalized to `dataset_id` when `dataset_id` is unset. |
| `db` | `str` | - | Short alias normalized to `dataset_id` when `dataset_id` is unset. |
| `use_query_cache` | `bool` | `True` | Read from BigQuery cached query results when unmodified (defaults to `True`). |
| `maximum_bytes_billed` | `int` | - | Per-query cost guard failing queries estimated to scan above this byte limit. |
| `query_timeout_ms` | `int` | - | Server-side query execution timeout in milliseconds applied to `job_timeout_ms`. |
| `job_timeout_ms` | `int` | - | Server-side BigQuery job execution timeout in milliseconds. |
| `labels` | `dict[str, str]` | - | Billing and governance key-value labels attached to executed query jobs. |
| `priority` | `Literal['INTERACTIVE', 'BATCH']` | - | BigQuery query scheduling priority (`'INTERACTIVE'` or `'BATCH'`). |
| `reservation` | `str` | - | BigQuery Editions slot reservation resource path assigned to query jobs. |
| `max_slots` | `int` | - | Maximum slot concurrency cap applied to individual query jobs. |
| `extra` | `dict[str, Any]` | - | Additional keyword metadata retained in normalized connection configuration. |
<!-- GENERATED:END config-table:sqlspec.adapters.bigquery.config.BigQueryConnectionParams -->

### Driver Features

Pass `BigQueryDriverFeatures` keys inside `driver_features={...}` on `BigQueryConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.bigquery.config.BigQueryDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `connection_instance` | `BigQueryConnection` | - | Pre-created `google.cloud.bigquery.Client` reused without transferring lifecycle ownership. |
| `default_query_job_config` | `QueryJobConfig` | - | Default `QueryJobConfig` for a directly constructed driver; config-managed drivers use the client's default. |
| `on_connection_create` | `Callable[[Any], None]` | - | Callback invoked with the `bigquery.Client` when instantiated by the config. |
| `json_serializer` | `Callable[[Any], str]` | `sqlspec.utils.serializers.to_json` | Serializer converting dict/list parameters to JSON strings (defaults to `sqlspec.utils.serializers.to_json`). |
| `json_deserializer` | `Callable[[str], Any]` | - | Deserializer converting JSON strings to native dict/list objects. |
| `enable_uuid_conversion` | `bool` | `True` | Automatically coerce `UUID` strings and parameters (defaults to `True`). |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channel support. |
| `events_backend` | `Literal['poll_queue']` | - | Event queue backend name (`'poll_queue'`). |
| `job_retry_deadline` | `float` | `60.0` | Total seconds to retry transient query failures (defaults to `60.0`; `<= 0` disables retries). |
| `job_result_timeout` | `float` | - | Maximum seconds to poll `QueryJob.result()` before raising a timeout error. |
| `enable_query_and_wait` | `bool` | `True` | Execute single-statement queries via low-latency `Client.query_and_wait()` outside transactions (defaults to `True`). |
| `request_timeout` | `float` | `job_result_timeout` | Per-request HTTP transport timeout in seconds for job submission calls (defaults to `job_result_timeout` or `120.0`). |
| `query_page_size` | `int` | - | Page size forwarded to `QueryJob.result()` when fetching `SELECT` rows. |
| `query_max_results` | `int` | - | Maximum total rows fetched from `QueryJob.result()`. |
| `enable_storage_write_api` | `bool` | `False` | Route append `load_from_arrow` calls through the BigQuery Storage Write API with Parquet load fallback (defaults to `False`). |
| `storage_write_stream_type` | `Literal['COMMITTED', 'PENDING']` | - | Storage Write API stream mode (`'PENDING'` default atomic commit or `'COMMITTED'` immediate visibility). |
| `enable_native_storage` | `bool` | `True` | Route eligible remote object-store exports through server-side `EXPORT DATA` (defaults to `True`). |
| `native_export_connection` | `str` | - | BigQuery Cloud Resource connection ID (`project.location.connection`) required for cross-cloud S3/Azure `EXPORT DATA`. |
<!-- GENERATED:END config-table:sqlspec.adapters.bigquery.config.BigQueryDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `INT64` | `int` | 64-bit signed integer (`ScalarQueryParameter` `INT64`). |
| `FLOAT64` / `NUMERIC` / `BIGNUMERIC` | `float` / `decimal.Decimal` | IEEE 754 floating-point and 38-digit / 76-digit exact decimal types. |
| `STRING` / `BYTES` | `str` / `bytes` | UTF-8 strings and raw byte sequences. |
| `JSON` / `STRUCT` / `ARRAY` | `dict` / `list` | Bound via `StructQueryParameter`, `ArrayQueryParameter`, and `json_serializer` / `json_deserializer`. |
| `TIMESTAMP` / `DATETIME` / `DATE` | `datetime.datetime` / `datetime.date` | UTC microsecond `TIMESTAMP` and civil `DATETIME` / `DATE`. |
| `GEOGRAPHY` | `str` (WKT / GeoJSON) | Well-Known Text and GeoJSON spatial literals. |

- **Named `@param` Compilation and Default Job Governance**: Write `:param` or `@param` in SQLSpec queries; the compiler emits BigQuery `@param` syntax and merges `default_query_job_config`, `default_load_job_config`, `default_job_creation_mode`, `dataset_id`, and `location` onto every submitted job.
- **Storage Write API (`enable_storage_write_api`) and Native GCS Export**: `session.load_from_arrow(...)` streams Arrow batches through BigQuery's gRPC Storage Write API using `storage_write_stream_type="PENDING"` (atomic commit) or `"COMMITTED"` (immediate visibility), while `enable_native_storage=True` and `native_export_connection` accelerate GCS `gs://` Parquet/CSV/JSON transfers.
- **ADK Session and Event Store (`BigQueryADKStore`)**: `sqlspec.adapters.bigquery.adk` provides `BigQueryADKStore`, `BigQueryADKConfig`, and `BigQueryADKRetentionConfig` (`BaseSyncADKStore[BigQueryConfig]`) for durable analytical session and event retention in BigQuery.
- **Multi-Statement Procedural Scripts**: `session.execute_script(sql)` submits `DECLARE`, `BEGIN ... END`, temporary tables, and script-level transactions as a single un-split BigQuery script job rather than splitting on semicolons client-side.
- **Preserving Verbatim Alias Casing in QueryBuilder**: SQLGlot's default optimizer (`enable_optimization=True`) qualifies and lowercases BigQuery column aliases. Configure `StatementConfig(dialect="bigquery", enable_optimization=False)` on `BigQueryConfig` when downstream consumers require exact mixed-case aliases.

## See Also

- [BigQuery API Reference](/reference/adapters/bigquery/)
- [BigQuery Dialect Reference](/reference/data-dictionary/dialects/bigquery/)
