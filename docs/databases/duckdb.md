# DuckDB

`sqlspec.adapters.duckdb` wraps the in-process analytical DuckDB engine (`duckdb` + `pyarrow`), combining thread-local connection pooling, declarative extension and cloud secret management, hybrid native/Arrow row materialization, zero-copy Arrow transfers, and per-statement JSON execution profiling.

## Overview & Capabilities

Choose `DuckDBConfig` for embedded OLAP analytics, local lakehouse queries over Parquet/CSV/JSON on S3 or GCS, fast feature engineering pipelines, and columnar ETL jobs running inside a single Python worker.

| Capability | Details |
| --- | --- |
| **Adapter Config** | `DuckDBConfig` (`sqlspec.adapters.duckdb`) |
| **Driver Extra** | `sqlspec[duckdb]` (`duckdb`, `pyarrow`) |
| **Execution Mode** | Synchronous |
| **Pool Architecture** | `DuckDBConnectionPool` (thread-local cursor/connection manager with `pool_recycle_seconds`, `health_check_interval`, `:memory:shared_db` normalization) |
| **Extensions & Storage** | Declarative `DuckDBExtensionConfig`, `DuckDBSecretConfig`, zero-copy Arrow `select_to_arrow()` / `load_from_arrow()`, ADK FTS + cosine similarity |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[duckdb]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[duckdb]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[duckdb]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[duckdb]"
    ```

```python
from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

spec = SQLSpec()
config = spec.add_config(
    DuckDBConfig(
        connection_config={
            "database": ":memory:",
            "memory_limit": "4GB",
            "threads": 4,
            "preserve_insertion_order": True,
            "pool_recycle_seconds": 86400,
            "health_check_interval": 60,
        },
        driver_features={
            "extensions": [{"name": "httpfs"}, {"name": "json"}],
            "enable_statement_profiling": False,
        },
    )
)
```

For persistent analytical databases, set `"database": "var/analytics.duckdb"` (and `"read_only": True` on read-only replica processes).

## Connection & Pool Configuration

### Connection and Pool Parameters

> [!NOTE]
> `DuckDBConnectionPool` is a thread-local connection manager configured via `pool_recycle_seconds` and `health_check_interval` on `DuckDBPoolParams`. When `database` is omitted or set to `":memory:"`, `DuckDBConfig` normalizes it to `":memory:shared_db"` so all pooled checkouts share the same in-memory catalog. In Linux containers, `DuckDBConfig` also clamps default memory limits to cgroup v1/v2 container quotas automatically.

Pass `DuckDBConnectionParams` and `DuckDBPoolParams` keys inside `connection_config={...}` on `DuckDBConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.duckdb.config.DuckDBConnectionParams,sqlspec.adapters.duckdb.config.DuckDBPoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | `str` | - | Database file path, `':memory:shared_db'` shared memory identifier, or MotherDuck URI. |
| `read_only` | `bool` | - | Open the database file in read-only mode without taking a write lock. |
| `config` | `dict[str, Any]` | - | Raw DuckDB engine configuration dictionary merged with modeled settings. |
| `memory_limit` | `str` | - | Maximum buffer-manager memory ceiling (for example `'4GB'`). |
| `memory_ceiling_fraction` | `float` | `0.8` | Fraction of container or host memory used as an upper ceiling when `memory_limit` is unset or exceeds available memory (defaults to `0.8`). |
| `threads` | `int` | - | Number of DuckDB execution worker threads. |
| `temp_directory` | `str` | - | Directory path used when spilling out-of-core intermediates to disk. |
| `reject_tmpfs_temp_directory` | `bool` | `False` | Raise `ImproperConfigurationError` when `temp_directory` resides on an in-memory `tmpfs` or `ramfs` mount (defaults to `False`). |
| `max_temp_directory_size` | `str` | - | Maximum disk space cap for `temp_directory` spill files (for example `'20GB'`). |
| `autoload_known_extensions` | `bool` | - | Automatically load installed DuckDB extensions when referenced by SQL syntax. |
| `autoinstall_known_extensions` | `bool` | - | Automatically download and install known DuckDB extensions on first reference. |
| `allow_community_extensions` | `bool` | - | Permit installing extensions from the DuckDB Community Extensions repository. |
| `allow_unsigned_extensions` | `bool` | - | Permit loading unsigned local or third-party DuckDB extension binaries. |
| `extension_directory` | `str` | - | Custom filesystem directory where DuckDB stores downloaded extensions. |
| `custom_extension_repository` | `str` | - | Custom HTTP or local repository URL searched when installing extensions. |
| `autoinstall_extension_repository` | `str` | - | Repository URL used specifically for automatic on-demand extension installs. |
| `allow_persistent_secrets` | `bool` | - | Allow reading and writing persistent secrets on disk. |
| `enable_external_access` | `bool` | - | Allow SQL statements to access local files, HTTP URLs, and object stores. |
| `secret_directory` | `str` | - | Filesystem directory storing persistent DuckDB secret files. |
| `enable_object_cache` | `bool` | - | Cache parsed Parquet metadata and object-store footers in memory. |
| `parquet_metadata_cache` | `bool` | - | Enable caching of Parquet file footers across repeated scans. |
| `enable_external_file_cache` | `bool` | - | Cache remote HTTP and object-store byte ranges in DuckDB's buffer pool. |
| `checkpoint_threshold` | `str` | - | WAL size threshold triggering an automatic database checkpoint (for example `'16MB'`). |
| `enable_progress_bar` | `bool` | - | Display interactive terminal progress bars during long-running queries. |
| `progress_bar_time` | `int` | - | Milliseconds elapsed before DuckDB displays the query progress bar. |
| `enable_logging` | `bool` | - | Enable DuckDB internal query and engine logging. |
| `log_query_path` | `str` | - | Filesystem path receiving logged SQL queries when logging is enabled. |
| `logging_level` | `str` | - | Minimum severity level recorded by DuckDB's internal logger. |
| `preserve_insertion_order` | `bool` | - | Preserve input row ordering during table scans and bulk operations. |
| `default_null_order` | `str` | - | Default `NULLS FIRST` or `NULLS LAST` ordering when `ORDER BY` omits null placement. |
| `default_order` | `str` | - | Default `ASC` or `DESC` sort direction when `ORDER BY` omits direction. |
| `ieee_floating_point_ops` | `bool` | - | Follow IEEE-754 floating-point division-by-zero semantics instead of returning `NULL`. |
| `binary_as_string` | `bool` | - | Interpret untyped Parquet binary columns as `VARCHAR` strings. |
| `arrow_large_buffer_size` | `bool` | - | Emit 64-bit `LargeUtf8` and `LargeBinary` Arrow buffers on Arrow exports. |
| `errors_as_json` | `bool` | - | Format DuckDB exception payloads as structured JSON strings. |
| `extra` | `dict[str, Any]` | - | Additional DuckDB configuration options merged into `config`. |
| `pool_recycle_seconds` | `int` | `86400` | Maximum connection lifetime in seconds before recycling (defaults to `86400`; `0` disables). |
| `health_check_interval` | `float` | - | Idle seconds between lightweight connection health checks. |
<!-- GENERATED:END config-table:sqlspec.adapters.duckdb.config.DuckDBConnectionParams,sqlspec.adapters.duckdb.config.DuckDBPoolParams -->

### Driver Features

Pass `DuckDBDriverFeatures` keys inside `driver_features={...}` on `DuckDBConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.duckdb.config.DuckDBDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `extensions` | `Sequence[DuckDBExtensionConfig]` | - | Extensions installed and loaded automatically when each pooled connection opens. |
| `secrets` | `Sequence[DuckDBSecretConfig]` | - | Secrets provisioned on new connections for object stores and external catalogs. |
| `on_connection_create` | `Callable[[DuckDBConnection], DuckDBConnection or None]` | - | Callback invoked when a new `DuckDBPyConnection` is opened. |
| `json_serializer` | `Callable[[Any], str]` | `sqlspec.utils.serializers.to_json` | Serializer converting dict/list parameters to JSON text (defaults to `sqlspec.utils.serializers.to_json`). |
| `enable_uuid_conversion` | `bool` | `True` | Convert `UUID` parameters to strings on bind while returning `UUID` objects from `UUID` columns (defaults to `True`). |
| `extension_flags` | `dict[str, Any]` | - | Startup-only extension security flags merged into `connection_config` before `duckdb.connect`. |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channel support. |
| `events_backend` | `Literal['poll_queue']` | - | Event queue backend name (`'poll_queue'`). |
| `enable_statement_profiling` | `bool` | `False` | Capture per-statement DuckDB JSON operator profiles on `StatementEvent.profile` (defaults to `False`). |
<!-- GENERATED:END config-table:sqlspec.adapters.duckdb.config.DuckDBDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `INTEGER` / `BIGINT` / `HUGEINT` / `UHUGEINT` | `int` | 32-bit, 64-bit, and 128-bit integer conversion. |
| `DOUBLE` / `DECIMAL` | `float` / `decimal.Decimal` | IEEE 754 floats and exact arbitrary-precision decimals. |
| `VARCHAR` / `BLOB` / `BIT` | `str` / `bytes` | UTF-8 text, binary blobs, and bitstrings. |
| `JSON` / `STRUCT` / `MAP` / `LIST` | `dict` / `list` | Nested analytical types and JSON values serialized via `json_serializer` / `json_deserializer`. |
| `TIMESTAMP` / `TIMESTAMPTZ` / `INTERVAL` | `datetime.datetime` / `datetime.timedelta` | Nanosecond/microsecond timestamps and interval durations. |
| `UUID` | `uuid.UUID` / `str` | Normalized consistently across native and Arrow execution paths when `enable_uuid_conversion=True` (default). |

- **Hybrid Native/Arrow Materialization (`ARROW_MIN_ROWS = 64`)**: Small point lookups (`< 64` observed rows) and queries returning Arrow-divergent types (`HUGEINT`, `UHUGEINT`, `BIT`, `TIMETZ`, `TIMESTAMP_NS`, `INTERVAL`, `MAP`, `BIGNUM`/`VARINT`, fixed-size arrays, or nested `UUID`) materialize directly via `cursor.fetchall()` so cache-miss and cache-hit calls always return identical DuckDB-native Python types without paying Arrow conversion overhead on 1-row reads. Larger analytical scans route through `to_arrow_table()` automatically.
- **Declarative Extensions and Cloud Secrets**: Configure `extensions` (`DuckDBExtensionConfig` dicts with `name`, `repository`, `force_install`) and `secrets` (`DuckDBSecretConfig` dicts configuring S3, GCS, Azure, R2, or HuggingFace credentials via `CREATE SECRET`, including dynamic credential rotation via `value_provider` callables).
- **Zero-Copy Arrow Reads and Writes**: Call `session.select_to_arrow(...)` (`return_format="table"`, `"batches"`, or `"reader"`) and `session.load_from_arrow(...)` to exchange columnar tables with Polars, Pandas, or PyArrow without dictionary allocations.
- **ADK Full-Text and Cosine Vector Search**: `DuckDBADKMemoryStore` supports DuckDB's `fts` extension alongside `ARRAY` cosine similarity for local RAG and agent memory persistence.

## Trusted Table Text

Builder table text must come from a trusted source. DuckDB resolves table names through replacement scans, so a
quoted name such as `from_('"data/report.csv"')` reads that file. Function text passed to `lateral_join` runs as
named, and DuckDB functions such as `read_text()` and `query()` read files or run SQL text.

To stop SQL from reaching local files, HTTP URLs, and object stores, disable external access when you configure the
connection:

```python
config = DuckDBConfig(connection_config={"database": "app.duckdb", "enable_external_access": False})
```

File reads then raise `PermissionDeniedError`. This setting does not limit `query()`, which can still run SQL against
data inside the database, so lateral function text must stay trusted either way.

## See Also

- [DuckDB API Reference](/reference/adapters/duckdb/)
- [DuckDB Dialect Reference](/reference/data-dictionary/dialects/duckdb/)
