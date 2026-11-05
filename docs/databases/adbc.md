# Apache Arrow ADBC

`sqlspec.adapters.adbc` integrates Apache Arrow Database Connectivity (`adbc-driver-manager` + `pyarrow`), exchanging query results and bulk table loads directly over the C-level Arrow C Data Interface (`__arrow_c_stream__`) without row-by-row Python object marshaling.

## Overview & Capabilities

Choose `AdbcConfig` when your primary workload extracts or ingests Arrow, Polars, Pandas, or Parquet datasets across analytical warehouses, or when you need pure-Go zero-client wire drivers for Oracle or IBM Db2 on Linux ARM64.

| Target Engine | PyPI Package | Supported URI Schemes | Short `driver` Aliases |
| --- | --- | --- | --- |
| PostgreSQL / AlloyDB | `adbc-driver-postgresql` | `postgresql://`, `postgres://` | `"postgres"`, `"postgresql"`, `"pg"` |
| SQLite | `adbc-driver-sqlite` | `sqlite://`, `file:app.db`, `":memory:"` | `"sqlite"`, `"sqlite3"` |
| DuckDB (In-Process) | `duckdb` | `duckdb://`, `duckdb:///path.db` | `"duckdb"` |
| DuckDB Quack (Remote gRPC) | `adbc-driver-quack` | `quack+tls://`, `quack://` | `"quack"` |
| GizmoSQL / Arrow FlightSQL | `adbc-driver-gizmosql` or `adbc-driver-flightsql` | `grpc+tls://`, `grpc://`, `gizmosql://` | `"flightsql"`, `"gizmosql"`, `"grpc"`, `"gizmo"` |
| Snowflake | `adbc-driver-snowflake` | `snowflake://` | `"snowflake"`, `"sf"` |
| Google Cloud BigQuery | `adbc-driver-bigquery` | `bigquery://` | `"bigquery"`, `"bq"` |
| Oracle Database | `adbc-driver-oracle` | `oracle://` (pure Go wire driver) | `"oracle"`, `"oracledb"` |
| IBM Db2 | `adbc-driver-db2` | `db2://` (pure Go DRDA wire driver) | `"db2"`, `"ibmdb2"`, `"ibm_db"` |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[adbc]" adbc-driver-postgresql
    ```

=== "pip"

    ```bash
    pip install "sqlspec[adbc]" adbc-driver-postgresql
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[adbc]" adbc-driver-postgresql
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[adbc]" adbc-driver-postgresql
    ```

=== "URI Scheme Resolution"

    ```python
    from sqlspec import SQLSpec, StatementConfig
    from sqlspec.adapters.adbc import AdbcConfig

    spec = SQLSpec()
    config = spec.add_config(
        AdbcConfig(
            connection_config={
                "uri": "postgresql://app:secret@localhost:5432/prod"
            },
            statement_config=StatementConfig(dialect="postgres"),
        )
    )
    ```

=== "Short Driver Alias"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.adbc import AdbcConfig

    spec = SQLSpec()
    config = spec.add_config(
        AdbcConfig(
            connection_config={
                "driver": "duckdb",
                "path": "analytics.duckdb",
            }
        )
    )
    ```

## Connection & Pool Configuration

### PostgreSQL Connection Reuse

| Driver | Underlying Engine | Arrow Path | Pool / Reuse Model |
| --- | --- | --- | --- |
| `AdbcConfig` (`driver="postgres"`) | `adbc-driver-postgresql` (`>= 1.12.0`) | C Data Interface (`__arrow_c_stream__` + binary `COPY` ingest) | Optional `AdbcPostgresConnectionPool` via `pool_config={"max_size": 4, "timeout": 30.0}` (`DISCARD ALL` reset on return; `pool_config=None` opens per-session connections). |
| `AdbcConfig` (`duckdb`, `sqlite`, `snowflake`, `bigquery`, `oracle`, `db2`, `flightsql`) | Respective ADBC C/Go/C++ driver | C Data Interface (`__arrow_c_stream__` + `adbc_ingest`) | Per-session connection (or caller-managed `connection_instance`); supports dynamic `auth_token_provider` in `connection_config`. |
| [`ArrowOdbcConfig`](arrow-odbc.md) | Rust `arrow-odbc` over unixODBC / Windows ODBC | Pre-allocated Rust ODBC columnar batch buffers | Process-wide ODBC Driver Manager connection pooling (`driver_features={"enable_pooling": True}`). |

### Connection and Pool Parameters

Pass `AdbcConnectionParams` keys (including `auth_token_provider`) inside `connection_config={...}` and `AdbcPoolConfig` keys inside `pool_config={...}` on `AdbcConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.adbc.config.AdbcConnectionParams,sqlspec.adapters.adbc.config.AdbcPoolConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `uri` | `str` | - | Backend connection URI (such as `postgresql://`, `sqlite://`, `duckdb://`, `grpc+tls://`, `oracle://`, `db2://`, or `quack://`). |
| `dsn` | `str` | - | Connection string alias normalized to `uri` when `uri` is unset. |
| `url` | `str` | - | Connection URL alias normalized to `uri` when `uri` is unset. |
| `connection_string` | `str` | - | Connection string alias normalized to `uri` when `uri` is unset. |
| `db` | `str` | - | Database path or identifier alias normalized for file-backed and embedded ADBC drivers. |
| `path` | `str` | - | Filesystem database path for `adbc-driver-sqlite` or `adbc-driver-duckdb`. |
| `driver` | `str` | - | Short driver alias (such as `'postgresql'`, `'sqlite'`, `'duckdb'`, `'flightsql'`, `'gizmosql'`, `'snowflake'`, `'bigquery'`, `'oracle'`, `'db2'`, or `'quack'`) or shared library path. |
| `driver_name` | `str` | - | Explicit ADBC Python module connect path or shared library name overriding URI inference. |
| `db_kwargs` | `dict[str, Any]` | - | Driver-specific `adbc_driver_manager.AdbcDatabase` options passed to `dbapi.connect`. |
| `conn_kwargs` | `dict[str, Any]` | - | Driver-specific `adbc_driver_manager.AdbcConnection` options passed to `dbapi.connect`. |
| `entrypoint` | `str` | - | C symbol entrypoint name when loading a shared-object ADBC driver directly. |
| `profile` | `str` | - | Named credential or configuration profile forwarded to cloud ADBC drivers. |
| `auth_type` | `str` | - | Authentication mechanism (for example `'external'` OAuth/SSO on `adbc-driver-gizmosql` or Snowflake auth modes). |
| `adbc_driver_manager_entrypoint` | `str` | - | Explicit `adbc_driver_manager` C initialization entrypoint symbol. |
| `autocommit` | `bool` | - | Enable autocommit mode on newly opened DB-API ADBC connections. |
| `isolation_level` | `str` | - | Transaction isolation level applied to the ADBC connection. |
| `batch_size` | `int` | - | Target Arrow `RecordBatch` row count during streaming reads and ingests. |
| `query_timeout` | `float` | - | Per-statement execution timeout in seconds forwarded to supporting ADBC backends. |
| `connection_timeout` | `float` | - | Connection establishment timeout in seconds. |
| `ssl_mode` | `str` | - | TLS verification mode forwarded to PostgreSQL and compatible ADBC drivers. |
| `ssl_cert` | `str` | - | Client TLS certificate path for mutual TLS connections. |
| `ssl_key` | `str` | - | Client TLS private key path paired with `ssl_cert`. |
| `ssl_ca` | `str` | - | Root CA bundle path used to verify server TLS certificates. |
| `username` | `str` | - | Database authentication username lifted into `db_kwargs` or `connect()` as required by the driver. |
| `password` | `str` | - | Database authentication password lifted into `db_kwargs` or `connect()` as required by the driver. |
| `token` | `str` | - | Bearer, OAuth refresh, or Quack/FlightSQL authentication token. |
| `auth_token_provider` | `Callable[[], str]` | - | Callable returning a fresh password or bearer token on each `create_connection` call. |
| `project_id` | `str` | - | Google Cloud project ID mapped to `adbc.bigquery.sql.project_id` for `adbc-driver-bigquery`. |
| `dataset_id` | `str` | - | Default BigQuery dataset ID mapped to `adbc.bigquery.sql.dataset_id`. |
| `account` | `str` | - | Snowflake account identifier for `adbc-driver-snowflake`. |
| `warehouse` | `str` | - | Snowflake virtual warehouse name activated on connect. |
| `database` | `str` | - | Target catalog or database name for Snowflake, PostgreSQL, or FlightSQL backends. |
| `schema` | `str` | - | Default schema name for Snowflake or multi-schema ADBC backends. |
| `role` | `str` | - | Session role activated on connect for Snowflake or role-aware backends. |
| `authorization_header` | `str` | - | Explicit HTTP/gRPC `Authorization` header value for FlightSQL and GizmoSQL servers. |
| `grpc_options` | `dict[str, Any]` | - | Low-level gRPC channel options forwarded to FlightSQL, GizmoSQL, or Quack transports. |
| `grpc_max_message_size` | `int` | - | Maximum inbound gRPC message size in bytes for Arrow Flight transfers. |
| `fetch_timeout` | `float` | - | Timeout in seconds when fetching Arrow Flight result streams. |
| `gizmosql_backend` | `str` | - | Underlying SQL engine dialect hint when connecting through `adbc-driver-gizmosql` or `adbc-driver-flightsql`. |
| `tls_skip_verify` | `bool` | - | Disable server certificate verification on TLS-enabled FlightSQL or GizmoSQL endpoints. |
| `tls_root_certs` | `str` | - | PEM-encoded root certificate bundle for FlightSQL or GizmoSQL TLS verification. |
| `mtls_cert_chain` | `str` | - | PEM-encoded client certificate chain for FlightSQL or GizmoSQL mTLS. |
| `mtls_private_key` | `str` | - | PEM-encoded client private key paired with `mtls_cert_chain`. |
| `with_cookie_middleware` | `bool` | - | Enable stateful session cookie middleware on FlightSQL/GizmoSQL connections. |
| `headers` | `dict[str, str]` | - | Custom gRPC metadata headers attached to FlightSQL or GizmoSQL requests. |
| `extra` | `dict[str, Any]` | - | Additional driver options merged into `db_kwargs`. |
| `max_size` | `int` | - | Maximum simultaneous clone leases, excluding the original seed (default 4). |
| `timeout` | `float` | - | Maximum seconds to wait for a lease (default 30.0). |
<!-- GENERATED:END config-table:sqlspec.adapters.adbc.config.AdbcConnectionParams,sqlspec.adapters.adbc.config.AdbcPoolConfig -->

### Driver Features

Pass `AdbcDriverFeatures` keys inside `driver_features={...}` on `AdbcConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.adbc.config.AdbcDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `json_serializer` | `Callable[[Any], str]` | `sqlspec.utils.serializers.to_json` | Serializer converting dict/list parameters to JSON text (defaults to `sqlspec.utils.serializers.to_json`). |
| `enable_cast_detection` | `bool` | `True` | Detect explicit SQL casts such as `::jsonb` or `::vector` during parameter preparation (defaults to `True`). |
| `enable_strict_type_coercion` | `bool` | `False` | Raise an error on unsupported parameter types instead of best-effort conversion (defaults to `False`). |
| `enable_arrow_extension_types` | `bool` | `True` | Preserve PyArrow extension type metadata when materializing Arrow results (defaults to `True`). |
| `enable_pgvector` | `bool` | `True` | Probe `pg_extension` for `vector` on PostgreSQL ADBC connections (defaults to `True` when `pgvector` is installed). |
| `enable_paradedb` | `bool` | `True` | Probe `pg_extension` for ParadeDB `pg_search` on PostgreSQL ADBC connections (defaults to `True`). |
| `enable_pg_textsearch` | `bool` | `True` | Probe `pg_extension` for `pg_textsearch` BM25 support on PostgreSQL ADBC connections (defaults to `True`). |
| `enable_lowercase_column_names` | `bool` | `True` | Normalize unquoted uppercase column names to lowercase (defaults to `True` on Oracle, Db2, and Snowflake ADBC backends). |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channel support. |
| `on_connection_create` | `Callable[[AdbcConnection], None]` | - | Callback invoked with the raw ADBC connection immediately after `connect()`. |
| `events_backend` | `Literal['poll_queue']` | - | Event queue backend name (`'poll_queue'`). |
| `enable_statement_profiling` | `bool` | `False` | Capture per-statement DuckDB JSON operator profiles on `StatementEvent.profile` when using DuckDB ADBC (defaults to `False`). |
<!-- GENERATED:END config-table:sqlspec.adapters.adbc.config.AdbcDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| Arrow `int8` .. `int64` | `int` | Zero-copy Arrow integer arrays (`select_to_arrow`) or Python `int` in row queries. |
| Arrow `float32` / `float64` / `decimal128` | `float` / `decimal.Decimal` | Enforced via `enable_strict_type_coercion` during parameter and ingest conversion. |
| Arrow `utf8` / `large_utf8` / `binary` | `str` / `bytes` | Columnar string and binary buffers. |
| `JSON` / `JSONB` | `dict` / `list` | Serialized and deserialized via `json_serializer` and `json_deserializer`. |
| `VECTOR` (`pgvector`) | `list[float]` / `numpy.ndarray` | Bound via `?::text::vector` when `enable_pgvector=True` on PostgreSQL ADBC connections. |
| `TIMESTAMP` / `DATE` | `datetime.datetime` / `datetime.date` | Arrow timestamp arrays with timezone metadata preserved. |

- **Zero-Copy adbc_ingest**: `session.load_from_arrow(table, arrow_table_or_reader)` passes `pyarrow.Table` and `pyarrow.RecordBatchReader` objects straight to `cursor.adbc_ingest` over `__arrow_c_stream__` without materializing intermediate rows.
- **Cross-Engine Placeholder Rewriting**: Although `adbc_driver_manager` binds parameters as positional Arrow `RecordBatch` columns, SQLSpec translates `:named` parameters to `$1` (`postgres`), `:1` (`oracle`), or `?` (`sqlite`, `duckdb`, `db2`) automatically.
- **DuckDB ADBC COPY TO Parameters**: DuckDB over ADBC only inlines a bound parameter in `COPY (...) TO :output_path` when `:output_path` is the statement's sole parameter. When combining `COPY` with filter parameters, set filter values via `SET VARIABLE cutoff = :val` beforehand and reference `getvariable('cutoff')` inside the query.
- **Enterprise Error and Savepoint Parity**: `AdbcDriver` translates Oracle `ORA-xxxxx` and Db2 `SQLCODE` errors into SQLSpec exceptions and emits dialect-native savepoints (`ON ROLLBACK RETAIN CURSORS` on Db2).

## See Also

- [ADBC API Reference](/reference/adapters/adbc/)
- [Arrow ODBC Guide](arrow-odbc.md)
- [PostgreSQL Dialect Reference](/reference/data-dictionary/dialects/postgres/)
- [DuckDB Dialect Reference](/reference/data-dictionary/dialects/duckdb/)
- [BigQuery Dialect Reference](/reference/data-dictionary/dialects/bigquery/)
- [Oracle Dialect Reference](/reference/data-dictionary/dialects/oracle/)
- [IBM Db2 Dialect Reference](/reference/data-dictionary/dialects/db2/)
