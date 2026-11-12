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
| Teradata Vantage | `dbc install teradata` (Columnar `dbc` CLI + Teradata Tools & Utilities `TTU` ODBC/CLI libraries) or `adbc-driver-flightsql` (`statement_config=StatementConfig(dialect="teradata")`) | `teradata://`, `grpc+tls://`, `grpc://` | `"teradata"`, `"flightsql"` |

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
| `AdbcConfig` (`driver="postgres"`) | `adbc-driver-postgresql` | C Data Interface (`__arrow_c_stream__` + binary `COPY` ingest) | Optional `AdbcConnectionPool` via `pool_config={"max_size": 4, "timeout": 30.0}` (`DISCARD ALL` reset on return; `pool_config=None` opens per-session connections). |
| `AdbcConfig` (`duckdb`, `sqlite`, `snowflake`, `bigquery`, `oracle`, `db2`, `teradata`, `flightsql`) | Respective ADBC C/Go/C++ driver | C Data Interface (`__arrow_c_stream__` + `adbc_ingest`) | Per-session connection (or caller-managed `connection_instance`); supports dynamic `auth_token_provider` in `connection_config`. |
| [`ArrowOdbcConfig`](arrow-odbc.md) | Rust `arrow-odbc` over unixODBC / Windows ODBC | Pre-allocated Rust ODBC columnar batch buffers | Process-wide ODBC Driver Manager connection pooling (`driver_features={"enable_pooling": True}`). |

### Snowflake Authentication and Stage SQL

Install `sqlspec[adbc]` and `adbc-driver-snowflake` to use Snowflake through the Arrow driver.
Snowflake opens a connection for each session; close sessions with `config.provide_session()`.
For the native connector, pooling, and staged Parquet uploads, see the [Snowflake guide](snowflake.md).

=== "Access token"

    ```python
    import os

    from sqlspec.adapters.adbc import AdbcConfig


    def load_token() -> str:
        return os.environ["SNOWFLAKE_TOKEN"]


    config = AdbcConfig(
        connection_config={
            "driver": "snowflake",
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "warehouse": "COMPUTE_WH",
            "auth_type": "pat",
            "auth_token_provider": load_token,
            "login_timeout": 30,
            "query_tag": "orders-etl",
        }
    )
    with config.provide_session() as session:
        rows = session.select("SELECT :status AS status", status="ready")
    ```

    Use `auth_type="oauth"` with the same token provider for OAuth.
    The provider runs for each new connection.
    Token modes refresh the authentication token without replacing a configured password.

=== "Workload identity"

    ```python
    import os

    from sqlspec.adapters.adbc import AdbcConfig

    config = AdbcConfig(
        connection_config={
            "driver": "snowflake",
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "username": "SVC_ORDERS",
            "auth_type": "workload_identity",
            "identity_provider": "AWS",
        }
    )
    ```

    Choose the identity provider configured for your Snowflake account.
    `auth_token_provider` can supply a refreshed token when your identity setup requires one.

=== "Key pair"

    ```python
    import os
    from pathlib import Path

    from sqlspec.adapters.adbc import AdbcConfig

    config = AdbcConfig(
        connection_config={
            "driver": "snowflake",
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "username": "SVC_ORDERS",
            "private_key": Path(os.environ["SNOWFLAKE_KEY_FILE"]),
            "private_key_passphrase": os.environ["SNOWFLAKE_KEY_PASSPHRASE"],
        }
    )
    ```

    `private_key` accepts PEM text, a path, DER bytes, or a `cryptography` RSA private key.
    Supplying a key selects JWT authentication unless you explicitly choose another mode.

Numeric timeouts use seconds: `login_timeout=30` becomes `"30s"`, and `request_timeout=0.5` becomes `"500ms"`.
You can also pass Go duration strings such as `"2m"`.
A `snowflake://` URI prefix is removed before the driver reads its connection string.
Explicit `db_kwargs` entries take precedence over convenience fields.
`use_high_precision` keeps the upstream default unless you set it; set it to `False` to request lower-precision integer results.

Snowflake uses `?` wire parameters. SQLSpec rewrites named placeholders while preserving stage references, session variables, and pipe result ordinals:

```sql
SELECT t.$1 FROM @orders_stage/events t WHERE t.$2 = :status;
SHOW WAREHOUSES ->> SELECT "name" FROM $1 WHERE "state" = :state;
SELECT * FROM IDENTIFIER($orders_table) WHERE order_id = :order_id;
```

Only `:status`, `:state`, and `:order_id` are bind parameters in these examples.
Literal URLs, `//` comments, and `$$...$$` scripting blocks retain their contents.
Set `query_tag` for a connection tag; SQLSpec also passes the tag through the ADBC statement option `adbc.snowflake.statement.query_tag`.
Active correlation context augments the statement tag.
Snowflake telemetry uses `db.system="snowflake"`.

### Connection and Pool Parameters

Pass `AdbcConnectionParams` keys (including `auth_token_provider`) inside `connection_config={...}` and `AdbcPoolConfig` keys inside `pool_config={...}` on `AdbcConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.adbc.config.AdbcConnectionParams,sqlspec.adapters.adbc.config.AdbcPoolConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `str` | - | Teradata server hostname or IP address. |
| `port` | `int` | - | Teradata server port, defaulting to 1025. |
| `user` | `str` | - | Alias for the Teradata connection username. |
| `adbc_dialect` | `str` | - | Explicit database dialect for generic FlightSQL transports. |
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
| `region` | `str` | - | Snowflake cloud region identifier. |
| `host` | `str` | - | Snowflake or proxy endpoint hostname. |
| `port` | `int or str` | - | Snowflake endpoint port. |
| `protocol` | `str` | - | Snowflake endpoint transport protocol. |
| `application` | `str` | - | Client application identifier. |
| `keep_session_alive` | `bool` | - | Keep the Snowflake session active during idle intervals. |
| `login_timeout` | `int or float or str` | - | Authentication timeout in seconds or a Go duration string. |
| `request_timeout` | `int or float or str` | - | HTTP request timeout in seconds or a Go duration string. |
| `client_timeout` | `int or float or str` | - | Overall client timeout in seconds or a Go duration string. |
| `ocsp_fail_open_mode` | `bool` | - | Allow connections when the certificate status responder is unreachable. |
| `use_high_precision` | `bool` | - | Return fixed-point Snowflake numbers as lossless Arrow decimal values by default. |
| `okta_url` | `str` | - | Okta authentication endpoint URL. |
| `identity_provider` | `str` | - | Workload identity provider name. |
| `workload_identity_provider` | `str` | - | Alias for identity_provider. |
| `private_key` | `str or bytes or Path or Any` | - | JWT private key as PEM text, DER bytes, a path, or a private key object. |
| `private_key_passphrase` | `str or bytes` | - | Passphrase for an encrypted private key. |
| `jwt_private_key` | `str or Path` | - | Filesystem path to a private key. |
| `jwt_private_key_pkcs8_value` | `str` | - | In-memory PKCS8 private key text. |
| `jwt_private_key_pkcs8_password` | `str` | - | Passphrase for the in-memory private key. |
| `jwt_expire_timeout` | `int or float or str` | - | JWT lifetime in seconds or a Go duration string. |
| `query_tag` | `str` | - | Default Snowflake query tag for session and statement correlation. |
| `session_parameters` | `Mapping[str, Any]` | - | Additional Snowflake session parameters. |
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
| `query_tag` | `str` | - | Default Snowflake statement tag combined with the current correlation ID. |
| `enable_statement_profiling` | `bool` | `False` | Capture per-statement DuckDB JSON operator profiles on `StatementEvent.profile` when using DuckDB ADBC (defaults to `False`). |
<!-- GENERATED:END config-table:sqlspec.adapters.adbc.config.AdbcDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| Arrow `int8` .. `int64` | `int` | Zero-copy Arrow integer arrays (`select_to_arrow`) or Python `int` in row queries. |
| Arrow `float32` / `float64` / `decimal128` | `float` / `decimal.Decimal` | Enforced via `enable_strict_type_coercion` during parameter and ingest conversion. |
| Arrow `utf8` / `large_utf8` / `binary` | `str` / `bytes` | Columnar string and binary buffers. |
| `JSON` / `JSONB` | `dict` / `list` | Serialized and deserialized via `json_serializer` and `json_deserializer`. |
| `VECTOR` (`pgvector`) | `list[float]` / `numpy.ndarray` | Supported on PostgreSQL ADBC connections when `enable_pgvector=True` (defaults to `True` when `pgvector` is installed); explicit `::vector` casts are detected by `enable_cast_detection`. |
| `TIMESTAMP` / `DATE` | `datetime.datetime` / `datetime.date` | Arrow timestamp arrays with timezone metadata preserved. |

- **Zero-Copy adbc_ingest**: `session.load_from_arrow(table, arrow_table_or_reader)` passes `pyarrow.Table` and `pyarrow.RecordBatchReader` objects straight to `cursor.adbc_ingest` over `__arrow_c_stream__` without materializing intermediate rows.
- **Cross-Engine Placeholder Rewriting**: Although `adbc_driver_manager` binds parameters as positional Arrow `RecordBatch` columns, SQLSpec translates `:named` parameters to `$1` (`postgres`), `:1` (`oracle`), or `?` `qmark` (`sqlite`, `duckdb`, `db2`, `snowflake`, `teradata`) automatically. Snowflake stage references and session variables retain their native meaning.
- **DuckDB ADBC COPY TO Parameters**: DuckDB over ADBC only inlines a bound parameter in `COPY (...) TO :output_path` when `:output_path` is the statement's sole parameter. When combining `COPY` with filter parameters, set filter values via `SET VARIABLE cutoff = :val` beforehand and reference `getvariable('cutoff')` inside the query.
- **Enterprise Error and Savepoint Parity**: `AdbcDriver` translates Oracle `ORA-xxxxx`, Db2 `SQLCODE`, six-digit Snowflake error codes, and Teradata `[Error NNNN]` errors into SQLSpec exceptions and emits dialect-native savepoints (`ON ROLLBACK RETAIN CURSORS` on Db2; Snowflake and Teradata savepoints raise `NotImplementedError`).

## Teradata

Teradata ADBC uses Columnar's `dbc` driver manifest (`dbc install teradata`) rather than a PyPI `adbc-driver-teradata` package. Install the Teradata driver and its required Teradata Tools & Utilities (`TTU`) ODBC/CLI native libraries through your approved `dbc` installation, then use a `teradata://` URI with `AdbcConfig`. Keep credentials out of logged URIs.

```python
import os

from sqlspec.adapters.adbc import AdbcConfig

config = AdbcConfig(
    connection_config={
        "driver": "teradata",
        "uri": os.environ["SQLSPEC_TERADATA_ADBC_URI"],
    }
)
with config.provide_session() as session:
    rows = session.select(
        "SELECT order_id FROM orders WHERE customer_id = ?", (42,)
    )
```

For an Arrow Flight SQL endpoint (`adbc-driver-flightsql`), select the Flight SQL driver and explicitly provide `statement_config=StatementConfig(dialect="teradata")`. The SQL dialect controls `?` (`qmark`) parameter handling, catalog SQL fallback, and extension DDL. Flight SQL endpoint credentials and TLS settings belong in the driver's connection options.

Columnar Teradata driver v0.1.1 supports autocommit only. Explicit `begin()` raises `TransactionError` when the driver refuses transaction mode; savepoints are unavailable. Native Arrow ingestion depends on the installed driver and server capabilities. The native [Teradata adapter](teradata.md) is available when DB-API transaction support is required.

Live ADBC tests use `SQLSPEC_TERADATA_ADBC_URI`; Flight SQL tests use `SQLSPEC_TERADATA_FLIGHTSQL_URI`. Both skip when their URI is unset.

## See Also

- [ADBC API Reference](/reference/adapters/adbc/)
- [Arrow ODBC Guide](arrow-odbc.md)
- [Teradata Vantage](teradata.md)
- [PostgreSQL Dialect Reference](/reference/data-dictionary/dialects/postgres/)
- [DuckDB Dialect Reference](/reference/data-dictionary/dialects/duckdb/)
- [BigQuery Dialect Reference](/reference/data-dictionary/dialects/bigquery/)
- [Oracle Dialect Reference](/reference/data-dictionary/dialects/oracle/)
- [IBM Db2 Dialect Reference](/reference/data-dictionary/dialects/db2/)
- [Snowflake Dialect Reference](/reference/data-dictionary/dialects/snowflake/)
- [Teradata Data Dictionary Catalog](../reference/data-dictionary/dialects/teradata.md)
