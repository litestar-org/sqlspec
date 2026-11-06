# Arrow ODBC

`sqlspec.adapters.arrow_odbc` wraps the Rust-powered `arrow-odbc` bridge (`>= 10.4` + `pyarrow`), streaming columnar Apache Arrow record batches directly from existing ODBC drivers for Microsoft SQL Server (`tsql`), PostgreSQL (`postgres`), and IBM Db2 (`db2`).

## Overview & Capabilities

Choose `ArrowOdbcConfig` when your environment standardizes on enterprise unixODBC or Windows ODBC drivers (such as `ODBC Driver 18 for SQL Server`, `psqlodbc`, or IBM Db2 `libdb2o.so`) and needs fast, bounded-memory Arrow batch extraction without row-by-row `pyodbc` tuple overhead.

| Capability | Details |
| --- | --- |
| **Adapter Config** | `ArrowOdbcConfig` (`sqlspec.adapters.arrow_odbc`) |
| **Driver Extra** | `sqlspec[arrow-odbc]` (`arrow-odbc >= 10.4`, `pyarrow`) |
| **Execution Mode** | Synchronous (`NoPoolSyncConfig` with process-wide ODBC Driver Manager pooling) |
| **Supported Dialects** | Auto-detected from ODBC `DBMS_NAME`: `tsql` (SQL Server), `postgres` (PostgreSQL), `db2` (IBM Db2) |
| **Arrow Streaming** | Double-buffered concurrent Rust ODBC batch reader (`chunk_size`, `max_bytes_per_batch`, `fetch_concurrently`) |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[arrow-odbc]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[arrow-odbc]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[arrow-odbc]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[arrow-odbc]"
    ```

```python
from sqlspec import SQLSpec, StatementConfig
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

spec = SQLSpec()
config = spec.add_config(
    ArrowOdbcConfig(
        connection_config={
            "connection_string": (
                "Driver={ODBC Driver 18 for SQL Server};"
                "Server=localhost,1433;"
                "Database=analytics;"
                "UID=sa;PWD=StrongPassword123;"
                "TrustServerCertificate=yes;"
            )
        },
        statement_config=StatementConfig(dialect="tsql"),
        driver_features={
            "enable_pooling": True,
            "chunk_size": 65536,
            "fetch_concurrently": True,
        },
    )
)
```

## Connection & Pool Configuration

### Connection Parameters

Pass `ArrowOdbcConnectionParams` keys inside `connection_config={...}` on `ArrowOdbcConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.arrow_odbc.config.ArrowOdbcConnectionParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `connection_string` | `str` | - | Full ODBC `KEY=VALUE;...` connection string overridden by explicit keyword fields. |
| `dsn` | `str` | - | Registered ODBC Data Source Name (`DSN=...`). |
| `driver` | `str` | - | Installed ODBC driver name or library path (`Driver={...}`). |
| `server` | `str` | - | Target database server address or `host,port` endpoint. |
| `host` | `str` | - | Hostname alias used when `server` is not specified. |
| `database` | `str` | - | Target catalog or database name (`Database=...`). |
| `uid` | `str` | - | ODBC authentication user identifier (`UID=...`). |
| `pwd` | `str` | - | ODBC authentication password (`PWD=...`). |
| `user` | `str` | - | Alias normalized to `uid` when building the ODBC connection string. |
| `password` | `str` | - | Alias normalized to `pwd` when building the ODBC connection string. |
| `trusted_connection` | `bool or str` | - | Use integrated OS or Kerberos authentication (`Trusted_Connection`). |
| `trust_server_certificate` | `bool or str` | - | Trust self-signed server TLS certificates without CA chain validation. |
| `encrypt` | `bool or str` | - | Require or configure TLS encryption on the ODBC connection. |
| `login_timeout` | `int` | - | Connection login timeout in seconds forwarded to `arrow_odbc.connect`. |
| `login_timeout_sec` | `int` | - | Alias for `login_timeout` in seconds. |
| `packet_size` | `int` | - | Network packet size in bytes negotiated by the ODBC driver. |
| `autocommit` | `bool` | `True` | Initial connection autocommit state (defaults to `True`). |
| `extra` | `dict[str, Any]` | - | Additional ODBC connection-string attributes merged verbatim. |
<!-- GENERATED:END config-table:sqlspec.adapters.arrow_odbc.config.ArrowOdbcConnectionParams -->

### Driver Features

Pass `ArrowOdbcDriverFeatures` keys inside `driver_features={...}` on `ArrowOdbcConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.arrow_odbc.config.ArrowOdbcDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `chunk_size` | `int` | `65536` | Maximum rows per Arrow `RecordBatch` fetched from ODBC cursors (defaults to `65536`). |
| `max_bytes_per_batch` | `int` | - | Upper memory budget in bytes per Arrow `RecordBatch` buffer. |
| `max_text_size` | `int` | - | Maximum byte length allocated for unbounded `VARCHAR(MAX)` / `TEXT` columns. |
| `max_binary_size` | `int` | - | Maximum byte length allocated for unbounded `VARBINARY(MAX)` / `BLOB` columns. |
| `fetch_concurrently` | `bool` | - | Prefetch the next ODBC batch on a background Rust thread while Arrow batches are consumed. |
| `query_timeout_sec` | `int` | - | Per-statement ODBC query execution timeout in seconds. |
| `payload_text_encoding` | `TextEncoding` | - | Wire text encoding override (`'auto'`, `'utf8'`, or `'utf16'`) passed to `arrow-odbc`. |
| `enable_pooling` | `bool` | `False` | Enable process-global ODBC driver-manager connection pooling prior to first connect (defaults to `False`). |
| `connection_string` | `str` | - | Resolved connection string inspected for SQL dialect auto-detection. |
| `dbms_name` | `str` | - | Database engine hint (such as `'PostgreSQL'`, `'Microsoft SQL Server'`, `'Db2'`, or `'SQLite'`) selecting the SQLGlot dialect. |
| `connection_autocommit` | `bool` | - | Effective autocommit baseline derived from `connection_config['autocommit']`. |
| `enable_lowercase_column_names` | `bool` | `True` | Normalize unquoted uppercase column names to lowercase (defaults to `True` on Db2 and `False` elsewhere). |
| `json_serializer` | `Callable[[Any], str]` | `sqlspec.utils.serializers.to_json` | Serializer converting dict/list parameters to JSON text (defaults to `sqlspec.utils.serializers.to_json`). |
| `json_deserializer` | `Callable[[str], Any]` | `sqlspec.utils.serializers.from_json` | Deserializer parsing JSON text payloads (defaults to `sqlspec.utils.serializers.from_json`). |
| `enable_events` | `bool` | - | Enable SQLSpec table-backed event channel support. |
| `events_backend` | `Literal['poll_queue']` | - | Event queue backend name (`'poll_queue'`). |
| `on_connection_create` | `Callable[[ArrowOdbcConnection], None]` | - | Callback invoked with the raw `arrow_odbc.Connection` immediately after connect. |
<!-- GENERATED:END config-table:sqlspec.adapters.arrow_odbc.config.ArrowOdbcDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `INT` / `BIGINT` / `SMALLINT` | `int` | Extracted into typed Arrow integer arrays and converted for dict queries. |
| `DECIMAL` / `NUMERIC` / `FLOAT` | `decimal.Decimal` / `float` | Preserves ODBC precision and scale metadata in Arrow `decimal128` columns. |
| `VARCHAR(MAX)` / `NVARCHAR(MAX)` | `str` | Bounded by `max_text_size` buffer allocation per column in `driver_features`. |
| `VARBINARY(MAX)` / `BLOB` | `bytes` | Bounded by `max_binary_size` buffer allocation per column in `driver_features`. |
| `JSON` / `NVARCHAR` (JSON) | `dict` / `list` | Serialized and deserialized via `json_serializer` and `json_deserializer`. |
| `DATETIME2` / `TIMESTAMP` / `DATE` | `datetime.datetime` / `datetime.date` | Native Arrow timestamp and date column buffers. |

- **Automatic Dialect Resolution**: If `StatementConfig(dialect=...)` is not passed explicitly, `ArrowOdbcDriver` inspects the ODBC `DBMS_NAME` metadata (`resolve_dialect_from_dbms_name`) to select `tsql`, `postgres`, or `db2` SQLGlot dialect rules automatically.
- **64-Bit Linux unixODBC with IBM Db2**: On 64-bit Linux, point `Driver=` at `libdb2o.so` (64-bit `SQLLEN`) rather than `libdb2.so` (legacy 32-bit `SQLLEN`) to avoid ODBC buffer width mismatches.
- **Lexical T-SQL Pagination Inlining**: ODBC drivers for SQL Server reject bound `?` parameters in certain `TOP (?)` and `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` positions. `ArrowOdbcDriver` uses a lexical SQLGlot T-SQL tokenizer to inline validated integer pagination bounds in exact source token order without disturbing CTE parameter ordinals.
- **Batched DML (`execute_many`)**: Because the underlying Rust `arrow-odbc` bridge focuses on columnar reads and does not expose DB-API rowcounts on statement execution, `execute_many` reports `rows_affected == -1` while preserving SQL expressions (`DEFAULT`, `CURRENT_TIMESTAMP`, `CAST`).

## See Also

- [Arrow ODBC API Reference](/reference/adapters/arrow-odbc/)
- [Apache Arrow ADBC Guide](adbc.md)
- [SQL Server Dialect Reference](/reference/data-dictionary/dialects/mssql/)
- [PostgreSQL Dialect Reference](/reference/data-dictionary/dialects/postgres/)
- [IBM Db2 Dialect Reference](/reference/data-dictionary/dialects/db2/)
