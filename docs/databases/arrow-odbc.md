# Arrow ODBC

`sqlspec.adapters.arrow_odbc` wraps the Rust-powered `arrow-odbc` bridge (`>= 10.4` + `pyarrow`), streaming columnar Apache Arrow record batches directly from existing ODBC drivers for Microsoft SQL Server (`tsql`), PostgreSQL (`postgres`), and IBM Db2 (`db2`).

## Install and When to Choose

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

Choose `ArrowOdbcConfig` when your environment standardizes on enterprise unixODBC / Windows ODBC drivers (such as `ODBC Driver 18 for SQL Server`, `psqlodbc`, or IBM Db2 `libdb2o.so`) and needs fast, bounded-memory Arrow batch extraction without row-by-row `pyodbc` tuple overhead.

## Minimal Connection and Pool Setup

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

## Key Driver Features and Pool Tuning

`ArrowOdbcConfig` subclasses `NoPoolSyncConfig` and delegates connection reuse to ODBC Driver Manager pooling when `enable_pooling=True`:

| Setting | Location | Description |
| --- | --- | --- |
| `connection_string` | `connection_config` | Standard semicolon-delimited ODBC connection string (`Driver={...};Server=...;`). |
| `user` / `password` | `connection_config` | Optional credentials passed alongside the ODBC connection string. |
| `enable_pooling` | `driver_features` | Enables process-wide ODBC Driver Manager connection pooling for low-latency session checkouts. |
| `chunk_size` / `max_bytes_per_batch` | `driver_features` | Maximum rows and memory bytes allocated per Arrow `RecordBatch` buffer during streaming reads. |
| `fetch_concurrently` | `driver_features` | Allocates double-buffered background ODBC fetch threads in Rust while Python processes the current Arrow batch. |
| `max_text_size` / `max_binary_size` | `driver_features` | Upper byte bounds for unbounded `VARCHAR(MAX)` / `NVARCHAR(MAX)` / `VARBINARY(MAX)` columns. |
| `query_timeout_sec` | `driver_features` | Per-statement ODBC execution timeout in seconds. |
| `enable_lowercase_column_names` | `driver_features` | Normalizes uppercase unquoted column names (useful on IBM Db2 ODBC). |

See [Arrow ODBC API Reference](/reference/adapters/arrow-odbc/) for complete `ArrowOdbcConnectionParams` and `ArrowOdbcDriverFeatures` TypedDict tables.

## Type Mapping and Dialect Notes

- **Automatic Dialect Resolution**: If `StatementConfig(dialect=...)` is not passed explicitly, `ArrowOdbcDriver` inspects the ODBC `DBMS_NAME` metadata (`resolve_dialect_from_dbms_name`) to select `tsql`, `postgres`, or `db2` SQLGlot dialect rules automatically.
- **64-Bit Linux unixODBC with IBM Db2**: On 64-bit Linux, point `Driver=` at `libdb2o.so` (64-bit `SQLLEN`) rather than `libdb2.so` (legacy 32-bit `SQLLEN`) to avoid ODBC buffer width mismatches.
- **Lexical T-SQL Pagination Inlining**: ODBC drivers for SQL Server reject bound `?` parameters in certain `TOP (?)` and `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` positions. `ArrowOdbcDriver` uses a lexical SQLGlot T-SQL tokenizer to inline validated integer pagination bounds in exact source token order without disturbing CTE parameter ordinals.
- **Batched DML (`execute_many`)**: Because the underlying Rust `arrow-odbc` bridge focuses on columnar reads and does not expose DB-API rowcounts on statement execution, `execute_many` reports `rows_affected == -1` while preserving SQL expressions (`DEFAULT`, `CURRENT_TIMESTAMP`, `CAST`).
