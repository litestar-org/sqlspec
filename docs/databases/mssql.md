# Microsoft SQL Server

SQLSpec provides two synchronous Microsoft SQL Server and Azure SQL adapters—`sqlspec.adapters.mssql_python` (`MssqlPythonConfig`) and `sqlspec.adapters.pymssql` (`PymssqlConfig`)—plus zero-copy columnar ODBC streaming via [`arrow_odbc`](arrow-odbc.md).

## Install and When to Choose

=== "uv"

    ```bash
    uv add "sqlspec[mssql-python]"
    # or: uv add "sqlspec[pymssql]"
    # or: uv add "sqlspec[arrow-odbc]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[mssql-python]"
    # or: pip install "sqlspec[pymssql]"
    # or: pip install "sqlspec[arrow-odbc]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[mssql-python]"
    # or: pdm add "sqlspec[pymssql]"
    # or: pdm add "sqlspec[arrow-odbc]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[mssql-python]"
    # or: poetry add "sqlspec[pymssql]"
    # or: poetry add "sqlspec[arrow-odbc]"
    ```

| Adapter | Extra | Best Fit |
| --- | --- | --- |
| `mssql_python` | `sqlspec[mssql-python]` | Microsoft's official modern Python driver with Entra ID (`ActiveDirectoryDefault`) auth and native Arrow `BulkCopy` ingestion. |
| `pymssql` | `sqlspec[pymssql]` | Lightweight FreeTDS-based synchronous driver for classic SQL authentication and portable Linux containers. |
| [`arrow_odbc`](arrow-odbc.md) | `sqlspec[arrow-odbc]` | High-throughput analytical reads streaming ODBC batches directly into Apache Arrow tables. |

## Minimal Connection and Pool Setup

=== "mssql-python (`MssqlPythonConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.mssql_python import MssqlPythonConfig

    spec = SQLSpec()
    config = spec.add_config(
        MssqlPythonConfig(
            connection_config={
                "server": "localhost",
                "port": 1433,
                "database": "app_db",
                "user": "sa",
                "password": "StrongPassword123",
                "encrypt": "mandatory",
                "trust_server_certificate": True,
            }
        )
    )
    ```

=== "pymssql (`PymssqlConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.pymssql import PymssqlConfig

    spec = SQLSpec()
    config = spec.add_config(
        PymssqlConfig(
            connection_config={
                "server": "localhost",
                "port": 1433,
                "user": "sa",
                "password": "StrongPassword123",
                "database": "app_db",
                "encryption": "request",
            }
        )
    )
    ```

## Key Driver Features and Pool Tuning

| Setting | Adapter | Description |
| --- | --- | --- |
| `connection_string` / `server`, `port`, `database` | `mssql_python` | Full semicolon-delimited ODBC/TDS string or normalized discrete keys (`uid`/`user`, `pwd`/`password`). |
| `authentication` | `mssql_python` | Azure Entra ID and ODBC auth modes (`ActiveDirectoryDefault`, `ActiveDirectoryManagedIdentity`, `SqlPassword`). |
| `encrypt` / `trust_server_certificate` / `server_certificate` | `mssql_python` | TLS 1.2/1.3 encryption policy (`strict`, `mandatory`, `optional`) and certificate pinning. |
| `application_intent` / `multi_subnet_failover` | `mssql_python` | Always On availability group read-only routing (`ReadOnly` / `ReadWrite`) and parallel subnet failover. |
| `pool_min_size` / `pool_max_size` | Both | Connection pool bounds on `MssqlPythonConnectionPool` and `PymssqlConnectionPool`. |
| `enable_pooling` | `mssql_python`, `pymssql` (`driver_features`) | Toggles SQLSpec's managed connection pooling (`True` by default). |
| `encryption` / `tds_version` / `login_timeout` | `pymssql` | FreeTDS transport encryption and TDS protocol negotiation settings. |

See [MSSQL Python Reference](/reference/adapters/mssql-python/) and [PyMSSQL Reference](/reference/adapters/pymssql/) for complete TypedDict field documentation.

## Type Mapping and Dialect Notes

- **Streaming Arrow `BulkCopy` (`mssql_python`)**: `session.load_from_arrow(table_name, arrow_data)` streams `pyarrow.Table`, `RecordBatch`, or `RecordBatchReader` batches directly through `mssql_python`'s native TDS `bulkcopy` engine without materializing intermediate Python dicts, forwarding `batch_size`, `check_constraints`, `fire_triggers`, `keep_identity`, `keep_nulls`, and `table_lock` options.
- **Placeholder Translation**: Write `:named` or `?` parameters in SQLSpec; `MssqlPythonDriver` compiles to positional `?` placeholders while `PymssqlDriver` compiles to `%(name)s` / `%s` `pyformat` placeholders.
- **T-SQL Pagination Requires `ORDER BY`**: SQL Server's `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY` syntax requires an explicit `ORDER BY` clause. Always attach `.order_by(...)` or `OrderByFilter` when using `LimitOffsetFilter` on `tsql`.
- **Savepoint Syntax**: Nested savepoints emit T-SQL `SAVE TRANSACTION <name>` and `ROLLBACK TRANSACTION <name>` automatically.
- **Error Code Disambiguation**: TDS error numbers (`2601`/`2627` unique violation, `547` foreign-key vs check constraint inspection, `515` not-null, `1205` deadlock) map into SQLSpec's standard exception classes.
