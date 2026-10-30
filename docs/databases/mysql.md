# MySQL and MariaDB

SQLSpec supports four MySQL and MariaDB adapters sharing unified error translation (`ER_DUP_ENTRY`, `ER_LOCK_DEADLOCK`, `ER_NO_REFERENCED_ROW_2`), catalog introspection, and bulk `LOAD DATA LOCAL INFILE` Arrow ingestion: `asyncmy`, `pymysql`, `aiomysql`, and `mysqlconnector`.

## Install and When to Choose

| Adapter | Install Command | Mode | Best Fit |
| --- | --- | --- | --- |
| `asyncmy` | `uv add "sqlspec[asyncmy]"` | Async | Recommended Cython-accelerated `asyncio` driver for MySQL 8+ and MariaDB. |
| `pymysql` | `uv add "sqlspec[pymysql]"` | Sync | Pure-Python synchronous driver with Google Cloud SQL connector support (`sqlspec[cloud-sql]`). |
| `aiomysql` | `uv add "sqlspec[aiomysql]"` | Async | Pure-Python `asyncio` driver built over `PyMySQL` for PyPy or pure-Python environments. |
| `mysqlconnector` | `uv add "sqlspec[mysql-connector]"` | Sync + Async | Official Oracle MySQL Connector/Python supporting both sync pools and native async connections. |

## Minimal Connection and Pool Setup

```{ .python title="docs/examples/drivers/mysqlconnector_connection.py" }
--8<-- "docs/examples/drivers/mysqlconnector_connection.py:example"
```

=== "asyncmy (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    spec = SQLSpec()
    config = spec.add_config(
        AsyncmyConfig(
            connection_config={
                "host": "localhost",
                "port": 3306,
                "user": "app",
                "password": "secret",
                "db": "app_db",
                "minsize": 2,
                "maxsize": 20,
            }
        )
    )
    ```

=== "pymysql (Sync)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.pymysql import PyMysqlConfig

    spec = SQLSpec()
    config = spec.add_config(
        PyMysqlConfig(
            connection_config={
                "host": "localhost",
                "port": 3306,
                "user": "app",
                "password": "secret",
                "database": "app_db",
                "charset": "utf8mb4",
            }
        )
    )
    ```

=== "aiomysql (Async)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiomysql import AiomysqlConfig

    spec = SQLSpec()
    config = spec.add_config(
        AiomysqlConfig(
            connection_config={"host": "localhost", "port": 3306, "user": "app", "password": "secret", "db": "app_db"}
        )
    )
    ```

## Key Driver Features and Pool Tuning

| Setting | Adapters | Description |
| --- | --- | --- |
| `minsize` / `maxsize` (or `pool_size`) | All | Pool capacity bounds (`pool_min_size`/`pool_max_size` on `PyMysqlConfig`, `pool_size` on `MysqlConnector*Config`). |
| `ssl` / `ssl_ca`, `ssl_verify_cert` | All | TLS `ssl.SSLContext` (`asyncmy`, `pymysql`, `aiomysql`) or certificate path options (`mysqlconnector`). |
| `local_infile` + `enable_local_infile_bulk_load` | All | Enables streaming TSV `LOAD DATA LOCAL INFILE` acceleration in `session.load_from_arrow(...)`. |
| `enable_cloud_sql` / `cloud_sql_instance` | `pymysql` | Connects to Google Cloud SQL for MySQL via `cloud-sql-python-connector` with optional IAM auth. |
| `cursor_options` | `mysqlconnector` | Configures buffered, raw, or prepared cursor options on `MysqlConnectorSyncConfig` / `MysqlConnectorAsyncConfig`. |
| `json_serializer` / `json_deserializer` | All | Customizes `JSON` column encoding and decoding (cached directly on driver slots). |

See [AsyncMy](/reference/adapters/asyncmy/), [PyMySQL](/reference/adapters/pymysql/), [AioMySQL](/reference/adapters/aiomysql/), and [MySQL Connector](/reference/adapters/mysqlconnector/) in the API Reference for per-driver TypedDict fields.

## Type Mapping and Dialect Notes

- **Placeholder Syntax**: Write `:named` or `?` parameters in SQLSpec; all four MySQL adapters compile placeholders to `%s` automatically while preserving literal `%` signs in `LIKE '100%'` expressions.
- **`utf8mb4` Collation**: Always use `utf8mb4` (default in MySQL 8+ and configured automatically where supported) so 4-byte UTF-8 characters and emojis never truncate under legacy 3-byte `utf8`.
- **Implicit DDL Commits**: MySQL and MariaDB issue an implicit `COMMIT` before and after `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, and `TRUNCATE TABLE`. Keep schema migrations separate from transactional application tests that rely on rollback isolation.
- **`RETURNING` and Generated Keys**: MariaDB 10.5+ supports `INSERT ... RETURNING` and `DELETE ... RETURNING` directly via `session.select(...)`. Standard MySQL does not support `RETURNING`, but SQLSpec handles this automatically: `result = await session.execute("INSERT ...")` captures the generated key into `result.last_inserted_id` and row count into `result.rows_affected` on the returned `SQLResult`.
- **`SHOW` and `DESCRIBE` Queries**: All four adapters classify `SHOW ...`, `DESCRIBE ...`, and `EXPLAIN ...` as row-returning statements automatically in `session.select(...)`.
