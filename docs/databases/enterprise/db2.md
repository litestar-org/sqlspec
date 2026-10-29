# IBM Db2

`sqlspec.adapters.db2` supports IBM Db2 LUW 11.5+ via `ibm_db` (`>= 3.3.0`), offering both synchronous (`Db2SyncConfig`) and `asyncio` (`Db2AsyncConfig`) managed connection pools, custom SQLGlot `db2` dialect transpilation, automatic lowercase column normalization, and Litestar/Events/ADK stores.

## Install and When to Choose

```bash
uv add "sqlspec[db2]"
```

- Choose **`Db2SyncConfig`** or **`Db2AsyncConfig`** for transactional OLTP workloads over IBM's native `clidriver` (`ibm_db` / `ibm_db_dbi`).
- Note on Linux `aarch64`: IBM publishes prebuilt `clidriver` wheels for Linux `x86_64`, macOS `arm64`, and Windows. On Linux ARM64 hosts, connect to Db2 via [`adbc`](../cloud-analytics/adbc.md) (`adbc-driver-db2` pure-Go DRDA wheel) or guard `ibm-db` with environment markers.
- For high-volume columnar Arrow extracts on `x86_64`, you can also pair [`arrow_odbc`](../cloud-analytics/arrow-odbc.md) (`dialect="db2"` over 64-bit `libdb2o.so`).

## Minimal Connection and Pool Setup

=== "Async (`Db2AsyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.db2 import Db2AsyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        Db2AsyncConfig(
            connection_config={
                "database": "appdb",
                "hostname": "localhost",
                "port": 50000,
                "user": "db2inst1",
                "password": "secret",
                "current_schema": "APP",
                "max_size": 10,
                "acquire_timeout": 30.0,
            }
        )
    )
    ```

=== "Sync (`Db2SyncConfig`)"

    ```python
    from sqlspec import SQLSpec
    from sqlspec.adapters.db2 import Db2SyncConfig

    spec = SQLSpec()
    config = spec.add_config(
        Db2SyncConfig(
            connection_config={
                "database": "appdb",
                "hostname": "localhost",
                "port": 50000,
                "user": "db2inst1",
                "password": "secret",
                "current_schema": "APP",
            }
        )
    )
    ```

## Key Driver Features and Pool Tuning

SQLSpec provides `Db2SyncConnectionPool` and `Db2AsyncConnectionPool` with health-check pings and recycle intervals rather than relying on `ibm_db.pconnect` (which can retain dropped `SQL0900N` sockets):

| Setting | Location | Description |
| --- | --- | --- |
| `dsn` / `database`, `hostname`, `port`, `protocol` | `connection_config` | Full DRDA DSN (`DATABASE=...;HOSTNAME=...;PORT=...;`) or validated discrete connection keys (note `hostname` and `user`, not `host` or `username`). |
| `current_schema` | `connection_config` | Default Db2 schema applied to unqualified table references (`CURRENT SCHEMA`). |
| `security` / `ssl_server_certificate` | `connection_config` | Enables TLS transport (`security="SSL"`) and server CA certificate verification. |
| `max_size` / `acquire_timeout` | `connection_config` | Pool concurrency cap and checkout timeout in seconds. |
| `health_check_interval` / `pool_recycle_seconds` | `connection_config` | Idle liveness probe interval (`VALUES (1)`) and maximum connection age before rotation. |
| `enable_lowercase_column_names` | `driver_features` | Converts Db2's default uppercase unquoted column identifiers (`USER_ID`) to lowercase (`user_id`) (`True` by default). |
| `json_serializer` / `json_deserializer` | `driver_features` | Customizes JSON text/CLOB serialization callables. |

See [Db2 API Reference](/reference/adapters/db2/) for complete `Db2ConnectionParams`, `Db2PoolParams`, and `Db2DriverFeatures` TypedDict tables.

## Type Mapping and Dialect Notes

- **Strict Connection Key Validation**: `Db2SyncConfig` and `Db2AsyncConfig` validate `connection_config` keys eagerly, raising `ImproperConfigurationError` if unknown aliases such as `host`, `username`, or `min_size` are passed instead of `hostname`, `user`, and `max_size`.
- **Savepoint Cursor Retention**: Db2 closes open cursors on savepoint rollback unless `ON ROLLBACK RETAIN CURSORS` is specified; SQLSpec emits `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS` and `RELEASE SAVEPOINT <name>` automatically.
- **Schema-Guided Arrow Export**: `select_to_arrow` converts fetched rows using cursor `column_types` metadata so even empty or all-`NULL` columns preserve exact PyArrow types.
- **SQLCODE and SQLSTATE Normalization**: Db2 error codes (`-803` unique, `-530`/`-532` foreign key, `-407` not null, `-911` deadlock/lock timeout, `-952` query timeout, `-551` permission denied, `-104`/`-204` syntax/undefined object) map into SQLSpec's unified exception hierarchy.
- **Events and ADK Stores**: `Db2EventQueueStore` uses `FETCH FIRST n ROWS ONLY` with `FOR UPDATE WITH RS SKIP LOCKED DATA` for lock-free concurrent queue workers, and `Db2ADKMemoryStore` uses `POSSTR(LOWER(content_text), LOWER(?)) > 0` for case-insensitive text search.
