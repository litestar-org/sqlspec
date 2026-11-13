# SAP SQL Anywhere

`sqlspec.adapters.sqlanywhere` provides synchronous database connectivity for SAP SQL Anywhere 12+ through the official `sqlanydb` driver (`>= 1.0.14`), offering bounded connection pooling (`SqlanywhereConfig`), custom SQLGlot `sqlanywhere` dialect compilation, recursive foreign-key dependency analysis, and Litestar, Events, and Google ADK session/memory store extensions.

## Overview & Capabilities

| Capability | Details |
| --- | --- |
| **Adapter Config** | `SqlanywhereConfig` (`sqlspec.adapters.sqlanywhere`) |
| **Driver Extra** | `sqlspec[sqlanywhere]` (`sqlanydb >= 1.0.14` wrapping SAP `dbcapi`) |
| **Execution Mode** | Synchronous |
| **Pool Architecture** | `SqlanywhereConnectionPool` (`pool_min_size`, `pool_max_size`, `pool_timeout`, `pool_recycle_seconds`, `health_check_interval`) |
| **Dialect & Extensions** | Custom SQLGlot `sqlanywhere` dialect, Litestar store, Events queue store, Google ADK session & memory stores |

## Installation & Quickstart

=== "uv"

    ```bash
    uv add "sqlspec[sqlanywhere]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[sqlanywhere]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[sqlanywhere]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[sqlanywhere]"
    ```

`sqlanydb` loads SAP's native C client library (`libdbcapi_r.so` on Linux, `libdbcapi_r.dylib` on macOS, or `dbcapi.dll` on Windows) via `ctypes`. Ensure the SQL Anywhere client runtime is present on the library search path:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlanywhere import SqlanywhereConfig

spec = SQLSpec()
config = spec.add_config(
    SqlanywhereConfig(
        connection_config={
            "uid": "dba",
            "pwd": "sql",
            "eng": "demo_server",
            "dbn": "demo",
            "host": "localhost:2638",
            "pool_min_size": 1,
            "pool_max_size": 10,
        }
    )
)
```

## Connection & Pool Configuration

### Connection and Pool Parameters

Pass `SqlanywhereConnectionParams` and `SqlanywherePoolParams` keys inside `connection_config={...}` on `SqlanywhereConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.sqlanywhere.config.SqlanywhereConnectionParams,sqlspec.adapters.sqlanywhere.config.SqlanywherePoolParams -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `nodetype` | `str` | - | SQL Anywhere mirror node role, including READONLY. |
| `uid` | `str` | - | User ID / login name for authentication. |
| `user` | `str` | - | Alias for `uid`. |
| `username` | `str` | - | Alias for `uid`. |
| `pwd` | `str` | - | Password for authentication. |
| `password` | `str` | - | Alias for `pwd`. |
| `dbn` | `str` | - | Database name on the SQL Anywhere server. |
| `database` | `str` | - | Alias for `dbn`. |
| `db` | `str` | - | Alias for `dbn`. |
| `eng` | `str` | - | SQL Anywhere database engine name. |
| `server` | `str` | - | Alias for `eng`. |
| `engine` | `str` | - | Alias for `eng`. |
| `server_name` | `str` | - | Alias for `eng`. |
| `dbf` | `str` | - | Database file path for local or embedded engines. |
| `database_file` | `str` | - | Alias for `dbf`. |
| `host` | `str` | - | TCP hostname or IP address of the server. |
| `port` | `int or str` | - | TCP port number of the server. |
| `dsn` | `str` | - | ODBC data source name. |
| `links` | `str` | - | Network communications links protocol configuration. |
| `cset` | `str` | - | Character set used for character data conversion. |
| `extra` | `dict[str, Any]` | - | Additional connection parameters passed to the driver. |
| `pool_min_size` | `int` | - | Minimum number of connections maintained in the pool. |
| `pool_max_size` | `int` | - | Maximum number of connections allowed in the pool. |
| `pool_timeout` | `float` | - | Seconds to wait for a connection before timing out. |
| `pool_recycle_seconds` | `int` | - | Maximum age in seconds before a connection is recycled. |
| `health_check_interval` | `float` | - | Interval in seconds between background health checks. |
<!-- GENERATED:END config-table:sqlspec.adapters.sqlanywhere.config.SqlanywhereConnectionParams,sqlspec.adapters.sqlanywhere.config.SqlanywherePoolParams -->

### Driver Features

Pass `SqlanywhereDriverFeatures` keys inside `driver_features={...}` on `SqlanywhereConfig`:

<!-- GENERATED:START config-table:sqlspec.adapters.sqlanywhere.config.SqlanywhereDriverFeatures -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `json_serializer` | `Callable[[Any], str]` | - | Custom callable for serializing objects to JSON strings. |
| `json_deserializer` | `Callable[[str], Any]` | - | Custom callable for deserializing JSON strings. |
| `on_connection_create` | `Callable[[SqlanywhereConnection], None]` | - | Callback invoked immediately when a new connection is established. |
| `enable_events` | `bool` | - | Enable database event listening. |
| `events_backend` | `Literal['poll_queue']` | - | Backend mechanism used for dispatching events. |
<!-- GENERATED:END config-table:sqlspec.adapters.sqlanywhere.config.SqlanywhereDriverFeatures -->

## Type Mappings & Dialect Notes

| Database Type | Python Type | Notes / Feature Flag |
| --- | --- | --- |
| `BIT` | `bool` / `int` | Nullable or non-nullable boolean bit column (`0` / `1`). |
| `MONEY` / `SMALLMONEY` / `DECIMAL` | `decimal.Decimal` | Exact decimal arithmetic; Arrow schema inference supports up to `127` digits of decimal precision. |
| `TIMESTAMP` / `TIMESTAMP WITH TIME ZONE` | `datetime.datetime` | `CreateTable.from_arrow_schema()` maps naive and timezone-aware Arrow timestamps directly. |
| `LONG BINARY` / `IMAGE` / `VARBINARY` | `bytes` / `bytearray` | Binary and BLOB storage. |
| `UNIQUEIDENTIFIER` / `UNIQUEIDENTIFIERSTR` | `uuid.UUID` / `str` | Native 16-byte binary UUID and 36-character formatted UUID string types. |
| `XML` / `LONG VARCHAR` (JSON) | `dict` / `list` / `str` | JSON values serialize via `json_serializer` and `json_deserializer`. |

- **Automatic Rollback on Context Exit**: SQL Anywhere connections open an implicit transaction upon statement execution. `SqlanywhereSessionContext` automatically rolls back uncommitted work if a session exits with open changes or an exception so pooled connections return clean.
- **Pagination (`LIMIT ... OFFSET` and `TOP ... START AT`)**: The `sqlanywhere` dialect supports both standard suffix `SELECT ... LIMIT n OFFSET m` and native `SELECT TOP n START AT (m + 1)` pagination.
- **Arrow Schema Inference and Staged Storage Loads**: `CreateTable.from_arrow_schema()` maps string primary keys to `VARCHAR(255)` and raises `SQLBuilderError` when decimal precision exceeds `127`. Staged storage loads use native `MERGE` statements over `conflict_keys` (supporting `on_conflict="nothing"`, `exclude_update_columns`, and `drop_indexes=True` secondary index rebuilds).
- **System Catalog Introspection**: Schema discovery queries `SYS.SYSTAB`, `SYS.SYSTABCOL`, `SYS.SYSIDX`, `SYS.SYSIDXCOL`, `SYS.SYSFKEY`, and `SYS.SYSDOMAIN`, and fixture seeding resets identity sequences via `CALL sa_reset_identity('<table>', '<schema>', <max_id>)`.
- **Migration Schemas**: The SQL Anywhere driver cannot switch schemas during a migration, so `-- schema:` directives, `default_schema`, and `version_table_schema` raise `MigrationError`. Qualify objects in migration SQL, and set `version_table_name` to `"<schema>.ddl_migrations"` to place the tracking table.

## Read-Only Mirror Connections

`SqlanywhereConfig(replicas=True)` sets `NodeType=READONLY` for a mirror or read-scale node. Pass explicit `replicas=` overrides to choose a different connection string. Derived configs keep native aliases and user hooks.

SQLCODE `-757` maps to `ReadOnlyTransactionError`. The server must supply the read-only node; this option does not create one. Use `provide_read_session()` for a reader and ordinary sessions for primary work. See [Read Replicas](../learn/recipes/read-replicas.md).

## See Also

- [SQL Anywhere API Reference](/reference/adapters/sqlanywhere/)
- [SQL Anywhere Dialect Reference](/reference/data-dictionary/dialects/sqlanywhere/)
- [Arrow and Storage Guide](../learn/arrow-and-storage.md)
