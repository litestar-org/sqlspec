# SAP SQL Anywhere

`sqlspec.adapters.sqlanywhere` provides synchronous database connectivity for SAP SQL Anywhere 12+ through the official `sqlanydb` driver (`>= 1.0.14`), offering connection pooling (`SqlanywhereConfig`), custom SQLGlot `sqlanywhere` dialect compilation, recursive foreign-key dependency analysis, and Litestar, Events, and Google ADK session/memory store extensions.

## Install and When to Choose

```bash
uv add "sqlspec[sqlanywhere]"
```

- Choose **`SqlanywhereConfig`** for embedded, edge, and client-server applications interacting with SAP SQL Anywhere database engines.
- `sqlanydb` wraps SAP's native client C library (`dbcapi`) via `ctypes`. Ensure `libdbcapi_r.so` (Linux), `libdbcapi_r.dylib` (macOS), or `dbcapi.dll` (Windows) is installed and available in the system library path.

## Minimal Connection and Pool Setup

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
        }
    )
)
```

## Key Configuration Parameters

| Setting | Location | Description |
| --- | --- | --- |
| `uid` / `user` | `connection_config` | Database user name (default DBA administrative principal). |
| `pwd` / `password` | `connection_config` | Database user password. |
| `eng` / `server` | `connection_config` | Running SQL Anywhere database engine name. |
| `dbn` / `database` | `connection_config` | Target database catalog name. |
| `host` | `connection_config` | Network host and port string (`host:port`, default port 2638). |
| `dbf` | `connection_config` | Direct database file path for auto-starting engines. |
| `pool_recycle_seconds` | `connection_config` | Maximum connection age in seconds before recycling. |
| `health_check_interval` | `connection_config` | Idle duration in seconds before running ping probes. |
| `json_serializer` / `json_deserializer` | `driver_features` | Custom JSON serialization/deserialization callables. |

## Dialect and Catalog Notes

- **Automatic Rollback on Context Exit**: SQL Anywhere connections maintain an implicit transaction upon statement execution. `SqlanywhereSessionContext` automatically rolls back uncommitted changes if a session exits with open work or an error, ensuring clean connection return to the pool.
- **Pagination**: Supports standard suffix `SELECT ... LIMIT n OFFSET m` as well as SQL Anywhere `TOP n START AT (m + 1)` syntax.
- **Identity Resync**: Fixture seeding supports primary key identity resetting via `CALL sa_reset_identity('<table>', '<schema>', <max_id>)`.
- **System Catalogs**: Schema discovery queries `SYS.SYSTAB`, `SYS.SYSTABCOL`, `SYS.SYSIDX`, `SYS.SYSIDXCOL`, `SYS.SYSFKEY`, and `SYS.SYSDOMAIN`.
