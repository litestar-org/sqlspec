# sqlite Adapter

```python
from sqlspec.adapters.sqlite import (
    SqliteConfig,
    SqliteConnectionParams,
    SqliteConnectionPool,
    SqliteDriver,
    SqliteDriverFeatures,
    SqliteExceptionHandler,
)
```

Synchronous SQLite adapter built on Python's standard library `sqlite3` with thread-local connection pooling and WAL pragma helpers. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [SQLite Setup Guide](../../databases/sqlite.md) for in-memory URI and WAL configuration.

## Configuration & Parameters

::: sqlspec.adapters.sqlite.SqliteConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sqlite.SqliteConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.sqlite.SqliteDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.sqlite.SqliteDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sqlite.SqliteExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sqlite.SqliteConnectionPool
    options:
      heading_level: 3
      members:
        - acquire
        - release
        - close_all
        - size

## Extension Stores

::: sqlspec.adapters.sqlite.litestar.SQLiteStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.sqlite.adk.SqliteADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.sqlite.adk.SqliteADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.sqlite.events.SqliteEventQueueStore
    options:
      heading_level: 4
      members: false
