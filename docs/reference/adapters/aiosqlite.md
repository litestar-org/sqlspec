# aiosqlite Adapter

```python
from sqlspec.adapters.aiosqlite import (
    AiosqliteConfig,
    AiosqliteConnectionParams,
    AiosqliteConnectionPool,
    AiosqliteDriver,
    AiosqliteDriverFeatures,
    AiosqliteExceptionHandler,
    AiosqlitePoolParams,
)
```

Asynchronous SQLite adapter built on `aiosqlite`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [SQLite Setup Guide](../../databases/sqlite.md) for async pool and shared-cache in-memory URI configuration, and the [SQLite Data Dictionary & SQL Catalog](../data-dictionary/dialects/sqlite.md).

## Configuration & Parameters

::: sqlspec.adapters.aiosqlite.AiosqliteConfig
    options:
      heading_level: 3

::: sqlspec.adapters.aiosqlite.AiosqliteConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiosqlite.AiosqlitePoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiosqlite.AiosqliteDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.aiosqlite.AiosqliteDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.aiosqlite.AiosqliteExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.aiosqlite.AiosqliteConnectionPool
    options:
      heading_level: 3

::: sqlspec.adapters.aiosqlite.data_dictionary.AiosqliteDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.aiosqlite.litestar.AiosqliteLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiosqlite.litestar.AiosqliteStore
    options:
      heading_level: 4

::: sqlspec.adapters.aiosqlite.adk.AiosqliteADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiosqlite.adk.AiosqliteADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.aiosqlite.adk.AiosqliteADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.aiosqlite.events.AiosqliteEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiosqlite.events.AiosqliteEventQueueStore
    options:
      heading_level: 4
