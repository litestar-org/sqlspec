# sqlanywhere Adapter

```python
from sqlspec.adapters.sqlanywhere import (
    SqlanywhereConfig,
    SqlanywhereConnectionParams,
    SqlanywhereConnectionPool,
    SqlanywhereDriver,
    SqlanywhereDriverFeatures,
    SqlanywhereExceptionHandler,
    SqlanywherePoolParams,
    SqlanywhereSyncDataDictionary,
    SqlanywhereSyncMigrationTracker,
)
```

Synchronous SAP SQL Anywhere 12+ adapter built on `sqlanydb` (`dbcapi` C library). Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [SAP SQL Anywhere Setup Guide](../../databases/sqlanywhere.md) for engine connection parameters and pooling, and the [SQL Anywhere Data Dictionary & SQL Catalog](../data-dictionary/dialects/sqlanywhere.md) for system catalog introspection queries.

## Configuration & Parameters

::: sqlspec.adapters.sqlanywhere.SqlanywhereConfig
    options:
      heading_level: 3

::: sqlspec.adapters.sqlanywhere.SqlanywhereConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.sqlanywhere.SqlanywherePoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.sqlanywhere.SqlanywhereDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.sqlanywhere.SqlanywhereDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sqlanywhere.SqlanywhereExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sqlanywhere.SqlanywhereConnectionPool
    options:
      heading_level: 3

::: sqlspec.adapters.sqlanywhere.data_dictionary.SqlanywhereSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sqlanywhere.migrations.SqlanywhereSyncMigrationTracker
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.sqlanywhere.litestar.SqlanywhereStore
    options:
      heading_level: 4

::: sqlspec.adapters.sqlanywhere.adk.SqlanywhereADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.sqlanywhere.adk.SqlanywhereADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.sqlanywhere.events.SqlanywhereEventQueueStore
    options:
      heading_level: 4
