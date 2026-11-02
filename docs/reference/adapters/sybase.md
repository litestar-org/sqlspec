# sybase Adapter

```python
from sqlspec.adapters.sybase import (
    SybaseConfig,
    SybaseConnectionParams,
    SybaseConnectionPool,
    SybaseDriver,
    SybaseDriverFeatures,
    SybaseExceptionHandler,
    SybasePoolParams,
    SybaseSyncDataDictionary,
    SybaseSyncMigrationTracker,
)
```

Synchronous SAP Adaptive Server Enterprise (Sybase ASE) 16.0+ adapter built on `pymssql` over FreeTDS TDS 5.0 wire protocol. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [Sybase ASE Setup Guide](../../databases/sybase.md) for TDS 5.0 protocol negotiation and connection pooling, and the [Sybase Data Dictionary & SQL Catalog](../data-dictionary/dialects/sybase.md) for system catalog introspection queries.

## Configuration & Parameters

::: sqlspec.adapters.sybase.SybaseConfig
    options:
      heading_level: 3

::: sqlspec.adapters.sybase.SybaseConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.sybase.SybasePoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.sybase.SybaseDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.sybase.SybaseDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sybase.SybaseExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sybase.SybaseConnectionPool
    options:
      heading_level: 3

::: sqlspec.adapters.sybase.data_dictionary.SybaseSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.sybase.migrations.SybaseSyncMigrationTracker
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.sybase.litestar.SybaseStore
    options:
      heading_level: 4

::: sqlspec.adapters.sybase.adk.SybaseADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.sybase.adk.SybaseADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.sybase.events.SybaseEventQueueStore
    options:
      heading_level: 4
