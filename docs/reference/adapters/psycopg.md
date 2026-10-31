# psycopg Adapter

```python
from sqlspec.adapters.psycopg import (
    PsycopgAsyncConfig,
    PsycopgAsyncDriver,
    PsycopgAsyncExceptionHandler,
    PsycopgConnectionParams,
    PsycopgDriverFeatures,
    PsycopgPoolParams,
    PsycopgSyncConfig,
    PsycopgSyncDriver,
    PsycopgSyncExceptionHandler,
)
```

Synchronous and asynchronous PostgreSQL adapters built on `psycopg` v3 and `psycopg_pool`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (psycopg) Setup Guide](../../databases/postgres/psycopg.md) for setup and `COPY` streaming examples, and the [PostgreSQL Data Dictionary & SQL Catalog](../data-dictionary/dialects/postgres.md).

## Configuration & Parameters

::: sqlspec.adapters.psycopg.PsycopgSyncConfig
    options:
      heading_level: 3

::: sqlspec.adapters.psycopg.PsycopgAsyncConfig
    options:
      heading_level: 3

::: sqlspec.adapters.psycopg.PsycopgConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.PsycopgPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.PsycopgDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.psycopg.PsycopgSyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psycopg.PsycopgAsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psycopg.PsycopgSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psycopg.PsycopgAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psycopg.data_dictionary.PsycopgSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psycopg.data_dictionary.PsycopgAsyncDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.psycopg.litestar.PsycopgLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.litestar.PsycopgSyncStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.litestar.PsycopgAsyncStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.adk.PsycopgADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.adk.PsycopgAsyncADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.adk.PsycopgSyncADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.adk.PsycopgAsyncADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.events.PsycopgEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.events.PsycopgSyncEventQueueStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.events.PsycopgAsyncEventQueueStore
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.events.PsycopgSyncEventsBackend
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.events.PsycopgAsyncEventsBackend
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.events.PsycopgSyncHybridEventsBackend
    options:
      heading_level: 4

::: sqlspec.adapters.psycopg.events.PsycopgAsyncHybridEventsBackend
    options:
      heading_level: 4
