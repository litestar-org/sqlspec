# db2 Adapter

```python
from sqlspec.adapters.db2 import (
    Db2AsyncConfig,
    Db2AsyncDriver,
    Db2AsyncExceptionHandler,
    Db2AsyncPoolParams,
    Db2ConnectionParams,
    Db2DriverFeatures,
    Db2PoolParams,
    Db2SyncConfig,
    Db2SyncDriver,
    Db2SyncExceptionHandler,
)
```

Synchronous and asynchronous IBM Db2 LUW adapters built on `ibm_db`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [IBM Db2 Setup Guide](../../databases/db2.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.db2.Db2SyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.db2.Db2AsyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.db2.Db2ConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.db2.Db2PoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.db2.Db2AsyncPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.db2.Db2DriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.db2.Db2SyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.db2.Db2AsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.db2.Db2SyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.db2.Db2AsyncExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.db2.litestar.Db2SyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.litestar.Db2AsyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.adk.Db2SyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.adk.Db2AsyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.adk.Db2SyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.adk.Db2AsyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.events.Db2SyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.events.Db2AsyncEventQueueStore
    options:
      heading_level: 4
      members: false
