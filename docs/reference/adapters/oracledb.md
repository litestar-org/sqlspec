# oracledb Adapter

```python
from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleConnectionParams,
    OracleDriverFeatures,
    OraclePoolParams,
    OracleSyncConfig,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
)
```

Synchronous and asynchronous Oracle Database adapters built on `python-oracledb` (Thin and Thick modes) with `VECTOR`, `JSON`, `LOB`, and Advanced Queuing support. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [Oracle Database Setup Guide](../../databases/oracledb.md) for wallet, pool, and vector configuration.

## Configuration & Parameters

::: sqlspec.adapters.oracledb.OracleSyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleAsyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.OraclePoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.OracleDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.oracledb.OracleSyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleAsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.oracledb.litestar.OracleSyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.litestar.OracleAsyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.adk.OracleSyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.adk.OracleAsyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.adk.OracleSyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.adk.OracleAsyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleSyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleAsyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleSyncAQEventBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleAsyncAQEventBackend
    options:
      heading_level: 4
      members: false
