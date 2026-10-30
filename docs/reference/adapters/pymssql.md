# pymssql Adapter

```python
from sqlspec.adapters.pymssql import (
    PymssqlConfig,
    PymssqlConnectionParams,
    PymssqlConnectionPool,
    PymssqlDriver,
    PymssqlDriverFeatures,
    PymssqlExceptionHandler,
    PymssqlPoolParams,
)
```

Synchronous Microsoft SQL Server adapter built on `pymssql` (FreeTDS). Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [SQL Server Setup Guide](../../databases/mssql.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.pymssql.PymssqlConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.pymssql.PymssqlConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.pymssql.PymssqlPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.pymssql.PymssqlDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.pymssql.PymssqlDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.pymssql.PymssqlExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.pymssql.PymssqlConnectionPool
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.pymssql.litestar.PymssqlStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymssql.adk.PymssqlADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymssql.adk.PymssqlADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymssql.events.PymssqlEventQueueStore
    options:
      heading_level: 4
      members: false
