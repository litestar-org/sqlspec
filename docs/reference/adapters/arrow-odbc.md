# arrow-odbc Adapter

```python
from sqlspec.adapters.arrow_odbc import (
    ArrowOdbcConfig,
    ArrowOdbcConnectionParams,
    ArrowOdbcDriver,
    ArrowOdbcDriverFeatures,
    ArrowOdbcExceptionHandler,
)
```

Apache Arrow over ODBC adapter built on `arrow-odbc` for zero-copy Arrow batch streaming from any ODBC-compliant data source (SQL Server, PostgreSQL, Oracle, Snowflake, Db2). Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [Arrow ODBC Setup Guide](../../databases/arrow-odbc.md) for connection string and batch streaming examples.

## Configuration & Parameters

::: sqlspec.adapters.arrow_odbc.ArrowOdbcConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.arrow_odbc.ArrowOdbcConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.arrow_odbc.ArrowOdbcDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.arrow_odbc.ArrowOdbcDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.arrow_odbc.ArrowOdbcExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.arrow_odbc.litestar.ArrowOdbcStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.arrow_odbc.events.ArrowOdbcEventQueueStore
    options:
      heading_level: 4
      members: false
