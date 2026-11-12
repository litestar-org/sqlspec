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
    PymssqlSessionContextStrategy,
)
```

Synchronous Microsoft SQL Server adapter built on `pymssql` (FreeTDS). Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [SQL Server Setup Guide](../../databases/mssql.md) for usage examples and the [SQL Server Data Dictionary & SQL Catalog](../data-dictionary/dialects/mssql.md).

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

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}`; `"session_context"`, `"default"`, and `True` select the same strategy. Both providers use one standalone connection per lease rather than the normal thread-local connection. Cleanup or setup failure physically closes that connection. Standalone `create_connection()` is rejected in tenant mode.

Each boundary uses one bound SQL batch: capture/apply/verify at checkout, restore/verify at checkin. Owned keys remain mutable (`@read_only=0`). The strategy neither commits nor rolls back nor changes autocommit. The lease rolls back unfinished work before cleanup and closes its standalone connection after release.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.pymssql.PymssqlSessionContextStrategy
    options:
      heading_level: 3
      inherited_members: [__init__]
      members: [__init__, checkout, checkin]

## Driver, Pool & Data Dictionary

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

::: sqlspec.adapters.pymssql.data_dictionary.PymssqlSyncDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.pymssql.litestar.PymssqlStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymssql.adk.PymssqlADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

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
