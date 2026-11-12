# mssql-python Adapter

```python
from sqlspec.adapters.mssql_python import (
    MssqlPythonBulkCopyResult,
    MssqlPythonConfig,
    MssqlPythonConnectionParams,
    MssqlPythonConnectionPool,
    MssqlPythonDriver,
    MssqlPythonDriverFeatures,
    MssqlPythonExceptionHandler,
    MssqlPythonPoolParams,
    MssqlPythonSessionContextStrategy,
)
```

Microsoft SQL Server and Azure SQL adapter built on Microsoft's official `mssql-python` driver with native Bulk Copy (`BCP`) support. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [SQL Server Setup Guide](../../databases/mssql.md) for setup and bulk copy examples, and the [SQL Server Data Dictionary & SQL Catalog](../data-dictionary/dialects/mssql.md).

## Configuration & Parameters

::: sqlspec.adapters.mssql_python.MssqlPythonConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.MssqlPythonConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mssql_python.MssqlPythonPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mssql_python.MssqlPythonDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}` (`"session_context"`, `"default"`, and `True` select the same strategy). Enabling a tenant strategy automatically defaults `pool_enabled` to `False` when omitted from `connection_config` and rejects explicit `pool_enabled=True`. Because `mssql-python` sets driver pooling process-wide, any non-tenant administrative `MssqlPythonConfig` in the same process must still pass `connection_config={"pool_enabled": False, ...}` before opening its first connection; conflicting process-wide pooling modes fail construction.

Checkout captures and applies mutable owned keys, then verifies their values in one SQL batch. Checkin restores and verifies them in one batch. Keys use `@read_only=0`; values are bound. Strategies do not commit, roll back, or toggle autocommit. Both providers run the lease; cleanup failure physically closes the unpooled connection.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.mssql_python.MssqlPythonSessionContextStrategy
    options:
      heading_level: 3
      inherited_members: [__init__]
      members: [__init__, checkout, checkin]

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.mssql_python.MssqlPythonDriver
    options:
      heading_level: 3
      members:
        - bulk_copy

::: sqlspec.adapters.mssql_python.MssqlPythonBulkCopyResult
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.MssqlPythonExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.MssqlPythonConnectionPool
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.data_dictionary.MssqlPythonSyncDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.mssql_python.litestar.MssqlPythonStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.mssql_python.adk.MssqlPythonADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mssql_python.adk.MssqlPythonADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.mssql_python.adk.MssqlPythonADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.mssql_python.events.MssqlPythonEventQueueStore
    options:
      heading_level: 4
      members: false
