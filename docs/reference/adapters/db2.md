# db2 Adapter

```python
from sqlspec.adapters.db2 import (
    Db2AsyncConfig,
    Db2AsyncDriver,
    Db2AsyncExceptionHandler,
    Db2AsyncGlobalVariableStrategy,
    Db2AsyncPoolParams,
    Db2ConnectionParams,
    Db2DriverFeatures,
    Db2PoolParams,
    Db2SyncConfig,
    Db2SyncDriver,
    Db2SyncExceptionHandler,
    Db2SyncGlobalVariableStrategy,
)
```

Synchronous and asynchronous IBM Db2 LUW adapters built on `ibm_db`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [IBM Db2 Setup Guide](../../databases/db2.md) for usage examples and the [IBM Db2 Data Dictionary & SQL Catalog](../data-dictionary/dialects/db2.md).

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

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rcac"}`; `"global_variable"`, `"default"`, and `True` select the same mode-specific strategy. The strategy writes the tenant identity into a session global variable that row and column access control (RCAC) permissions and masks read. The default variable is `APP.CURRENT_TENANT_ID`; pass `tenant_variable=` and `variable_keys=` to an explicit strategy to use other schema-qualified variables. Names are validated as ordinary identifiers and folded to upper case; values are always bound.

A DBA provisions the variable and the policy, and grants the application `READ` and `WRITE` on the variable:

```sql
CREATE VARIABLE APP.CURRENT_TENANT_ID VARCHAR(256) DEFAULT NULL;
CREATE PERMISSION APP.PROJECTS_TENANT ON APP.PROJECTS
    FOR ROWS WHERE TENANT_ID = APP.CURRENT_TENANT_ID
    ENFORCED FOR ALL ACCESS ENABLE;
ALTER TABLE APP.PROJECTS ACTIVATE ROW ACCESS CONTROL;
```

Session global variable values are not affected by `COMMIT` or `ROLLBACK`. Checkout reads the previous values, assigns the payload with one `SET`, and reads the values back; checkin assigns the captured values and verifies them again. A failed verification disposes of the connection: the synchronous adapter uses one standalone connection per lease and closes it, and the asynchronous pool discards the connection instead of returning it. Standalone `create_connection()` is rejected in tenant mode. Tenant leases keep the native commit behavior: sessions roll back unfinished work before release, as they do without a tenant strategy.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.db2.Db2SyncGlobalVariableStrategy
    options:
      heading_level: 3
      inherited_members: [__init__]
      members: [__init__, checkout, checkin]

::: sqlspec.adapters.db2.Db2AsyncGlobalVariableStrategy
    options:
      heading_level: 3
      inherited_members: [__init__]
      members: [__init__, checkout, checkin]

## Driver, Pool & Data Dictionary

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
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.Db2AsyncExceptionHandler
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.data_dictionary.Db2SyncDataDictionary
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.db2.data_dictionary.Db2AsyncDataDictionary
    options:
      heading_level: 4
      members: false

## Extension Stores & Configs

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
