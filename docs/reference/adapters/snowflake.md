# snowflake Adapter

```python
from sqlspec.adapters.snowflake import (
    SnowflakeConfig,
    SnowflakeConnectionParams,
    SnowflakeConnectionPool,
    SnowflakeDriver,
    SnowflakeDriverFeatures,
    SnowflakeExceptionHandler,
    SnowflakePoolParams,
    SnowflakeSessionAttributeStrategy,
    SnowflakeSyncDataDictionary,
    SnowflakeSyncMigrationTracker,
)
```

Synchronous Snowflake adapter built on `snowflake-connector-python`, with bounded pooling, native Arrow batches, and Parquet stage loads.
Scripts use SQLSpec's Snowflake splitter and a managed cursor per statement. Transaction checks use server `CURRENT_TRANSACTION()`; aware datetime binds preserve timezone offsets in server binding modes.
Inherited query and transaction methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md).
See the [Snowflake setup guide](../../databases/snowflake.md) and [Snowflake catalog reference](../data-dictionary/dialects/snowflake.md).

## Configuration and Parameters

::: sqlspec.adapters.snowflake.SnowflakeConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.snowflake.SnowflakeConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.snowflake.SnowflakePoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.snowflake.SnowflakeDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool, and Data Dictionary

::: sqlspec.adapters.snowflake.SnowflakeDriver
    options:
      heading_level: 3
      members:
        - execute_async_query
        - fetch_async_query_result

::: sqlspec.adapters.snowflake.SnowflakeExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.snowflake.SnowflakeConnectionPool
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.snowflake.SnowflakeSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.snowflake.SnowflakeSyncMigrationTracker
    options:
      heading_level: 4
      members: false

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}`; `"session_attributes"`, `"default"`, and `True` select the same strategy. The strategy writes the tenant identity into an immutable session attribute that row access policies read with `SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', 'tenant_id')`. Pass `tenant_attribute=` and `variable_keys=` to an explicit strategy to use other attribute names. Names are validated as identifiers and keep their case, because Snowflake attribute names are case-sensitive; values are always bound.

Checkout requires the owned attributes to be unset, binds each one with `CALL SET_SYS_CONTEXT`, and reads them back before the session is used. Snowflake cannot overwrite or clear an immutable attribute, so each lease opens one standalone connection and closes it on release, verifying that it closed; tenant sessions never return to the pool. Sessions roll back unfinished work before release, as they do without a tenant strategy. Standalone `create_connection()` is rejected in tenant mode.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.snowflake.SnowflakeSessionAttributeStrategy
    options:
      heading_level: 3
      members: [__init__, checkout, checkin]

## Extension Stores

::: sqlspec.adapters.snowflake.litestar.SnowflakeStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.snowflake.events.SnowflakeEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.snowflake.adk.SnowflakeADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.snowflake.adk.SnowflakeADKMemoryStore
    options:
      heading_level: 4
      members: false
