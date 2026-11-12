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
    SnowflakePoolSessionFactory,
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

::: sqlspec.adapters.snowflake.SnowflakePoolSessionFactory
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.snowflake.SnowflakeSyncMigrationTracker
    options:
      heading_level: 4
      members: false

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
