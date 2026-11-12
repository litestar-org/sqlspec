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
    PsycopgSyncRLSStrategy,
    PsycopgAsyncRLSStrategy,
)
```

Synchronous and asynchronous PostgreSQL adapters built on `psycopg` v3 and `psycopg_pool`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (psycopg) Setup Guide](../../databases/postgres/psycopg.md) for setup and `COPY` streaming examples, and the [PostgreSQL Data Dictionary & SQL Catalog](../data-dictionary/dialects/postgres.md).

## Configuration & Parameters

::: sqlspec.adapters.psycopg.PsycopgSyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psycopg.PsycopgAsyncConfig
    options:
      heading_level: 3
      members: false

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

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}` to protect both `provide_session()` and `provide_connection()`. `"default"` and `True` select the mode-specific strategy. Both configs accept structural strategy instances, including unhashable ones. Missing scope raises `TenantContextError`; failed cleanup physically closes the connection and raises `TenantCleanupError` when no body exception is pending.

Tenant mode defaults `pooler_mode` to `"session"` and rejects transaction, statement, and auto modes. `close_returns=True` is rejected because invalidation must physically close the connection. Tenant leases keep the native commit behavior: `provide_connection()` commits on normal exit and rolls back on exceptional exit, and `provide_session()` discards uncommitted work exactly as it does without a tenant strategy. Management queries run in their own finished transactions when autocommit is disabled. Each boundary uses one capture/apply or restore/verify query, with transaction completion as a separate operation.

See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) for database policy setup and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md) for workers and cleanup.

::: sqlspec.adapters.psycopg.PsycopgSyncRLSStrategy
    options:
      heading_level: 4
      inherited_members: [__init__]
      members:
        - __init__
        - checkout
        - checkin

::: sqlspec.adapters.psycopg.PsycopgAsyncRLSStrategy
    options:
      heading_level: 4
      inherited_members: [__init__]
      members:
        - __init__
        - checkout
        - checkin

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
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.PsycopgAsyncExceptionHandler
    options:
      heading_level: 4
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
      members: false

::: sqlspec.adapters.psycopg.litestar.PsycopgAsyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.adk.PsycopgADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.adk.PsycopgAsyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.adk.PsycopgSyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.adk.PsycopgAsyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.events.PsycopgEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psycopg.events.PsycopgSyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.events.PsycopgAsyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.events.PsycopgSyncEventsBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.events.PsycopgAsyncEventsBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.events.PsycopgSyncHybridEventsBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psycopg.events.PsycopgAsyncHybridEventsBackend
    options:
      heading_level: 4
      members: false
