# asyncpg Adapter

```python
from sqlspec.adapters.asyncpg import (
    AsyncpgConfig,
    AsyncpgConnectionConfig,
    AsyncpgDriver,
    AsyncpgDriverFeatures,
    AsyncpgExceptionHandler,
    AsyncpgPool,
    AsyncpgPoolConfig,
    AsyncpgRLSStrategy,
)
```

Asynchronous PostgreSQL adapter built on `asyncpg`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (asyncpg) Setup Guide](../../databases/postgres/asyncpg.md) for connection pooling, JSON codecs, and pgvector configuration, and the [PostgreSQL Data Dictionary & SQL Catalog](../data-dictionary/dialects/postgres.md).

## Configuration & Parameters

::: sqlspec.adapters.asyncpg.AsyncpgConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.asyncpg.AsyncpgConnectionConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.AsyncpgPoolConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.AsyncpgDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}` to protect both `provide_session()` and `provide_connection()`. `"default"` and `True` select the same strategy. Missing scope raises `TenantContextError` before acquisition; failed cleanup invalidates the connection and raises `TenantCleanupError` when no body exception is pending.

Tenant mode defaults `pooler_mode` to `"session"` and rejects transaction, statement, and auto modes. `tenant_session_baseline` declares settings replayed before each checkout after the native pool reset. Hooks that change session settings require an explicit baseline declaration. Capturing and applying tenant settings uses one query; restoring and verifying them uses one query. Baseline replay and pool reset are separate operations.

See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) for database policy setup and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md) for workers and cleanup.

::: sqlspec.adapters.asyncpg.AsyncpgRLSStrategy
    options:
      heading_level: 3
      inherited_members: [__init__]
      members:
        - __init__
        - checkout
        - checkin

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.asyncpg.AsyncpgDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.asyncpg.AsyncpgExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.asyncpg.AsyncpgPool
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.asyncpg.litestar.AsyncpgLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.litestar.AsyncpgStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncpg.adk.AsyncpgADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.adk.AsyncpgADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncpg.adk.AsyncpgADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncpg.events.AsyncpgEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.events.AsyncpgEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncpg.events.AsyncpgEventsBackend
    options:
      heading_level: 4
      members: false
