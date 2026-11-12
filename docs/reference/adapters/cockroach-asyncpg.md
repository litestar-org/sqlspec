# cockroach-asyncpg Adapter

```python
from sqlspec.adapters.cockroach_asyncpg import (
    CockroachAsyncpgConfig,
    CockroachAsyncpgConnectionConfig,
    CockroachAsyncpgDriver,
    CockroachAsyncpgDriverFeatures,
    CockroachAsyncpgExceptionHandler,
    CockroachAsyncpgPool,
    CockroachAsyncpgPoolConfig,
    CockroachAsyncpgRetryConfig,
    CockroachAsyncpgRLSStrategy,
)
```

Asynchronous CockroachDB adapter built on `asyncpg` with `SQLSTATE 40001` serialization retry handling and follower-read support. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [CockroachDB Setup Guide](../../databases/cockroachdb.md) for cluster topology and retry configuration, and the [CockroachDB Data Dictionary & SQL Catalog](../data-dictionary/dialects/cockroachdb.md).

## Configuration & Parameters

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgConnectionConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgPoolConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgRetryConfig
    options:
      heading_level: 3
      members: false

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}`; `"default"` and `True` select the CockroachDB strategy. Omitted or `None` pooler mode becomes `"session"`; transaction, statement, and auto modes are rejected. Both providers acquire through the tenant lease. Dirty connections are terminated and released to native pool bookkeeping only after physical disposal.

Declare settings that raw `asyncpg` `init` or `setup` pool hooks change in `tenant_session_baseline`; the lease replays them after the native pool reset. Those hooks require an explicit declaration (`{}` when they leave no lasting settings), and a baseline value must agree with `server_settings`.

One management query captures/applies owned GUCs; another restores/verifies them. Unset owned keys restore to an empty string because NULL does not reset a CockroachDB setting. Cleanup uses no blanket RESET ALL. Existing retry and follower-read behavior remains available; database policies must enforce tenant access.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgRLSStrategy
    options:
      heading_level: 3
      inherited_members: [__init__]
      members: [__init__, checkout, checkin]

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgDriver
    options:
      heading_level: 3
      members:
        - run_transaction_with_retry

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgPool
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.cockroach_asyncpg.litestar.CockroachAsyncpgLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_asyncpg.litestar.CockroachAsyncpgStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_asyncpg.events.CockroachAsyncpgEventQueueStore
    options:
      heading_level: 4
      members: false
