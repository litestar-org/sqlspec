# cockroach-psycopg Adapter

```python
from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgAsyncExceptionHandler,
    CockroachPsycopgConnectionConfig,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgPoolConfig,
    CockroachPsycopgRetryConfig,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
    CockroachPsycopgSyncExceptionHandler,
)
```

Synchronous and asynchronous CockroachDB adapters built on `psycopg` v3 with `SQLSTATE 40001` retry policies and follower-read support. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [CockroachDB Setup Guide](../../databases/cockroachdb.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgConnectionConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgPoolConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgRetryConfig
    options:
      heading_level: 3
      members: false

## Driver & Pool

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.cockroach_psycopg.litestar.CockroachPsycopgSyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.litestar.CockroachPsycopgAsyncStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgAsyncADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgAsyncADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.events.CockroachPsycopgSyncEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.cockroach_psycopg.events.CockroachPsycopgAsyncEventQueueStore
    options:
      heading_level: 4
      members: false
