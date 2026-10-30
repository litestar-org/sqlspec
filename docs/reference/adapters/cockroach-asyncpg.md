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
)
```

Asynchronous CockroachDB adapter built on `asyncpg` with `SQLSTATE 40001` serialization retry handling and follower-read support. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [CockroachDB Setup Guide](../../databases/cockroachdb.md) for cluster topology and retry configuration.

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

## Driver & Pool

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgPool
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.cockroach_asyncpg.litestar.CockroachAsyncpgStore
    options:
      heading_level: 4
      members: false

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
