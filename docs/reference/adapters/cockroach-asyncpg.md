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

Asynchronous CockroachDB adapter built on `asyncpg` with `SQLSTATE 40001` serialization retry handling and follower-read support. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [CockroachDB Setup Guide](../../databases/cockroachdb.md) for cluster topology and retry configuration, and the [CockroachDB Data Dictionary & SQL Catalog](../data-dictionary/dialects/cockroachdb.md).

## Configuration & Parameters

::: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgConfig
    options:
      heading_level: 3

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

::: sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.cockroach_asyncpg.events.CockroachAsyncpgEventQueueStore
    options:
      heading_level: 4
