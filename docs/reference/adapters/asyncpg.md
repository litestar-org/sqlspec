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
)
```

Asynchronous PostgreSQL adapter built on `asyncpg`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (asyncpg) Setup Guide](../../databases/postgres/asyncpg.md) for connection pooling, JSON codecs, and pgvector configuration, and the [PostgreSQL Data Dictionary & SQL Catalog](../data-dictionary/dialects/postgres.md).

## Configuration & Parameters

::: sqlspec.adapters.asyncpg.AsyncpgConfig
    options:
      heading_level: 3

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

::: sqlspec.adapters.asyncpg.adk.AsyncpgADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.adk.AsyncpgADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.asyncpg.adk.AsyncpgADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.asyncpg.events.AsyncpgEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncpg.events.AsyncpgEventQueueStore
    options:
      heading_level: 4

::: sqlspec.adapters.asyncpg.events.AsyncpgEventsBackend
    options:
      heading_level: 4
