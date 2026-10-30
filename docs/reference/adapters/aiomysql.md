# aiomysql Adapter

```python
from sqlspec.adapters.aiomysql import (
    AiomysqlConfig,
    AiomysqlConnectionParams,
    AiomysqlDriver,
    AiomysqlDriverFeatures,
    AiomysqlExceptionHandler,
    AiomysqlPoolParams,
)
```

Asynchronous MySQL and MariaDB adapter built on `aiomysql`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [MySQL & MariaDB Setup Guide](../../databases/mysql.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.aiomysql.AiomysqlConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.aiomysql.AiomysqlConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiomysql.AiomysqlPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiomysql.AiomysqlDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.aiomysql.AiomysqlDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.aiomysql.AiomysqlExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.aiomysql.litestar.AiomysqlStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.aiomysql.adk.AiomysqlADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.aiomysql.adk.AiomysqlADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.aiomysql.events.AiomysqlEventQueueStore
    options:
      heading_level: 4
      members: false
