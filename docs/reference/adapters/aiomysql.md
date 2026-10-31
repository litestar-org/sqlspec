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

Asynchronous MySQL and MariaDB adapter built on `aiomysql`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [MySQL & MariaDB Setup Guide](../../databases/mysql.md) for usage examples and the [MySQL](../data-dictionary/dialects/mysql.md) and [MariaDB](../data-dictionary/dialects/mariadb.md) Data Dictionary SQL Catalogs.

## Configuration & Parameters

::: sqlspec.adapters.aiomysql.AiomysqlConfig
    options:
      heading_level: 3

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

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.aiomysql.AiomysqlDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.aiomysql.AiomysqlExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.aiomysql.data_dictionary.AiomysqlDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.aiomysql.litestar.AiomysqlLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiomysql.litestar.AiomysqlStore
    options:
      heading_level: 4

::: sqlspec.adapters.aiomysql.adk.AiomysqlADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.aiomysql.adk.AiomysqlADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.aiomysql.adk.AiomysqlADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.aiomysql.events.AiomysqlEventQueueStore
    options:
      heading_level: 4
