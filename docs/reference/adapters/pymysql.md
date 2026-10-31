# pymysql Adapter

```python
from sqlspec.adapters.pymysql import (
    PyMysqlConfig,
    PyMysqlConnectionParams,
    PyMysqlDriver,
    PyMysqlDriverFeatures,
    PyMysqlExceptionHandler,
    PyMysqlPoolParams,
)
```

Synchronous MySQL and MariaDB adapter built on `pymysql`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [MySQL & MariaDB Setup Guide](../../databases/mysql.md) for usage examples and the [MySQL](../data-dictionary/dialects/mysql.md) and [MariaDB](../data-dictionary/dialects/mariadb.md) Data Dictionary SQL Catalogs.

## Configuration & Parameters

::: sqlspec.adapters.pymysql.PyMysqlConfig
    options:
      heading_level: 3

::: sqlspec.adapters.pymysql.PyMysqlConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.pymysql.PyMysqlPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.pymysql.PyMysqlDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.pymysql.PyMysqlDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.pymysql.PyMysqlExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.pymysql.data_dictionary.PyMysqlDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.pymysql.litestar.PyMysqlLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.pymysql.litestar.PyMysqlStore
    options:
      heading_level: 4

::: sqlspec.adapters.pymysql.adk.PyMysqlADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.pymysql.adk.PyMysqlADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.pymysql.adk.PyMysqlADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.pymysql.events.PyMysqlEventQueueStore
    options:
      heading_level: 4
