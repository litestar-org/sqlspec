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

Synchronous MySQL and MariaDB adapter built on `pymysql`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [MySQL & MariaDB Setup Guide](../../databases/mysql.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.pymysql.PyMysqlConfig
    options:
      heading_level: 3
      members: false

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

## Driver & Pool

::: sqlspec.adapters.pymysql.PyMysqlDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.pymysql.PyMysqlExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.pymysql.litestar.PyMysqlStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymysql.adk.PyMysqlADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymysql.adk.PyMysqlADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.pymysql.events.PyMysqlEventQueueStore
    options:
      heading_level: 4
      members: false
