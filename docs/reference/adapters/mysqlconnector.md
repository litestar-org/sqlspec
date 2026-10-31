# mysqlconnector Adapter

```python
from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncConnectionParams,
    MysqlConnectorAsyncDriver,
    MysqlConnectorAsyncExceptionHandler,
    MysqlConnectorDriverFeatures,
    MysqlConnectorPoolParams,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncConnectionParams,
    MysqlConnectorSyncDriver,
    MysqlConnectorSyncExceptionHandler,
)
```

Synchronous and asynchronous MySQL and MariaDB adapters built on `mysql-connector-python`. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [MySQL & MariaDB Setup Guide](../../databases/mysql.md) for usage examples and the [MySQL](../data-dictionary/dialects/mysql.md) and [MariaDB](../data-dictionary/dialects/mariadb.md) Data Dictionary SQL Catalogs.

## Configuration & Parameters

::: sqlspec.adapters.mysqlconnector.MysqlConnectorSyncConfig
    options:
      heading_level: 3

::: sqlspec.adapters.mysqlconnector.MysqlConnectorAsyncConfig
    options:
      heading_level: 3

::: sqlspec.adapters.mysqlconnector.MysqlConnectorSyncConnectionParams
    options:
      heading_level: 3
      inherited_members: true
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mysqlconnector.MysqlConnectorAsyncConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mysqlconnector.MysqlConnectorPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mysqlconnector.MysqlConnectorDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.mysqlconnector.MysqlConnectorSyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mysqlconnector.MysqlConnectorAsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mysqlconnector.MysqlConnectorSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mysqlconnector.MysqlConnectorAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mysqlconnector.data_dictionary.MysqlConnectorSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mysqlconnector.data_dictionary.MysqlConnectorAsyncDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.mysqlconnector.litestar.MysqlConnectorLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mysqlconnector.litestar.MysqlConnectorSyncStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.litestar.MysqlConnectorAsyncStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.adk.MysqlConnectorADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.adk.MysqlConnectorAsyncADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.adk.MysqlConnectorAsyncADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.events.MysqlConnectorSyncEventQueueStore
    options:
      heading_level: 4

::: sqlspec.adapters.mysqlconnector.events.MysqlConnectorAsyncEventQueueStore
    options:
      heading_level: 4
