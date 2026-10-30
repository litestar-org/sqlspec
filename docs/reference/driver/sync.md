# Synchronous Driver Base

```python
from sqlspec.driver import (
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
    SyncRowStream,
)
```

Base synchronous driver adapter class inherited by all synchronous SQLSpec database adapters (`SqliteDriver`, `PsycopgSyncDriver`, `PymysqlDriver`, `OracleSyncDriver`, `DuckDBDriver`, `BigQueryDriver`, `SpannerSyncDriver`, `Db2SyncDriver`, `MssqlSyncDriver`, `PymssqlDriver`, `AdbcDriver`, `ArrowOdbcDriver`). See [Querying](../../learn/querying.md) for usage examples.

## Synchronous Driver Adapter

::: sqlspec.driver._sync.SyncDriverAdapterBase
    options:
      heading_level: 3
      members:
        - execute
        - execute_many
        - execute_script
        - execute_stack
        - select
        - select_one
        - select_one_or_none
        - select_value
        - select_value_or_none
        - select_with_total
        - select_to_arrow
        - load_from_arrow
        - begin
        - commit
        - rollback
        - transaction

## Synchronous Data Dictionary & Streaming

::: sqlspec.driver._sync.SyncDataDictionaryBase
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._stream.SyncRowStream
    options:
      heading_level: 3
      members: false

## Pool Context Managers

::: sqlspec.driver._sync.SyncPoolConnectionContext
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._sync.SyncPoolSessionFactory
    options:
      heading_level: 3
      members: false
