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

Base synchronous driver adapter class inherited by all synchronous SQLSpec database adapters (`SqliteDriver`, `PsycopgSyncDriver`, `PymysqlDriver`, `OracleSyncDriver`, `DuckDBDriver`, `BigQueryDriver`, `SpannerSyncDriver`, `Db2SyncDriver`, `MssqlSyncDriver`, `PymssqlDriver`, `SybaseSyncDriver`, `SqlAnywhereSyncDriver`, `AdbcDriver`, `ArrowOdbcDriver`). See [Querying](../../learn/querying.md) for usage examples.

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
        - select_stream
        - select_to_arrow
        - select_to_storage
        - load_from_arrow
        - load_from_storage
        - without_indexes
        - begin
        - commit
        - rollback
        - transaction

## Synchronous Data Dictionary & Streaming

See [Runtime Introspection & DDL Synthesis](../data-dictionary/runtime.md) and [Dialect SQL Catalogs](../data-dictionary/dialects/index.md) for full `SyncDataDictionaryBase` method signatures and SQL queries.

::: sqlspec.driver._sync.SyncDataDictionaryBase
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._stream.SyncRowStream
    options:
      heading_level: 3

## Pool Context Managers

::: sqlspec.driver._sync.SyncPoolConnectionContext
    options:
      heading_level: 3

::: sqlspec.driver._sync.SyncPoolSessionFactory
    options:
      heading_level: 3
