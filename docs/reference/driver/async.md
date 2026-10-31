# Asynchronous Driver Base

```python
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    AsyncRowStream,
)
```

Base asynchronous driver adapter class inherited by all asynchronous SQLSpec database adapters (`AsyncpgDriver`, `PsycopgAsyncDriver`, `PsqlpyDriver`, `AiosqliteDriver`, `AsyncmyDriver`, `AiomysqlDriver`, `OracleAsyncDriver`, `SpannerAsyncDriver`, `Db2AsyncDriver`, `CockroachAsyncpgDriver`, `CockroachPsycopgAsyncDriver`). See [Querying](../../learn/querying.md) for usage examples.

## Asynchronous Driver Adapter

::: sqlspec.driver._async.AsyncDriverAdapterBase
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

## Asynchronous Data Dictionary & Streaming

See [Runtime Introspection & DDL Synthesis](../data-dictionary/runtime.md) and [Dialect SQL Catalogs](../data-dictionary/dialects/index.md) for full `AsyncDataDictionaryBase` method signatures and SQL queries.

::: sqlspec.driver._async.AsyncDataDictionaryBase
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._stream.AsyncRowStream
    options:
      heading_level: 3

## Pool Context Managers

::: sqlspec.driver._async.AsyncPoolConnectionContext
    options:
      heading_level: 3

::: sqlspec.driver._async.AsyncPoolSessionFactory
    options:
      heading_level: 3
