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

Base asynchronous driver adapter class inherited by all asynchronous SQLSpec database adapters (`AsyncpgDriver`, `PsycopgAsyncDriver`, `PsqlpyDriver`, `AiosqliteDriver`, `AsyncmyDriver`, `AiomysqlDriver`, `OracleAsyncDriver`, `SpannerAsyncDriver`, `Db2AsyncDriver`, `MssqlAsyncDriver`, `CockroachAsyncpgDriver`, `CockroachPsycopgAsyncDriver`). See [Querying](../../learn/querying.md) for usage examples.

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
        - select_to_arrow
        - load_from_arrow
        - begin
        - commit
        - rollback
        - transaction

## Asynchronous Data Dictionary & Streaming

See [Data Dictionary](../data-dictionary.md) for catalog introspection, DDL extraction, and column profiling method signatures.

::: sqlspec.driver._async.AsyncDataDictionaryBase
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._stream.AsyncRowStream
    options:
      heading_level: 3
      members: false

## Pool Context Managers

::: sqlspec.driver._async.AsyncPoolConnectionContext
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._async.AsyncPoolSessionFactory
    options:
      heading_level: 3
      members: false
