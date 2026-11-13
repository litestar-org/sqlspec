# Driver Base Classes

```python
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    AsyncExceptionHandler,
    AsyncRowStream,
    AsyncRoutingSession,
    DataDictionaryMixin,
    DriverAdapterProtocol,
    ExecutionResult,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
    SyncExceptionHandler,
    SyncRowStream,
    SyncRoutingSession,
)
```

Base driver adapter classes, row streaming iterators, exception handler context managers, and data dictionary mixins shared by all database adapters.

## Reference Pages

| Page | Primary Symbols | Description |
| --- | --- | --- |
| [Synchronous Driver (`SyncDriverAdapterBase`)](sync.md) | `SyncDriverAdapterBase`, `SyncDataDictionaryBase`, `SyncPoolConnectionContext`, `SyncPoolSessionFactory`, `SyncRowStream` | Base synchronous execution, selection, pagination, transaction, and Arrow methods inherited by all synchronous adapters. |
| [Asynchronous Driver (`AsyncDriverAdapterBase`)](async.md) | `AsyncDriverAdapterBase`, `AsyncDataDictionaryBase`, `AsyncPoolConnectionContext`, `AsyncPoolSessionFactory`, `AsyncRowStream` | Base asynchronous execution, selection, pagination, transaction, and Arrow methods inherited by all asynchronous adapters. |
| [Shared Driver Types (`common`)](common.md) | `ExecutionResult`, `SyncExceptionHandler`, `AsyncExceptionHandler`, `DataDictionaryMixin`, `CommonDriverAttributesMixin`, `CachedQuery`, `StackExecutionObserver` | Shared execution metadata, exception handler protocols, query cache entries, and helper functions. |

## Routing Session Wrappers

These opt-in facades acquire concrete drivers lazily. They preserve the portable query and stream surface while pinning primary after writes and transactions. Use a concrete driver session for vendor-specific APIs. See [Read Replicas](../../learn/recipes/read-replicas.md).

::: sqlspec.driver.SyncRoutingSession
    options:
      heading_level: 4
      members: false

::: sqlspec.driver.AsyncRoutingSession
    options:
      heading_level: 4
      members: false
