# Driver Base Classes

```python
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    AsyncExceptionHandler,
    AsyncRowStream,
    DataDictionaryMixin,
    DriverAdapterProtocol,
    ExecutionResult,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
    SyncExceptionHandler,
    SyncRowStream,
)
```

Base driver adapter classes, row streaming iterators, exception handler context managers, and data dictionary mixins shared by all database adapters.

## Reference Pages

| Page | Primary Symbols | Description |
| --- | --- | --- |
| [Synchronous Driver (`SyncDriverAdapterBase`)](sync.md) | `SyncDriverAdapterBase`, `SyncDataDictionaryBase`, `SyncPoolConnectionContext`, `SyncPoolSessionFactory`, `SyncRowStream` | Base synchronous execution, selection, pagination, transaction, and Arrow methods inherited by all synchronous adapters. |
| [Asynchronous Driver (`AsyncDriverAdapterBase`)](async.md) | `AsyncDriverAdapterBase`, `AsyncDataDictionaryBase`, `AsyncPoolConnectionContext`, `AsyncPoolSessionFactory`, `AsyncRowStream` | Base asynchronous execution, selection, pagination, transaction, and Arrow methods inherited by all asynchronous adapters. |
| [Shared Driver Types (`common`)](common.md) | `ExecutionResult`, `SyncExceptionHandler`, `AsyncExceptionHandler`, `DataDictionaryMixin`, `CommonDriverAttributesMixin`, `CachedQuery`, `StackExecutionObserver` | Shared execution metadata, exception handler protocols, query cache entries, and helper functions. |
