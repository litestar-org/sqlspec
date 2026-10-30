# Shared Driver Types

```python
from sqlspec.driver import (
    AsyncExceptionHandler,
    BaseAsyncExceptionHandler,
    BaseSyncExceptionHandler,
    CachedQuery,
    CommonDriverAttributesMixin,
    DataDictionaryDialectMixin,
    DataDictionaryMixin,
    ExecutionResult,
    StackExecutionObserver,
    SyncExceptionHandler,
    convert_to_dialect,
    rows_to_dicts,
    validate_savepoint_name,
)
```

Shared driver execution results, exception handler protocols and base classes, mixins, and low-level driver utilities.

## Execution Metadata & Caching

::: sqlspec.driver._common.ExecutionResult
    options:
      heading_level: 3
      members: false

::: sqlspec.core.cache.CachedQuery
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._common.StackExecutionObserver
    options:
      heading_level: 3
      members: false

## Exception Handlers

::: sqlspec.driver._common.SyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._common.AsyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._exception_handler.BaseSyncExceptionHandler
    options:
      heading_level: 4
      members: false

::: sqlspec.driver._exception_handler.BaseAsyncExceptionHandler
    options:
      heading_level: 4
      members: false

## Driver Mixins & Helper Functions

::: sqlspec.driver._common.CommonDriverAttributesMixin
    options:
      heading_level: 3
      members:
        - prepare_statement
        - compile_statement
        - build_statement_result
        - create_execution_result

::: sqlspec.driver._common.DataDictionaryMixin
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._common.DataDictionaryDialectMixin
    options:
      heading_level: 3
      members: false

::: sqlspec.driver._sql_helpers.convert_to_dialect
    options:
      heading_level: 4

::: sqlspec.driver._stream.rows_to_dicts
    options:
      heading_level: 4

::: sqlspec.driver._common.validate_savepoint_name
    options:
      heading_level: 4
