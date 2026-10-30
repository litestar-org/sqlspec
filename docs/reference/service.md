# Service Layer

```python
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
```

Slotted, high-throughput service base classes wrapping `SyncDriverAdapterBase` and `AsyncDriverAdapterBase` sessions for clean domain repository and service patterns across Litestar, FastAPI, Starlette, Flask, Sanic, and Dishka. See the [Integrations Overview](../integrations/index.md) for wiring examples.

## Service Base Classes

::: sqlspec.service._base.SQLSpecSyncService
    options:
      heading_level: 3
      members:
        - provide_session
        - paginate
        - get_one
        - exists

::: sqlspec.service._base.SQLSpecAsyncService
    options:
      heading_level: 3
      members:
        - provide_session
        - paginate
        - get_one
        - exists
