# FastAPI Extension

```python
from sqlspec.extensions.fastapi import (
    DependencyDefaults,
    FieldNameType,
    FilterConfig,
    SQLSpecAsyncService,
    SQLSpecAutocommitMiddleware,
    SQLSpecManualMiddleware,
    SQLSpecPlugin,
    SQLSpecSyncService,
    provide_filters,
)
```

FastAPI lifespan plugin, request-scoped `Depends()` session/connection providers, and declarative query-parameter filter dependency builders. See the [FastAPI Integration Guide](../../integrations/fastapi.md) for usage examples.

## Plugin & Middlewares

::: sqlspec.extensions.fastapi.extension.SQLSpecPlugin
    options:
      heading_level: 3
      members:
        - provide_session
        - provide_async_session
        - provide_sync_session
        - provide_connection
        - provide_async_connection
        - provide_sync_connection
        - provide_filters

::: sqlspec.extensions.starlette.middleware.SQLSpecAutocommitMiddleware
    options:
      heading_level: 3

::: sqlspec.extensions.starlette.middleware.SQLSpecManualMiddleware
    options:
      heading_level: 3

## Filter Dependency Providers

::: sqlspec.extensions.fastapi.providers.provide_filters
    options:
      heading_level: 3

::: sqlspec.extensions.fastapi.providers.FilterConfig
    options:
      heading_level: 3

::: sqlspec.extensions.fastapi.providers.DependencyDefaults
    options:
      heading_level: 3
