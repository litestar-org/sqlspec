# FastAPI Extension

```python
from sqlspec.extensions.fastapi import (
    DependencyDefaults,
    FieldNameType,
    FilterConfig,
    SQLSpecAutocommitMiddleware,
    SQLSpecManualMiddleware,
    SQLSpecPlugin,
    provide_filters,
    set_request_tenant,
    tenant_bypass,
)
```

FastAPI lifespan plugin, request-scoped `Depends()` session/connection providers, deferred tenant receipt helpers, and declarative query-parameter filter dependency builders (`SQLSpecSyncService` and `SQLSpecAsyncService` are documented in [Service Layer](../service.md)). See the [FastAPI Integration Guide](../../integrations/fastapi.md) for usage examples.

## Plugin & Middlewares

::: sqlspec.extensions.fastapi.extension.SQLSpecPlugin
    options:
      heading_level: 3
      inherited_members:
        - get_config
        - get_session_async
        - get_connection_async
      members:
        - get_config
        - provide_session
        - provide_async_session
        - provide_sync_session
        - provide_connection
        - provide_async_connection
        - provide_sync_connection
        - get_session_async
        - get_connection_async
        - provide_filters

### Shared Starlette Components

FastAPI reuses the Starlette request middleware and tenant helpers. They are documented once on the [Starlette Extension](starlette.md) page:

- [`SQLSpecAutocommitMiddleware`](starlette.md#sqlspec.extensions.starlette.middleware.SQLSpecAutocommitMiddleware) and [`SQLSpecManualMiddleware`](starlette.md#sqlspec.extensions.starlette.middleware.SQLSpecManualMiddleware) own the per-request connection and transaction.
- [`set_request_tenant`](starlette.md#sqlspec.extensions.starlette._utils.set_request_tenant) records an authorized tenant receipt for the request.
- [`tenant_bypass`](../tenant.md#sqlspec.utils.tenant.tenant_bypass) marks a route as a trusted cross-tenant route.

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

::: sqlspec.extensions.fastapi.providers.FieldNameType
    options:
      heading_level: 4
