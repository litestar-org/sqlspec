# Starlette Extension

```python
from sqlspec.extensions.starlette import (
    SQLSpecAutocommitMiddleware,
    SQLSpecConfigState,
    SQLSpecManualMiddleware,
    SQLSpecPlugin,
    get_connection_from_request,
    get_or_create_session,
    set_request_tenant,
    tenant_bypass,
)
```

Starlette lifespan management, autocommit and manual transaction ASGI middlewares, and `request.state` session extraction helpers (`SQLSpecSyncService` and `SQLSpecAsyncService` are documented in [Service Layer](../service.md)). See the [Starlette Integration Guide](../../integrations/starlette.md) for usage examples.

## Plugin & State

::: sqlspec.extensions.starlette.extension.SQLSpecPlugin
    options:
      heading_level: 3

::: sqlspec.extensions.starlette._state.SQLSpecConfigState
    options:
      heading_level: 3

## ASGI Transaction Middlewares

::: sqlspec.extensions.starlette.middleware.SQLSpecAutocommitMiddleware
    options:
      heading_level: 3

::: sqlspec.extensions.starlette.middleware.SQLSpecManualMiddleware
    options:
      heading_level: 3

## Request Session Helpers

::: sqlspec.extensions.starlette._utils.get_or_create_session
    options:
      heading_level: 3

::: sqlspec.extensions.starlette._utils.get_connection_from_request
    options:
      heading_level: 3

::: sqlspec.extensions.starlette._utils.set_request_tenant
    options:
      heading_level: 3

::: sqlspec.utils.tenant.tenant_bypass
    options:
      heading_level: 3
