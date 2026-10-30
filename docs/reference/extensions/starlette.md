# Starlette Extension

```python
from sqlspec.extensions.starlette import (
    SQLSpecAsyncService,
    SQLSpecAutocommitMiddleware,
    SQLSpecConfigState,
    SQLSpecManualMiddleware,
    SQLSpecPlugin,
    SQLSpecSyncService,
    get_connection_from_request,
    get_or_create_session,
)
```

Starlette lifespan management, autocommit and manual transaction ASGI middlewares, and `request.state` session extraction helpers. See the [Starlette Integration Guide](../../integrations/starlette.md) for usage examples.

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
