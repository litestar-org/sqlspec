# Sanic Extension

```python
from sqlspec.extensions.sanic import (
    SanicConfigState,
    SQLSpecAsyncService,
    SQLSpecPlugin,
    SQLSpecSyncService,
    get_connection_from_request,
    get_or_create_session,
)
```

Sanic server listener and request/response middleware extension for pool lifecycle management and `request.ctx` session injection. See the [Sanic Integration Guide](../../integrations/sanic.md) for usage examples.

## Plugin & Configuration State

::: sqlspec.extensions.sanic.extension.SQLSpecPlugin
    options:
      heading_level: 3

::: sqlspec.extensions.sanic._state.SanicConfigState
    options:
      heading_level: 3

## Request Context Helpers

::: sqlspec.extensions.sanic._utils.get_or_create_session
    options:
      heading_level: 3

::: sqlspec.extensions.sanic._utils.get_connection_from_request
    options:
      heading_level: 3
