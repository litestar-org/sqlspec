# Sanic Extension

```python
from sqlspec.extensions.sanic import (
    SanicConfigState,
    SQLSpecPlugin,
    get_connection_from_request,
    get_or_create_session,
    set_request_tenant,
    tenant_bypass,
)
```

Sanic server listener and request/response middleware extension for pool lifecycle management, deferred tenant leases, and `request.ctx` session injection (`SQLSpecSyncService` and `SQLSpecAsyncService` are documented in [Service Layer](../service.md)). See the [Sanic Integration Guide](../../integrations/sanic.md) for usage examples.

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

::: sqlspec.extensions.sanic._utils.set_request_tenant
    options:
      heading_level: 3

::: sqlspec.utils.tenant.tenant_bypass
    options:
      heading_level: 3
