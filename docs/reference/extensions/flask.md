# Flask Extension

```python
from sqlspec.extensions.flask import (
    FlaskConfigState,
    SQLSpecPlugin,
    set_request_tenant,
    tenant_bypass,
)
```

Flask WSGI extension managing `flask.g` request-scoped sessions, deferred tenant leases, automatic commit/rollback teardown hooks, and background event-loop portal bridging for async adapters (`SQLSpecSyncService` and `SQLSpecAsyncService` are documented in [Service Layer](../service.md)). See the [Flask Integration Guide](../../integrations/flask.md) for usage examples.

## Plugin & Configuration State

::: sqlspec.extensions.flask.extension.SQLSpecPlugin
    options:
      heading_level: 3

::: sqlspec.extensions.flask._state.FlaskConfigState
    options:
      heading_level: 3

## Request Tenant Helpers

::: sqlspec.extensions.flask._utils.set_request_tenant
    options:
      heading_level: 3

::: sqlspec.utils.tenant.tenant_bypass
    options:
      heading_level: 3
