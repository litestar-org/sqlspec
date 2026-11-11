# Litestar Extension

```python
from sqlspec.extensions.litestar import (
    BaseSQLSpecStore,
    CommitMode,
    CorrelationMiddleware,
    LitestarConfig,
    SQLSpecChannelsBackend,
    SQLSpecPlugin,
    TenantResolutionSettings,
    get_sqlspec_scope_state,
    is_scope_authenticated,
    require_tenant,
    resolve_litestar_tenant,
    set_connection_tenant,
    set_sqlspec_scope_state,
)
from sqlspec.extensions.litestar.plugin import (
    integrity_error_handler,
    not_found_error_handler,
)
```

Litestar plugin, ASGI transaction and correlation middlewares, automatic HTTP `404`/`409` exception handlers, server-side session store base class, and Litestar Channels backend (`SQLSpecSyncService` and `SQLSpecAsyncService` are documented in [Service Layer](../service.md); `TenantResolutionSettings`, `require_tenant`, `set_connection_tenant`, `resolve_litestar_tenant`, and `is_scope_authenticated` are documented in [Tenant APIs](../tenant.md#litestar-tenant-resolution)). Adapter-specific `LitestarConfig` TypedDicts and `BaseSQLSpecStore` subclasses are documented on each [Database Adapter](../adapters/index.md) page. See the [Litestar Integration Guide](../../integrations/litestar.md) for usage examples.

## Plugin & Configuration

::: sqlspec.extensions.litestar.plugin.SQLSpecPlugin
    options:
      heading_level: 3
      members:
        - sqlspec
        - config
        - on_app_init
        - on_cli_init
        - get_config
        - resolve_tenant_settings
        - get_annotations
        - provide_request_session
        - provide_request_session_sync
        - provide_request_session_async

::: sqlspec.extensions.litestar.plugin.CommitMode
    options:
      heading_level: 3

::: sqlspec.config.LitestarConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Middleware, Stores & Channels

::: sqlspec.extensions.litestar.plugin.CorrelationMiddleware
    options:
      heading_level: 3

::: sqlspec.extensions.litestar.store.BaseSQLSpecStore
    options:
      heading_level: 3
      members:
        - create_table
        - get
        - set
        - delete
        - delete_all
        - exists
        - expires_in
        - delete_expired

::: sqlspec.extensions.litestar.channels.SQLSpecChannelsBackend
    options:
      heading_level: 3
      members:
        - on_startup
        - on_shutdown
        - publish
        - subscribe
        - unsubscribe
        - stream_events

## Exception Handlers & Scope State Helpers

::: sqlspec.extensions.litestar.plugin.not_found_error_handler
    options:
      heading_level: 4

::: sqlspec.extensions.litestar.plugin.integrity_error_handler
    options:
      heading_level: 4

::: sqlspec.extensions.litestar._utils.get_sqlspec_scope_state
    options:
      heading_level: 4

::: sqlspec.extensions.litestar._utils.set_sqlspec_scope_state
    options:
      heading_level: 4
