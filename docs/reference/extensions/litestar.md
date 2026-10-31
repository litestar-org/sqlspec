# Litestar Extension

```python
from sqlspec.extensions.litestar import (
    BaseSQLSpecStore,
    CommitMode,
    CorrelationMiddleware,
    LitestarConfig,
    SQLSpecAsyncService,
    SQLSpecChannelsBackend,
    SQLSpecPlugin,
    SQLSpecSyncService,
    database_group,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
```

Litestar plugin, ASGI transaction and correlation middlewares, server-side session store base class, and Litestar Channels backend. Adapter-specific `LitestarConfig` TypedDicts and `BaseSQLSpecStore` subclasses are documented on each [Database Adapter](../adapters/index.md) page. See the [Litestar Integration Guide](../../integrations/litestar.md) for usage examples.

## Plugin & Configuration

::: sqlspec.extensions.litestar.plugin.SQLSpecPlugin
    options:
      heading_level: 3
      members:
        - config
        - on_app_init
        - on_cli_init
        - get_config
        - get_annotations

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

## Scope State Helpers

::: sqlspec.extensions.litestar._utils.get_sqlspec_scope_state
    options:
      heading_level: 4

::: sqlspec.extensions.litestar._utils.set_sqlspec_scope_state
    options:
      heading_level: 4
