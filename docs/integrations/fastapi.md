# FastAPI Integration

`sqlspec.extensions.fastapi` provides `SQLSpecPlugin` for FastAPI (`>= 0.162.0`), wiring database connection pools into FastAPI's lifespan context manager and exposing `Depends(...)` callables for request-scoped driver sessions, connections, and declarative query filters.

## Installation

=== "uv"

    ```bash
    uv add "sqlspec[fastapi]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[fastapi]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[fastapi]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[fastapi]"
    ```

## Basic Setup and Session Injection

Pass a single database configuration, a sequence of configurations, or a `SQLSpec` registry to `SQLSpecPlugin(..., app)` and inject the session into route handlers using `Depends(db_ext.provide_session())`:

```{ .python title="docs/examples/frameworks/fastapi/basic_setup.py" }
--8<-- "docs/examples/frameworks/fastapi/basic_setup.py:example"
```

`SQLSpecPlugin` automatically opens configured connection pools when FastAPI starts and closes them when the application shuts down.

## Multi-Database Configuration

Pass multiple synchronous or asynchronous configurations with distinct `session_key`, `connection_key`, and `pool_key` names under `extension_config["fastapi"]`, then pass the key (or `bind_key`) to `db_plugin.provide_session("<key>")`:

```{ .python title="docs/examples/frameworks/fastapi/multi_database.py" }
--8<-- "docs/examples/frameworks/fastapi/multi_database.py:example"
```

## Transaction Commit Modes and Options

Configure `extension_config["fastapi"]` (`FastAPIConfig`, which inherits from `StarletteConfig`) per database bind:

<!-- GENERATED:START config-table:sqlspec.config.FastAPIConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[[Any], Any]` | - | Trusted scope extractor returning a tenant payload, string, UUID, None (no tenant from this source), or an awaitable. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start. |
| `connection_key` | `str` | `'db_connection'` | Key for storing connection in request.state. Default: 'db_connection' |
| `pool_key` | `str` | `'db_pool'` | Key for storing connection pool in app.state. Default: 'db_pool' |
| `session_key` | `str` | `'db_session'` | Key for storing session in request.state. Default: 'db_session' |
| `commit_mode` | `Literal['manual', 'autocommit', 'autocommit_include_redirect']` | `'manual'` | Transaction commit mode. Default: 'manual'. manual: No automatic commit/rollback; autocommit: Commit on 2xx, rollback otherwise; autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise |
| `extra_commit_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger commit. Default: set() |
| `extra_rollback_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger rollback. Default: set() |
| `disable_di` | `bool` | `False` | Disable built-in dependency injection. Default: False. When True, the Starlette/FastAPI extension will not add middleware for managing database connections and sessions. Users are responsible for managing the database lifecycle manually via their own DI solution. |
| `enable_sqlcommenter_middleware` | `bool` | `True` | Control automatic SQLCommenter middleware registration. Default: True. When the driver's :class:`~sqlspec.core.statement.StatementConfig` has `enable_sqlcommenter=True`, the middleware is registered automatically. Set to `False` to explicitly disable middleware registration. |
| `sqlcommenter_framework` | `str` | `'starlette'` | Framework name for SQLCommenter attributes. Default: 'starlette'. Set to 'fastapi' when using FastAPI. |
| `enable_correlation_middleware` | `bool` | `True` | Enable request correlation ID middleware. Default: True |
| `correlation_header` | `str` | `X-Request-ID` | HTTP header to read the request correlation ID from when middleware is enabled. Default: `X-Request-ID` |
| `correlation_headers` | `tuple[str, ...] or list[str]` | - | Additional HTTP headers to read as correlation ID fallbacks. |
| `auto_trace_headers` | `bool` | `True` | Read standard trace context headers as correlation ID fallbacks. Default: True. |
<!-- GENERATED:END config-table:sqlspec.config.FastAPIConfig -->

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Tenant-Aware Requests

With `driver_features={"tenant_strategy": "rls"}` on a supported adapter, an authorization dependency verifies membership and calls `set_request_tenant(request, org_id)`, and `Depends(db_ext.provide_session())` checks out the tenant connection only when a route resolves it. See [FastAPI Multi-Tenancy](../learn/recipes/multi-tenancy-fastapi.md) for single-database, separate auth database, and upfront extractor examples, and the [Tenant Request Contract](../learn/recipes/multi-tenancy-frameworks.md#one-request-contract) for the rules every framework shares.

## Declarative Filter Dependencies

Use `db_ext.provide_filters(FilterConfig(...))` (or `from sqlspec.extensions.fastapi import FilterConfig, provide_filters`) to generate typed FastAPI `Depends` callables that parse query parameters directly into `StatementFilter` lists (`LimitOffsetFilter`, `CursorFilter` returning `HTTP 422` on malformed cursor tokens, `OrderByFilter`, `SearchFilter`, `BeforeAfterFilter`, and `InCollectionFilter`):

```python
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI
from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import FilterTypes
from sqlspec.extensions.fastapi import FilterConfig, SQLSpecPlugin, provide_filters

user_filters = provide_filters(
    FilterConfig(
        id_filter=UUID,
        sort_field={"created_at", "name"},
        pagination_type="cursor",
        cursor_keys="id",
        pagination_size=25,
        search="name,email",
        search_ignore_case=True,
    )
)


def register_user_routes(app: FastAPI, db_ext: SQLSpecPlugin) -> None:
    @app.get("/users")
    async def list_users(
        db: Annotated[AsyncpgDriver, Depends(db_ext.provide_session())],
        filters: Annotated[list[FilterTypes], Depends(user_filters)],
    ) -> list[dict[str, Any]]:
        return await db.select(
            "SELECT id, name, email, created_at FROM users", *filters
        )
```

## Recap

- Instantiate `db_ext = SQLSpecPlugin(config, app)` (passing a single config, a list of configs, or a `SQLSpec` registry) to bind pool lifecycles and request sessions to FastAPI.
- Use `Depends(db_ext.provide_session())` for single-database routes, `Depends(db_ext.provide_session("key"))` for multi-database setups, `set_request_tenant(request, tenant_id)` in auth dependencies for deferred tenant leases, and `Depends(db_ext.provide_filters(FilterConfig(...)))` for declarative query filters.
- See the [FastAPI Extension Reference](/reference/extensions/fastapi/) for `SQLSpecPlugin`, `FastAPIConfig`, `set_request_tenant`, and `provide_filters` signatures.
