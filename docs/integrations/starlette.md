# Starlette Integration

`sqlspec.extensions.starlette` provides `SQLSpecPlugin` for Starlette (`>= 1.10.0`), managing database connection pools in Starlette's lifespan and attaching request-scoped driver sessions to `request.state`.

## Installation

=== "uv"

    ```bash
    uv add "sqlspec[starlette]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[starlette]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[starlette]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[starlette]"
    ```

## Basic Setup

Pass a single database config, a sequence of configs, or a `SQLSpec` registry to `SQLSpecPlugin(config, app)` (or call `db_plugin.init_app(app)`), and retrieve the session in route handlers via `db_plugin.get_session(request)`:

```{ .python title="docs/examples/frameworks/starlette/basic_setup.py" }
--8<-- "docs/examples/frameworks/starlette/basic_setup.py:example"
```

## Multi-Database Setup

Bind multiple async or sync configurations by specifying unique `session_key`, `connection_key`, and `pool_key` values inside `extension_config["starlette"]` (`StarletteConfig`):

```{ .python title="docs/examples/frameworks/starlette/multi_database.py" }
--8<-- "docs/examples/frameworks/starlette/multi_database.py:example"
```

## Tenant-Aware Requests

When a configuration sets `driver_features={"tenant_strategy": "rls"}` without a `tenant_extractor`, Starlette defers connection acquisition until your route or auth middleware verifies membership, calls `set_request_tenant(request, org_id)`, and awaits `db_plugin.get_session_async(request)` (or calls `db_plugin.get_session(request)` for sync adapters). Public and login routes skip tenant checkout entirely and can query user records through `config.provide_session(bypass_tenant=True)` or a dedicated auth bind:

```python
from starlette.requests import Request
from starlette.responses import JSONResponse
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.starlette import SQLSpecPlugin, set_request_tenant

db_plugin = SQLSpecPlugin(
    AsyncpgConfig(
        connection_config={"dsn": "postgresql://app_user@localhost/app"},
        driver_features={"tenant_strategy": "rls"},
        extension_config={"starlette": {"commit_mode": "autocommit"}},
    )
)


async def list_projects(request: Request) -> JSONResponse:
    user = request.scope.get("user")
    org_id = request.path_params["org_id"]
    if user is None or org_id not in user.org_ids:
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    set_request_tenant(request, org_id)
    session = await db_plugin.get_session_async(request)
    rows = await session.select("SELECT id, name FROM projects")
    return JSONResponse(rows)
```

Alternatively, configure an independent sync or async `tenant_extractor` per bind in `extension_config["starlette"]` when upstream ASGI authentication populates `scope` before SQLSpec runs. Request transaction finalization (`commit_mode`) executes under the captured tenant identity before resetting the connection lease. See [Tenant Framework Integration](../learn/recipes/multi-tenancy-frameworks.md).

## Configuration Options

<!-- GENERATED:START config-table:sqlspec.config.StarletteConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[[Any], Any]` | - | Trusted scope extractor returning a tenant payload, string, UUID, None, or an awaitable. Optional when handlers or dependencies call set_request_tenant() before acquiring a session. |
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
<!-- GENERATED:END config-table:sqlspec.config.StarletteConfig -->

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Recap

- Pass a config, config list, or `SQLSpec` registry to `SQLSpecPlugin(config, app)` (or `db_plugin.init_app(app)`) and retrieve sessions with `db_plugin.get_session(request)` or `await db_plugin.get_session_async(request)`.
- Use `set_request_tenant(request, tenant_id)` for deferred tenant leases, or configure `commit_mode`, `tenant_extractor`, multi-database keys, and `sqlcommenter` middleware in `extension_config["starlette"]`.
- See the [Starlette Extension Reference](/reference/extensions/starlette/) for `SQLSpecPlugin`, `set_request_tenant`, and `StarletteConfig` details.
