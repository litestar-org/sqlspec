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

Pass a single database config, a sequence of configs, or a `SQLSpec` registry to `SQLSpecPlugin(config, app)` (or call `db_plugin.init_app(app)`), and retrieve the session in route handlers via `await db_plugin.get_session_async(request)`:

```{ .python title="docs/examples/frameworks/starlette/basic_setup.py" }
--8<-- "docs/examples/frameworks/starlette/basic_setup.py:example"
```

Request connections are checked out on first access, so routes that never touch the database open none. With an async configuration, `db_plugin.get_session(request)` returns a session that opens the connection on its first awaited call, so `await session.select(...)`, `async for row in session.select_stream(...)`, and `async with session.transaction():` work unchanged; `await db_plugin.get_session_async(request)` opens it immediately. Reading `session.connection` before the first awaited call raises `ImproperConfigurationError`, and `get_connection(request)` returns an async connection only once it is open, so use `await db_plugin.get_connection_async(request)` before that. Autocommit modes commit or roll back by response status only when the request used a connection.

## Multi-Database Setup

Bind multiple async or sync configurations by specifying unique `session_key`, `connection_key`, and `pool_key` values inside `extension_config["starlette"]` (`StarletteConfig`):

```{ .python title="docs/examples/frameworks/starlette/multi_database.py" }
--8<-- "docs/examples/frameworks/starlette/multi_database.py:example"
```

## Tenant-Aware Requests

With `driver_features={"tenant_strategy": "rls"}` on a supported adapter, Starlette defers checkout until a route verifies membership, calls `set_request_tenant(request, org_id)`, and awaits `db_plugin.get_session_async(request)`; a per-bind `tenant_extractor` can resolve the tenant from the verified ASGI scope instead. See [Starlette Multi-Tenancy](../learn/recipes/multi-tenancy-starlette.md) for complete examples and the [Tenant Request Contract](../learn/recipes/multi-tenancy-frameworks.md#one-request-contract) for the rules every framework shares.

## Configuration Options

<!-- GENERATED:START config-table:sqlspec.config.StarletteConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[[Any], Any]` | - | Trusted scope extractor returning a tenant payload, string, UUID, None (no tenant from this source), or an awaitable. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start. |
| `connection_key` | `str` | `'db_connection'` | Key for storing the connection in request.state once it is checked out on first access. Default: 'db_connection' |
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
| `read_session_key` | `str` | - | Request key for a read session. |
| `read_connection_key` | `str` | - | Request key for a read connection. |
| `routing_session_key` | `str` | - | Request key for a routing session. |
| `default_routing_mode` | `Literal['primary', 'auto']` | `'auto'` | Default request routing mode. Default: `'auto'`. |
| `enable_routing_middleware` | `bool` | `True` | Install request-scoped routing middleware. Default: True. |
| `routing_header` | `str or None` | `None` | Trusted application routing header name when explicitly enabled behind a gateway. Default: None. |
| `write_pin_cookie` | `str or None` | `None` | Optional application write-pin cookie name. Default: None. |
| `routing_secret` | `str or bytes or None` | `None` | Optional HMAC secret verifying signed write-pin cookies. Default: None. |
| `read_pool_key` | `str` | - | Request key for the selected read pool. |
<!-- GENERATED:END config-table:sqlspec.config.StarletteConfig -->

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Read Replica and Routing Sessions

`db_ext.get_read_session(request, key=...)` returns a reader session. For an async config it is a `DeferredAsyncSession`: the first awaited database call opens the physical connection. Use `await db_ext.get_read_session_async(request, key=...)` when the handler needs an acquired session immediately. The raw helpers are `get_read_connection` and `get_read_connection_async`.

`get_routing_session` and `get_routing_session_async` create an opt-in facade that selects a reader or pins primary as work requires. Pure ASGI middleware scopes the request and closes owned checkouts. Add `SQLSpecRoutingMiddleware` from `sqlspec.utils.routing` to read routing hints and scope write pins. Tenant identity remains a separate access check. See [Read Replicas](../learn/recipes/read-replicas.md).

## Recap

- Pass a config, config list, or `SQLSpec` registry to `SQLSpecPlugin(config, app)` (or `db_plugin.init_app(app)`) and retrieve sessions with `db_plugin.get_session(request)` or `await db_plugin.get_session_async(request)`; connections are checked out on first access.
- Use `set_request_tenant(request, tenant_id)` for deferred tenant leases, or configure `commit_mode`, `tenant_extractor`, multi-database keys, and `sqlcommenter` middleware in `extension_config["starlette"]`.
- See the [Starlette Extension Reference](/reference/extensions/starlette/) for `SQLSpecPlugin`, `set_request_tenant`, and `StarletteConfig` details.
