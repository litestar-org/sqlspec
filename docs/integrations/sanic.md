# Sanic Integration

`sqlspec.extensions.sanic` provides `SQLSpecPlugin` for Sanic applications, registering `before_server_start` and `after_server_stop` pool listeners, managing per-request sessions on `request.ctx`, and supporting status-aware transaction commit modes and observability middleware.

## Installation

=== "uv"

    ```bash
    uv add "sqlspec[sanic]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[sanic]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[sanic]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[sanic]"
    ```

## Basic Setup

Pass a single database configuration, a sequence of configurations, or a `SQLSpec` registry to `SQLSpecPlugin(config, app)` (or call `db_plugin.init_app(app)`), configure `extension_config["sanic"]` (`SanicConfig`), and retrieve the session with `await db_plugin.get_session_async(request, "db")`:

```{ .python title="docs/examples/frameworks/sanic/basic_setup.py" }
--8<-- "docs/examples/frameworks/sanic/basic_setup.py:example"
```

Request connections are checked out on first access, so routes that never touch the database open none. With an async configuration, `db_plugin.get_session(request)` returns a session that opens the connection on its first awaited call, so `await session.select(...)`, `async for row in session.select_stream(...)`, and `async with session.transaction():` work unchanged; `await db_plugin.get_session_async(request)` opens it immediately. Reading `session.connection` before the first awaited call raises `ImproperConfigurationError`, and `get_connection(request)` returns an async connection only once it is open, so use `await db_plugin.get_connection_async(request)` before that. Autocommit modes commit or roll back by response status only when the request used a connection.

## Commit Modes and Custom Rollback Statuses

Set `commit_mode` (`"autocommit"`, `"autocommit_include_redirect"`, or `"manual"`) in `extension_config["sanic"]`:

| Commit Mode | Commits On | Rolls Back On | Best For |
| --- | --- | --- | --- |
| `"autocommit"` (default) | Clean HTTP `200`–`299` responses | HTTP `>= 300` responses (`3xx`, `4xx`, `5xx`), any raised exception (including mapped `2xx` responses), or cancellation | JSON REST APIs where every `2xx` handler mutates state |
| `"autocommit_include_redirect"` | Clean HTTP `200`–`399` responses | HTTP `>= 400` responses, any raised exception (including mapped `2xx`/`3xx` responses), or cancellation | HTML, HTMX, and Inertia.js form handlers that commit and return `302` / `303` redirects |
| `"manual"` | Explicit `session.commit()` calls only | Uncommitted handler work on clean return, unhandled or mapped exceptions, and request cancellations | Explicit service-layer transaction boundaries |

Add `extra_commit_statuses` / `extra_rollback_statuses` sets in `extension_config["sanic"]` when `"autocommit"` or `"autocommit_include_redirect"` requires custom status overrides:

```{ .python title="docs/examples/frameworks/sanic/commit_modes.py" }
--8<-- "docs/examples/frameworks/sanic/commit_modes.py:example"
```

## Request Failure Handling

During request setup, `_on_request` acquires configured connections under `BaseException` protection, rolling back and releasing any already-acquired connections and observability contexts if multi-database setup fails or `asyncio.CancelledError` interrupts acquisition.

When a route handler or middleware raises an exception, Sanic dispatches `http.lifecycle.exception`, which records the exception on `request.ctx` before response middleware runs. During response teardown, the plugin checks this recorded exception before evaluating `commit_mode` status codes (rolling back even when an `@app.exception(...)` handler maps a failure to a `2xx` fallback response) and forwards `(exc_type, exc_val, exc_tb)` to the connection context exit (`__aexit__`). If `asyncio.CancelledError` or an abrupt client disconnect bypasses `http.lifecycle.exception` and response middleware, the `http.lifecycle.complete` signal listener rolls back and exits any residual connection contexts with a cancellation exception and restores observability contexts idempotently.

Both unhandled exceptions and exceptions mapped to HTTP responses by Sanic `@app.exception(...)` handlers roll back uncommitted work. By contrast, Starlette, FastAPI, and Flask treat exceptions already converted into responses by inner framework exception handlers as normal responses at the middleware boundary unless status-based rollback applies. In `"manual"` mode, uncommitted handler writes roll back before returning connections to the pool, and lazy-begin adapters (`adbc`, `arrow_odbc`, `asyncpg`, `cockroach_asyncpg`, `db2`, `duckdb`, `psqlpy`, `snowflake`, `sybase`, `teradata`) defer `BEGIN` until the first write statement across all three commit modes.

## Multi-Database Routing

Assign distinct `bind_key`, `session_key`, `connection_key`, and `pool_key` values when connecting a Sanic service to multiple databases:

```{ .python title="docs/examples/frameworks/sanic/multi_database.py" }
--8<-- "docs/examples/frameworks/sanic/multi_database.py:example"
```

## Pool-Only Mode and Observability Middleware

For high-throughput endpoints that only acquire connections on demand rather than opening a request-scoped session on every request, set `disable_di: True` and use `request.app.ctx.<pool_key>` directly:

=== "Pool-Only Mode"

    ```{ .python title="docs/examples/frameworks/sanic/disable_di.py" }
    --8<-- "docs/examples/frameworks/sanic/disable_di.py:example"
    ```

=== "Correlation & SQLCommenter Middleware"

    ```{ .python title="docs/examples/frameworks/sanic/observability.py" }
    --8<-- "docs/examples/frameworks/sanic/observability.py:example"
    ```

## Tenant-Aware Requests

With `driver_features={"tenant_strategy": "rls"}` on a supported adapter, Sanic defers checkout until middleware or a route verifies membership, calls `set_request_tenant(request, org_id)`, and awaits `db_plugin.get_session_async(request)`; a per-bind `tenant_extractor` can resolve the tenant from `request.ctx.user` instead. See [Sanic Multi-Tenancy](../learn/recipes/multi-tenancy-sanic.md) for complete examples, including streaming and cleanup behavior, and the [Tenant Request Contract](../learn/recipes/multi-tenancy-frameworks.md#one-request-contract) for the rules every framework shares.

## Configuration Options

Configure `extension_config["sanic"]` (`SanicConfig`) per database configuration:

<!-- GENERATED:START config-table:sqlspec.config.SanicConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[[Any], TenantExtractorValue or Awaitable[TenantExtractorValue]]` | - | Return the authorized tenant payload, string, UUID, or None (no tenant from this source), optionally as an awaitable. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start. |
| `connection_key` | `str` | `'db_connection'` | Key for storing the connection in request.ctx once it is checked out on first access. Default: 'db_connection' |
| `pool_key` | `str` | `'db_pool'` | Key for storing connection pool in app.ctx. Default: 'db_pool' |
| `session_key` | `str` | `'db_session'` | Key for storing session in request.ctx. Default: 'db_session' |
| `commit_mode` | `Literal['manual', 'autocommit', 'autocommit_include_redirect']` | `'autocommit'` | Transaction commit mode. Default: 'autocommit'. manual: No automatic commit; uncommitted work rolls back on release; autocommit: Commit on 2xx, rollback otherwise; autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise |
| `extra_commit_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger commit. Default: set() |
| `extra_rollback_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger rollback. Default: set() |
| `disable_di` | `bool` | `False` | Disable built-in dependency injection. Default: False. When True, the Sanic extension will not register request middleware for managing database connections and sessions. Users are responsible for managing the database lifecycle manually via their own DI solution. |
| `enable_correlation_middleware` | `bool` | `True` | Enable request correlation ID middleware. Default: True. |
| `correlation_header` | `str` | `X-Request-ID` | HTTP header to read the request correlation ID from when middleware is enabled. Default: `X-Request-ID`. |
| `correlation_headers` | `tuple[str, ...] or list[str]` | - | Additional HTTP headers to read as correlation ID fallbacks. |
| `auto_trace_headers` | `bool` | `True` | Read standard trace context headers as correlation ID fallbacks. Default: True. |
| `enable_sqlcommenter_middleware` | `bool` | `True` | Control automatic SQLCommenter middleware registration. Default: True. When the driver's :class:`~sqlspec.core.statement.StatementConfig` has `enable_sqlcommenter=True`, the middleware is registered automatically. Set to `False` to explicitly disable middleware registration. |
| `sqlcommenter_framework` | `str` | `'sanic'` | Framework name for SQLCommenter attributes. Default: 'sanic'. |
| `replica_session_key` | `str` | - | Request key for a read session. |
| `replica_connection_key` | `str` | - | Request key for a read connection. |
| `routing_session_key` | `str` | - | Request key for a routing session. |
| `default_routing_mode` | `Literal['primary', 'auto']` | `'auto'` | Default request routing mode. Default: `'auto'`. |
| `enable_routing_middleware` | `bool` | `True` | Install request-scoped routing middleware. Default: True. |
| `routing_header` | `str or None` | `None` | Trusted application routing header name when explicitly enabled behind a gateway. Default: None. |
| `primary_pin_cookie` | `str or None` | `None` | Optional application primary pin cookie name. Default: None. |
| `primary_pin_secret` | `str or bytes or None` | `None` | Optional HMAC secret verifying signed primary pin cookies. Default: None. |
| `read_pool_key` | `str` | - | Request key for the selected read pool. |
<!-- GENERATED:END config-table:sqlspec.config.SanicConfig -->

## Read Replica and Routing Sessions

Use `get_replica_session(request, key=...)` for a lazy reader, or await `get_replica_session_async(request, key=...)` to acquire it now. Raw connections use `get_replica_connection` and `get_replica_connection_async`. Opt-in routing facades use `get_db_routing_session` and `get_db_routing_session_async`. The first awaited call on a deferred async reader performs checkout.

Startup and shutdown listeners manage the primary and its reader pools together. Request cleanup closes every acquired session on success, errors, and cancellation. Tenant receipt checks apply to readers too. Use `SQLSpecRoutingMiddleware` for a request-local routing scope and stateless hints; see [Read Replicas](../learn/recipes/read-replicas.md).

## Connection Lifetime

- **Normal responses**: The request connection is committed or rolled back and released when the handler returns and the response completes.
- **Streaming responses**: The connection stays held while a `ResponseStream` body or a handler that called `request.respond()` runs, and is released when the handler returns.
- **Background work**: Tasks that outlive the handler run after the connection is released. Open a new session with `config.provide_session()` inside the task.

A session or stream cannot be used after its connection is released; trying to raises `ImproperConfigurationError`.

## Recap

- Configure `session_key`, `commit_mode`, `disable_di`, and observability flags in `extension_config["sanic"]`.
- Retrieve sessions via `db_plugin.get_session(request, key)` or `await db_plugin.get_session_async(request, key)`; connections are checked out on first access. Set `disable_di` to acquire connections from `request.app.ctx.<pool_key>` yourself.
- See the [Sanic Extension Reference](/reference/extensions/sanic/) for `SQLSpecPlugin` and `SanicConfig` signatures.
