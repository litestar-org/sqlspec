# Flask Integration

`sqlspec.extensions.flask` provides `SQLSpecPlugin` for Flask applications, managing synchronous (and portal-bridged asynchronous) connection pools across Flask's application factory lifecycle (`init_app`), caching request-scoped sessions on `flask.g`, and finalizing transactions in `teardown_appcontext`. A request connection is checked out the first time `plugin.get_session()` or `plugin.get_connection()` asks for it, so views that never touch the database open none.

## Installation

=== "uv"

    ```bash
    uv add "sqlspec[flask]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[flask]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[flask]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[flask]"
    ```

## Application Factory Setup

Pass a single database configuration, a sequence of configurations, or a `SQLSpec` registry to `SQLSpecPlugin(...)`, initialize it inside your `create_app` factory with `plugin.init_app(app)` (or `SQLSpecPlugin(config, app)`), and retrieve the active request session via `plugin.get_session()`:

```{ .python title="docs/examples/frameworks/flask/basic_setup.py" }
--8<-- "docs/examples/frameworks/flask/basic_setup.py:example"
```

Calling `plugin.get_session()` multiple times within the same HTTP request returns the same `flask.g`-cached driver session. With an async configuration, `plugin.get_session()` returns a blocking session whose calls (`session.select(...)`, `for row in session.select_stream(...)`, `with session.transaction():`) run on the plugin's portal event loop, so views use it without `await`.

## Multi-Database Binds

Configure distinct `session_key` and optional `connection_key` values under `extension_config["flask"]` (`FlaskConfig`; when `connection_key` is omitted, SQLSpec derives an internal `flask.g` key automatically from `session_key`) and pass the key to `plugin.get_session("<key>")`:

```{ .python title="docs/examples/frameworks/flask/multi_database.py" }
--8<-- "docs/examples/frameworks/flask/multi_database.py:example"
```

## Tenant-Aware Requests

With `driver_features={"tenant_strategy": "rls"}` on a supported adapter, Flask defers checkout until a view or `before_request` hook verifies membership, calls `set_request_tenant(org_id)`, and calls `plugin.get_session()`; a synchronous per-bind `tenant_extractor` can resolve the tenant from `g.user` instead. See [Flask Multi-Tenancy](../learn/recipes/multi-tenancy-flask.md) for complete examples and the [Tenant Request Contract](../learn/recipes/multi-tenancy-frameworks.md#one-request-contract) for the rules every framework shares.

## Configuration Options

<!-- GENERATED:START config-table:sqlspec.config.FlaskConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[..., TenantExtractorValue]` | - | Return the authorized tenant payload, string, UUID, or None (no tenant from this source) for the request. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start. Accepts zero arguments or the Flask request. |
| `connection_key` | `str` | - | Key for storing the connection in Flask g once it is checked out on first access. Default: auto-generated from session_key. |
| `session_key` | `str` | `'db_session'` | Key for accessing session via plugin.get_session(). Default: 'db_session'. |
| `commit_mode` | `Literal['manual', 'autocommit', 'autocommit_include_redirect']` | `'manual'` | Transaction commit mode. Default: 'manual'. manual: No automatic commits, user handles explicitly; autocommit: Commits on 2xx status, rollback otherwise; autocommit_include_redirect: Commits on 2xx-3xx status, rollback otherwise |
| `extra_commit_statuses` | `set[int]` | `None` | Additional HTTP status codes that trigger commit. Default: None. |
| `extra_rollback_statuses` | `set[int]` | `None` | Additional HTTP status codes that trigger rollback. Default: None. |
| `disable_di` | `bool` | `False` | Disable built-in dependency injection. Default: False. When True, the Flask extension will not register request hooks for managing database connections and sessions. Users are responsible for managing the database lifecycle manually via their own DI solution. |
| `enable_sqlcommenter_middleware` | `bool` | `True` | Control automatic SQLCommenter context population. Default: True. When the driver's :class:`~sqlspec.core.statement.StatementConfig` has `enable_sqlcommenter=True`, request attributes are populated automatically. Set to `False` to explicitly disable this behavior. |
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
<!-- GENERATED:END config-table:sqlspec.config.FlaskConfig -->

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Read Replica Sessions and Portal Context

`db_ext.get_read_session(key=...)` uses the selected reader on first database use. `get_read_connection(key=...)` exposes its physical connection after checkout. `get_routing_session(key=...)` supplies the opt-in routing facade. A bind key or config instance selects the database.

For an async adapter, Flask runs these calls through `PortalProvider`. Tenant and routing payloads copy into that work. Writes merge their primary pin back to the request; a portal timeout pins conservatively before it raises if completion is uncertain. Each request still owns cleanup. Use explicit worker scopes outside HTTP requests. See [Replica Operations](../learn/recipes/read-replicas-operations.md) for lag and failure behavior.

## Recap

- Initialize `SQLSpecPlugin` with `plugin.init_app(app)` and access request-scoped sessions via `plugin.get_session()`.
- Configure `commit_mode` and multi-database `session_key` names in `extension_config["flask"]`.
- See the [Flask Extension Reference](/reference/extensions/flask/) for `SQLSpecPlugin` and `FlaskConfig` signatures.
