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

Register your database config on `SQLSpec`, create `db_plugin = SQLSpecPlugin(spec)`, call `db_plugin.init_app(app)`, and retrieve the session in route handlers via `db_plugin.get_session(request)`:

```{ .python title="docs/examples/frameworks/starlette/basic_setup.py" }
--8<-- "docs/examples/frameworks/starlette/basic_setup.py:example"
```

## Multi-Database Setup

Bind multiple async or sync configurations by specifying unique `session_key`, `connection_key`, and `pool_key` values inside `extension_config["starlette"]` (`StarletteConfig`):

```{ .python title="docs/examples/frameworks/starlette/multi_database.py" }
--8<-- "docs/examples/frameworks/starlette/multi_database.py:example"
```

## `StarletteConfig` Options

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `session_key` | `str` | `"db_session"` | Key on `request.state` used by `db_plugin.get_session(request, key)`. |
| `connection_key` | `str` | `"db_connection"` | Key on `request.state` used by `db_plugin.get_connection(request, key)`. |
| `pool_key` | `str` | `"db_pool"` | Key on `app.state` storing the active connection pool. |
| `commit_mode` | `"manual" | "autocommit" | "autocommit_include_redirect"` | `"manual"` | Controls automatic commit/rollback middleware behavior based on response status codes. |
| `extra_commit_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger an automatic commit. |
| `extra_rollback_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger an automatic rollback. |
| `enable_correlation_middleware` | `bool` | `False` | Propagates `X-Request-ID` / `X-Correlation-ID` headers into `CorrelationContext`. |
| `enable_sqlcommenter_middleware` | `bool` | `False` | Populates `SQLCommenterContext` (`framework="starlette"`, route path, method). |

## Recap

- Call `db_plugin.init_app(app)` on your `Starlette` instance and retrieve sessions with `db_plugin.get_session(request)`.
- Use `extension_config["starlette"]` to configure `commit_mode`, multi-database keys, and `sqlcommenter` middleware.
- See the [Starlette Extension Reference](/reference/extensions/starlette/) for `SQLSpecPlugin` and `StarletteConfig` details.
