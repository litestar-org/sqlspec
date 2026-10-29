# Sanic Integration

`sqlspec.extensions.sanic` provides `SQLSpecPlugin` for Sanic applications, registering `before_server_start` and `after_server_stop` pool listeners, managing per-request sessions on `request.ctx`, integrating with `sanic-ext` constructor injection, and supporting status-aware transaction commit modes and observability middleware.

## Installation

```bash
uv add "sqlspec[sanic]"
```

## Basic Setup

Configure `extension_config["sanic"]` (`SanicConfig`), attach the plugin via `db_plugin.init_app(app)`, and retrieve the session with `db_plugin.get_session(request, "db")`:

```{ .python title="docs/examples/frameworks/sanic/basic_setup.py" hl_lines="15-32" }
--8<-- "docs/examples/frameworks/sanic/basic_setup.py"
```

## Commit Modes and Custom Rollback Statuses

Set `commit_mode` (`"manual"`, `"autocommit"`, or `"autocommit_include_redirect"`) and optional `extra_commit_statuses` / `extra_rollback_statuses` sets in `extension_config["sanic"]`:

```{ .python title="docs/examples/frameworks/sanic/commit_modes.py" hl_lines="15-35" }
--8<-- "docs/examples/frameworks/sanic/commit_modes.py"
```

## Multi-Database Routing

Assign distinct `bind_key`, `session_key`, `connection_key`, and `pool_key` values when connecting a Sanic service to multiple databases:

```{ .python title="docs/examples/frameworks/sanic/multi_database.py" hl_lines="16-56" }
--8<-- "docs/examples/frameworks/sanic/multi_database.py"
```

## Pool-Only Mode (`disable_di=True`) and Observability Middleware

For high-throughput endpoints that only acquire connections on demand rather than opening a request-scoped session on every request, set `disable_di: True` and use `request.app.ctx.<pool_key>` directly:

=== "Pool-Only Mode (`disable_di`)"

    ```{ .python title="docs/examples/frameworks/sanic/disable_di.py" hl_lines="15-32" }
    --8<-- "docs/examples/frameworks/sanic/disable_di.py"
    ```

=== "Correlation & SQLCommenter Middleware"

    ```{ .python title="docs/examples/frameworks/sanic/observability.py" hl_lines="15-38" }
    --8<-- "docs/examples/frameworks/sanic/observability.py"
    ```

## Recap

- Configure `session_key`, `commit_mode`, `disable_di`, and observability flags in `extension_config["sanic"]`.
- Retrieve sessions via `db_plugin.get_session(request, key)` or inject drivers directly when `sanic-ext` is installed.
- See the [Sanic Extension Reference](/reference/extensions/sanic/) for `SQLSpecPlugin` and `SanicConfig` signatures.
