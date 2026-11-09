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

Configure `extension_config["sanic"]` (`SanicConfig`), attach the plugin via `db_plugin.init_app(app)`, and retrieve the session with `db_plugin.get_session(request, "db")`:

```{ .python title="docs/examples/frameworks/sanic/basic_setup.py" }
--8<-- "docs/examples/frameworks/sanic/basic_setup.py:example"
```

## Commit Modes and Custom Rollback Statuses

Set `commit_mode` (`"manual"`, `"autocommit"`, or `"autocommit_include_redirect"`) and optional `extra_commit_statuses` / `extra_rollback_statuses` sets in `extension_config["sanic"]`:

```{ .python title="docs/examples/frameworks/sanic/commit_modes.py" }
--8<-- "docs/examples/frameworks/sanic/commit_modes.py:example"
```

## Request Failure Handling

During request setup, `_on_request` acquires configured connections under `BaseException` protection, rolling back and releasing any already-acquired connections and observability contexts if multi-database setup fails or `asyncio.CancelledError` interrupts acquisition.

When a route handler or middleware raises an exception, Sanic dispatches `http.lifecycle.exception`, which records the exception on `request.ctx` before response middleware runs. During response teardown, the plugin checks this recorded exception before evaluating `commit_mode` status codes (rolling back even when an `@app.exception(...)` handler maps a failure to a `2xx` fallback response) and forwards `(exc_type, exc_val, exc_tb)` to the connection context exit (`__aexit__`). If `asyncio.CancelledError` or an abrupt client disconnect bypasses `http.lifecycle.exception` and response middleware, the `http.lifecycle.complete` signal listener rolls back and exits any residual connection contexts with a cancellation exception and restores observability contexts idempotently.

Both unhandled exceptions and exceptions mapped to HTTP responses by Sanic `@app.exception(...)` handlers roll back uncommitted work. By contrast, Starlette, FastAPI, and Flask treat exceptions already converted into responses by inner framework exception handlers as normal responses at the middleware boundary unless status-based rollback applies.

When a route handler in `"manual"` mode returns cleanly without calling `commit()` or `rollback()`, the outcome for open uncommitted work depends on the adapter:

| Clean return with uncommitted work (`manual` mode) | Adapters |
| --- | --- |
| Commits open transaction on context exit | `sqlite`, `aiosqlite`, `duckdb`, `psycopg`, `cockroach_psycopg`, `spanner` |
| Rolls back uncommitted transaction on pool release or close | `pymysql`, `aiomysql`, `asyncmy`, `mysqlconnector`, `oracledb`, `mssql_python`, `adbc` |
| Autocommits single statements; rolls back open `BEGIN` blocks on pool release | `asyncpg`, `cockroach_asyncpg` |
| Autocommits single statements by default (explicit `BEGIN` or `autocommit=False` requires explicit commit or `commit_mode`) | `psqlpy`, `db2`, `arrow_odbc`, `bigquery` |
| Retains open transaction on thread-local connection until explicit `commit()` / `rollback()` (discards connection on exception) | `pymssql`, `sybase`, `sqlanywhere` |

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

## Configuration Options

Configure `extension_config["sanic"]` (`SanicConfig`) per database configuration:

<!-- GENERATED:START config-table:sqlspec.config.SanicConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `connection_key` | `str` | `'db_connection'` | Key for storing connection in request.ctx. Default: 'db_connection' |
| `pool_key` | `str` | `'db_pool'` | Key for storing connection pool in app.ctx. Default: 'db_pool' |
| `session_key` | `str` | `'db_session'` | Key for storing session in request.ctx. Default: 'db_session' |
| `commit_mode` | `Literal['manual', 'autocommit', 'autocommit_include_redirect']` | `'manual'` | Transaction commit mode. Default: 'manual'. manual: No automatic commit/rollback; autocommit: Commit on 2xx, rollback otherwise; autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise |
| `extra_commit_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger commit. Default: set() |
| `extra_rollback_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger rollback. Default: set() |
| `disable_di` | `bool` | `False` | Disable built-in dependency injection. Default: False. When True, the Sanic extension will not register request middleware for managing database connections and sessions. Users are responsible for managing the database lifecycle manually via their own DI solution. |
| `enable_correlation_middleware` | `bool` | `True` | Enable request correlation ID middleware. Default: True. |
| `correlation_header` | `str` | `X-Request-ID` | HTTP header to read the request correlation ID from when middleware is enabled. Default: `X-Request-ID`. |
| `correlation_headers` | `tuple[str, ...] or list[str]` | - | Additional HTTP headers to read as correlation ID fallbacks. |
| `auto_trace_headers` | `bool` | `True` | Read standard trace context headers as correlation ID fallbacks. Default: True. |
| `enable_sqlcommenter_middleware` | `bool` | `True` | Control automatic SQLCommenter middleware registration. Default: True. When the driver's :class:`~sqlspec.core.statement.StatementConfig` has `enable_sqlcommenter=True`, the middleware is registered automatically. Set to `False` to explicitly disable middleware registration. |
| `sqlcommenter_framework` | `str` | `'sanic'` | Framework name for SQLCommenter attributes. Default: 'sanic'. |
<!-- GENERATED:END config-table:sqlspec.config.SanicConfig -->

## Recap

- Configure `session_key`, `commit_mode`, `disable_di`, and observability flags in `extension_config["sanic"]`.
- Retrieve sessions via `db_plugin.get_session(request, key)`, or set `disable_di` and acquire connections from `request.app.ctx.<pool_key>`.
- See the [Sanic Extension Reference](/reference/extensions/sanic/) for `SQLSpecPlugin` and `SanicConfig` signatures.
