# Flask Integration

`sqlspec.extensions.flask` provides `SQLSpecPlugin` for Flask applications, managing synchronous (and portal-bridged asynchronous) connection pools across Flask's application factory lifecycle (`init_app`), caching request-scoped sessions on `flask.g`, and finalizing transactions in `teardown_appcontext`.

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

Create a `SQLSpecPlugin(spec)` instance, initialize it inside your `create_app` factory with `plugin.init_app(app)`, and retrieve the active request session via `plugin.get_session()`:

```{ .python title="docs/examples/frameworks/flask/basic_setup.py" }
--8<-- "docs/examples/frameworks/flask/basic_setup.py:example"
```

Calling `plugin.get_session()` multiple times within the same HTTP request returns the same `flask.g`-cached driver session.

## Multi-Database Binds

Configure distinct `session_key`, `connection_key`, and `pool_key` values under `extension_config["flask"]` (`FlaskConfig`) and pass the key to `plugin.get_session("<key>")`:

```{ .python title="docs/examples/frameworks/flask/multi_database.py" }
--8<-- "docs/examples/frameworks/flask/multi_database.py:example"
```

## `FlaskConfig` Options

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `session_key` | `str` | `"db_session"` | Key passed to `plugin.get_session(key)` and cached on `flask.g`. |
| `connection_key` | `str` | `"db_connection"` | Key passed to `plugin.get_connection(key)` on `flask.g`. |
| `pool_key` | `str` | `"db_pool"` | Key stored in `app.extensions["sqlspec"]` for the connection pool. |
| `commit_mode` | `"manual" | "autocommit" | "autocommit_include_redirect"` | `"manual"` | Commits on `2xx` (`autocommit`) or `2xx`/`3xx` (`autocommit_include_redirect`) in `after_request` and rolls back on errors. |
| `extra_commit_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger an automatic commit. |
| `extra_rollback_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger an automatic rollback. |
| `enable_correlation_middleware` | `bool` | `True` | Binds request correlation headers into `CorrelationContext`. |
| `enable_sqlcommenter_middleware` | `bool` | `False` | Populates `SQLCommenterContext` (`framework="flask"`, route endpoint, method) for wire-level SQL comments. |

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Recap

- Initialize `SQLSpecPlugin` with `plugin.init_app(app)` and access request-scoped sessions via `plugin.get_session()`.
- Configure `commit_mode` and multi-database `session_key` names in `extension_config["flask"]`.
- See the [Flask Extension Reference](/reference/extensions/flask/) for `SQLSpecPlugin` and `FlaskConfig` signatures.
