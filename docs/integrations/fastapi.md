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

Attach `SQLSpecPlugin(spec, app)` to your `FastAPI` instance and inject the session into route handlers using `Depends(db_ext.provide_session())`:

```{ .python title="docs/examples/frameworks/fastapi/basic_setup.py" }
--8<-- "docs/examples/frameworks/fastapi/basic_setup.py:example"
```

`SQLSpecPlugin` automatically opens configured connection pools when FastAPI starts and closes them when the application shuts down.

## Multi-Database Configuration

Register multiple synchronous or asynchronous adapters on `SQLSpec` with distinct `session_key` names under `extension_config["fastapi"]`, then pass the key to `db_plugin.provide_session("<key>")`:

```{ .python title="docs/examples/frameworks/fastapi/multi_database.py" }
--8<-- "docs/examples/frameworks/fastapi/multi_database.py:example"
```

## Transaction Commit Modes and Options

Configure `extension_config["fastapi"]` (`FastAPIConfig`, which shares `StarletteConfig` options) per database bind:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `session_key` | `str` | `"db_session"` | Identifier passed to `provide_session(key)` and stored on `request.state`. |
| `connection_key` | `str` | `"db_connection"` | Identifier used for raw connection dependencies. |
| `pool_key` | `str` | `"db_pool"` | Attribute key on `app.state` storing the adapter connection pool. |
| `commit_mode` | `"manual" | "autocommit" | "autocommit_include_redirect"` | `"manual"` | Controls automatic transaction commit or rollback based on response status codes. |
| `extra_commit_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger an automatic commit. |
| `extra_rollback_statuses` | `set[int]` | `set()` | Additional HTTP status codes that trigger an automatic rollback. |
| `enable_correlation_middleware` | `bool` | `True` | Extracts or generates `X-Request-ID` correlation IDs into `CorrelationContext`. |
| `enable_sqlcommenter_middleware` | `bool` | `False` | Populates `SQLCommenterContext` with FastAPI route, method, and trace attributes. |

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Declarative Filter Dependencies

FastAPI shares the same `FilterConfig` builder as Litestar via `sqlspec.extensions.fastapi.providers.create_filter_dependencies`, generating typed `Depends` callables for `LimitOffsetFilter`, `CursorFilter` (returning `HTTP 422` on malformed cursor tokens), `OrderByFilter`, `SearchFilter`, and `BeforeAfterFilter`.

## Recap

- Instantiate `db_ext = SQLSpecPlugin(spec, app)` to bind pool lifecycles and request sessions to FastAPI.
- Use `Depends(db_ext.provide_session())` for single-database routes or `Depends(db_ext.provide_session("key"))` for multi-database setups.
- See the [FastAPI Extension Reference](/reference/extensions/fastapi/) for `SQLSpecPlugin` and provider signatures.
