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

Configure distinct `session_key` and optional `connection_key` values under `extension_config["flask"]` (`FlaskConfig`; when `connection_key` is omitted, SQLSpec derives an internal `flask.g` key automatically from `session_key`) and pass the key to `plugin.get_session("<key>")`:

```{ .python title="docs/examples/frameworks/flask/multi_database.py" }
--8<-- "docs/examples/frameworks/flask/multi_database.py:example"
```

## Tenant-Aware Requests

Use `tenant_extractor` when a Flask config enables `tenant_strategy`. Install the matching driver, for example `uv add "sqlspec[flask,psycopg]"`. Start with the role and policy in [Multi-Tenancy](../learn/recipes/multi-tenancy.md).

Your login hook must set the trusted `g.user` before SQLSpec's request hook runs. Register the login hook before `plugin.init_app(app)`. The extractor checks that the chosen tenant belongs to that user:

```python
from flask import Request, g

from sqlspec.adapters.psycopg import PsycopgSyncConfig


def verified_tenant(request: Request) -> str | None:
    user = getattr(g, "user", None)
    if user is None:
        return None
    tenant = getattr(user, "tenant_id", None)
    return tenant if tenant in getattr(user, "tenant_ids", ()) else None


def tenant_config(dsn: str) -> PsycopgSyncConfig:
    return PsycopgSyncConfig(
        connection_config={"conninfo": dsn},
        driver_features={"tenant_strategy": "rls"},
        extension_config={"flask": {
            "tenant_extractor": verified_tenant,
            "commit_mode": "autocommit",
        }},
    )
```

Add the returned config to your `SQLSpec` instance, then use the factory and `plugin.get_session()` shown above. The extractor can take the Flask request or take no arguments and read `g`. It must be a sync callable; Flask rejects async extractors. The result can be a `TenantContextPayload`, `str`, `UUID`, or `None`.

SQLSpec sets the tenant before it opens a connection. Teardown exits the lease, then restores the outer tenant scope, even when setup or the handler fails. `None` clears the current tenant; a tenant-enabled config then fails before taking a pool slot. A config with tenant mode and no extractor fails at `init_app`. All managed configs in one plugin must share the same extractor object.

For async drivers, Flask passes the captured payload into its portal's async acquisition call. The lease keeps that identity until cleanup. Request code must use the request session rather than share it with worker threads. Workers open their own leases in explicit tenant scopes.

A raw header never proves tenant access. To switch tenants, check the requested value against the user's trusted memberships. An explicit bypass payload skips tenant setup but keeps database policies in force. Use a separate config for privileged global work. See [Tenant Framework Integration](../learn/recipes/multi-tenancy-frameworks.md) and [Tenant Operations](../learn/recipes/multi-tenancy-operations.md).

## Configuration Options

<!-- GENERATED:START config-table:sqlspec.config.FlaskConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[..., TenantExtractorValue]` | - | Return the authorized tenant payload, string, UUID, or None for the request. Required with a tenant strategy. Accepts zero arguments or the Flask request. |
| `connection_key` | `str` | - | Key for storing connection in Flask g object. Default: auto-generated from session_key. |
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
<!-- GENERATED:END config-table:sqlspec.config.FlaskConfig -->

Set `enable_correlation_middleware=False` to stop this plugin from reading request IDs or adding them to response headers.

## Recap

- Initialize `SQLSpecPlugin` with `plugin.init_app(app)` and access request-scoped sessions via `plugin.get_session()`.
- Configure `commit_mode` and multi-database `session_key` names in `extension_config["flask"]`.
- See the [Flask Extension Reference](/reference/extensions/flask/) for `SQLSpecPlugin` and `FlaskConfig` signatures.
