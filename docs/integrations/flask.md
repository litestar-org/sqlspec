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

Pass a single database configuration, a sequence of configurations, or a `SQLSpec` registry to `SQLSpecPlugin(...)`, initialize it inside your `create_app` factory with `plugin.init_app(app)` (or `SQLSpecPlugin(config, app)`), and retrieve the active request session via `plugin.get_session()`:

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

SQLSpec resolves the request tenant lazily on first use with one precedence order in every framework, returns 401 or 403 when no tenant resolves, and exports `tenant_bypass` for public routes; see [One Request Contract](../learn/recipes/multi-tenancy-frameworks.md#one-request-contract).

Enable `driver_features={"tenant_strategy": "rls"}` on supported database adapters (`psycopg`, `asyncpg`, `psqlpy`, `cockroach_psycopg`, `cockroach_asyncpg`, `oracledb`, `mssql_python`, `pymssql`, `db2`; start with the role and policy setup in [Multi-Tenancy](../learn/recipes/multi-tenancy.md)).

When no `tenant_extractor` is configured, Flask defers checking out the tenant connection until your view or `@app.before_request` hook verifies membership, calls `set_request_tenant(org_id)` (accepting `key=...` for multi-database binds), and calls `plugin.get_session()`. Unauthenticated endpoints such as `/login` or `/health` skip tenant checkout completely and can query user memberships through a dedicated auth bind or `config.provide_session(bypass_tenant=True)`:

```python
from typing import Any
from flask import Flask, abort, g, jsonify
from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.extensions.flask import SQLSpecPlugin, set_request_tenant

plugin = SQLSpecPlugin(
    PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://app_user@localhost/app"},
        driver_features={"tenant_strategy": "rls"},
        extension_config={"flask": {"commit_mode": "autocommit"}},
    )
)


def register_routes(app: Flask) -> None:
    plugin.init_app(app)

    @app.get("/orgs/<org_id>/projects")
    def list_projects(org_id: str) -> Any:
        user = getattr(g, "user", None)
        if user is None or org_id not in user.org_ids:
            abort(403)
        set_request_tenant(org_id)
        session = plugin.get_session()
        return jsonify(session.select("SELECT id, name FROM projects"))
```

When an upstream authentication hook registered before `plugin.init_app(app)` already sets `g.user`, you can instead configure a synchronous `tenant_extractor` per bind in `extension_config["flask"]` (taking either zero arguments or the Flask `Request` and returning a `TenantContextPayload`, `str`, `UUID`, or `None`; Flask rejects async extractors). Multiple database binds on one plugin can each configure their own extractor or use deferred `set_request_tenant(...)` independently.

SQLSpec binds the verified tenant before opening a connection, forwards that captured payload into its portal for async drivers, and exits the lease during `teardown_appcontext` before restoring any outer scope. Calling `plugin.get_session()` on a tenant-enabled bind without an active tenant raises `TenantContextError` before taking a pool slot. Request sessions belong to the handling request; background threads should open independent leases via `config.provide_session(tenant=...)`. See [Tenant Framework Integration](../learn/recipes/multi-tenancy-frameworks.md) and [Tenant Operations](../learn/recipes/multi-tenancy-operations.md).

## Configuration Options

<!-- GENERATED:START config-table:sqlspec.config.FlaskConfig -->
| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `tenant_extractor` | `Callable[..., TenantExtractorValue]` | - | Return the authorized tenant payload, string, UUID, or None (no tenant from this source) for the request. Requires tenant_strategy on the same configuration; called on first tenant use, never at request start. Accepts zero arguments or the Flask request. |
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
