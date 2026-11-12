# Flask Multi-Tenancy

Resolve the request tenant in a Flask view or `before_request` hook, then fetch a
tenant-bound session with `plugin.get_session()`. Read the
[Tenant Request Contract](multi-tenancy-frameworks.md#one-request-contract) for
the precedence and failure rules shared by every framework, and create the
account tables from
[Account Tables and User Lookup SQL](multi-tenancy-frameworks.md#account-tables-and-user-lookup-sql).

## How Flask Resolves the Tenant

When a configuration sets `driver_features={"tenant_strategy": "rls"}`, Flask
defers checking out the tenant connection until the view first calls
`plugin.get_session()` or `plugin.get_connection()`. Before that, the view or an
`@app.before_request` hook verifies membership and calls
`set_request_tenant(org_id)`. Flask's version takes no request argument because
it stores the receipt on `flask.g`; pass `key=` with a session, connection, or
bind key when several configurations are registered. SQLSpec binds the verified
tenant before opening the connection and exits the lease during
`teardown_appcontext` before restoring any outer scope.

## Single-Database Example

Verify membership with a lookup session opened by
`config.provide_session(bypass_tenant=True)`, call `set_request_tenant(...)`,
and then fetch the tenant session:

```python
from typing import Any

from flask import Flask, abort, g, jsonify

from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.extensions.flask import SQLSpecPlugin, set_request_tenant

config = PsycopgSyncConfig(
    connection_config={"conninfo": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={"flask": {"commit_mode": "autocommit"}},
)
plugin = SQLSpecPlugin(config)


def create_app() -> Flask:
    app = Flask(__name__)
    plugin.init_app(app)

    @app.get("/orgs/<org_id>/projects")
    def list_projects(org_id: str) -> Any:
        user = getattr(g, "user", None)
        if user is None:
            abort(401)
        with config.provide_session(bypass_tenant=True) as lookup:
            allowed = lookup.select_one_or_none(
                "SELECT 1 FROM public.user_org_memberships "
                "WHERE user_id = :uid AND org_id = :org",
                uid=user.id,
                org=org_id,
            )
        if allowed is None:
            abort(403)
        set_request_tenant(org_id)
        session = plugin.get_session()
        return jsonify(session.select("SELECT id, name FROM public.projects"))

    return app
```

`g.user` holds the user your authentication hook verified.

## Upfront Extractor

When an authentication hook registered before `plugin.init_app(app)` already
sets `g.user`, set a synchronous `tenant_extractor` in
`extension_config["flask"]` so views can call `plugin.get_session()` without
`set_request_tenant(...)`. Flask calls the extractor with the Flask request, or
with no argument when it takes none, and rejects async extractors:

```python
from typing import Any

from flask import g

from sqlspec.adapters.psycopg import PsycopgSyncConfig


def verified_tenant() -> str | None:
    user: Any = g.get("user")
    if user is None:
        return None
    selected = getattr(user, "default_org", None)
    return selected if selected in getattr(user, "org_ids", ()) else None


config = PsycopgSyncConfig(
    connection_config={"conninfo": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "flask": {
            "tenant_extractor": verified_tenant,
            "commit_mode": "autocommit",
        }
    },
)
```

Multiple configurations on one plugin can each set their own extractor or use
`set_request_tenant(..., key=...)` independently. See
[Upfront Tenant Extractors](multi-tenancy-frameworks.md#upfront-tenant-extractors)
for the rules every extractor follows.

## Public and Bypass Routes

Views that never call `plugin.get_session()`, such as `/login` or `/health`,
skip tenant checkout entirely. When a public view needs the database before a
tenant exists, mark it with `tenant_bypass` from `sqlspec.extensions.flask`;
its session skips SQLSpec's tenant session setup while database RLS policies
stay active. Inside `create_app`:

```python
from sqlspec.extensions.flask import tenant_bypass


@app.get("/health")
@tenant_bypass
def health() -> Any:
    return {"ok": plugin.get_session().select_value("SELECT 1")}
```

## Streaming and Background Tasks

Wrap streaming generators in `stream_with_context`. SQLSpec rebinds the request
tenant when Flask pushes the context for the generator, so sessions opened
inside it with `config.provide_session()` use that tenant. Request sessions
belong to the handling request; background threads open their own leases with
`config.provide_session(tenant=tenant_id)` using the verified tenant
identifier. See [Tenant Operations](multi-tenancy-operations.md).

## Sync and Async Accessors

`plugin.get_session()` and `plugin.get_connection()` serve sync adapters
directly and async adapters through SQLSpec's portal, acquiring deferred tenant
leases lazily in both cases. SQLSpec forwards the captured tenant payload into
the portal for async drivers. Tenant extractors must be synchronous in either
case.
