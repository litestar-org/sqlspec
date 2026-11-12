# Sanic Multi-Tenancy

Resolve the request tenant in Sanic request middleware or inside the route, then
fetch a tenant-bound session with `await db_plugin.get_session_async(request)`.
Read the [Tenant Request Contract](multi-tenancy-frameworks.md#one-request-contract)
for the precedence and failure rules shared by every framework, and create the
account tables from
[Account Tables and User Lookup SQL](multi-tenancy-frameworks.md#account-tables-and-user-lookup-sql).

## How Sanic Resolves the Tenant

When a configuration sets `driver_features={"tenant_strategy": "rls"}`, Sanic
defers checking out the tenant connection until the route asks for a session.
Authentication middleware or the route verifies membership and calls
`set_request_tenant(request, org_id)`; pass `key=` with a session, connection,
or bind key when several configurations are registered. Routes that never ask
for a session never check out a tenant connection.

## Single-Database Example

Verify membership with a lookup session opened by
`config.provide_session(bypass_tenant=True)`, call `set_request_tenant(...)`,
and then fetch the tenant session:

```python
from typing import Any

from sanic import Sanic, json
from sanic.exceptions import Forbidden, Unauthorized

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.sanic import SQLSpecPlugin, set_request_tenant

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={"sanic": {"commit_mode": "autocommit"}},
)
db_plugin = SQLSpecPlugin(config)


def register_routes(app: Sanic[Any, Any]) -> None:
    db_plugin.init_app(app)

    @app.get("/orgs/<org_id:str>/projects")
    async def list_projects(request: Any, org_id: str) -> Any:
        user = getattr(request.ctx, "user", None)
        if user is None:
            raise Unauthorized("Authentication required")
        async with config.provide_session(bypass_tenant=True) as lookup:
            allowed = await lookup.select_one_or_none(
                "SELECT 1 FROM public.user_org_memberships "
                "WHERE user_id = :uid AND org_id = :org",
                uid=user.id,
                org=org_id,
            )
        if allowed is None:
            raise Forbidden("Forbidden organization")
        set_request_tenant(request, org_id)
        session = await db_plugin.get_session_async(request)
        return json(await session.select("SELECT id, name FROM public.projects"))
```

`request.ctx.user` holds the user your authentication middleware verified.

## Upfront Extractor

When authentication middleware registered before the plugin populates
`request.ctx.user`, set `tenant_extractor` in `extension_config["sanic"]` so
routes can await `db_plugin.get_session_async(request)` without calling
`set_request_tenant(...)`. Sanic passes the Sanic request; the extractor may be
sync or async:

```python
from typing import Any

from sqlspec.adapters.asyncpg import AsyncpgConfig


def verified_tenant(request: Any) -> str | None:
    user = getattr(request.ctx, "user", None)
    if user is None:
        return None
    selected = getattr(user, "default_org", None)
    return selected if selected in getattr(user, "org_ids", ()) else None


config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "sanic": {
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

Login and health routes that never fetch a session skip tenant checkout
entirely. When a public route needs the database before a tenant exists, mark
it with `tenant_bypass` from `sqlspec.extensions.sanic`; its session skips
SQLSpec's tenant session setup while database RLS policies stay active. Inside
`register_routes`:

```python
from sqlspec.extensions.sanic import tenant_bypass


@app.get("/health")
@tenant_bypass
async def health(request: Any) -> Any:
    session = await db_plugin.get_session_async(request)
    return json({"ok": await session.select_value("SELECT 1")})
```

## Streaming and Background Tasks

Response cleanup and the `http.lifecycle.complete` signal listener exit every
open tenant lease and restore outer scopes on normal responses, mapped
exceptions, and cancellations. At cleanup, most tenant-enabled connection
leases roll back open transactions (including in `"manual"` mode), while
psycopg and CockroachDB psycopg leases commit a clean exit, so commit
explicitly or set `commit_mode="autocommit"` to control persistence.

For a streaming handler, SQLSpec releases the request connection at the
`request.respond()` call. The request tenant stays in effect until the handler
returns, so open a new session with `config.provide_session()` after
`respond()` instead of reusing the request session. This example uses the
extractor configuration from [Upfront Extractor](#upfront-extractor):

```python
@app.get("/projects/export")
async def export_projects(request: Any) -> None:
    response = await request.respond(content_type="text/csv")
    async with config.provide_session() as session:
        rows = await session.select("SELECT id, name FROM public.projects")
    for row in rows:
        await response.send(f"{row['id']},{row['name']}\n")
    await response.eof()
```

For work that outlives the handler, pass the verified tenant identifier to the
task and open `config.provide_session(tenant=tenant_id)` there; see
[Tenant Operations](multi-tenancy-operations.md).

## Sync and Async Accessors

Async configurations, including every async tenant configuration, use
`await db_plugin.get_session_async(request)` or
`await db_plugin.get_connection_async(request)`; both acquire deferred tenant
leases on demand. Sync adapters can call `db_plugin.get_session(request)`. The
synchronous accessors raise `ImproperConfigurationError` for async tenant
configurations, which are checked out on first use.
