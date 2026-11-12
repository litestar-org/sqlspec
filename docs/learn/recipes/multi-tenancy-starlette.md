# Starlette Multi-Tenancy

Resolve the request tenant in Starlette inside the route or in authentication
middleware, then fetch a tenant-bound session with
`await db_plugin.get_session_async(request)`. Read the
[Tenant Request Contract](multi-tenancy-frameworks.md#one-request-contract) for
the precedence and failure rules shared by every framework, and create the
account tables from
[Account Tables and User Lookup SQL](multi-tenancy-frameworks.md#account-tables-and-user-lookup-sql).

## How Starlette Resolves the Tenant

When a configuration sets `driver_features={"tenant_strategy": "rls"}`,
Starlette defers connection acquisition until the route asks for a session. The
route (or middleware that runs before it) verifies membership and calls
`set_request_tenant(request, org_id)`; pass `key=` with a session, connection,
or bind key when several configurations are registered. Routes that never ask
for a session never check out a tenant connection. Request transaction
finalization (`commit_mode`) runs under the captured tenant identity before the
connection lease resets.

## Single-Database Example

Verify membership with a lookup session opened by
`config.provide_session(bypass_tenant=True)`, call `set_request_tenant(...)`,
and then fetch the tenant session:

```python
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.starlette import SQLSpecPlugin, set_request_tenant

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={"starlette": {"commit_mode": "autocommit"}},
)


async def list_projects(request: Request) -> JSONResponse:
    user = request.scope.get("user")
    if user is None:
        return JSONResponse({"detail": "Authentication required"}, status_code=401)
    org_id = request.path_params["org_id"]
    async with config.provide_session(bypass_tenant=True) as lookup:
        allowed = await lookup.select_one_or_none(
            "SELECT 1 FROM public.user_org_memberships "
            "WHERE user_id = :uid AND org_id = :org",
            uid=user.id,
            org=org_id,
        )
    if allowed is None:
        return JSONResponse({"detail": "Forbidden organization"}, status_code=403)
    set_request_tenant(request, org_id)
    session = await db_plugin.get_session_async(request)
    rows = await session.select("SELECT id, name FROM public.projects")
    return JSONResponse(rows)


app = Starlette(routes=[Route("/orgs/{org_id}/projects", list_projects)])
db_plugin = SQLSpecPlugin(config, app)
```

`request.scope["user"]` holds the user your authentication middleware verified.

## Upfront Extractor

When ASGI authentication middleware populates `scope["user"]` before SQLSpec
runs, set `tenant_extractor` in `extension_config["starlette"]` so routes can
fetch sessions without calling `set_request_tenant(...)`. Starlette passes the
ASGI scope; the extractor may be sync or async:

```python
from typing import Any

from sqlspec.adapters.asyncpg import AsyncpgConfig


def verified_tenant(scope: dict[str, Any]) -> str | None:
    user = scope.get("user")
    if user is None or not getattr(user, "is_authenticated", False):
        return None
    selected = getattr(user, "default_org", None)
    return selected if selected in getattr(user, "org_ids", ()) else None


config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "starlette": {
            "tenant_extractor": verified_tenant,
            "commit_mode": "autocommit",
        }
    },
)
```

A `set_request_tenant(...)` receipt still takes precedence over the extractor.
See [Upfront Tenant Extractors](multi-tenancy-frameworks.md#upfront-tenant-extractors)
for the rules every extractor follows.

## Public and Bypass Routes

Login and health routes that never fetch a session skip tenant checkout
entirely. When a public route needs the database before a tenant exists, mark
it with `tenant_bypass` from `sqlspec.extensions.starlette`; its session skips
SQLSpec's tenant session setup while database RLS policies stay active:

```python
from sqlspec.extensions.starlette import tenant_bypass


@tenant_bypass
async def health(request: Request) -> JSONResponse:
    session = await db_plugin.get_session_async(request)
    return JSONResponse({"ok": await session.select_value("SELECT 1")})
```

## Streaming and Background Tasks

Starlette holds the request lease open through `StreamingResponse` bodies and
`BackgroundTask` completion, and the tenant resolved for the request stays in
effect for both. Sessions opened inside them with `config.provide_session()`
use that tenant. For work that outlives the request, pass the verified tenant
identifier to the worker and open `config.provide_session(tenant=tenant_id)`
there; see [Tenant Operations](multi-tenancy-operations.md).

## Sync and Async Accessors

Every request connection, tenant or not, is checked out on first access. With
an async configuration, `db_plugin.get_session(request)` returns a session that
opens the connection, under the request tenant, on its first awaited call, so
`await session.select(...)`, `async for row in session.select_stream(...)`, and
`async with session.transaction():` work unchanged. Reading `session.connection`
before that first call raises `ImproperConfigurationError`; call
`await db_plugin.get_session_async(request)` to open the connection
immediately. A raw connection has no deferred form, so use
`await db_plugin.get_connection_async(request)` for async configurations;
`db_plugin.get_connection(request)` returns it only once it is open.
