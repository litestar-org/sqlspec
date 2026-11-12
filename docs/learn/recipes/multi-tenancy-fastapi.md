# FastAPI Multi-Tenancy

Resolve the request tenant in FastAPI with an authorization dependency, then
inject a tenant-bound session with `Depends(db_ext.provide_session())`. Read the
[Tenant Request Contract](multi-tenancy-frameworks.md#one-request-contract) for
the precedence and failure rules shared by every framework, and create the
account tables from
[Account Tables and User Lookup SQL](multi-tenancy-frameworks.md#account-tables-and-user-lookup-sql).

## How FastAPI Resolves the Tenant

FastAPI authentication usually runs inside route dependencies rather than outer
ASGI middleware. An authorization dependency verifies membership and records the
selected tenant with `set_request_tenant(request, org_id)`; pass `key=` with a
session, connection, or bind key when several configurations are registered.
SQLSpec checks out the tenant connection only when a route resolves
`Depends(db_ext.provide_session(...))`, after the authorization dependency has
run. Endpoints that never request a session, such as `/health`, `/login`, and
FastAPI's `/docs`, never check one out.

## Single-Database Example

Verify membership inside an async dependency with a lookup session opened by
`config.provide_session(bypass_tenant=True)`, call `set_request_tenant(...)`,
and inject the tenant session after it:

```python
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.fastapi import SQLSpecPlugin, set_request_tenant

app = FastAPI()
config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://tenant_app@localhost/app"},
    driver_features={"tenant_strategy": "rls"},
    extension_config={"fastapi": {"commit_mode": "autocommit"}},
)
db_ext = SQLSpecPlugin(config, app)


async def authorize_org(request: Request, org_id: str) -> str:
    user = request.scope.get("user")
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    async with config.provide_session(bypass_tenant=True) as lookup:
        allowed = await lookup.select_one_or_none(
            "SELECT 1 FROM public.user_org_memberships "
            "WHERE user_id = :uid AND org_id = :org",
            uid=user.id,
            org=org_id,
        )
    if allowed is None:
        raise HTTPException(status_code=403, detail="Forbidden organization")
    set_request_tenant(request, org_id)
    return org_id


@app.get("/orgs/{org_id}/projects")
async def list_projects(
    _org: Annotated[str, Depends(authorize_org)],
    db: Annotated[AsyncpgDriver, Depends(db_ext.provide_session())],
) -> list[dict[str, Any]]:
    return await db.select("SELECT id, name FROM public.projects")
```

`request.scope["user"]` holds the user your authentication middleware verified;
replace it with your own current-user dependency if authentication runs inside
FastAPI.

## Separate Auth and Tenant Databases

With a dedicated authentication database, register both configurations on one
plugin. The authorization dependency reads memberships from `auth_db`, which has
no tenant strategy, and scopes its receipt to the tenant configuration with
`key="tenant_db"`:

```{ .python title="docs/examples/frameworks/fastapi/tenant_setup.py" }
--8<-- "docs/examples/frameworks/fastapi/tenant_setup.py:example"
```

The runnable example uses in-memory SQLite, which has no tenant strategy, so it
shows only the authorization and `set_request_tenant` flow; tenant isolation
applies once the tenant bind uses a tenant-enabled adapter such as
`AsyncpgConfig(driver_features={"tenant_strategy": "rls"})`.

## Upfront Extractor

When ASGI middleware ahead of FastAPI already verifies the caller and stores the
user in `scope["user"]`, set `tenant_extractor` in `extension_config["fastapi"]`
so routes can inject sessions without an authorization dependency. FastAPI
passes the ASGI scope; the extractor may be sync or async:

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
        "fastapi": {
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

Routes that do not depend on `db_ext.provide_session()` never check out a tenant
connection. When a public route needs the database before a tenant exists, mark
it with `tenant_bypass` from `sqlspec.extensions.fastapi`; its session skips
SQLSpec's tenant session setup while database RLS policies stay active. This
example continues the [single-database example](#single-database-example):

```python
from sqlspec.extensions.fastapi import tenant_bypass


@app.get("/health")
@tenant_bypass
async def health(
    db: Annotated[AsyncpgDriver, Depends(db_ext.provide_session())],
) -> dict[str, int]:
    return {"ok": await db.select_value("SELECT 1")}
```

## Streaming and Background Tasks

FastAPI holds the request lease open through `StreamingResponse` bodies and
background tasks attached to the response, and the tenant resolved for the
request stays in effect for both. Sessions opened inside them with
`config.provide_session()` use that tenant. For work that outlives the request,
pass the verified tenant identifier to the worker and open
`config.provide_session(tenant=tenant_id)` there; see
[Tenant Operations](multi-tenancy-operations.md).

## Sync and Async Accessors

`Depends(db_ext.provide_session())` is an async dependency for sync and async
configurations alike and checks out the request connection on first use.
Outside dependency injection, call `await db_ext.get_session_async(request)`;
the synchronous `db_ext.get_session(request)` raises `ImproperConfigurationError`
for async configurations, which are checked out on first use.
