# Tenant Framework Integration

Bind each request to a tenant the user has a right to access. Start with the role and `projects` policy in [Multi-Tenancy](multi-tenancy.md). A tenant header lets a user choose among their tenants. It does not grant access.

## Litestar Authentication and Guards

Install `sqlspec[asyncpg,litestar]`. Set `DATABASE_URL` to the app role's DSN. Set `JWT_SIGNING_KEY` to the key used to check your tokens. The example keeps user records on the server. In your app, load users from your trusted account store.

```python
import os
from dataclasses import dataclass
from typing import Any

from litestar import Litestar, get
from litestar.security.jwt import JWTAuth, Token

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.litestar import SQLSpecPlugin, require_tenant


@dataclass(frozen=True)
class User:
    tenant_id: str
    tenant_ids: tuple[str, ...]


users = {"alice": User("acme", ("acme", "globex"))}


async def retrieve_user_handler(token: Token, connection: Any) -> User | None:
    return users.get(token.sub)


config = AsyncpgConfig(
    connection_config={"dsn": os.environ["DATABASE_URL"]},
    driver_features={"tenant_strategy": "rls"},
)
sqlspec = SQLSpec()
sqlspec.add_config(config)
auth = JWTAuth(
    token_secret=os.environ["JWT_SIGNING_KEY"],
    retrieve_user_handler=retrieve_user_handler,
)


@get("/orgs/{org_id:str}/projects", guards=[require_tenant(path_parameter="org_id")])
async def projects(db_session: AsyncpgDriver) -> list[dict[str, Any]]:
    return await db_session.select("SELECT id, name FROM public.projects ORDER BY id")


@get("/health", opt={"exclude_from_auth": True, "tenant_bypass": True})
async def health(db_session: AsyncpgDriver) -> int:
    return await db_session.select_value("SELECT 1")


app = Litestar(
    route_handlers=[projects, health],
    on_app_init=[auth.on_app_init],
    plugins=[SQLSpecPlugin(sqlspec=sqlspec)],
)
```

With a valid token for `alice`, `/orgs/acme/projects` returns Billing. `/orgs/globex/projects` returns Search. The guard rejects `/orgs/evil/projects` with 403. A request with no logged-in user gets 401. `SessionAuth` also works: both auth systems place the trusted user in `scope["user"]`. Keep your existing auth middleware.

For a route with no tenant path, SQLSpec reads `tenant_id` from the trusted user or auth claims. It can also choose a sole tenant from `tenant_ids`. If the user has more than one tenant and no default, they must choose one.

`require_tenant()` accepts `path_parameter=None`, `header=None`, `session_key="tenant_id"`, `user_attr="tenant_id"`, `memberships_attr="tenant_ids"`, and `variables_attr="tenant_variables"`. Once it checks access, the guard stores the tenant for the request. Your own guard can call `set_connection_tenant(connection_or_scope, tenant_id, variables=...)` after it checks access.

SQLSpec checks route `opt={"tenant_bypass": True}` first. Next come a trusted scope receipt, your `tenant_extractor`, and trusted user/auth data, in that order. `resolve_litestar_tenant(scope, settings)` returns the chosen payload. It does not change the current context.

## Authorize Organization Switching

Use these Litestar options to name the fields your app uses:

```python
from sqlspec.config import LitestarConfig

settings: LitestarConfig = {
    "tenant_user_attr": "tenant_id",
    "tenant_memberships_attr": "tenant_ids",
    "tenant_path_parameter": "org_id",
    "tenant_session_key": "tenant_id",
    "tenant_header": "x-tenant-id",
}
```

Pass this dict as `extension_config={"litestar": settings}` on the config. SQLSpec checks the path first, then the session, then the header. The chosen tenant must be one the user can access. A wrong tenant fails closed, even if the user has a valid default. Set `tenant_header`, or a guard's `header`, to let a header choose the tenant.

With `litestar-security`, `Principal` and `SecurityContext.authorization` hold the trusted user and their grants. `AuthorizationSnapshot.tenant_ids` and `tenant_roles` state which tenants the user can access. Build them from trusted account data in your auth resolver:

```python
from litestar_security import (
    AuthorizationSnapshot,
    NullSessionHandle,
    Principal,
    SecurityContext,
    requires_tenant,
    requires_tenant_role,
)

principal = Principal(id="alice")
security_context = SecurityContext(
    session=NullSessionHandle(),
    authorization=AuthorizationSnapshot(
        tenant_ids=frozenset({"acme", "globex"}),
        tenant_roles={"acme": frozenset({"editor"})},
    ),
)
membership_guard = requires_tenant(tenant_parameter="org_id")
editor_guard = requires_tenant_role(tenant_parameter="org_id", roles={"editor"})
```

The security plugin puts the principal in `scope["user"]` and context in `scope["auth"]`. Use the guards in the route's `guards` list. A tenant guard inside `requires_any_of()` names a path field. If a different role branch passes, it grants no tenant access. SQLSpec still checks the chosen tenant against the user's grants.

## Starlette and FastAPI Extractors

Set `extension_config["starlette"]["tenant_extractor"]` or `extension_config["fastapi"]["tenant_extractor"]` on each config. The callback reads the ASGI scope, which your auth middleware must fill first. Sync and async callbacks can return `TenantContextPayload`, `str`, `UUID`, or `None`.

This callback checks that the user can access their chosen default tenant:

```python
from typing import Any

from sqlspec.adapters.asyncpg import AsyncpgConfig


def verified_tenant(scope: dict[str, Any]) -> str | None:
    user = scope.get("user")
    if user is None or not getattr(user, "is_authenticated", False):
        return None
    selected = getattr(user, "tenant_id", None)
    return selected if selected in getattr(user, "tenant_ids", ()) else None


def tenant_config(dsn: str) -> AsyncpgConfig:
    return AsyncpgConfig(
        connection_config={"dsn": dsn},
        driver_features={"tenant_strategy": "rls"},
        extension_config={"starlette": {
            "tenant_extractor": verified_tenant,
            "commit_mode": "autocommit",
        }},
    )
```

Register the config with the [Starlette](../../integrations/starlette.md) or [FastAPI](../../integrations/fastapi.md) plugin. For FastAPI, use the `"fastapi"` config key. The plugin calls the extractor before it opens the lease. Its result replaces any outer tenant for that lease. `None` clears the tenant; a tenant-enabled config then raises `TenantContextError` before it takes a pool slot.

If your app owns its leases, wrap it with `SQLSpecTenantMiddleware(app, extractor=verified_tenant)`. It sets the tenant for the full HTTP or WebSocket call, then restores the outer scope. Import it from `sqlspec.utils.tenant`, `sqlspec.extensions.starlette`, or `sqlspec.extensions.fastapi`. `single_membership_tenant_extractor(accessor)` chooses a tenant only when the trusted accessor returns one tenant.

## Request Lifetime and Authentication Lookups

Starlette and FastAPI keep the request driver alive through the stream and background work. Autocommit waits until the body and background work finish. A body, send, or background error rolls back the work. So does a cancelled request. Manual mode needs an explicit commit. If commit fails after headers are sent, the server gets the error, but the client's status cannot change. Commit before you yield a success message if the client needs proof the write is saved.

Auth code may need a query before it knows the tenant. Inside `retrieve_user_handler`, wrap that lookup in `async with async_tenant_bypass(), config.provide_session() as session:`. Import `async_tenant_bypass` from `sqlspec`. The database must permit the query under its normal rules. Bypass skips tenant setup; it does not turn off RLS or grant admin rights. Use a separate config for admin queries, global account stores, and migrations.

Each scope restores its caller when the request ends or fails. Workers open their own sessions in a tenant scope. They must not keep the request's driver. See [Tenant Operations](multi-tenancy-operations.md), [Flask](../../integrations/flask.md), and [Sanic](../../integrations/sanic.md) for worker and request-hook use.
