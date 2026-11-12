# Litestar Multi-Tenancy

Resolve the request tenant in Litestar from verified authentication state, route
guards, or an upfront extractor, then let `db_session` and `db_connection`
check out a tenant-bound connection. Read the
[Tenant Request Contract](multi-tenancy-frameworks.md#one-request-contract) for
the precedence and failure rules shared by every framework, and create the
account tables from
[Account Tables and User Lookup SQL](multi-tenancy-frameworks.md#account-tables-and-user-lookup-sql).

## How Litestar Resolves the Tenant

Enable `driver_features={"tenant_strategy": "rls"}` on a supported adapter and
configure the tenant selector and user attribute names once in
`extension_config["litestar"]`. Injected sessions and connections, and
`guards=[require_tenant()]`, inherit those settings from `app.state.sqlspec`:

```{ .python title="docs/examples/extensions/litestar/tenant_resolution.py" }
--8<-- "docs/examples/extensions/litestar/tenant_resolution.py:example"
```

The runnable example uses in-memory SQLite, which has no tenant strategy, so it
shows only selector validation by `require_tenant()`; tenant checkout applies
once the configuration uses a tenant-enabled adapter such as
`AsyncpgConfig(driver_features={"tenant_strategy": "rls"})`.

Litestar tries these sources in order for each tenant configuration:

1. A route marked `opt={"tenant_bypass": True}` or decorated with `@tenant_bypass`.
2. A receipt from `set_connection_tenant(connection, tenant_id)`.
3. The configured `tenant_extractor`.
4. A verified path parameter, server session key, or configured header,
   checked against the user's memberships, falling back to the verified
   `tenant_user_attr` value or a sole membership.
5. An ambient `TenantContext` scope bound by outer middleware.

A selector outside the caller's memberships always fails closed with `403`.
In custom guards or dependencies, verify membership yourself and call
`set_connection_tenant(connection, org_id)`; pass `key=` with a session,
connection, or bind key to scope the receipt to one database configuration.

## Single-Database JWT Flow

Pass `AsyncpgConfig` directly to `SQLSpecPlugin(config)`. Inside
`retrieve_user_handler`, open a short-lived lookup session with
`config.provide_session(bypass_tenant=True)` so authentication can read `users`
and `user_org_memberships` before a tenant is chosen.

```python
import os
from typing import Any

import msgspec
from litestar import Litestar, get, post
from litestar.di import NamedDependency
from litestar.exceptions import NotAuthorizedException
from litestar.security.jwt import JWTAuth, Token

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.litestar import SQLSpecPlugin, require_tenant


class User(msgspec.Struct):
    id: str
    default_org: str | None
    org_ids: list[str]


class LoginRequest(msgspec.Struct):
    email: str
    password: str


config = AsyncpgConfig(
    connection_config={"dsn": os.environ["DATABASE_URL"]},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "litestar": {
            "commit_mode": "autocommit",
            "tenant_user_attr": "default_org",
            "tenant_memberships_attr": "org_ids",
            "tenant_path_parameter": "org_id",
            "tenant_header": "x-org-id",
        }
    },
)


def verify_password(password: str, password_hash: str) -> bool:
    """Verify the password with the application's password hasher."""
    ...


async def retrieve_user_handler(token: Token, connection: Any) -> User | None:
    async with config.provide_session(bypass_tenant=True) as session:
        return await session.select_one_or_none(
            "SELECT u.id, u.default_org, "
            "array_remove(array_agg(m.org_id ORDER BY m.org_id), NULL) AS org_ids "
            "FROM public.users AS u "
            "LEFT JOIN public.user_org_memberships AS m ON m.user_id = u.id "
            "WHERE u.id = :id GROUP BY u.id, u.default_org",
            id=token.sub,
            schema_type=User,
        )


jwt_auth = JWTAuth[User](
    token_secret=os.environ["JWT_SIGNING_KEY"],
    retrieve_user_handler=retrieve_user_handler,
    exclude=["/login", "/health"],
)


@post("/login", opt={"exclude_from_auth": True, "tenant_bypass": True})
async def login(
    data: LoginRequest, db_session: NamedDependency[AsyncpgDriver]
) -> dict[str, str]:
    row = await db_session.select_one_or_none(
        "SELECT id, password_hash FROM public.users WHERE email = :email",
        email=data.email,
    )
    if row is None or not verify_password(data.password, row["password_hash"]):
        raise NotAuthorizedException("Invalid credentials")
    return {"access_token": jwt_auth.create_token(identifier=row["id"])}


@get("/me/orgs", opt={"tenant_bypass": True})
async def my_orgs(request: Any) -> list[str]:
    return list(request.user.org_ids)


@get("/orgs/{org_id:str}/projects", guards=[require_tenant()])
async def org_projects(
    db_session: NamedDependency[AsyncpgDriver],
) -> list[dict[str, Any]]:
    return await db_session.select("SELECT id, name FROM public.projects")


app = Litestar(
    route_handlers=[login, my_orgs, org_projects],
    on_app_init=[jwt_auth.on_app_init],
    plugins=[SQLSpecPlugin(config)],
)
```

Key behaviors:

- `SQLSpecPlugin(config)` needs no separate `SQLSpec()` registry and attaches to
  `app.state.sqlspec` in `on_app_init`. Litestar's `JWTAuth` and `SessionAuth`
  still attach via `on_app_init` because they expose an app-init hook rather than
  a plugin object.
- `require_tenant()` inherits `tenant_user_attr`, `tenant_memberships_attr`,
  `tenant_path_parameter`, `tenant_session_key`, `tenant_header`,
  `tenant_variables_attr`, and `tenant_jwt_claim` from the plugin configuration.
  Pass keyword overrides only for per-route differences (or `config_key` when
  multiple tenant databases use distinct selector names), and place
  `guards=[require_tenant()]` on a `Controller` to cover all of its routes.
- Routes without a path parameter accept `x-org-id` or fall back to
  `User.default_org` when in `org_ids`. Even without `require_tenant()`,
  injecting `db_session` or `db_connection` enforces the same rules (`401` /
  `403` on HTTP, `4001` / `4003` on WebSockets).

## Separate Auth and Tenant Databases

When user accounts and sessions live in a dedicated authentication database,
pass a list of configs to `SQLSpecPlugin([auth_config, tenant_config])`. The
authentication store and login routes read `auth_config` without any bypass
scope, and organization switching verifies membership before updating the
server-side session:

```python
import os
from typing import Any

import msgspec
from litestar import Litestar, Request, get, post
from litestar.di import NamedDependency
from litestar.exceptions import PermissionDeniedException
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.params import FromPath
from litestar.security.session_auth import SessionAuth

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
from sqlspec.extensions.litestar import SQLSpecPlugin, require_tenant


class Account(msgspec.Struct):
    id: str
    org_ids: list[str]


auth_config = AsyncpgConfig(
    bind_key="auth",
    connection_config={"dsn": os.environ["AUTH_DATABASE_URL"]},
    extension_config={
        "litestar": {
            "session_key": "auth_db",
            "connection_key": "auth_conn",
            "pool_key": "auth_pool",
            "session_table": True,
        }
    },
)

tenant_config = AsyncpgConfig(
    bind_key="tenant",
    connection_config={"dsn": os.environ["TENANT_DATABASE_URL"]},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "litestar": {
            "commit_mode": "autocommit",
            "tenant_memberships_attr": "org_ids",
            "tenant_path_parameter": "org_id",
            "tenant_session_key": "active_org",
        }
    },
)


async def retrieve_session_user(
    session: dict[str, Any], connection: Any
) -> Account | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    async with auth_config.provide_session() as auth_db:
        return await auth_db.select_one_or_none(
            "SELECT u.id, array_remove(array_agg(m.org_id), NULL) AS org_ids "
            "FROM public.users AS u "
            "LEFT JOIN public.user_org_memberships AS m ON m.user_id = u.id "
            "WHERE u.id = :user_id GROUP BY u.id",
            user_id=user_id,
            schema_type=Account,
        )


session_auth = SessionAuth[Account, Any](
    retrieve_user_handler=retrieve_session_user,
    session_backend_config=ServerSideSessionConfig(),
    exclude=["/health"],
)


@post("/session/org/{org_id:str}")
async def switch_org(
    org_id: FromPath[str], request: Request[Account, Any, Any]
) -> dict[str, str]:
    if org_id not in request.user.org_ids:
        raise PermissionDeniedException("Not a member of this organization")
    request.set_session({**request.session, "active_org": org_id})
    return {"active_org": org_id}


@get("/projects", guards=[require_tenant()])
async def projects(
    db_session: NamedDependency[AsyncpgDriver],
) -> list[dict[str, Any]]:
    return await db_session.select("SELECT id, name FROM public.projects")


app = Litestar(
    route_handlers=[switch_org, projects],
    on_app_init=[session_auth.on_app_init],
    stores={"sessions": AsyncpgStore(config=auth_config)},
    plugins=[SQLSpecPlugin([auth_config, tenant_config])],
)
```

## Litestar Security Integration

When using `litestar-security`, register `SecurityPlugin` alongside
`SQLSpecPlugin` with no `on_app_init` hooks. SQLSpec natively reads
`Principal`, `SecurityContext`, `AuthorizationSnapshot` (`tenant_ids`,
`tenant_roles`, and `attributes`), `SessionHandle`, `JWTClaims`,
`CredentialRestrictions`, and route guards built with `requires_tenant(...)` or
`requires_tenant_role(...)`:

```python
import os
from typing import Any

import msgspec
from litestar import Litestar, get
from litestar.di import NamedDependency
from litestar_security import (
    AuthorizationSnapshot, CurrentUser, JWTAuthConfig, Principal,
    SecureController, SecurityConfig, SecurityPlugin, public,
    requires_tenant, requires_tenant_role,
)

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.litestar import SQLSpecPlugin


class User(msgspec.Struct):
    id: str
    default_org: str | None


config = AsyncpgConfig(
    connection_config={"dsn": os.environ["DATABASE_URL"]},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "litestar": {
            "commit_mode": "autocommit",
            "tenant_user_attr": "default_org",
            "tenant_path_parameter": "org_id",
        }
    },
)


class OrgAuthorizationResolver:
    async def resolve(self, principal: Principal[User]) -> AuthorizationSnapshot:
        async with config.provide_session(bypass_tenant=True) as session:
            rows = await session.select(
                "SELECT org_id, role FROM public.user_org_memberships "
                "WHERE user_id = :id",
                id=principal.id,
            )
        roles: dict[str, set[str]] = {}
        for row in rows:
            roles.setdefault(row["org_id"], set()).add(row["role"])
        frozen = {org: frozenset(items) for org, items in roles.items()}
        default = principal.user.default_org if principal.user else None
        return AuthorizationSnapshot(
            tenant_ids=frozenset(frozen),
            tenant_roles=frozen,
            attributes={"default_org": default} if default else {},
        )


class ProjectController(SecureController):
    path = "/orgs/{org_id:str}/projects"

    @get("/", guards=[requires_tenant(tenant_parameter="org_id")])
    async def list_projects(
        self,
        current_user: CurrentUser[User],
        db_session: NamedDependency[AsyncpgDriver],
    ) -> list[dict[str, Any]]:
        return await db_session.select("SELECT id, name FROM public.projects")

    @get(
        "/editable",
        guards=[
            requires_tenant_role(
                tenant_parameter="org_id", roles=frozenset({"editor", "admin"})
            )
        ],
    )
    async def list_editable(
        self, db_session: NamedDependency[AsyncpgDriver]
    ) -> list[dict[str, Any]]:
        return await db_session.select("SELECT id, name FROM public.projects")


@get("/health", auth=public(), opt={"tenant_bypass": True})
async def health(db_session: NamedDependency[AsyncpgDriver]) -> dict[str, int]:
    return {"ok": await db_session.select_value("SELECT 1")}


app = Litestar(
    route_handlers=[ProjectController, health],
    plugins=[
        SQLSpecPlugin(config),
        SecurityPlugin(SecurityConfig(
            providers=[JWTAuthConfig(signing_key=os.environ["JWT_SIGNING_KEY"])],
            authorization_resolver=OrgAuthorizationResolver(),
        )),
    ],
)
```

How SQLSpec cooperates with `litestar-security`:

- `AuthorizationSnapshot.tenant_ids` and non-empty `tenant_roles` entries merge
  into the caller's verified tenant set automatically.
- `CredentialRestrictions(tenant_ids=...)` attached by scoped API keys or
  delegated tokens clamp the allowed tenant set by intersection before selector
  validation; an out-of-scope tenant path or header fails closed with `403`.
- `requires_tenant(tenant_parameter="org_id")` and
  `requires_tenant_role(tenant_parameter="org_id", roles=...)` (including guards
  nested inside `requires_all_of` or `requires_any_of`) expose the path selector
  name to SQLSpec, so you do not need to stack `require_tenant()` next to them.
  Inside `requires_any_of`, a non-tenant branch passing still requires the path
  value to match a verified membership before database checkout.

## Upfront Extractor

When your authentication middleware already stores a verified user in
`scope["user"]`, set `tenant_extractor` in `extension_config["litestar"]`.
Litestar passes the ASGI scope; the extractor may be sync or async and runs
ahead of the automatic path, session, and header selectors:

```python
import os
from typing import Any

from sqlspec.adapters.asyncpg import AsyncpgConfig


def verified_tenant(scope: dict[str, Any]) -> str | None:
    user = scope.get("user")
    if user is None:
        return None
    selected = getattr(user, "default_org", None)
    return selected if selected in getattr(user, "org_ids", ()) else None


config = AsyncpgConfig(
    connection_config={"dsn": os.environ["DATABASE_URL"]},
    driver_features={"tenant_strategy": "rls"},
    extension_config={
        "litestar": {
            "commit_mode": "autocommit",
            "tenant_extractor": verified_tenant,
        }
    },
)
```

Return `None` to let resolution continue with the automatic selectors and the
ambient scope. See
[Upfront Tenant Extractors](multi-tenancy-frameworks.md#upfront-tenant-extractors)
for the rules every extractor follows.

## Public and Bypass Routes

Mark login, health, and other cross-tenant routes with
`opt={"tenant_bypass": True}` or the `@tenant_bypass` decorator from
`sqlspec.extensions.litestar`. `require_tenant()` on a `Controller` honors the
marker, so public routes can live next to guarded ones. Injected sessions on a
bypassed route skip SQLSpec's tenant session setup while database RLS policies
stay active. Routes that inject no session or connection never check one out.

```python
from litestar import get
from litestar.di import NamedDependency

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.extensions.litestar import tenant_bypass


@get("/health")
@tenant_bypass
async def health(db_session: NamedDependency[AsyncpgDriver]) -> dict[str, int]:
    return {"ok": await db_session.select_value("SELECT 1")}
```

Combine the marker with `exclude_from_auth` on login routes, as in the
[JWT flow](#single-database-jwt-flow).

## Streaming and Background Tasks

Litestar releases the request connection when the response starts. The tenant
resolved for the request stays in effect for `Stream` bodies and
`BackgroundTask` callables, so open a new session inside them with
`config.provide_session()` instead of reusing `db_session`. This example uses
the `config` from the [JWT flow](#single-database-jwt-flow):

```python
import logging
from collections.abc import AsyncIterator

from litestar import get
from litestar.background_tasks import BackgroundTask
from litestar.response import Response, Stream

from sqlspec.extensions.litestar import require_tenant

logger = logging.getLogger(__name__)


@get("/orgs/{org_id:str}/export", guards=[require_tenant()])
async def export_projects() -> Stream:
    async def body() -> AsyncIterator[bytes]:
        async with config.provide_session() as session:
            rows = await session.select("SELECT id, name FROM public.projects")
        for row in rows:
            yield f"{row['id']},{row['name']}\n".encode()

    return Stream(body())


async def log_project_count() -> None:
    async with config.provide_session() as session:
        count = await session.select_value("SELECT count(*) FROM public.projects")
    logger.info("projects visible to tenant: %s", count)


@get("/orgs/{org_id:str}/ping", guards=[require_tenant()])
async def ping() -> Response[dict[str, bool]]:
    return Response({"ok": True}, background=BackgroundTask(log_project_count))
```

For work that outlives the request, pass the verified tenant identifier to the
worker and open `config.provide_session(tenant=tenant_id)` there; see
[Tenant Operations](multi-tenancy-operations.md).

## Sync and Async Accessors

Dependency injection provides `db_session` and `db_connection` for sync and
async configurations alike, and checks out a tenant connection only when a
route asks for one. In guards, middleware, and other code without dependency
injection, use `await plugin.provide_request_session_async(key, state, scope)`
for async configurations and `plugin.provide_request_session_sync(key, state,
scope)` for sync ones; both acquire under the resolved request tenant. See
[On-Demand Session and Connection Acquisition](../../integrations/litestar.md#on-demand-session-and-connection-acquisition).
