"""Resolve verified Litestar tenant selectors automatically from SQLSpecPlugin settings."""

# --8<-- [start:example]
from typing import Any

import msgspec
from litestar import Controller, Litestar, get
from litestar.connection import ASGIConnection
from litestar.di import NamedDependency
from litestar.middleware import DefineMiddleware
from litestar.middleware.authentication import AbstractAuthenticationMiddleware, AuthenticationResult
from litestar.params import FromPath

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.litestar import SQLSpecPlugin, require_tenant


class User(msgspec.Struct):
    id: str
    default_org: str | None
    org_ids: tuple[str, ...]


USERS = {
    "alice": User(id="alice", default_org="acme", org_ids=("acme", "globex")),
    "bob": User(id="bob", default_org=None, org_ids=("acme", "globex")),
}


class HeaderAuthMiddleware(AbstractAuthenticationMiddleware):
    async def authenticate_request(self, connection: ASGIConnection[Any, Any, Any, Any]) -> AuthenticationResult:
        user = USERS.get(connection.headers.get("x-user", ""))
        return AuthenticationResult(user=user, auth={"authenticated": True} if user else None)


class OrgController(Controller):
    path = "/orgs"
    guards = [require_tenant()]

    @get("/health", sync_to_thread=False, opt={"tenant_bypass": True})
    def health(self, db_session: NamedDependency[SqliteDriver]) -> dict[str, Any]:
        return db_session.select_one("SELECT 'ok' AS status")

    @get("/{org_id:str}/status", sync_to_thread=False)
    def org_status(self, org_id: FromPath[str], db_session: NamedDependency[SqliteDriver]) -> dict[str, Any]:
        return db_session.select_one("SELECT :org_id AS org", org_id=org_id)


plugin = SQLSpecPlugin(
    SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={
            "litestar": {
                "tenant_user_attr": "default_org",
                "tenant_memberships_attr": "org_ids",
                "tenant_path_parameter": "org_id",
                "tenant_header": "x-org-id",
            }
        },
    )
)
app = Litestar(route_handlers=[OrgController], middleware=[DefineMiddleware(HeaderAuthMiddleware)], plugins=[plugin])
# --8<-- [end:example]

__all__ = ("USERS", "HeaderAuthMiddleware", "OrgController", "User", "app", "main", "plugin")


def main() -> Litestar:
    """Return the configured Litestar application."""
    assert app.state.sqlspec is plugin
    return app


if __name__ == "__main__":
    print(main())
