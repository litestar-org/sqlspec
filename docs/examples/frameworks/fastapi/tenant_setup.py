"""Defer FastAPI tenant binding to an authorization dependency with set_request_tenant."""

# --8<-- [start:example]
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.fastapi import SQLSpecPlugin, set_request_tenant

USERS: dict[str, tuple[str, ...]] = {"Bearer alice": ("acme", "globex")}

app = FastAPI()
db_ext = SQLSpecPlugin(
    [
        AiosqliteConfig(
            bind_key="auth",
            connection_config={"database": ":memory:"},
            extension_config={
                "fastapi": {"session_key": "auth_db", "connection_key": "auth_conn", "pool_key": "auth_pool"}
            },
        ),
        AiosqliteConfig(
            bind_key="tenant",
            connection_config={"database": ":memory:"},
            extension_config={
                "fastapi": {
                    "session_key": "tenant_db",
                    "connection_key": "tenant_conn",
                    "pool_key": "tenant_pool",
                    "commit_mode": "autocommit",
                }
            },
        ),
    ],
    app,
)


async def authorize_org(
    request: Request, org_id: str, auth_db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session("auth_db"))]
) -> str:
    await auth_db.select_one("SELECT 1 AS ok")
    memberships = USERS.get(request.headers.get("authorization", ""))
    if memberships is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if org_id not in memberships:
        raise HTTPException(status_code=403, detail="Forbidden organization")
    set_request_tenant(request, org_id, key="tenant_db")
    return org_id


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/orgs/{org_id}/projects")
async def list_projects(
    org_id: Annotated[str, Depends(authorize_org)],
    db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session("tenant_db"))],
) -> dict[str, Any]:
    return await db.select_one("SELECT :org_id AS org", org_id=org_id)


# --8<-- [end:example]

__all__ = ("USERS", "app", "authorize_org", "db_ext", "health", "list_projects", "main")


def main() -> FastAPI:
    """Return the configured multi-database FastAPI application."""
    assert db_ext.get_config("tenant").bind_key == "tenant"
    return app


if __name__ == "__main__":
    print(main())
