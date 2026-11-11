"""Authorized Litestar tenant scopes against native PostgreSQL row policies."""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, Request, get
from litestar.di import NamedDependency
from litestar.middleware import DefineMiddleware
from litestar.middleware.authentication import AbstractAuthenticationMiddleware, AuthenticationResult
from litestar.middleware.session.client_side import CookieBackendConfig
from litestar.params import FromPath
from litestar.testing import AsyncTestClient, TestClient
from psycopg.conninfo import make_conninfo
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin, require_tenant
from sqlspec.utils.tenant import TenantContext
from sqlspec.utils.text import quote_identifier

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture(params=["asyncpg", "psycopg"])
async def tenant_database(
    request: pytest.FixtureRequest, postgres_service: PostgresService
) -> AsyncGenerator[Any, None]:
    credentials = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }
    admin = await AsyncpgConfig(connection_config=credentials).create_connection()
    suffix = uuid4().hex[:12]
    role, table = "tenant_web_" + suffix, "tenant_web_rows_" + suffix
    quoted_role, quoted_table = quote_identifier(role), quote_identifier(table)
    role_sql = await admin.fetchval(
        "SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD %L', $1::text, $2::text)",
        role,
        "sqlspec-tenant-web",
    )
    await admin.execute(role_sql)
    tenant_credentials = {**credentials, "user": role, "password": "sqlspec-tenant-web"}
    if request.param == "asyncpg":
        config: Any = AsyncpgConfig(
            connection_config={**tenant_credentials, "min_size": 1, "max_size": 1},
            driver_features={"tenant_strategy": True},
        )
    else:
        conninfo = make_conninfo(
            host=postgres_service.host,
            port=str(postgres_service.port),
            user=role,
            password="sqlspec-tenant-web",
            dbname=postgres_service.database,
        )
        config = PsycopgAsyncConfig(
            connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 1},
            driver_features={"tenant_strategy": True},
        )
    try:
        await admin.execute(f"CREATE TABLE {quoted_table} (tenant_id text NOT NULL, value integer NOT NULL)")
        await admin.execute(f"ALTER TABLE {quoted_table} ENABLE ROW LEVEL SECURITY")
        await admin.execute(f"ALTER TABLE {quoted_table} FORCE ROW LEVEL SECURITY")
        await admin.execute(
            f"CREATE POLICY tenant_policy ON {quoted_table} USING (tenant_id = current_setting('app.current_tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true))"
        )
        await admin.execute(f"GRANT SELECT, INSERT ON {quoted_table} TO {quoted_role}")
        await admin.executemany(f"INSERT INTO {quoted_table} VALUES ($1, $2)", [("acme", 1), ("globex", 2)])
        yield config, quoted_table
    finally:
        await config.close_pool()
        await admin.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        await admin.execute(f"DROP ROLE {quoted_role}")
        await admin.close()


async def test_tenant_native_auth_bypass_switching_and_rollback(tenant_database: Any) -> None:
    config, table = tenant_database
    lookup_scopes: list[bool] = []

    class Authentication(AbstractAuthenticationMiddleware):
        async def authenticate_request(self, connection: Any) -> AuthenticationResult:
            identity = connection.headers.get("authorization")
            if identity not in {"acme", "globex"}:
                return AuthenticationResult(user=None, auth=None)
            async with config.provide_session(bypass_tenant=True) as session:
                assert await session.select_value("SELECT 1") == 1
                lookup_scopes.append(TenantContext.is_bypassed())
            return AuthenticationResult(user={"tenant_id": identity, "tenant_ids": ["acme", "globex"]}, auth={})

    @get("/items", guards=[require_tenant()])
    async def items(db_session: NamedDependency[AsyncDriverAdapterBase]) -> list[int]:
        return [row["value"] for row in await db_session.select(f"SELECT value FROM {table} ORDER BY value")]

    @get("/switch", guards=[require_tenant(header="x-tenant-id")])
    async def switch(db_session: NamedDependency[AsyncDriverAdapterBase]) -> str:
        return str(await db_session.select_value("SELECT current_setting('app.current_tenant_id', true)"))

    @get("/tenants/{tenant_id:str}")
    async def selected(tenant_id: FromPath[str], db_session: NamedDependency[AsyncDriverAdapterBase]) -> list[int]:
        return [row["value"] for row in await db_session.select(f"SELECT value FROM {table}")]

    @get("/health", opt={"tenant_bypass": True})
    async def health(db_session: NamedDependency[AsyncDriverAdapterBase]) -> bool:
        assert await db_session.select_value("SELECT current_setting('app.current_tenant_id', true)") in {None, ""}
        return bool(await db_session.select_value("SELECT 1") == 1)

    @get("/failure", guards=[require_tenant()])
    async def failure(db_session: NamedDependency[AsyncDriverAdapterBase]) -> None:
        await db_session.begin()
        await db_session.execute(f"INSERT INTO {table} VALUES (:tenant, :value)", {"tenant": "acme", "value": 99})
        raise RuntimeError("handler failure")

    @get("/worker")
    async def worker() -> list[int]:
        async def work() -> list[int]:
            async with TenantContext.scope("acme"), config.provide_session() as session:
                return [row["value"] for row in await session.select(f"SELECT value FROM {table}")]

        return await asyncio.create_task(work())

    app = Litestar(
        [items, switch, selected, health, failure, worker],
        plugins=[SQLSpecPlugin(config)],
        middleware=[DefineMiddleware(Authentication)],
    )
    async with AsyncTestClient(app=app, raise_server_exceptions=False) as client:
        assert (await client.get("/health")).json() is True
        assert (await client.get("/items")).status_code == 401
        assert (await client.get("/items", headers={"x-tenant-id": "evil"})).status_code == 401
        assert (await client.get("/tenants/acme")).status_code == 401
        for tenant, expected in [("acme", [1]), ("globex", [2])]:
            assert (await client.get("/items", headers={"authorization": tenant})).json() == expected
        assert (
            await client.get("/switch", headers={"authorization": "acme", "x-tenant-id": "globex"})
        ).text == "globex"
        assert (
            await client.get("/switch", headers={"authorization": "acme", "x-tenant-id": "evil"})
        ).status_code == 403
        assert (await client.get("/tenants/evil", headers={"authorization": "acme"})).status_code == 403
        assert (await client.get("/failure", headers={"authorization": "acme"})).status_code == 500
        assert (await client.get("/items", headers={"authorization": "acme"})).json() == [1]
        assert (await client.get("/worker")).json() == [1]
        responses = await asyncio.gather(
            *(client.get("/items", headers={"authorization": tenant}) for tenant in ["acme", "globex"])
        )
        assert [response.json() for response in responses] == [[1], [2]]
        assert lookup_scopes and all(lookup_scopes)
    assert TenantContext.get() is None


async def test_tenant_security_composites_cannot_authorize_unrelated_tenants(tenant_database: Any) -> None:
    security = pytest.importorskip("litestar_security")

    config, table = tenant_database
    config.extension_config["litestar"] = {"tenant_header": "x-tenant-id"}

    class Authentication(AbstractAuthenticationMiddleware):
        async def authenticate_request(self, connection: Any) -> AuthenticationResult:
            return AuthenticationResult(
                user=security.Principal(id="reader"),
                auth=security.SecurityContext(
                    session=security.NullSessionHandle(),
                    authorization=security.AuthorizationSnapshot(
                        roles=frozenset({"reader"}),
                        tenant_ids=frozenset({"acme", "globex"}),
                        tenant_roles={"acme": frozenset({"reader"})},
                    ),
                ),
            )

    @get("/orgs/{org_id:str}/items", guards=[security.requires_tenant(tenant_parameter="org_id")])
    async def membership(org_id: FromPath[str], db_session: NamedDependency[AsyncDriverAdapterBase]) -> list[int]:
        return [row["value"] for row in await db_session.select(f"SELECT value FROM {table}")]

    @get(
        "/or/{org_id:str}",
        guards=[
            security.requires_any_of(
                security.requires_role("reader"), security.requires_tenant(tenant_parameter="org_id")
            )
        ],
    )
    async def unrelated_or(org_id: FromPath[str], db_session: NamedDependency[AsyncDriverAdapterBase]) -> list[int]:
        return [row["value"] for row in await db_session.select(f"SELECT value FROM {table}")]

    @get(
        "/role/{org_id:str}",
        guards=[
            security.requires_all_of(
                security.requires_role("reader"),
                security.requires_tenant_role(tenant_parameter="org_id", roles={"reader"}),
            )
        ],
    )
    async def role(org_id: FromPath[str], db_session: NamedDependency[AsyncDriverAdapterBase]) -> list[int]:
        return [row["value"] for row in await db_session.select(f"SELECT value FROM {table}")]

    @get("/switch", guards=[require_tenant(header="x-tenant-id")])
    async def switch(db_session: NamedDependency[AsyncDriverAdapterBase]) -> list[int]:
        return [row["value"] for row in await db_session.select(f"SELECT value FROM {table}")]

    session_config = CookieBackendConfig(secret=b"0123456789abcdef")
    app = Litestar(
        [membership, unrelated_or, role, switch],
        plugins=[SQLSpecPlugin(config)],
        middleware=[session_config.middleware, DefineMiddleware(Authentication)],
    )
    async with AsyncTestClient(app=app, raise_server_exceptions=False, session_config=session_config) as client:
        response = await client.get("/orgs/acme/items")
        assert response.json() == [1], response.text
        assert (await client.get("/orgs/evil/items")).status_code == 403
        assert (await client.get("/role/acme")).json() == [1]
        assert (await client.get("/role/globex")).status_code == 403
        assert (await client.get("/switch", headers={"x-tenant-id": "globex"})).json() == [2]
        assert (await client.get("/switch", headers={"x-tenant-id": "evil"})).status_code == 403
        await client.set_session_data({"tenant_id": "globex"})
        assert (await client.get("/switch", headers={"x-tenant-id": "acme"})).json() == [2]
        await client.set_session_data({"tenant_id": "evil"})
        assert (await client.get("/switch", headers={"x-tenant-id": "acme"})).status_code == 403
        await client.set_session_data({})
        assert (await client.get("/or/evil")).status_code == 403
        assert (await client.get("/or/acme")).json() == [1]
    assert TenantContext.get() is None


@pytest.mark.parametrize("second_tenant", ["acme", "globex"])
async def test_tenant_parallel_database_dependencies_capture_distinct_payloads(
    tenant_database: Any, second_tenant: str
) -> None:
    config, table = tenant_database
    config.extension_config["litestar"] = {
        "connection_key": "first_connection",
        "pool_key": "first_pool",
        "session_key": "first_session",
        "tenant_extractor": lambda scope: "acme",
    }
    second = type(config)(
        connection_config=dict(config.connection_config),
        driver_features={"tenant_strategy": True},
        extension_config={
            "litestar": {
                "connection_key": "second_connection",
                "pool_key": "second_pool",
                "session_key": "second_session",
                "tenant_extractor": lambda scope: second_tenant,
            }
        },
    )

    @get("/parallel")
    async def parallel(
        first_session: NamedDependency[AsyncDriverAdapterBase], second_session: NamedDependency[AsyncDriverAdapterBase]
    ) -> list[list[int]]:
        first_rows = await first_session.select(f"SELECT value FROM {table}")
        second_rows = await second_session.select(f"SELECT value FROM {table}")
        return [[row["value"] for row in first_rows], [row["value"] for row in second_rows]]

    app = Litestar([parallel], plugins=[SQLSpecPlugin([config, second])])
    async with AsyncTestClient(app=app, raise_server_exceptions=True) as client:
        for _ in range(2):
            response = await client.get("/parallel")
            assert response.status_code == 200, response.text
            assert response.json() == [[1], [1 if second_tenant == "acme" else 2]]
    assert TenantContext.get() is None


async def test_tenant_threaded_sync_on_demand_restores_its_context(tenant_database: Any) -> None:
    config, table = tenant_database
    if isinstance(config, AsyncpgConfig):
        parameters = config.connection_config
        conninfo = make_conninfo(
            host=parameters["host"],
            port=str(parameters["port"]),
            user=parameters["user"],
            password=parameters["password"],
            dbname=parameters["database"],
        )
    else:
        conninfo = config.connection_config["conninfo"]
    sync_config = PsycopgSyncConfig(
        connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 1},
        driver_features={"tenant_strategy": True},
        extension_config={"litestar": {"tenant_extractor": lambda scope: "acme"}},
    )
    plugin = SQLSpecPlugin(sync_config)

    @get("/threaded", sync_to_thread=True)
    def threaded(request: Request) -> list[int]:
        before = TenantContext.get()
        session = plugin.provide_request_session_sync("db_connection", request.app.state, request.scope)
        assert TenantContext.get() is before
        assert session.select_value("SELECT current_setting('app.current_tenant_id', true)") == "acme"
        return [row["value"] for row in session.select(f"SELECT value FROM {table}")]

    app = Litestar([threaded], plugins=[plugin])
    with TestClient(app=app) as client:
        for _ in range(2):
            response = client.get("/threaded")
            assert response.status_code == 200
            assert response.json() == [1]
    assert TenantContext.get() is None
