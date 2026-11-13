"""Five-framework lazy read sessions against native PostgreSQL row policies."""

from typing import Annotated, Any
from uuid import uuid4

import httpx
import pytest
from litestar import Litestar, get, post
from litestar.di import NamedDependency
from litestar.testing import AsyncTestClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.asyncpg.tenant import AsyncpgRLSStrategy
from sqlspec.core.tenant.base import TenantLeaseState
from sqlspec.exceptions import ReadOnlyTransactionError
from sqlspec.extensions.litestar import SQLSpecPlugin as LitestarPlugin
from sqlspec.extensions.starlette import SQLSpecPlugin as StarlettePlugin
from sqlspec.utils.tenant import TenantContextPayload
from sqlspec.utils.text import quote_identifier

pytestmark = pytest.mark.xdist_group("postgres")


class CountingRLS(AsyncpgRLSStrategy):
    """Count real tenant checkouts while preserving the native RLS implementation."""

    __slots__ = ("_checkouts", "_name")

    def __init__(self, checkouts: list[str], name: str) -> None:
        super().__init__(tenant_key="app.current_tenant_id")
        self._checkouts = checkouts
        self._name = name

    async def checkout(self, connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
        state = await super().checkout(connection, payload)
        self._checkouts.append(self._name)
        return state


def _configured_primary(credentials: dict[str, Any], framework: str, extractor: Any) -> tuple[AsyncpgConfig, list[str]]:
    options = {**credentials, "database": credentials["dbname"], "min_size": 1, "max_size": 1}
    options.pop("dbname")
    checkouts: list[str] = []
    replica = AsyncpgConfig(
        connection_config={**options, "server_settings": {"default_transaction_read_only": "on"}},
        driver_features={"tenant_strategy": CountingRLS(checkouts, "replica")},
    )
    primary = AsyncpgConfig(
        connection_config=options,
        driver_features={"tenant_strategy": CountingRLS(checkouts, "primary")},
        replicas=[replica],
        extension_config={framework: {"tenant_extractor": extractor, "commit_mode": "autocommit"}},
    )
    return primary, checkouts


def _scope_tenant(scope: Any) -> str | None:
    value = dict(scope["headers"]).get(b"x-tenant")
    return value.decode() if value is not None else None


async def _read_rows(session: Any, table: str) -> dict[str, Any]:
    rows = await session.select("SELECT value FROM " + table + " ORDER BY value")
    return {
        "values": [row["value"] for row in rows],
        "read_only": await session.select_value("SHOW transaction_read_only"),
    }


async def _write_row(session: Any, table: str) -> dict[str, bool]:
    await session.execute("INSERT INTO " + table + " VALUES (:tenant, :value)", {"tenant": "b", "value": 3})
    return {"written": True}


async def _refuse_write(session: Any, table: str) -> dict[str, bool]:
    try:
        await session.execute("INSERT INTO " + table + " VALUES (:tenant, :value)", {"tenant": "a", "value": 99})
    except ReadOnlyTransactionError:
        return {"refused": True}
    pytest.fail("A read session accepted a write against the native read-only PostgreSQL connection")


async def _assert_http_routing(client: Any, checkouts: list[str]) -> None:
    checkouts.clear()
    response = await client.get("/", headers={"X-Tenant": "a"})
    assert response.status_code == 200
    assert response.json() == {"values": [1], "read_only": "on"}
    assert checkouts == ["replica"]
    checkouts.clear()
    response = await client.post("/", headers={"X-Tenant": "b"})
    assert response.status_code == 200
    assert response.json() == {"written": True}
    assert checkouts == ["primary"]
    checkouts.clear()
    response = await client.get("/", headers={"X-Tenant": "b"})
    assert response.json() == {"values": [2, 3], "read_only": "on"}
    assert checkouts == ["replica"]
    checkouts.clear()
    response = await client.get("/blocked", headers={"X-Tenant": "a"})
    assert response.status_code == 200
    assert response.json() == {"refused": True}
    assert checkouts == ["replica"]


@pytest.mark.parametrize("framework", ["litestar", "starlette", "fastapi"])
async def test_asgi_lazy_read_di_isolates_native_rls_and_checkout_ownership(
    tenant_policy_database: tuple[dict[str, Any], str], framework: str
) -> None:
    credentials, table_name = tenant_policy_database
    table = quote_identifier(table_name)
    config, checkouts = _configured_primary(credentials, framework, _scope_tenant)
    if framework == "litestar":

        @get("/")
        async def read(read_session: NamedDependency[AsyncpgDriver]) -> dict[str, Any]:
            return await _read_rows(read_session, table)

        @post("/", status_code=200)
        async def write(db_session: NamedDependency[AsyncpgDriver]) -> dict[str, bool]:
            return await _write_row(db_session, table)

        @get("/blocked")
        async def blocked(read_session: NamedDependency[AsyncpgDriver]) -> dict[str, bool]:
            return await _refuse_write(read_session, table)

        app = Litestar([read, write, blocked], plugins=[LitestarPlugin(config)])
        async with AsyncTestClient(app=app) as client:
            await _assert_http_routing(client, checkouts)
        return
    plugin: Any
    if framework == "fastapi":
        from fastapi import Depends, FastAPI

        from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin

        fastapi = FastAPI()
        plugin = FastAPIPlugin(config, fastapi)
        read_dependency = plugin.provide_async_read_session()
        write_dependency = plugin.provide_async_session()

        @fastapi.get("/")
        async def fastapi_read(read_session: Annotated[Any, Depends(read_dependency)]) -> dict[str, Any]:
            return await _read_rows(read_session, table)

        @fastapi.post("/")
        async def fastapi_write(db_session: Annotated[Any, Depends(write_dependency)]) -> dict[str, bool]:
            return await _write_row(db_session, table)

        @fastapi.get("/blocked")
        async def fastapi_blocked(read_session: Annotated[Any, Depends(read_dependency)]) -> dict[str, bool]:
            return await _refuse_write(read_session, table)

        application: Any = fastapi
    else:

        async def starlette_read(request: Request) -> JSONResponse:
            return JSONResponse(await _read_rows(plugin.get_read_session(request), table))

        async def starlette_write(request: Request) -> JSONResponse:
            return JSONResponse(await _write_row(plugin.get_session(request), table))

        async def starlette_blocked(request: Request) -> JSONResponse:
            return JSONResponse(await _refuse_write(plugin.get_read_session(request), table))

        application = Starlette(
            routes=[
                Route("/", starlette_read),
                Route("/", starlette_write, methods=["POST"]),
                Route("/blocked", starlette_blocked),
            ]
        )
        plugin = StarlettePlugin(config, application)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://test") as client:
            await _assert_http_routing(client, checkouts)


def test_flask_portal_read_sessions_keep_rls_and_physical_checkout_ownership(
    tenant_policy_database: tuple[dict[str, Any], str],
) -> None:
    from flask import Flask

    from sqlspec.extensions.flask import SQLSpecPlugin as FlaskPlugin

    credentials, table_name = tenant_policy_database
    table = quote_identifier(table_name)
    config, checkouts = _configured_primary(credentials, "flask", lambda request: request.headers.get("X-Tenant"))
    app = Flask(__name__)
    plugin = FlaskPlugin(config, app)

    @app.get("/")
    def read() -> dict[str, Any]:
        session = plugin.get_read_session()
        rows = session.select("SELECT value FROM " + table + " ORDER BY value")
        return {
            "values": [row["value"] for row in rows],
            "read_only": session.select_value("SHOW transaction_read_only"),
        }

    @app.post("/")
    def write() -> dict[str, bool]:
        plugin.get_session().execute("INSERT INTO " + table + " VALUES (:tenant, :value)", {"tenant": "b", "value": 3})
        return {"written": True}

    @app.get("/blocked")
    def blocked() -> dict[str, bool]:
        with pytest.raises(ReadOnlyTransactionError):
            plugin.get_read_session().execute(
                "INSERT INTO " + table + " VALUES (:tenant, :value)", {"tenant": "a", "value": 99}
            )
        return {"refused": True}

    try:
        with app.test_client() as client:
            for method, path, tenant, expected, owner in [
                ("GET", "/", "a", {"values": [1], "read_only": "on"}, "replica"),
                ("POST", "/", "b", {"written": True}, "primary"),
                ("GET", "/", "b", {"values": [2, 3], "read_only": "on"}, "replica"),
                ("GET", "/blocked", "a", {"refused": True}, "replica"),
            ]:
                checkouts.clear()
                response = client.open(path, method=method, headers={"X-Tenant": tenant})
                assert response.status_code == 200
                assert response.get_json() == expected
                assert checkouts == [owner]
    finally:
        plugin.shutdown()


def test_sanic_lazy_read_sessions_keep_rls_and_checkout_ownership(
    tenant_policy_database: tuple[dict[str, Any], str],
) -> None:
    from sanic import Sanic, response

    from sqlspec.extensions.sanic import SQLSpecPlugin as SanicPlugin

    reusable_client = pytest.importorskip("sanic_testing.reusable").ReusableClient
    credentials, table_name = tenant_policy_database
    table = quote_identifier(table_name)
    config, checkouts = _configured_primary(credentials, "sanic", lambda request: request.headers.get("X-Tenant"))
    app = Sanic("ReadRouting" + uuid4().hex)
    app.config.USE_UVLOOP = False
    plugin = SanicPlugin(config, app)

    @app.get("/")
    async def read(request: Any) -> Any:
        return response.json(await _read_rows(plugin.get_read_session(request), table))

    @app.post("/")
    async def write(request: Any) -> Any:
        return response.json(await _write_row(plugin.get_session(request), table))

    @app.get("/blocked")
    async def blocked(request: Any) -> Any:
        return response.json(await _refuse_write(plugin.get_read_session(request), table))

    try:
        with reusable_client(app) as client:
            for method, path, tenant, expected, owner in [
                ("get", "/", "a", {"values": [1], "read_only": "on"}, "replica"),
                ("post", "/", "b", {"written": True}, "primary"),
                ("get", "/", "b", {"values": [2, 3], "read_only": "on"}, "replica"),
                ("get", "/blocked", "a", {"refused": True}, "replica"),
            ]:
                checkouts.clear()
                _, result = getattr(client, method)(path, headers={"X-Tenant": tenant})
                assert result.status == 200
                assert result.json == expected
                assert checkouts == [owner]
    finally:
        Sanic.unregister_app(app)
