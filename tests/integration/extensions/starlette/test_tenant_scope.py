"""Native PostgreSQL RLS request and streaming lease acceptance."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
from pytest_databases.docker.postgres import PostgresService
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.starlette import SQLSpecPlugin
from sqlspec.utils.tenant import TenantContext
from sqlspec.utils.text import quote_identifier

pytestmark = [pytest.mark.anyio, pytest.mark.xdist_group("postgres")]


@pytest.fixture
async def tenant_database(postgres_service: PostgresService) -> AsyncIterator[tuple[dict[str, Any], str]]:
    """Provision a genuine non-bypass role and an administrator-owned RLS table."""
    credentials = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }
    admin = await AsyncpgConfig(connection_config=credentials).create_connection()
    suffix = uuid4().hex[:12]
    role = "sqlspec_asgi_" + suffix
    table = quote_identifier("sqlspec_asgi_rows_" + suffix)
    quoted_role = quote_identifier(role)
    role_sql = await admin.fetchval(
        "SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD %L', $1::text, $2::text)",
        role,
        "sqlspec-tenant-test",
    )
    await admin.execute(role_sql)
    try:
        await admin.execute(f"CREATE TABLE {table} (tenant_id text NOT NULL, value integer NOT NULL)")
        await admin.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        await admin.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        await admin.execute(
            f"CREATE POLICY tenant_policy ON {table} USING (tenant_id = current_setting('app.current_tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true))"
        )
        await admin.execute(f"GRANT SELECT, INSERT ON {table} TO {quoted_role}")
        await admin.executemany(f"INSERT INTO {table} VALUES ($1, $2)", [("a", 1), ("b", 2)])
        yield {**credentials, "user": role, "password": "sqlspec-tenant-test", "min_size": 1, "max_size": 1}, table
    finally:
        await admin.execute(f"DROP TABLE IF EXISTS {table}")
        await admin.execute(f"DROP ROLE {quoted_role}")
        await admin.close()


def _tenant_extractor(scope: Any) -> str | None:
    verified_users = {b"alice": "a", b"bob": "b"}
    return verified_users.get(dict(scope["headers"]).get(b"authorization", b""))


def _tenant_app(
    credentials: dict[str, Any], framework: str, mode: str, endpoint: Any
) -> tuple[Any, Any, AsyncpgConfig]:
    config = AsyncpgConfig(
        connection_config=credentials,
        driver_features={"tenant_strategy": "rls"},
        extension_config={
            framework: {
                "commit_mode": mode,
                "tenant_extractor": _tenant_extractor,
                "enable_correlation_middleware": False,
                "enable_sqlcommenter_middleware": False,
            }
        },
    )
    if framework == "fastapi":
        from fastapi import FastAPI

        from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin

        app = FastAPI()
        app.add_api_route("/", endpoint)
        plugin = FastAPIPlugin(config, app)
    else:
        app = Starlette(routes=[Route("/", endpoint)])
        plugin = SQLSpecPlugin(config, app)
    return app, plugin, config


@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
async def test_tenant_request_isolation_missing_identity_and_worker(tenant_database: Any, framework: str) -> None:
    credentials, table = tenant_database
    plugin: Any = None

    async def endpoint(request: Request) -> JSONResponse:
        session = await plugin.get_session_async(request)
        rows = await session.select(f"SELECT value FROM {table} ORDER BY value")
        return JSONResponse([row["value"] for row in rows])

    app, plugin, config = _tenant_app(credentials, framework, "autocommit", endpoint)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
        ) as client:
            for user, values in [("alice", [1]), ("bob", [2])]:
                response = await client.get("/", headers={"authorization": user})
                assert response.status_code == 200
                assert response.json() == values
            for headers in ({}, {"authorization": "evil", "x-tenant-id": "a"}):
                assert (await client.get("/", headers=headers)).status_code == 401
                assert (await client.get("/", headers={"authorization": "bob"})).json() == [2]

        async def worker() -> list[dict[str, Any]]:
            async with TenantContext.scope("a"), config.provide_session() as session:
                return await session.select(f"SELECT value FROM {table}")

        assert await asyncio.create_task(worker()) == [{"value": 1}]
        assert TenantContext.get() is None
        assert (await config.provide_pool()).get_idle_size() == 1


@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
@pytest.mark.parametrize("mode", ["manual", "autocommit"])
@pytest.mark.parametrize("failure", [None, "body", "background", "cancel", "send"])
async def test_tenant_streaming_real_driver_and_slot_ownership(
    tenant_database: Any, framework: str, mode: str, failure: str | None
) -> None:
    credentials, table = tenant_database
    first_sent = asyncio.Event()
    gate = asyncio.Event()
    inserted = asyncio.Event()
    finish = asyncio.Event()
    competitor_acquired = asyncio.Event()
    events: list[str] = []
    plugin: Any = None

    async def endpoint(request: Request) -> StreamingResponse:
        session = await plugin.get_session_async(request)

        async def body() -> AsyncIterator[bytes]:
            try:
                yield b"first"
                await gate.wait()
                assert await session.select_value("SELECT current_setting('app.current_tenant_id', true)") == "a"
                assert await session.select_value(f"SELECT value FROM {table}") == 1
                await session.begin()
                await session.execute(f"INSERT INTO {table} VALUES ($1, $2)", ("a", 99))
                inserted.set()
                await finish.wait()
                if failure == "body":
                    raise ValueError("body failure")
                yield b"last"
            finally:
                events.append("generator_exit")

        async def background() -> None:
            assert await session.select_value("SELECT current_setting('app.current_tenant_id', true)") == "a"
            events.append("background_query")
            if failure == "background":
                raise ValueError("background failure")

        return StreamingResponse(body(), background=BackgroundTask(background))

    app, plugin, config = _tenant_app(credentials, framework, mode, endpoint)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": [(b"authorization", b"alice")],
        "client": ("127.0.0.1", 123),
        "server": ("test", 80),
        "app": app,
    }
    request_received = False

    async def receive() -> Any:
        nonlocal request_received
        if not request_received:
            request_received = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    async def send(message: Any) -> None:
        if message["type"] == "http.response.body":
            if message.get("body") == b"first":
                first_sent.set()
            if message.get("body") == b"last" and failure == "send":
                raise OSError("send failure")
            if not message.get("more_body", False):
                events.append("final_sent")

    async def competitor() -> None:
        async with TenantContext.scope("b"), config.provide_session() as session:
            competitor_acquired.set()
            assert await session.select_value(f"SELECT value FROM {table}") == 2
            assert await session.select_value("SELECT current_setting('app.current_tenant_id', true)") == "b"

    async with app.router.lifespan_context(app):
        with TenantContext.scope("outer"):
            task = asyncio.create_task(app(scope, receive, send))
            await asyncio.wait_for(first_sent.wait(), 5)
            assert (await config.provide_pool()).get_idle_size() == 0
            waiter = asyncio.create_task(competitor())
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(competitor_acquired.wait(), 0.03)
            gate.set()
            await asyncio.wait_for(inserted.wait(), 5)
            if failure == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 5)
            else:
                finish.set()
                if failure is None:
                    await asyncio.wait_for(task, 5)
                else:
                    with pytest.raises((ValueError, OSError), match=f"{failure} failure"):
                        await asyncio.wait_for(task, 5)
            await asyncio.wait_for(waiter, 5)
            assert TenantContext.get_tenant_id() == "outer"
        assert events.count("generator_exit") == 1
        if failure is None:
            assert events.index("final_sent") < events.index("background_query")
        async with TenantContext.scope("a"), config.provide_session() as session:
            expected = 1 if failure is None and mode == "autocommit" else 0
            assert await session.select_value(f"SELECT count(*) FROM {table} WHERE value = $1", (99,)) == expected
        assert (await config.provide_pool()).get_idle_size() == 1
