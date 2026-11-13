"""ASGI request read/routing accessors with real SQLite configurations."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import DeferredAsyncSession
from sqlspec.extensions.starlette import SQLSpecPlugin
from sqlspec.extensions.starlette.middleware import SQLSpecRequestLeaseMiddleware
from sqlspec.utils.routing import RoutingContext
from tests.unit.driver.test_sync_routing_session import seed_routing_databases


@pytest.mark.parametrize("async_config", [False, True])
def test_read_and_routing_accessors_select_and_pin(tmp_path: Path, async_config: bool) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config_type = AiosqliteConfig if async_config else SqliteConfig
    config = config_type(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"starlette": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )

    async def read(request: Request) -> JSONResponse:
        if async_config:
            session = plugin.get_read_session(request)
            assert isinstance(session, DeferredAsyncSession)
            with pytest.raises(ImproperConfigurationError):
                plugin.get_read_connection(request)
            one, two = await asyncio.gather(
                plugin.get_read_session_async(request), plugin.get_read_session_async(request)
            )
            assert one is two
            origin = await session.select_value("SELECT origin FROM notes WHERE id=1")
        else:
            origin = plugin.get_read_session(request).select_value("SELECT origin FROM notes WHERE id=1")
        return JSONResponse({"origin": origin})

    async def route(request: Request) -> JSONResponse:
        facade = plugin.get_routing_session(request)
        if async_config:
            assert isinstance(facade, DeferredAsyncSession)
            before = await facade.select_value("SELECT origin FROM notes WHERE id=1")
            real = await plugin.get_routing_session_async(request)
            assert plugin.get_routing_session(request) is real
            await facade.execute("INSERT INTO notes VALUES (4,'new')")
            after = await facade.select_value("SELECT origin FROM notes WHERE id=1")
        else:
            before = facade.select_value("SELECT origin FROM notes WHERE id=1")
            facade.execute("INSERT INTO notes VALUES (4,'new')")
            after = facade.select_value("SELECT origin FROM notes WHERE id=1")
        return JSONResponse({"before": before, "after": after})

    app = Starlette(routes=[Route("/read", read), Route("/route", route)])
    plugin = SQLSpecPlugin(config, app)
    assert sum(middleware.cls is SQLSpecRequestLeaseMiddleware for middleware in app.user_middleware) == 1
    with TestClient(app) as client:
        assert client.get("/read").json() == {"origin": "replica"}
        assert client.get("/route").json() == {"before": "replica", "after": "primary"}
        assert client.get("/read").json() == {"origin": "replica"}
    assert RoutingContext.current() is None


def test_streaming_and_background_can_use_read_lease(tmp_path: Path) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(connection_config={"database": primary}, replicas=[{"database": replica}])
    events: list[str] = []

    async def endpoint(request: Request) -> StreamingResponse:
        async def body() -> AsyncIterator[str]:
            session = await plugin.get_read_session_async(request)
            yield await session.select_value("SELECT origin FROM notes WHERE id=1")
            await asyncio.sleep(0)
            yield await session.select_value("SELECT origin FROM notes WHERE id=2")

        async def background() -> None:
            session = await plugin.get_read_session_async(request)
            events.append(await session.select_value("SELECT origin FROM notes WHERE id=3"))

        return StreamingResponse(body(), background=BackgroundTask(background))

    app = Starlette(routes=[Route("/", endpoint)])
    plugin = SQLSpecPlugin(config, app)
    with TestClient(app) as client:
        assert client.get("/").text == "replicareplica"
        assert events == ["replica"]


def test_multi_config_lease_layer_and_cross_key_collision() -> None:
    configs = [
        SqliteConfig(
            bind_key=name,
            extension_config={
                "starlette": {
                    "session_key": f"{name}_session",
                    "connection_key": f"{name}_connection",
                    "pool_key": f"{name}_pool",
                }
            },
        )
        for name in ("a", "b", "c")
    ]
    app = Starlette()
    SQLSpecPlugin(configs, app)
    assert sum(middleware.cls is SQLSpecRequestLeaseMiddleware for middleware in app.user_middleware) == 1
    invalid = SqliteConfig(extension_config={"starlette": {"read_session_key": "db_session"}})
    with pytest.raises(ImproperConfigurationError, match="Duplicate"):
        SQLSpecPlugin(invalid, Starlette())
