"""ASGI request read/routing accessors with real SQLite configurations."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec import sign_write_pin
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver._routing import SyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import DeferredAsyncSession
from sqlspec.extensions.starlette import SQLSpecPlugin
from sqlspec.extensions.starlette.middleware import SQLSpecRequestLeaseMiddleware
from sqlspec.utils.routing import RoutingContext, RoutingEntry
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
    assert (
        sum(
            isinstance(middleware.cls, type) and issubclass(middleware.cls, SQLSpecRequestLeaseMiddleware)
            for middleware in app.user_middleware
        )
        == 1
    )
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
    assert (
        sum(
            isinstance(middleware.cls, type) and issubclass(middleware.cls, SQLSpecRequestLeaseMiddleware)
            for middleware in app.user_middleware
        )
        == 1
    )
    invalid = SqliteConfig(extension_config={"starlette": {"read_session_key": "db_session"}})
    with pytest.raises(ImproperConfigurationError, match="Duplicate"):
        SQLSpecPlugin(invalid, Starlette())


def test_starlette_safe_routing_defaults_signed_cookie_and_deferred_error(tmp_path: Path) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={
            "starlette": {
                "routing_session_key": "router",
                "write_pin_cookie": "wp",
                "routing_secret": "starlette-secret",
            }
        },
    )

    async def inspect_route(request: Request) -> JSONResponse:
        deferred = plugin.get_routing_session(request)
        assert isinstance(deferred, DeferredAsyncSession)
        with pytest.raises(ImproperConfigurationError, match="get_routing_session_async"):
            _ = deferred.primary_session
        origin = await deferred.select_value("SELECT origin FROM notes WHERE id=1")
        entry = RoutingContext.get_entry("shop") or RoutingEntry()
        owner_present = hasattr(request.state, "sqlspec_response_owner_db_connection")
        return JSONResponse({"origin": origin, "token": entry.causal_token, "owner": owner_present})

    app = Starlette(routes=[Route("/inspect", inspect_route)])
    plugin = SQLSpecPlugin(config, app)
    with TestClient(app) as client:
        assert client.get("/inspect", headers={"x-sqlspec-routing": "primary"}).json() == {
            "origin": "replica",
            "token": None,
            "owner": True,
        }
        assert client.get("/inspect", headers={"cookie": "wp=1"}).json() == {
            "origin": "replica",
            "token": None,
            "owner": True,
        }
        signed = sign_write_pin("starlette-secret", bind_key="shop", causal_token="lsn-12")
        assert client.get("/inspect", headers={"cookie": f"wp={signed}"}).json() == {
            "origin": "primary",
            "token": "lsn-12",
            "owner": True,
        }


def test_starlette_commit_failure_rolls_back_and_finalizes_remaining_configs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    primary_a, replica_a = seed_routing_databases(tmp_path / "a")
    primary_b, replica_b = seed_routing_databases(tmp_path / "b")
    config_a = SqliteConfig(
        connection_config={"database": primary_a},
        replicas=[{"database": replica_a}],
        bind_key="a",
        extension_config={
            "starlette": {
                "session_key": "a_session",
                "connection_key": "a_conn",
                "pool_key": "a_pool",
                "read_session_key": "a_read_session",
                "read_connection_key": "a_read_conn",
                "read_pool_key": "a_read_pool",
                "routing_session_key": "a_router",
                "commit_mode": "autocommit",
            }
        },
    )
    config_b = SqliteConfig(
        connection_config={"database": primary_b},
        replicas=[{"database": replica_b}],
        bind_key="b",
        extension_config={
            "starlette": {
                "session_key": "b_session",
                "connection_key": "b_conn",
                "pool_key": "b_pool",
                "read_session_key": "b_read_session",
                "read_connection_key": "b_read_conn",
                "read_pool_key": "b_read_pool",
                "routing_session_key": "b_router",
                "commit_mode": "autocommit",
            }
        },
    )
    rollbacks: list[str] = []
    orig_commit = SyncRoutingSession.commit
    orig_rollback = SyncRoutingSession.rollback

    def fail_first_commit(self: SyncRoutingSession) -> None:
        if self._config is config_a:
            raise RuntimeError("commit failed on a")
        orig_commit(self)

    def track_rollback(self: SyncRoutingSession) -> None:
        rollbacks.append(str(self._config.bind_key))
        orig_rollback(self)

    monkeypatch.setattr(SyncRoutingSession, "commit", fail_first_commit)
    monkeypatch.setattr(SyncRoutingSession, "rollback", track_rollback)

    async def write_both(request: Request) -> JSONResponse:
        router_a: Any = plugin.get_routing_session(request, "a_router")
        router_b: Any = plugin.get_routing_session(request, "b_router")
        router_a.execute("INSERT INTO notes VALUES (4,'a')")
        router_b.execute("INSERT INTO notes VALUES (4,'b')")
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/write", write_both, methods=["POST"])])
    plugin = SQLSpecPlugin([config_a, config_b], app)
    with TestClient(app, raise_server_exceptions=True) as client, pytest.raises(RuntimeError, match="commit failed"):
        client.post("/write")
    assert rollbacks == ["a"]
    with config_b.provide_session() as verify_b:
        assert verify_b.select_value("SELECT origin FROM notes WHERE id=4") == "b"
    config_a.close_pool()
    config_b.close_pool()
