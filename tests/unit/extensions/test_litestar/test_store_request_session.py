"""SQLSpec session stores run on the request's database session while a request holds one."""

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, Request, get
from litestar.di import NamedDependency
from litestar.exceptions import HTTPException
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.testing import AsyncTestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.litestar import SqliteStore
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.extensions.litestar._utils import borrowed_request_session, clear_request_session, publish_request_session

pytestmark = pytest.mark.anyio

TABLE = "litestar_session"
KINDS = ["aiosqlite", "sqlite"]


def prepare_database(tmp_path: Path) -> str:
    database = str(tmp_path / "sessions.db")
    connection = sqlite3.connect(database)
    connection.execute(f"CREATE TABLE {TABLE} (session_id TEXT PRIMARY KEY, data BLOB NOT NULL, expires_at REAL)")
    connection.commit()
    connection.close()
    return database


class Harness:
    """App, store, and acquisition counter for one adapter."""

    def __init__(self, kind: str, tmp_path: Path) -> None:
        self.database = prepare_database(tmp_path)
        self.acquisitions: list[int] = []
        self.events: list[str] = []
        outer = self

        extension_config = {"litestar": {"session_table": TABLE, "commit_mode": "autocommit"}}
        if kind == "aiosqlite":

            class CountingAiosqlite(AiosqliteConfig):
                def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
                    outer.acquisitions.append(1)
                    return super().provide_connection(*args, **kwargs)

            self.config: Any = CountingAiosqlite(
                connection_config={"database": self.database, "pool_size": 1, "connect_timeout": 3.0},
                extension_config=extension_config,
            )
            self.store: Any = AiosqliteStore(self.config)
        else:

            class CountingSqlite(SqliteConfig):
                def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
                    outer.acquisitions.append(1)
                    return super().provide_connection(*args, **kwargs)

            self.config = CountingSqlite(
                connection_config={"database": self.database}, extension_config=extension_config
            )
            self.store = SqliteStore(self.config)

        plugin = SQLSpecPlugin(self.config)

        @get("/")
        async def touch(request: Request[Any, Any, Any], db_session: NamedDependency[Any]) -> dict[str, bool]:
            outer.events.append("handler")
            await _maybe_await(db_session.select_value("SELECT 1"))
            request.session["x"] = 1
            return {"ok": True}

        @get("/fail")
        async def fail(request: Request[Any, Any, Any], db_session: NamedDependency[Any]) -> dict[str, bool]:
            await _maybe_await(db_session.select_value("SELECT 1"))
            request.session["x"] = 1
            raise HTTPException(status_code=400, detail="rejected")

        self.app = Litestar(
            route_handlers=[touch, fail],
            plugins=[plugin],
            middleware=[ServerSideSessionConfig().middleware],
            stores={"sessions": self.store},
        )

    def row_count(self) -> int:
        connection = sqlite3.connect(self.database)
        try:
            return int(connection.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0])
        finally:
            connection.close()


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


@pytest.fixture(params=KINDS)
def harness(request: pytest.FixtureRequest, tmp_path: Path) -> Harness:
    return Harness(str(request.param), tmp_path)


async def test_concurrent_requests_single_connection_pool_complete(harness: Harness) -> None:
    async with AsyncTestClient(harness.app) as client:
        responses = await asyncio.wait_for(
            asyncio.gather(*(client.get("/", headers={"Cookie": ""}) for _ in range(8))), timeout=20
        )
    assert [response.status_code for response in responses] == [200] * 8
    assert len(harness.acquisitions) == 8


async def test_session_write_commits_with_successful_request(harness: Harness) -> None:
    async with AsyncTestClient(harness.app) as client:
        response = await client.get("/")
        session_id = client.cookies.get("session")
    assert response.status_code == 200
    assert session_id is not None
    assert harness.row_count() == 1
    assert await harness.store.get(session_id) is not None


async def test_session_write_rolls_back_with_failing_request(harness: Harness) -> None:
    async with AsyncTestClient(harness.app) as client:
        response = await client.get("/fail")
    assert response.status_code == 400
    assert harness.row_count() == 0


async def test_store_outside_request_opens_own_session(harness: Harness) -> None:
    await harness.store.set("k", b"v")
    assert await harness.store.get("k") == b"v"
    assert harness.row_count() == 1


async def test_store_write_precedes_finalize(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.extensions.litestar import handlers, plugin

    original_set = type(harness.store).set
    original_finalize = handlers._finalize_request_session  # pyright: ignore[reportPrivateUsage]

    async def recording_set(self: Any, *args: Any, **kwargs: Any) -> Any:
        harness.events.append("store-set")
        return await original_set(self, *args, **kwargs)

    async def recording_finalize(*args: Any, **kwargs: Any) -> Any:
        harness.events.append("sqlspec-finalize")
        return await original_finalize(*args, **kwargs)

    monkeypatch.setattr(type(harness.store), "set", recording_set)
    monkeypatch.setattr(handlers, "_finalize_request_session", recording_finalize)
    monkeypatch.setattr(plugin, "_finalize_request_session", recording_finalize)
    async with AsyncTestClient(harness.app) as client:
        await client.get("/")
    assert harness.events == ["handler", "store-set", "sqlspec-finalize"]


async def test_deferred_request_session_is_borrowed(tmp_path: Path) -> None:
    database = prepare_database(tmp_path)
    acquisitions: list[int] = []

    class CountingAiosqlite(AiosqliteConfig):
        def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
            acquisitions.append(1)
            return super().provide_connection(*args, **kwargs)

    config = CountingAiosqlite(
        connection_config={"database": database, "pool_size": 1, "connect_timeout": 3.0},
        extension_config={"litestar": {"session_table": TABLE, "commit_mode": "autocommit"}},
    )
    store = AiosqliteStore(config)
    plugin = SQLSpecPlugin(config)

    @get("/")
    async def deferred(request: Request[Any, Any, Any]) -> dict[str, bool]:
        session = plugin.provide_request_session(config, request.app.state, request.scope)
        await session.select_value("SELECT 1")
        request.session["x"] = 1
        return {"ok": True}

    app = Litestar(
        route_handlers=[deferred],
        plugins=[plugin],
        middleware=[ServerSideSessionConfig().middleware],
        stores={"sessions": store},
    )
    async with AsyncTestClient(app) as client:
        response = await asyncio.wait_for(client.get("/"), timeout=20)
    assert response.status_code == 200
    assert len(acquisitions) == 1


def test_borrowed_session_matches_config_identity() -> None:
    first, second = object(), object()
    session = object()
    publish_request_session(first, session)
    assert borrowed_request_session(first) is session
    assert borrowed_request_session(second) is None
    clear_request_session(first)
    assert borrowed_request_session(first) is None
