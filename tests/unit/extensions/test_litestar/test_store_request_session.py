"""SQLSpec session stores run on the request's database session while a request holds one."""

import asyncio
import sqlite3
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import anyio
import pytest
from litestar import Litestar, Request, get
from litestar.di import NamedDependency, Provide
from litestar.exceptions import HTTPException
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.testing import AsyncTestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.litestar import SqliteStore
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.extensions.litestar._utils import (
    clear_request_session,
    publish_request_session,
    request_session_scope,
    resolve_store_lease,
)

pytestmark = pytest.mark.anyio

TABLE = "litestar_session"
KINDS = ["aiosqlite", "sqlite"]


def prepare_database(tmp_path: Path) -> str:
    database = str(tmp_path / "sessions.db")
    connection = sqlite3.connect(database)
    connection.execute(f"CREATE TABLE {TABLE} (session_id TEXT PRIMARY KEY, data BLOB NOT NULL, expires_at REAL)")
    connection.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)")
    connection.commit()
    connection.close()
    return database


class Harness:
    """App, store, and acquisition counter for one adapter."""

    def __init__(self, kind: str, tmp_path: Path, commit_mode: str = "autocommit") -> None:
        self.database = prepare_database(tmp_path)
        self.acquisitions: list[int] = []
        self.events: list[str] = []
        self.release = threading.Event()
        self.background_done = threading.Event()
        self.background_errors: list[BaseException] = []
        outer = self

        extension_config: Any = {"litestar": {"session_table": TABLE, "commit_mode": commit_mode}}
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

        async def provide_settings() -> int:
            return 1

        async def provide_extra(settings: NamedDependency[int]) -> int:
            return settings

        @get("/sibling-fail", dependencies={"settings": Provide(provide_settings), "extra": Provide(provide_extra)})
        async def sibling_fail(
            request: Request[Any, Any, Any], db_session: NamedDependency[Any], extra: NamedDependency[int]
        ) -> dict[str, bool]:
            await _maybe_await(db_session.select_value("SELECT 1"))
            request.session["x"] = extra
            raise HTTPException(status_code=400, detail="rejected")

        @get("/commit")
        async def commit(request: Request[Any, Any, Any], db_session: NamedDependency[Any]) -> dict[str, bool]:
            await _maybe_await(db_session.execute("INSERT INTO notes (id) VALUES (1)"))
            await _maybe_await(db_session.commit())
            request.session["x"] = 1
            return {"ok": True}

        @get("/uncommitted")
        async def uncommitted(request: Request[Any, Any, Any], db_session: NamedDependency[Any]) -> dict[str, bool]:
            await _maybe_await(db_session.execute("INSERT INTO notes (id) VALUES (1)"))
            request.session["x"] = 1
            return {"ok": True}

        @get("/spawn-fail")
        async def spawn_fail(request: Request[Any, Any, Any], db_session: NamedDependency[Any]) -> dict[str, bool]:
            await _maybe_await(db_session.select_value("SELECT 1"))

            async def write_after_release() -> None:
                try:
                    await anyio.to_thread.run_sync(outer.release.wait, 20)
                    await outer.store.set("background", b"kept")
                except BaseException as exc:
                    outer.background_errors.append(exc)
                finally:
                    outer.background_done.set()

            background.add(asyncio.create_task(write_after_release()))
            request.session["x"] = 1
            raise HTTPException(status_code=400, detail="rejected")

        background: set[asyncio.Task[None]] = set()
        route_handlers: list[Any] = [touch, fail, sibling_fail, commit, uncommitted, spawn_fail]
        if kind == "sqlite":

            @get("/sync", sync_to_thread=True)
            def sync_touch(request: Request[Any, Any, Any]) -> dict[str, bool]:
                session = plugin.provide_db_session_sync(outer.config, request.app.state, request.scope)
                session.select_value("SELECT 1")
                request.session["x"] = 1
                return {"ok": True}

            @get("/sync-fail", sync_to_thread=True)
            def sync_fail(request: Request[Any, Any, Any]) -> dict[str, bool]:
                session = plugin.provide_db_session_sync(outer.config, request.app.state, request.scope)
                session.select_value("SELECT 1")
                request.session["x"] = 1
                raise HTTPException(status_code=400, detail="rejected")

            route_handlers.extend([sync_touch, sync_fail])

        self.app = Litestar(
            route_handlers=route_handlers,
            plugins=[plugin],
            middleware=[ServerSideSessionConfig().middleware],
            stores={"sessions": self.store},
        )

    def row_count(self, table: str = TABLE) -> int:
        connection = sqlite3.connect(self.database)
        try:
            return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        finally:
            connection.close()


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


@pytest.fixture(params=KINDS)
def harness(request: pytest.FixtureRequest, tmp_path: Path) -> Harness:
    return Harness(str(request.param), tmp_path)


@pytest.fixture(params=KINDS)
def manual_harness(request: pytest.FixtureRequest, tmp_path: Path) -> Harness:
    return Harness(str(request.param), tmp_path, commit_mode="manual")


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
    original_finalize = handlers._finalize_request_transaction  # pyright: ignore[reportPrivateUsage]

    async def recording_set(self: Any, *args: Any, **kwargs: Any) -> Any:
        harness.events.append("store-set")
        return await original_set(self, *args, **kwargs)

    async def recording_finalize(*args: Any, **kwargs: Any) -> Any:
        harness.events.append("sqlspec-finalize")
        return await original_finalize(*args, **kwargs)

    monkeypatch.setattr(type(harness.store), "set", recording_set)
    monkeypatch.setattr(handlers, "_finalize_request_transaction", recording_finalize)
    monkeypatch.setattr(plugin, "_finalize_request_transaction", recording_finalize)
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
        session = plugin.provide_db_session(config, request.app.state, request.scope)
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


async def test_sibling_dependency_failed_request_rolls_back_session_write(harness: Harness) -> None:
    async with AsyncTestClient(harness.app) as client:
        response = await asyncio.wait_for(client.get("/sibling-fail"), timeout=20)
    assert response.status_code == 400
    assert harness.row_count() == 0
    assert len(harness.acquisitions) == 1


@pytest.mark.parametrize("path,status,rows", [("/sync", 200, 1), ("/sync-fail", 400, 0)])
async def test_sync_handler_session_write_joins_request(tmp_path: Path, path: str, status: int, rows: int) -> None:
    harness = Harness("sqlite", tmp_path)
    async with AsyncTestClient(harness.app) as client:
        response = await asyncio.wait_for(client.get(path), timeout=20)
    assert response.status_code == status
    assert harness.row_count() == rows
    assert len(harness.acquisitions) == 1


async def test_manual_mode_store_commits_its_own_transaction(manual_harness: Harness) -> None:
    async with AsyncTestClient(manual_harness.app) as client:
        response = await asyncio.wait_for(client.get("/commit"), timeout=20)
        session_id = client.cookies.get("session")
    assert response.status_code == 200
    assert manual_harness.row_count() == 1
    assert manual_harness.row_count("notes") == 1
    assert len(manual_harness.acquisitions) == 1
    assert session_id is not None
    assert await manual_harness.store.get(session_id) is not None


async def test_manual_mode_store_write_rolls_back_with_uncommitted_work(manual_harness: Harness) -> None:
    async with AsyncTestClient(manual_harness.app) as client:
        response = await asyncio.wait_for(client.get("/uncommitted"), timeout=20)
    assert response.status_code == 200
    assert manual_harness.row_count() == 0
    assert manual_harness.row_count("notes") == 0


async def test_task_spawned_during_failed_request_writes_through_own_session(harness: Harness) -> None:
    async with AsyncTestClient(harness.app) as client:
        response = await asyncio.wait_for(client.get("/spawn-fail"), timeout=20)
        harness.release.set()
        assert await anyio.to_thread.run_sync(harness.background_done.wait, 20)
    assert response.status_code == 400
    assert harness.background_errors == []
    assert harness.row_count() == 1
    assert await harness.store.get("background") == b"kept"
    assert len(harness.acquisitions) >= 2


async def test_store_lease_follows_config_identity_and_owner_task() -> None:
    first, second, session = object(), object(), object()
    with request_session_scope():
        publish_request_session(first, session, manual=True)
        lease = resolve_store_lease(first)
        assert (lease.session, lease.manual, lease.discard) == (session, True, False)
        assert resolve_store_lease(second).session is None

        async def child_lease() -> Any:
            return resolve_store_lease(first)

        child = await asyncio.create_task(child_lease())
        assert (child.session, child.discard) == (None, False)

        clear_request_session(session, failed=True)
        assert resolve_store_lease(first).discard is True
        child_after = await asyncio.create_task(child_lease())
        assert child_after.discard is False
    assert resolve_store_lease(first).session is None


def _duckdb_store(tmp_path: Path) -> "tuple[Any, Any]":
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.litestar import DuckDBStore

    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "sessions.duckdb")},
        extension_config={"litestar": {"session_table": TABLE, "commit_mode": "autocommit"}},
    )
    return config, DuckDBStore(config)


def _duckdb_row_count(config: Any) -> int:
    with config.provide_session() as session:
        return int(session.select_value(f"SELECT COUNT(*) FROM {TABLE}"))


async def test_duckdb_failed_request_discards_session_write(tmp_path: Path) -> None:
    config, store = _duckdb_store(tmp_path)
    await store.create_table()
    plugin = SQLSpecPlugin(config)

    @get("/fail")
    async def fail(request: Request[Any, Any, Any], db_session: NamedDependency[Any]) -> dict[str, bool]:
        db_session.select_value("SELECT 1")
        request.session["x"] = 1
        raise HTTPException(status_code=400, detail="rejected")

    app = Litestar(
        route_handlers=[fail],
        plugins=[plugin],
        middleware=[ServerSideSessionConfig().middleware],
        stores={"sessions": store},
    )
    async with AsyncTestClient(app) as client:
        response = await asyncio.wait_for(client.get("/fail"), timeout=20)
    assert response.status_code == 400
    assert _duckdb_row_count(config) == 0


async def test_duckdb_store_write_after_failed_release_is_discarded(tmp_path: Path) -> None:
    config, store = _duckdb_store(tmp_path)
    await store.create_table()
    request_session = object()
    with request_session_scope():
        publish_request_session(config, request_session)
        clear_request_session(request_session, failed=True)
        await store.set("discarded", b"v")
    await store.set("kept", b"v")
    assert _duckdb_row_count(config) == 1
    assert await store.get("kept") == b"v"


class _AsyncpgTransaction:
    def __init__(self, connection: "_AsyncpgConnection") -> None:
        self._connection = connection

    async def start(self) -> None:
        self._connection.events.append("begin")
        self._connection.active = True

    async def commit(self) -> None:
        self._connection.events.append("commit")
        self._connection.active = False

    async def rollback(self) -> None:
        self._connection.events.append("rollback")
        self._connection.active = False


class _AsyncpgConnection:
    """Autocommit connection that records statements and transaction boundaries."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.active = False

    async def execute(self, sql: str, *_args: Any) -> str:
        self.events.append(sql.split()[0].upper())
        return "INSERT 0 1"

    def transaction(self) -> _AsyncpgTransaction:
        return _AsyncpgTransaction(self)

    def is_in_transaction(self) -> bool:
        return self.active


async def test_asyncpg_store_write_after_failed_release_runs_in_rolled_back_transaction() -> None:
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
    from sqlspec.adapters.asyncpg.litestar import AsyncpgStore

    connection = _AsyncpgConnection()

    class FakeAsyncpgConfig(AsyncpgConfig):
        @asynccontextmanager
        async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
            yield connection

        @asynccontextmanager
        async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:  # pyright: ignore[reportIncompatibleMethodOverride]
            yield AsyncpgDriver(cast("Any", connection))

    config = FakeAsyncpgConfig(extension_config={"litestar": {"session_table": TABLE}})
    store = AsyncpgStore(config)
    request_session = object()
    with request_session_scope():
        publish_request_session(config, request_session)
        clear_request_session(request_session, failed=True)
        await store.set("discarded", b"v")
    assert connection.events == ["begin", "INSERT", "rollback"]
