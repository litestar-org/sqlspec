"""Integration tests for Litestar exception forwarding and commit mode handling."""

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from litestar import Litestar, Request, get
from litestar.di import NamedDependency
from litestar.response import Response
from litestar.testing import TestClient

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = pytest.mark.xdist_group("sqlite")


def _setup_sqlite_db(db_path: Path) -> None:
    """Create test table for SQLite integration tests.

    Args:
        db_path: Path to the SQLite database file.
    """
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE items (name TEXT NOT NULL)")


def _read_sqlite_rows(db_path: Path) -> list[str]:
    """Read all item names from the SQLite database.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        List of item names in insertion order.
    """
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.execute("SELECT name FROM items ORDER BY rowid")
        return [row[0] for row in cursor.fetchall()]


def test_sqlite_autocommit_commit_and_rollback_by_status(tmp_path: Path) -> None:
    """Autocommit mode commits 200, rolls back 400, and rolls back 500."""
    db_path = tmp_path / "autocommit.db"
    _setup_sqlite_db(db_path)

    config = SqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"litestar": {"commit_mode": "autocommit"}}
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/ok")
    def ok_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> dict[str, str]:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("good",))
        return {"status": "ok"}

    @get("/bad")
    def bad_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> Response[dict[str, str]]:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("bad",))
        return Response({"status": "bad"}, status_code=400)

    @get("/raise")
    def raise_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> None:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("ugly",))
        raise RuntimeError("failed in handler")

    app = Litestar(route_handlers=[ok_route, bad_route, raise_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res_ok = client.get("/ok")
        res_bad = client.get("/bad")
        res_raise = client.get("/raise")

    assert res_ok.status_code == 200
    assert res_bad.status_code == 400
    assert res_raise.status_code == 500
    assert _read_sqlite_rows(db_path) == ["good"]


def test_sqlite_manual_commits_clean_and_rolls_back_exceptions(tmp_path: Path) -> None:
    """Manual mode commits on clean 200 and 400, and rolls back on unhandled exception."""
    db_path = tmp_path / "manual.db"
    _setup_sqlite_db(db_path)

    config = SqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"litestar": {"commit_mode": "manual"}}
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/ok")
    def ok_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> dict[str, str]:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("good",))
        return {"status": "ok"}

    @get("/bad")
    def bad_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> Response[dict[str, str]]:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("bad",))
        return Response({"status": "bad"}, status_code=400)

    @get("/raise")
    def raise_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> None:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("ugly",))
        raise RuntimeError("failed in handler")

    app = Litestar(route_handlers=[ok_route, bad_route, raise_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res_ok = client.get("/ok")
        res_bad = client.get("/bad")
        res_raise = client.get("/raise")

    assert res_ok.status_code == 200
    assert res_bad.status_code == 400
    assert res_raise.status_code == 500
    assert _read_sqlite_rows(db_path) == ["good", "bad"]


def test_sqlite_extra_commit_statuses_persists_row(tmp_path: Path) -> None:
    """Autocommit mode with extra_commit_statuses={400} commits 400 responses."""
    db_path = tmp_path / "extra_commit.db"
    _setup_sqlite_db(db_path)

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        extension_config={"litestar": {"commit_mode": "autocommit", "extra_commit_statuses": {400}}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/bad")
    def bad_route(db_session: NamedDependency[SyncDriverAdapterBase]) -> Response[dict[str, str]]:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("bad_committed",))
        return Response({"status": "bad"}, status_code=400)

    app = Litestar(route_handlers=[bad_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res_bad = client.get("/bad")

    assert res_bad.status_code == 400
    assert _read_sqlite_rows(db_path) == ["bad_committed"]


def test_sqlite_on_demand_session_rolls_back_on_exception(tmp_path: Path) -> None:
    """On-demand session in manual mode rolls back when handler raises."""
    db_path = tmp_path / "on_demand.db"
    _setup_sqlite_db(db_path)

    config = SqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"litestar": {"commit_mode": "manual"}}
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/raise")
    def raise_route(request: Request) -> None:
        p: SQLSpecPlugin = request.app.state.sqlspec
        session = p.provide_request_session_sync("db_connection", request.app.state, request.scope)
        session.execute("INSERT INTO items (name) VALUES (?)", ("on_demand_fail",))
        raise RuntimeError("on demand error")

    app = Litestar(route_handlers=[raise_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res = client.get("/raise")

    assert res.status_code == 500
    assert _read_sqlite_rows(db_path) == []


def test_psycopg_injected_connection_rolls_back_on_raise(postgres_service: "PostgresService") -> None:
    """Psycopg injected connection in manual mode rolls back on exception and next request succeeds."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    import psycopg

    from sqlspec.adapters.psycopg import PsycopgAsyncConfig
    from tests.integration.fixtures.postgres import _postgres_conninfo

    conninfo = _postgres_conninfo(postgres_service)
    with psycopg.connect(conninfo, autocommit=True) as p_conn:
        with p_conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS litestar_psycopg_items (name VARCHAR(64) NOT NULL);")
            cur.execute("DELETE FROM litestar_psycopg_items;")

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 4},
        extension_config={"litestar": {"commit_mode": "manual"}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/raise")
    async def raise_route(db_connection: NamedDependency[Any]) -> None:
        await db_connection.execute("INSERT INTO litestar_psycopg_items (name) VALUES (%s)", ("failing",))
        raise RuntimeError("psycopg handler crash")

    @get("/ok")
    async def ok_route(db_connection: NamedDependency[Any]) -> dict[str, str]:
        await db_connection.execute("INSERT INTO litestar_psycopg_items (name) VALUES (%s)", ("healthy",))
        return {"status": "ok"}

    app = Litestar(route_handlers=[raise_route, ok_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res_raise = client.get("/raise")
        res_ok = client.get("/ok")

    assert res_raise.status_code == 500
    assert res_ok.status_code == 200

    with psycopg.connect(conninfo, autocommit=True) as p_conn:
        with p_conn.cursor() as cur:
            cur.execute("SELECT name FROM litestar_psycopg_items ORDER BY name;")
            rows = [row[0] for row in cur.fetchall()]

    assert rows == ["healthy"]


def test_psycopg_injected_connection_clean_return_commits(postgres_service: "PostgresService") -> None:
    """Psycopg injected connection in manual mode commits uncommitted work on clean return."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig
    from tests.integration.fixtures.postgres import _postgres_conninfo

    conninfo = _postgres_conninfo(postgres_service)
    import psycopg

    with psycopg.connect(conninfo, autocommit=True) as p_conn:
        with p_conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS litestar_psycopg_clean (name VARCHAR(64) NOT NULL);")
            cur.execute("DELETE FROM litestar_psycopg_clean;")

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 4},
        extension_config={"litestar": {"commit_mode": "manual"}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/clean")
    async def clean_route(db_connection: NamedDependency[Any]) -> dict[str, str]:
        await db_connection.execute("INSERT INTO litestar_psycopg_clean (name) VALUES (%s)", ("clean_work",))
        return {"status": "clean"}

    app = Litestar(route_handlers=[clean_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res = client.get("/clean")

    assert res.status_code == 200

    with psycopg.connect(conninfo, autocommit=True) as p_conn:
        with p_conn.cursor() as cur:
            cur.execute("SELECT name FROM litestar_psycopg_clean ORDER BY name;")
            rows = [row[0] for row in cur.fetchall()]

    assert rows == ["clean_work"]


def test_litestar_autocommit_rolls_back_when_exception_handler_returns_200(tmp_path: Path) -> None:
    """Autocommit rolls back uncommitted writes when a Litestar exception handler returns HTTP 200."""

    class FallbackError(Exception):
        pass

    def fallback_handler(_request: Request[Any, Any, Any], _exc: FallbackError) -> Response[dict[str, bool]]:
        return Response({"fallback": True}, status_code=200)

    db_path = tmp_path / "mapped_200.db"
    _setup_sqlite_db(db_path)

    config = SqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"litestar": {"commit_mode": "autocommit"}}
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/injected-fail")
    def injected_fail(db_session: NamedDependency[SyncDriverAdapterBase]) -> dict[str, str]:
        db_session.execute("INSERT INTO items (name) VALUES (?)", ("injected_partial",))
        raise FallbackError("abort injected")

    @get("/ondemand-fail")
    def ondemand_fail(request: Request[Any, Any, Any]) -> dict[str, str]:
        p: SQLSpecPlugin = request.app.state.sqlspec
        session = p.provide_request_session_sync("db_connection", request.app.state, request.scope)
        session.execute("INSERT INTO items (name) VALUES (?)", ("ondemand_partial",))
        raise FallbackError("abort ondemand")

    app = Litestar(
        route_handlers=[injected_fail, ondemand_fail],
        plugins=[plugin],
        exception_handlers={FallbackError: fallback_handler},
    )

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res_inj = client.get("/injected-fail")
        res_ond = client.get("/ondemand-fail")

    assert res_inj.status_code == 200
    assert res_inj.json() == {"fallback": True}
    assert res_ond.status_code == 200
    assert res_ond.json() == {"fallback": True}
    assert _read_sqlite_rows(db_path) == []


def test_litestar_pre_response_cancelled_error_after_handover_releases_pool(tmp_path: Path) -> None:
    """Pre-response.start CancelledError after DI handover rolls back aiosqlite and releases single-slot pool."""
    import asyncio
    import contextlib

    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.driver import AsyncDriverAdapterBase

    db_path = tmp_path / "cancel_handover.db"
    _setup_sqlite_db(db_path)

    config = AiosqliteConfig(
        connection_config={"database": str(db_path), "pool_size": 1, "min_size": 1, "connect_timeout": 0.5},
        extension_config={"litestar": {"commit_mode": "manual"}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    class _CancellingPreStartResponse(Response[dict[str, str]]):
        def to_asgi_response(self, *args: Any, **kwargs: Any) -> Any:
            _ = (args, kwargs)

            async def cancelling_asgi_app(scope: Any, receive: Any, send: Any) -> None:
                _ = (scope, receive, send)
                raise asyncio.CancelledError()

            return cancelling_asgi_app

    @get("/abort")
    async def abort_route(db_session: NamedDependency[AsyncDriverAdapterBase]) -> Response[dict[str, str]]:
        await db_session.execute("INSERT INTO items (name) VALUES (:name)", name="aborted_write")
        return _CancellingPreStartResponse({"status": "aborting"})

    @get("/healthy")
    async def healthy_route(db_session: NamedDependency[AsyncDriverAdapterBase]) -> dict[str, str]:
        await db_session.execute("INSERT INTO items (name) VALUES (:name)", name="healthy_write")
        return {"status": "ok"}

    app = Litestar(route_handlers=[abort_route, healthy_route], plugins=[plugin])

    with TestClient(app=app, raise_server_exceptions=False) as client:
        with contextlib.suppress(BaseException):
            client.get("/abort")
        res_ok = client.get("/healthy")

    assert res_ok.status_code == 200
    assert _read_sqlite_rows(db_path) == ["healthy_write"]


def test_litestar_streaming_post_response_start_on_demand_session_releases_pool(tmp_path: Path) -> None:
    """On-demand session acquired inside a Stream response commits and releases single-slot pool."""
    from collections.abc import AsyncGenerator

    from litestar.response import Stream

    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    db_path = tmp_path / "streaming_ondemand.db"
    _setup_sqlite_db(db_path)

    config = AiosqliteConfig(
        connection_config={"database": str(db_path), "pool_size": 1, "min_size": 1, "connect_timeout": 0.5},
        extension_config={"litestar": {"commit_mode": "autocommit"}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/stream")
    def stream_route(request: Request[Any, Any, Any]) -> Stream:
        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"chunk-1:"
            p: SQLSpecPlugin = request.app.state.sqlspec
            session = await p.provide_request_session_async("db_connection", request.app.state, request.scope)
            await session.execute("INSERT INTO items (name) VALUES (:name)", name="stream_row")
            yield b"chunk-2"

        return Stream(iterator())

    @get("/verify")
    async def verify_route(request: Request[Any, Any, Any]) -> dict[str, list[str]]:
        p: SQLSpecPlugin = request.app.state.sqlspec
        session = await p.provide_request_session_async("db_connection", request.app.state, request.scope)
        result = await session.execute("SELECT name FROM items ORDER BY rowid")
        return {"rows": [row["name"] for row in result.all()]}

    app = Litestar(route_handlers=[stream_route, verify_route], plugins=[plugin])

    with TestClient(app=app, raise_server_exceptions=False) as client:
        res_stream = client.get("/stream")
        res_verify = client.get("/verify")

    assert res_stream.status_code == 200
    assert res_stream.text == "chunk-1:chunk-2"
    assert res_verify.status_code == 200
    assert res_verify.json() == {"rows": ["stream_row"]}


def test_litestar_autocommit_deferred_fk_violation_forwards_commit_error_without_double_commit(tmp_path: Path) -> None:
    """Deferred FK violation during autocommit COMMIT rolls back once without a second COMMIT on exit."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.driver import AsyncDriverAdapterBase

    db_path = tmp_path / "deferred_fk.db"
    with sqlite3.connect(str(db_path)) as setup_conn:
        setup_conn.execute("PRAGMA foreign_keys = ON")
        setup_conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        setup_conn.execute(
            "CREATE TABLE children ("
            "id INTEGER PRIMARY KEY, "
            "parent_id INTEGER NOT NULL REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED"
            ")"
        )

    traced_statements: list[str] = []
    config = AiosqliteConfig(
        connection_config={
            "database": str(db_path),
            "pool_size": 1,
            "min_size": 1,
            "connect_timeout": 0.5,
            "enable_foreign_keys": True,
        },
        driver_features={"trace_callback": traced_statements.append},
        extension_config={"litestar": {"commit_mode": "autocommit"}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)

    @get("/violate")
    async def violate_route(db_session: NamedDependency[AsyncDriverAdapterBase]) -> dict[str, str]:
        await db_session.execute("INSERT INTO children (id, parent_id) VALUES (:id, :parent_id)", id=1, parent_id=999)
        return {"status": "deferred_until_commit"}

    @get("/valid")
    async def valid_route(db_session: NamedDependency[AsyncDriverAdapterBase]) -> dict[str, str]:
        await db_session.execute("INSERT INTO parents (id) VALUES (:id)", id=10)
        await db_session.execute("INSERT INTO children (id, parent_id) VALUES (:id, :parent_id)", id=2, parent_id=10)
        return {"status": "ok"}

    app = Litestar(route_handlers=[violate_route, valid_route], plugins=[plugin], debug=True)

    with TestClient(app=app, raise_server_exceptions=False) as client:
        traced_statements.clear()
        res_violate = client.get("/violate")
        violate_sql = [stmt.strip().upper() for stmt in traced_statements]
        res_valid = client.get("/valid")

    assert res_violate.status_code == 500
    assert violate_sql.count("COMMIT") == 1
    assert "ROLLBACK" in violate_sql
    assert res_valid.status_code == 200

    with sqlite3.connect(str(db_path)) as check_conn:
        rows = check_conn.execute("SELECT id, parent_id FROM children ORDER BY id").fetchall()
    assert rows == [(2, 10)]
