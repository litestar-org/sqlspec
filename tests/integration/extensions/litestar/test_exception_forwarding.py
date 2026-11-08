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
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig
    from tests.integration.fixtures.postgres import _postgres_conninfo

    conninfo = _postgres_conninfo(postgres_service)
    with sqlite3.connect(":memory:") as _:
        import psycopg

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

    import psycopg

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
