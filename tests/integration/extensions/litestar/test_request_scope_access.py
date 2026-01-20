"""Integration tests for SQLSpecPlugin request scope access methods.

Tests verify that provide_request_session, provide_request_session_async,
provide_request_connection, and provide_request_connection_async work
correctly when accessed from route handlers and guards.
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pytest
from litestar import Litestar, Request, get
from litestar.connection import ASGIConnection
from litestar.handlers import BaseRouteHandler
from litestar.testing import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin

if TYPE_CHECKING:
    from litestar.handlers import HTTPRouteHandler
    from litestar.types import Guard

pytestmark = pytest.mark.xdist_group("sqlite")


@contextmanager
def create_test_app(
    route_handlers: Sequence[HTTPRouteHandler],
    config: SyncDatabaseConfig[Any, Any, Any] | AsyncDatabaseConfig[Any, Any, Any],
    guards: Sequence[Guard] | None = None,
) -> Generator[TestClient[Litestar], None, None]:
    """Create a test application with SQLSpec plugin configured.

    Args:
        route_handlers: List of route handlers for the app.
        config: Database configuration to use.
        guards: Optional list of guards to apply.

    Yields:
        TestClient instance for the configured app.
    """
    sql = SQLSpec()
    sql.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=sql)

    app = Litestar(
        route_handlers=list(route_handlers), plugins=[plugin], guards=list(guards) if guards else None, debug=True
    )

    with TestClient(app=app) as client:
        yield client


class TestAsyncRequestScopeAccess:
    """Tests for async request scope access methods with async configs."""

    @pytest.fixture
    def async_config(self, tmp_path: Any) -> AiosqliteConfig:
        """Create an async SQLite config."""
        db_path = tmp_path / "test.db"
        return AiosqliteConfig(connection_config={"database": str(db_path)}, extension_config={"litestar": {}})

    def test_di_session_injection_baseline(self, async_config: AiosqliteConfig) -> None:
        """Baseline: Verify standard DI injection of session works."""

        @get("/test")
        async def handler(db_session: AsyncDriverAdapterBase) -> dict[str, Any]:
            result = await db_session.execute("SELECT 1 as value")
            data = result.get_first()
            return {"value": data["value"] if data else None}

        with create_test_app([handler], async_config) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}

    def test_provide_session_from_handler(self, async_config: AiosqliteConfig) -> None:
        """Test provide_request_session_async from route handler."""

        @get("/test")
        async def handler(request: Request) -> dict[str, Any]:
            plugin: SQLSpecPlugin = request.app.state.sqlspec
            session = await plugin.provide_request_session_async("db_connection", request.app.state, request.scope)
            result = await session.execute("SELECT 1 as value")
            data = result.get_first()
            return {"value": data["value"] if data else None}

        with create_test_app([handler], async_config) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}

    def test_provide_connection_from_handler(self, async_config: AiosqliteConfig) -> None:
        """Test provide_request_connection_async from route handler."""

        @get("/test")
        async def handler(request: Request) -> dict[str, Any]:
            plugin: SQLSpecPlugin = request.app.state.sqlspec
            connection = await plugin.provide_request_connection_async(
                "db_connection", request.app.state, request.scope
            )
            assert connection is not None
            return {"success": True}

        with create_test_app([handler], async_config) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"success": True}

    def test_provide_connection_from_guard(self, async_config: AiosqliteConfig) -> None:
        """Test provide_request_connection_async from a guard."""
        guard_executed = {"value": False}

        async def db_guard(connection: ASGIConnection, handler: BaseRouteHandler) -> None:
            plugin: SQLSpecPlugin = connection.app.state.sqlspec
            db_conn = await plugin.provide_request_connection_async(
                "db_connection", connection.app.state, connection.scope
            )
            assert db_conn is not None
            guard_executed["value"] = True

        @get("/test")
        async def handler() -> dict[str, Any]:
            return {"success": True}

        with create_test_app([handler], async_config, guards=[db_guard]) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert guard_executed["value"] is True

    def test_provide_session_from_guard(self, async_config: AiosqliteConfig) -> None:
        """Test provide_request_session_async from a guard."""
        guard_result = {"value": None}

        async def db_guard(connection: ASGIConnection, handler: BaseRouteHandler) -> None:
            plugin: SQLSpecPlugin = connection.app.state.sqlspec
            session = await plugin.provide_request_session_async(
                "db_connection", connection.app.state, connection.scope
            )
            result = await session.execute("SELECT 42 as answer")
            data = result.get_first()
            guard_result["value"] = data["answer"] if data else None

        @get("/test")
        async def handler() -> dict[str, Any]:
            return {"guard_saw": guard_result["value"]}

        with create_test_app([handler], async_config, guards=[db_guard]) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"guard_saw": 42}


class TestSyncRequestScopeAccess:
    """Tests for sync request scope access methods with sync configs."""

    @pytest.fixture
    def sync_config(self, tmp_path: Any) -> SqliteConfig:
        """Create a sync SQLite config."""
        db_path = tmp_path / "test.db"
        return SqliteConfig(connection_config={"database": str(db_path)}, extension_config={"litestar": {}})

    def test_di_session_injection_baseline(self, sync_config: SqliteConfig) -> None:
        """Baseline: Verify standard DI injection of session works."""

        @get("/test", sync_to_thread=False)
        def handler(db_session: SyncDriverAdapterBase) -> dict[str, Any]:
            result = db_session.execute("SELECT 1 as value")
            data = result.get_first()
            return {"value": data["value"] if data else None}

        with create_test_app([handler], sync_config) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}

    def test_provide_session_from_handler(self, sync_config: SqliteConfig) -> None:
        """Test provide_request_session from route handler."""

        @get("/test", sync_to_thread=False)
        def handler(request: Request) -> dict[str, Any]:
            plugin: SQLSpecPlugin = request.app.state.sqlspec
            session = plugin.provide_request_session("db_connection", request.app.state, request.scope)
            result = session.execute("SELECT 1 as value")
            data = result.get_first()
            return {"value": data["value"] if data else None}

        with create_test_app([handler], sync_config) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}

    def test_provide_connection_from_handler(self, sync_config: SqliteConfig) -> None:
        """Test provide_request_connection from route handler."""

        @get("/test", sync_to_thread=False)
        def handler(request: Request) -> dict[str, Any]:
            plugin: SQLSpecPlugin = request.app.state.sqlspec
            connection = plugin.provide_request_connection("db_connection", request.app.state, request.scope)
            assert connection is not None
            return {"success": True}

        with create_test_app([handler], sync_config) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"success": True}

    def test_provide_connection_from_guard(self, sync_config: SqliteConfig) -> None:
        """Test provide_request_connection from a guard."""
        guard_executed = {"value": False}

        def db_guard(connection: ASGIConnection, handler: BaseRouteHandler) -> None:
            plugin: SQLSpecPlugin = connection.app.state.sqlspec
            db_conn = plugin.provide_request_connection("db_connection", connection.app.state, connection.scope)
            assert db_conn is not None
            guard_executed["value"] = True

        @get("/test", sync_to_thread=False)
        def handler() -> dict[str, Any]:
            return {"success": True}

        with create_test_app([handler], sync_config, guards=[db_guard]) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert guard_executed["value"] is True

    def test_provide_session_from_guard(self, sync_config: SqliteConfig) -> None:
        """Test provide_request_session from a guard."""
        guard_result = {"value": None}

        def db_guard(connection: ASGIConnection, handler: BaseRouteHandler) -> None:
            plugin: SQLSpecPlugin = connection.app.state.sqlspec
            session = plugin.provide_request_session("db_connection", connection.app.state, connection.scope)
            result = session.execute("SELECT 42 as answer")
            data = result.get_first()
            guard_result["value"] = data["answer"] if data else None

        @get("/test", sync_to_thread=False)
        def handler() -> dict[str, Any]:
            return {"guard_saw": guard_result["value"]}

        with create_test_app([handler], sync_config, guards=[db_guard]) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"guard_saw": 42}
