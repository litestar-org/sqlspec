# pyright: reportArgumentType=false
"""Tests for Starlette SQLSpec plugin."""

from types import SimpleNamespace
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.starlette import SQLSpecPlugin
from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY
from sqlspec.extensions.starlette.middleware import SQLSpecAutocommitMiddleware, SQLSpecManualMiddleware

pytest.importorskip("starlette")


def test_default_session_key_is_db_session() -> None:
    """Starlette should default to 'db_session' for consistency."""
    assert DEFAULT_SESSION_KEY == "db_session"


def test_uses_default_session_key_when_not_configured() -> None:
    """Plugin should use DEFAULT_SESSION_KEY when no extension_config provided."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    assert len(plugin._config_states) == 1
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key in extension_config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    assert len(plugin._config_states) == 1
    assert plugin._config_states[0].session_key == custom_key


def test_get_session_works_in_route() -> None:
    """Test that get_session() works correctly in Starlette routes."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)
    plugin_ref: SQLSpecPlugin | None = None

    async def test_route(request: Request) -> JSONResponse:
        assert plugin_ref is not None
        db = plugin_ref.get_session(request)
        result = await db.execute("SELECT 1 as value")
        return JSONResponse({"value": result.scalar()})

    routes = [Route("/test", test_route)]
    app = Starlette(routes=routes)
    plugin_ref = SQLSpecPlugin(sqlspec, app)
    with TestClient(app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


pytest.importorskip("starlette")


class _Connection:
    def __init__(self) -> None:
        self.closed = False
        self.committed = False
        self.rolled_back = False

    def close(self) -> None:
        self.closed = True

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class _ConnectionManager:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.entered = False
        self.exited = False

    def __enter__(self) -> _Connection:
        self.entered = True
        return self.connection

    def __exit__(self, *_: object) -> None:
        self.exited = True


class _Config:
    def __init__(self, *, pooled: bool, connection: _Connection, manager: _ConnectionManager | None = None) -> None:
        self.supports_connection_pooling = pooled
        self.connection = connection
        self.manager = manager
        self.created = False
        self.pool_seen: Any = None

    def create_connection(self) -> _Connection:
        self.created = True
        return self.connection

    def provide_connection(self, pool: Any) -> _ConnectionManager:
        self.pool_seen = pool
        if self.manager is None:
            msg = "manager is required for pooled config"
            raise AssertionError(msg)
        return self.manager


def _make_state(config: _Config) -> SQLSpecConfigState:
    return SQLSpecConfigState(
        config=config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )


def _make_request(pool: Any = None) -> Any:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db_pool=pool)), state=SimpleNamespace())


async def test_middleware_manual_middleware_connection_cm_no_pool_sets_and_closes_connection() -> None:
    connection = _Connection()
    config = _Config(pooled=False, connection=connection)
    middleware = SQLSpecManualMiddleware(app=object(), config_state=_make_state(config))
    request = _make_request()
    seen: list[_Connection] = []

    async def call_next(request_: Any) -> Response:
        seen.append(request_.state.db_connection)
        return Response(status_code=204)

    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 204
    assert seen == [connection]
    assert config.created is True
    assert connection.closed is True
    assert not hasattr(request.state, "db_connection")


async def test_middleware_manual_middleware_connection_cm_pool_uses_pool_context() -> None:
    connection = _Connection()
    manager = _ConnectionManager(connection)
    pool = object()
    config = _Config(pooled=True, connection=connection, manager=manager)
    middleware = SQLSpecManualMiddleware(app=object(), config_state=_make_state(config))
    request = _make_request(pool)

    async def call_next(request_: Any) -> Response:
        assert request_.state.db_connection is connection
        return Response(status_code=200)

    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 200
    assert config.pool_seen is pool
    assert manager.entered is True
    assert manager.exited is True
    assert connection.closed is False
    assert not hasattr(request.state, "db_connection")


async def test_middleware_autocommit_middleware_connection_cm_no_pool_commits_and_closes() -> None:
    connection = _Connection()
    config = _Config(pooled=False, connection=connection)
    middleware = SQLSpecAutocommitMiddleware(app=object(), config_state=_make_state(config))
    request = _make_request()

    async def call_next(request_: Any) -> Response:
        assert request_.state.db_connection is connection
        return Response(status_code=201)

    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 201
    assert connection.committed is True
    assert connection.rolled_back is False
    assert connection.closed is True
    assert not hasattr(request.state, "db_connection")


async def test_middleware_autocommit_middleware_connection_cm_pool_rolls_back_on_exception() -> None:
    connection = _Connection()
    manager = _ConnectionManager(connection)
    config = _Config(pooled=True, connection=connection, manager=manager)
    middleware = SQLSpecAutocommitMiddleware(app=object(), config_state=_make_state(config))
    request = _make_request(object())

    async def call_next(_request: Any) -> Response:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await middleware.dispatch(request, call_next)
    assert connection.committed is False
    assert connection.rolled_back is True
    assert connection.closed is False
    assert manager.exited is True
    assert not hasattr(request.state, "db_connection")
