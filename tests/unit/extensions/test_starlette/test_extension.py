# pyright: reportArgumentType=false
"""Tests for Starlette SQLSpec plugin."""

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.starlette import SQLSpecPlugin
from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY
from sqlspec.extensions.starlette.middleware import SQLSpecAutocommitMiddleware, SQLSpecManualMiddleware

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.starlette._state import CommitMode

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


def _make_state(config: _Config, commit_mode: "CommitMode" = "manual") -> SQLSpecConfigState:
    return SQLSpecConfigState(
        config=cast("DatabaseConfigProtocol[Any, Any, Any]", config),
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode=commit_mode,
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
    middleware = SQLSpecAutocommitMiddleware(app=object(), config_state=_make_state(config, "autocommit"))
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
    middleware = SQLSpecAutocommitMiddleware(app=object(), config_state=_make_state(config, "autocommit"))
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


def test_duplicate_state_keys_raise_improper_configuration_error() -> None:
    """Duplicate state keys should raise ImproperConfigurationError with the Starlette message."""
    plugin = SQLSpecPlugin(SQLSpec())
    config = _Config(pooled=False, connection=_Connection())
    plugin._config_states = [_make_state(config), _make_state(config)]
    with pytest.raises(ImproperConfigurationError, match="Duplicate state keys found"):
        plugin._ensure_unique_keys()


def test_config_state_by_key_unknown_key_raises_value_error() -> None:
    """Unknown session keys should raise ValueError with the Starlette message."""
    plugin = SQLSpecPlugin(SQLSpec())
    with pytest.raises(ValueError, match="No configuration found with session_key: missing"):
        plugin._config_state_by_key("missing")


@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
def test_plugin_tenant_extractor_state_and_validation(framework: str) -> None:
    from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin

    def extractor(scope: Any) -> str:
        return "a"

    config = AiosqliteConfig(extension_config={framework: {"tenant_extractor": extractor}})
    sqlspec = SQLSpec()
    sqlspec.add_config(config)
    plugin = (FastAPIPlugin if framework == "fastapi" else SQLSpecPlugin)(sqlspec)
    assert plugin._config_states[0].tenant_extractor is extractor

    invalid = AiosqliteConfig(extension_config={framework: {"tenant_extractor": 123}})
    invalid_sqlspec = SQLSpec()
    invalid_sqlspec.add_config(invalid)
    with pytest.raises(ImproperConfigurationError, match="tenant_extractor"):
        (FastAPIPlugin if framework == "fastapi" else SQLSpecPlugin)(invalid_sqlspec)


@pytest.mark.anyio
@pytest.mark.parametrize("middleware_type", [SQLSpecManualMiddleware, SQLSpecAutocommitMiddleware])
@pytest.mark.parametrize("value", ["inner", None])
async def test_tenant_extractor_binds_connection_entry_and_exit(middleware_type: Any, value: str | None) -> None:
    from contextlib import asynccontextmanager

    from sqlspec.utils.tenant import TenantContext

    seen = []
    connection = _Connection()

    @asynccontextmanager
    async def provide_connection(pool: Any) -> Any:
        seen.append(("enter", TenantContext.get_tenant_id()))
        try:
            yield connection
        finally:
            seen.append(("exit", TenantContext.get_tenant_id()))

    config = SimpleNamespace(supports_connection_pooling=True, provide_connection=provide_connection)
    state = _make_state(config)
    state.tenant_extractor = lambda scope: value
    middleware = middleware_type(app=object(), config_state=state)
    request = _make_request(object())
    request.scope = {}
    with TenantContext.scope("outer"):
        async with middleware._connection_cm(request):
            assert TenantContext.get_tenant_id() == value
        assert TenantContext.get_tenant_id() == "outer"
    assert seen == [("enter", value), ("exit", value)]


@pytest.mark.anyio
@pytest.mark.parametrize("middleware_type", [SQLSpecManualMiddleware, SQLSpecAutocommitMiddleware])
async def test_tenant_extractor_rejection_prevents_acquisition(middleware_type: Any) -> None:
    from sqlspec.exceptions import TenantContextError

    config = _Config(pooled=False, connection=_Connection())
    state = _make_state(config)
    state.tenant_extractor = lambda scope: 123
    middleware = middleware_type(app=object(), config_state=state)
    request = _make_request()
    request.scope = {}
    with pytest.raises(TenantContextError):
        async with middleware._connection_cm(request):
            pytest.fail("invalid tenant acquired a connection")
    assert config.created is False


@pytest.mark.anyio
@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
@pytest.mark.parametrize("mode", ["manual", "autocommit"])
@pytest.mark.parametrize("failure", [None, "body", "background", "cancel", "send"])
async def test_tenant_stream_keeps_connection_through_body_and_background(
    framework: str, mode: str, failure: str | None
) -> None:
    import asyncio
    from contextlib import asynccontextmanager

    from starlette.background import BackgroundTask
    from starlette.responses import StreamingResponse

    from sqlspec.utils.tenant import TenantContext

    events: list[str] = []
    first_sent = asyncio.Event()
    gate = asyncio.Event()
    active = False

    async def query(stage: str) -> None:
        assert active, "database connection released before delayed I/O"
        assert TenantContext.get_tenant_id() == "inner"
        events.append(stage)

    async def commit() -> None:
        await query("commit")

    async def rollback() -> None:
        await query("rollback")

    connection = SimpleNamespace(commit=commit, rollback=rollback)

    @asynccontextmanager
    async def provide_connection(pool: Any) -> Any:
        nonlocal active
        active = True
        events.append("acquire")
        try:
            yield connection
        finally:
            events.append("release")
            active = False

    async def body() -> Any:
        try:
            yield b"first"
            await gate.wait()
            await query("body_query")
            if failure == "body":
                raise ValueError("body failure")
            yield b"last"
        finally:
            events.append("generator_exit")

    async def background() -> None:
        await query("background_query")
        if failure == "background":
            raise ValueError("background failure")

    async def endpoint(request: Request) -> StreamingResponse:
        return StreamingResponse(body(), background=BackgroundTask(background))

    if framework == "fastapi":
        from fastapi import FastAPI

        app = FastAPI()
        app.add_api_route("/", endpoint)
    else:
        app = Starlette(routes=[Route("/", endpoint)])
    config = SimpleNamespace(supports_connection_pooling=True, provide_connection=provide_connection)
    state = _make_state(config, cast("CommitMode", mode))
    state.tenant_extractor = lambda scope: "inner"
    middleware_type = SQLSpecManualMiddleware if mode == "manual" else SQLSpecAutocommitMiddleware
    middleware = middleware_type(app=app, config_state=state)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 123),
        "server": ("test", 80),
        "app": SimpleNamespace(state=SimpleNamespace(db_pool=object())),
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
                events.append("first_sent")
                first_sent.set()
            if message.get("body") == b"last" and failure == "send":
                raise OSError("send failure")
            if not message.get("more_body", False):
                events.append("final_sent")

    with TenantContext.scope("outer"):
        task = asyncio.create_task(middleware(scope, receive, send))
        await asyncio.wait_for(first_sent.wait(), timeout=3)
        assert active
        assert events == ["acquire", "first_sent"]
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            gate.set()
            if failure is None:
                await asyncio.wait_for(task, timeout=3)
            else:
                with pytest.raises((ValueError, OSError), match=f"{failure} failure"):
                    await asyncio.wait_for(task, timeout=3)
        assert TenantContext.get_tenant_id() == "outer"
    assert events.count("acquire") == events.count("release") == 1
    assert events[-1] == "release"
    assert not active
    if failure is None:
        assert events.index("body_query") < events.index("final_sent")
        assert events.index("background_query") < events.index("release")
        if mode == "autocommit":
            assert events.index("background_query") < events.index("commit") < events.index("release")
    if mode == "manual":
        assert "commit" not in events and "rollback" not in events
    elif failure is not None:
        assert "commit" not in events
        assert events.count("rollback") == 1


@pytest.mark.anyio
async def test_tenant_incomplete_downstream_response_rolls_back() -> None:
    from contextlib import asynccontextmanager

    connection = _Connection()

    @asynccontextmanager
    async def provide_connection(pool: Any) -> Any:
        yield connection

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})

    async def receive() -> Any:
        return {"type": "http.request", "body": b"", "more_body": False}

    messages = []

    async def send(message: Any) -> None:
        messages.append(message)

    config = SimpleNamespace(supports_connection_pooling=True, provide_connection=provide_connection)
    state = _make_state(config, "autocommit")
    middleware = SQLSpecAutocommitMiddleware(app=app, config_state=state)
    scope = {"type": "http", "app": SimpleNamespace(state=SimpleNamespace(db_pool=object()))}
    await middleware(scope, receive, send)
    assert messages[-1] == {"type": "http.response.body", "body": b"", "more_body": False}
    assert connection.rolled_back is True
    assert connection.committed is False
