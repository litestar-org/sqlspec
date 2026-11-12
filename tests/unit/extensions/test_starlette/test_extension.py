# pyright: reportArgumentType=false
"""Tests for Starlette SQLSpec plugin."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.extensions.starlette import SQLSpecPlugin, set_request_tenant
from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY
from sqlspec.extensions.starlette.middleware import (
    SQLSpecAutocommitMiddleware,
    SQLSpecManualMiddleware,
    acquire_pending_connection_async,
)
from sqlspec.utils.tenant import TenantContext
from tests.unit.extensions._tenant_configs import AsyncRecordingStrategy, TenantAiosqliteConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.types import Message

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.starlette._state import CommitMode


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
        db = await plugin_ref.get_session_async(request)
        result = await db.execute("SELECT 1 as value")
        return JSONResponse({"value": result.scalar()})

    routes = [Route("/test", test_route)]
    app = Starlette(routes=routes)
    plugin_ref = SQLSpecPlugin(sqlspec, app)
    with TestClient(app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


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
        self.driver_features: dict[str, Any] = {}
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


def _make_state(config: _Config | SimpleNamespace, commit_mode: "CommitMode" = "manual") -> SQLSpecConfigState:
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
        seen.append(await acquire_pending_connection_async(request_, middleware.config_state))
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
        assert await acquire_pending_connection_async(request_, middleware.config_state) is connection
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
        assert await acquire_pending_connection_async(request_, middleware.config_state) is connection
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
        await acquire_pending_connection_async(_request, middleware.config_state)
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
    plugin_cls: Any = SQLSpecPlugin
    if framework == "fastapi":
        pytest.importorskip("fastapi")
        from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin

        plugin_cls = FastAPIPlugin

    def extractor(scope: Any) -> str:
        return "a"

    strategy = AsyncRecordingStrategy()
    config = TenantAiosqliteConfig(
        driver_features={"tenant_strategy": strategy}, extension_config={framework: {"tenant_extractor": extractor}}
    )
    plugin = plugin_cls(config)
    assert plugin._config_states[0].tenant_extractor is extractor
    assert plugin.get_config() is config

    invalid = TenantAiosqliteConfig(
        driver_features={"tenant_strategy": strategy}, extension_config={framework: {"tenant_extractor": 123}}
    )
    with pytest.raises(ImproperConfigurationError, match="tenant_extractor"):
        plugin_cls(invalid)
    without_strategy = AiosqliteConfig(extension_config={framework: {"tenant_extractor": extractor}})
    with pytest.raises(ImproperConfigurationError, match="tenant_strategy"):
        plugin_cls(without_strategy)


@pytest.mark.anyio
@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
@pytest.mark.parametrize("mode", ["manual", "autocommit"])
@pytest.mark.parametrize("failure", [None, "body", "background", "cancel", "send"])
async def test_tenant_stream_keeps_connection_through_body_and_background(
    framework: str, mode: str, failure: str | None
) -> None:
    events: list[str] = []
    first_sent = asyncio.Event()
    gate = asyncio.Event()
    active = False

    async def query(stage: str) -> None:
        assert active, "database connection released before delayed I/O"
        events.append(stage)

    async def commit() -> None:
        await query("commit")

    async def rollback() -> None:
        await query("rollback")

    connection = SimpleNamespace(commit=commit, rollback=rollback)

    @asynccontextmanager
    async def provide_connection(pool: object) -> "AsyncIterator[SimpleNamespace]":
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
        await acquire_pending_connection_async(request, state)
        return StreamingResponse(body(), background=BackgroundTask(background))

    if framework == "fastapi":
        from fastapi import FastAPI

        app = FastAPI()
        app.add_api_route("/", endpoint)
    else:
        app = Starlette(routes=[Route("/", endpoint)])
    app.state.db_pool = object()
    config = SimpleNamespace(supports_connection_pooling=True, provide_connection=provide_connection)
    state = _make_state(config, cast("CommitMode", mode))
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
    connection = _Connection()

    @asynccontextmanager
    async def provide_connection(pool: object) -> "AsyncIterator[_Connection]":
        yield connection

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await acquire_pending_connection_async(Request(scope), state)
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


@pytest.mark.anyio
@pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
@pytest.mark.parametrize("middleware_type", [SQLSpecManualMiddleware, SQLSpecAutocommitMiddleware])
async def test_tenant_connection_middleware_non_http_passthrough(scope_type: str, middleware_type: Any) -> None:
    events = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        events.append((scope["type"], TenantContext.get_tenant_id()))

    config = _Config(pooled=False, connection=_Connection())
    state = _make_state(config)
    state.tenant_extractor = lambda scope: pytest.fail("non-http extractor invoked")
    with TenantContext.scope("outer"):
        await middleware_type(app=app, config_state=state)({"type": scope_type}, None, None)
        assert TenantContext.get_tenant_id() == "outer"
    assert events == [(scope_type, "outer")]
    assert config.created is False


@pytest.mark.anyio
async def test_tenant_response_error_retains_identity_when_rollback_fails() -> None:
    body_error = ValueError("body failed")
    events = []

    async def rollback() -> None:
        events.append("rollback")
        raise RuntimeError("cleanup failed")

    @asynccontextmanager
    async def provide_connection(pool: object) -> "AsyncIterator[SimpleNamespace]":
        try:
            yield SimpleNamespace(rollback=rollback)
        except ValueError as exc:
            assert exc is body_error
            events.append("body_forwarded")
            raise
        finally:
            events.append("release")

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await acquire_pending_connection_async(Request(scope), state)
        raise body_error

    async def receive() -> "Message":
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Any) -> None:
        pytest.fail("body failure sent a response")

    config = SimpleNamespace(supports_connection_pooling=True, provide_connection=provide_connection)
    state = _make_state(config, "autocommit")
    state.tenant_extractor = lambda scope: "inner"
    scope = {"type": "http", "app": SimpleNamespace(state=SimpleNamespace(db_pool=object()))}
    with TenantContext.scope("outer"):
        with pytest.raises(ValueError) as caught:
            await SQLSpecAutocommitMiddleware(app=app, config_state=state)(scope, receive, send)
        assert caught.value is body_error
        assert TenantContext.get_tenant_id() == "outer"
    assert events == ["rollback", "body_forwarded", "release"]
    assert body_error.__notes__ == ["Request transaction rollback failed (RuntimeError)"]


def test_starlette_deferred_tenant_session_async_and_multi_config() -> None:
    """Deferred tenant configs wait for set_request_tenant + get_session_async without false extractor conflicts."""
    checkouts: list[tuple[str | None, dict[str, str]]] = []
    events: list[str] = []

    class _SessionDriver:
        def __init__(self, connection: Any, **_kwargs: Any) -> None:
            self.connection = connection

    class _DeferredTenantConfig(AiosqliteConfig):
        driver_type: ClassVar[type[Any]] = _SessionDriver

        async def create_pool(self) -> Any:
            return object()

        async def close_pool(self) -> None:
            return None

        @asynccontextmanager
        async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncIterator[Any]":
            payload = TenantContext.get()
            if payload is None:
                raise TenantContextError("Tenant context required")
            checkouts.append((payload.tenant_id, dict(payload.variables)))

            async def commit() -> None:
                events.append(f"commit:{payload.tenant_id}")

            async def rollback() -> None:
                events.append(f"rollback:{payload.tenant_id}")

            try:
                yield SimpleNamespace(tenant_id=payload.tenant_id, commit=commit, rollback=rollback)
            finally:
                events.append(f"release:{payload.tenant_id}")

    auth_cfg = AiosqliteConfig(
        bind_key="auth",
        connection_config={"database": ":memory:"},
        extension_config={
            "starlette": {
                "session_key": "auth_session",
                "connection_key": "auth_conn",
                "pool_key": "auth_pool",
                "enable_correlation_middleware": False,
                "enable_sqlcommenter_middleware": False,
            }
        },
    )
    raw_tenant_cfg = _DeferredTenantConfig(
        bind_key="tenant_db",
        connection_config={"database": ":memory:"},
        extension_config={
            "starlette": {
                "session_key": "tenant_session",
                "connection_key": "tenant_conn",
                "pool_key": "tenant_pool",
                "commit_mode": "autocommit",
                "enable_correlation_middleware": False,
                "enable_sqlcommenter_middleware": False,
            }
        },
    )
    raw_tenant_cfg.driver_features["tenant_strategy"] = object()  # type: ignore[typeddict-unknown-key]
    tenant_cfg = cast("DatabaseConfigProtocol[Any, Any, Any]", raw_tenant_cfg)

    async def health(_request: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def projects(request: Request) -> JSONResponse:
        auth_db = await plugin.get_session_async(request, auth_cfg)
        probe = await auth_db.execute("SELECT 1 AS ok")
        assert probe.scalar() == 1
        org_id = request.path_params["org_id"]
        if request.headers.get("authorization") != "Bearer valid":
            return JSONResponse({"detail": "unauthenticated"}, status_code=401)
        if org_id not in {"acme", "globex"}:
            return JSONResponse({"detail": "forbidden"}, status_code=403)
        set_request_tenant(request, org_id, variables={"tier": "enterprise"})
        session = await plugin.get_session_async(request, "tenant_session")
        cached = await plugin.get_session_async(request, tenant_cfg)
        conn = await plugin.get_connection_async(request, "tenant_db")
        assert session is cached
        assert session.connection is conn
        return JSONResponse({"tenant": session.connection.tenant_id})

    async def fail_route(request: Request) -> JSONResponse:
        set_request_tenant(request, "acme")
        await plugin.get_session_async(request, tenant_cfg)
        raise RuntimeError("boom")

    app = Starlette(
        routes=[Route("/health", health), Route("/orgs/{org_id}/projects", projects), Route("/fail", fail_route)]
    )
    plugin = SQLSpecPlugin([auth_cfg, tenant_cfg], app)
    assert plugin.config == [auth_cfg, tenant_cfg]
    assert plugin.get_config("tenant_db") is tenant_cfg

    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/health").json() == {"ok": True}
        assert client.get("/orgs/acme/projects").status_code == 401
        assert client.get("/orgs/evil/projects", headers={"authorization": "Bearer valid"}).status_code == 403
        assert checkouts == []

        ok = client.get("/orgs/globex/projects", headers={"authorization": "Bearer valid"})
        assert ok.status_code == 200
        assert ok.json() == {"tenant": "globex"}
        assert checkouts == [("globex", {"tier": "enterprise"})]
        assert events == ["commit:globex", "release:globex"]

        err = client.get("/fail")
        assert err.status_code == 500
        assert events[2:] == ["rollback:acme", "release:acme"]
    assert TenantContext.get() is None


@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
def test_request_connection_is_checked_out_on_first_access_only(framework: str, tmp_path: Any) -> None:
    import sqlite3

    database = str(tmp_path / "lazy.db")
    setup = sqlite3.connect(database)
    setup.execute("CREATE TABLE items (status INTEGER)")
    setup.commit()
    setup.close()
    checkouts: list[int] = []

    class CountingConfig(AiosqliteConfig):
        def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
            checkouts.append(1)
            return super().provide_connection(*args, **kwargs)

    config = CountingConfig(
        connection_config={"database": database}, extension_config={framework: {"commit_mode": "autocommit"}}
    )

    async def nodb(request: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def write(request: Request) -> JSONResponse:
        status = int(request.path_params["status"])
        session = await plugin.get_session_async(request)
        await session.begin()
        await session.execute("INSERT INTO items VALUES (:status)", {"status": status})
        assert await plugin.get_session_async(request) is session
        assert await plugin.get_connection_async(request) is session.connection
        return JSONResponse({"ok": True}, status_code=status)

    routes: list[Any] = [Route("/nodb", nodb), Route("/write/{status:int}", write)]
    plugin: Any
    if framework == "fastapi":
        from fastapi import FastAPI

        from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin

        app: Any = FastAPI(routes=routes)
        plugin = FastAPIPlugin(config, app)
    else:
        app = Starlette(routes=routes)
        plugin = SQLSpecPlugin(config, app)
    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/nodb").status_code == 200
        assert checkouts == []
        assert client.get("/write/201").status_code == 201
        assert checkouts == [1]
        assert client.get("/write/500").status_code == 500
        assert checkouts == [1, 1]
    reader = sqlite3.connect(database)
    try:
        assert reader.execute("SELECT status FROM items").fetchall() == [(201,)]
    finally:
        reader.close()


def test_sync_accessor_on_async_configuration_requires_async_access() -> None:
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    errors: list[str] = []

    async def route(request: Request) -> JSONResponse:
        try:
            plugin.get_session(request)
        except ImproperConfigurationError as exc:
            errors.append(str(exc))
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/", route)])
    plugin = SQLSpecPlugin(config, app)
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
    assert len(errors) == 1
    assert "get_session_async" in errors[0]
