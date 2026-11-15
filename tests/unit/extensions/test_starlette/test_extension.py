# pyright: reportArgumentType=false
"""Tests for Starlette SQLSpec plugin."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.extensions.starlette import SQLSpecPlugin, set_request_tenant
from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette._utils import _get_connected_db_session, get_state_value, set_state_value
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY
from sqlspec.extensions.starlette.middleware import (
    SQLSpecTransactionMiddleware,
    _finalize_request_transaction,
    _response_completion_key,
    _ResponseCompletion,
    ensure_request_connection_async,
)
from sqlspec.observability import ObservabilityConfig
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.tenant import TenantContext
from tests.unit.extensions._tenant_configs import AsyncRecordingStrategy, TenantAiosqliteConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.types import Message

    from sqlspec.config import DatabaseConfigProtocol, LifecycleConfig
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


def _make_state(
    config: "DatabaseConfigProtocol[Any, Any, Any] | _Config | SimpleNamespace", commit_mode: "CommitMode" = "manual"
) -> SQLSpecConfigState:
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


async def _serve_request(
    state: SQLSpecConfigState, *, pool: Any = None, status: int = 200, error: BaseException | None = None
) -> "list[Message]":
    """Run one HTTP request through ``SQLSpecTransactionMiddleware`` and return the messages sent.

    The downstream app checks out the request connection, then raises ``error`` or responds with ``status``.

    Returns:
        The ASGI messages the middleware forwarded to ``send``.
    """

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await ensure_request_connection_async(Request(scope), state)
        if error is not None:
            raise error
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def receive() -> "Message":
        return {"type": "http.request", "body": b"", "more_body": False}

    messages: list[Message] = []

    async def send(message: "Message") -> None:
        messages.append(message)

    scope = {"type": "http", "app": SimpleNamespace(state=SimpleNamespace(db_pool=pool))}
    await SQLSpecTransactionMiddleware(app, [state])(scope, receive, send)
    return messages


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("commit_mode", "status", "expect_commit"),
    [
        ("manual", 200, False),
        ("autocommit", 201, True),
        ("autocommit", 302, False),
        ("autocommit_include_redirect", 302, True),
    ],
)
async def test_transaction_middleware_closes_direct_connection_per_commit_mode(
    commit_mode: str, status: int, expect_commit: bool
) -> None:
    connection = _Connection()
    config = _Config(pooled=False, connection=connection)
    state = _make_state(config, cast("CommitMode", commit_mode))

    messages = await _serve_request(state, status=status)

    assert messages[0]["status"] == status
    assert config.created is True
    assert connection.committed is expect_commit
    assert connection.rolled_back is not expect_commit
    assert connection.closed is True


@pytest.mark.anyio
@pytest.mark.parametrize("commit_mode", ["manual", "autocommit"])
async def test_transaction_middleware_pool_context_wraps_the_request(commit_mode: str) -> None:
    connection = _Connection()
    manager = _ConnectionManager(connection)
    pool = object()
    config = _Config(pooled=True, connection=connection, manager=manager)
    state = _make_state(config, cast("CommitMode", commit_mode))

    messages = await _serve_request(state, pool=pool)

    assert messages[0]["status"] == 200
    assert config.pool_seen is pool
    assert manager.entered is True
    assert manager.exited is True
    assert connection.committed is (commit_mode == "autocommit")
    assert connection.closed is False


@pytest.mark.anyio
@pytest.mark.parametrize("commit_mode", ["manual", "autocommit", "autocommit_include_redirect"])
async def test_transaction_middleware_rolls_back_and_reraises_handler_errors(commit_mode: str) -> None:
    connection = _Connection()
    manager = _ConnectionManager(connection)
    config = _Config(pooled=True, connection=connection, manager=manager)
    state = _make_state(config, cast("CommitMode", commit_mode))

    with pytest.raises(RuntimeError, match="boom"):
        await _serve_request(state, pool=object(), error=RuntimeError("boom"))

    assert connection.committed is False
    assert connection.rolled_back is True
    assert manager.exited is True


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
        await ensure_request_connection_async(request, state)
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
    middleware = SQLSpecTransactionMiddleware(app=app, config_states=[state])
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
            elif failure == "send":
                with pytest.raises(ClientDisconnect):
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
        op = "commit" if mode == "autocommit" else "rollback"
        assert events.index("background_query") < events.index(op) < events.index("release")
    if mode == "manual":
        assert "commit" not in events
        assert events.count("rollback") == 1
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
        await ensure_request_connection_async(Request(scope), state)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})

    async def receive() -> Any:
        return {"type": "http.request", "body": b"", "more_body": False}

    messages = []

    async def send(message: Any) -> None:
        messages.append(message)

    config = SimpleNamespace(supports_connection_pooling=True, provide_connection=provide_connection)
    state = _make_state(config, "autocommit")
    middleware = SQLSpecTransactionMiddleware(app=app, config_states=[state])
    scope = {"type": "http", "app": SimpleNamespace(state=SimpleNamespace(db_pool=object()))}
    await middleware(scope, receive, send)
    assert messages[-1] == {"type": "http.response.body", "body": b"partial", "more_body": True}
    assert connection.rolled_back is True
    assert connection.committed is False


@pytest.mark.anyio
@pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
@pytest.mark.parametrize("commit_mode", ["manual", "autocommit"])
async def test_tenant_connection_middleware_non_http_passthrough(scope_type: str, commit_mode: str) -> None:
    events = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        events.append((scope["type"], TenantContext.get_tenant_id()))

    config = _Config(pooled=False, connection=_Connection())
    state = _make_state(config, cast("CommitMode", commit_mode))
    state.tenant_extractor = lambda scope: pytest.fail("non-http extractor invoked")
    with TenantContext.scope("outer"):
        await SQLSpecTransactionMiddleware(app=app, config_states=[state])(
            {"type": scope_type}, cast("Any", None), cast("Any", None)
        )
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
        await ensure_request_connection_async(Request(scope), state)
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
            await SQLSpecTransactionMiddleware(app=app, config_states=[state])(scope, receive, send)
        assert caught.value is body_error
        assert TenantContext.get_tenant_id() == "outer"
    assert events == ["rollback", "body_forwarded", "release"]
    assert body_error.__notes__ == ["Request transaction rollback failed (RuntimeError)"]


def test_starlette_deferred_tenant_session_async_and_multi_config() -> None:
    """Deferred tenant configs wait for set_request_tenant + get_session_async without false extractor conflicts."""
    checkouts: list[tuple[str | None, dict[str, str]]] = []
    events: list[str] = []

    class _SessionDriver:
        _cache_holder = None

        def __init__(self, connection: Any, statement_config: Any = None, driver_features: Any = None) -> None:
            self.connection: Any = connection
            self.statement_config = statement_config
            self.driver_features = driver_features or {}
            self.observability = SimpleNamespace(is_idle=True)

        def attach_observability(self, observability: Any) -> None:
            self.observability = observability

        async def commit(self) -> None:
            await self.connection.commit()

        async def rollback(self) -> None:
            await self.connection.rollback()

        def _mark_closed(self) -> None:
            self.connection = None

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


def test_starlette_session_lifecycle_events_detachment_and_response_completion() -> None:
    """Starlette builds sessions via _prepare_driver, emits start/end, uses slotted _ResponseCompletion, keeps StreamingResponse connected, and detaches."""
    lifecycle_events: list[tuple[str, Any]] = []
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"starlette": {"commit_mode": "autocommit"}},
        observability_config=ObservabilityConfig(
            lifecycle=cast(
                "LifecycleConfig",
                {
                    "on_session_start": [lambda ctx: lifecycle_events.append(("start", ctx["session"]))],
                    "on_session_end": [lambda ctx: lifecycle_events.append(("end", ctx["session"]))],
                },
            )
        ),
    )
    prepare_calls: list[Any] = []
    original_prepare = config._prepare_driver

    def _tracking_prepare(driver: Any) -> Any:
        prepare_calls.append(driver)
        return original_prepare(driver)

    config._prepare_driver = _tracking_prepare  # type: ignore[method-assign]
    retained_sessions: list[Any] = []
    seen_completions: list[Any] = []

    async def stream_route(request: Request) -> StreamingResponse:
        session = await plugin.get_session_async(request)
        assert await plugin.get_session_async(request) is session
        retained_sessions.append(session)
        owner = get_state_value(request.state, _response_completion_key("db_connection"))
        seen_completions.append(owner)

        async def chunks() -> "AsyncIterator[bytes]":
            first = await session.select_value("SELECT 1")
            yield f"{first}:".encode()
            second = await session.select_value("SELECT 2")
            yield f"{second}".encode()

        return StreamingResponse(chunks())

    app = Starlette(routes=[Route("/stream", stream_route)])
    plugin = SQLSpecPlugin(config, app)

    with TestClient(app) as client:
        resp = client.get("/stream")
        assert resp.status_code == 200
        assert resp.text == "1:2"

    assert len(prepare_calls) == 1
    assert len(retained_sessions) == 1
    session = retained_sessions[0]
    assert lifecycle_events == [("start", session), ("end", session)]
    assert len(seen_completions) == 1
    assert isinstance(seen_completions[0], _ResponseCompletion)
    assert not hasattr(seen_completions[0], "__dict__")
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        _ = session.connection.cursor


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_kind", "commit_mode", "in_tx", "expect_auto_begin", "expect_commit"),
    [
        ("asyncpg", "autocommit", False, True, False),
        ("asyncpg", "autocommit", True, True, True),
        ("asyncpg", "autocommit_include_redirect", False, True, False),
        ("asyncpg", "manual", False, True, False),
        ("asyncpg", "manual", True, True, False),
        ("aiosqlite", "autocommit", False, False, True),
    ],
)
async def test_starlette_armed_and_unarmed_session_finalization(
    adapter_kind: str, commit_mode: str, in_tx: bool, expect_auto_begin: bool, expect_commit: bool
) -> None:
    """Starlette arms _auto_begin_on_write on lazy-begin adapters and skips commit when idle outside a transaction."""
    config: Any = (
        AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/db"})
        if adapter_kind == "asyncpg"
        else AiosqliteConfig(connection_config={"database": ":memory:"})
    )
    state = _make_state(config, cast("CommitMode", commit_mode))
    request = _make_request()
    tx = AsyncMock()
    if adapter_kind == "asyncpg":
        conn: Any = AsyncMock(is_in_transaction=MagicMock(return_value=in_tx))
    else:
        conn = AsyncMock(_conn=None, autocommit=False, in_transaction=in_tx)
    set_state_value(request.state, state.connection_key, conn)

    session = _get_connected_db_session(request, state)
    assert session._auto_begin_on_write is expect_auto_begin
    if adapter_kind == "asyncpg":
        session._transaction = tx

    if commit_mode == "manual":
        await _finalize_request_transaction(request, state, conn, commit=False)
        rolled_back = (tx.rollback.await_count > 0) if adapter_kind == "asyncpg" else (conn.rollback.await_count > 0)
        assert rolled_back is in_tx
    else:
        await _finalize_request_transaction(request, state, conn, commit=True)

    committed = (tx.commit.await_count > 0) if adapter_kind == "asyncpg" else (conn.commit.await_count > 0)
    assert committed is expect_commit


@pytest.mark.anyio
async def test_starlette_autocommit_rolls_back_on_cancellation() -> None:
    """SQLSpecTransactionMiddleware rolls back the connection on asyncio.CancelledError."""
    conn = AsyncMock(_conn=None, autocommit=False, in_transaction=True)
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    config.provide_connection = MagicMock(return_value=_AsyncConnCtx(conn))  # type: ignore[method-assign]
    state = _make_state(config, "autocommit")

    with pytest.raises(asyncio.CancelledError):
        await _serve_request(state, pool=object(), error=asyncio.CancelledError())

    conn.rollback.assert_awaited_once()
    conn.commit.assert_not_awaited()


class _AsyncConnCtx:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def __aenter__(self) -> Any:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        return None


@pytest.mark.parametrize("use_deferred", [False, True])
def test_starlette_async_lifecycle_hooks_execute_on_async_and_deferred_sessions(
    use_deferred: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Starlette awaits async on_session_start and on_session_end hooks across async and deferred sessions."""
    events: list[str] = []

    async def _on_start(_ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        events.append("start")

    async def _on_end(_ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        events.append("end")

    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"starlette": {"commit_mode": "autocommit"}},
        observability_config=ObservabilityConfig(
            lifecycle=cast("LifecycleConfig", {"on_session_start": [_on_start], "on_session_end": [_on_end]})
        ),
    )

    async def route(request: Request) -> JSONResponse:
        session = plugin.get_session(request) if use_deferred else await plugin.get_session_async(request)
        value = await session.select_value("SELECT 1")
        return JSONResponse({"value": value})

    app = Starlette(routes=[Route("/items", route)])
    plugin = SQLSpecPlugin(config, app)

    with caplog.at_level("WARNING"), TestClient(app) as client:
        response = client.get("/items")
        assert response.status_code == 200
        assert response.json() == {"value": 1}

    assert events == ["start", "end"]
    assert "Async lifecycle hook invoked on sync path" not in caplog.text


@pytest.mark.parametrize(
    ("commit_mode", "status_code", "explicit_commit", "extra_commit", "expect_persisted"),
    [
        (None, 200, False, None, True),
        (None, 303, False, None, False),
        ("autocommit", 200, False, None, True),
        ("autocommit", 302, False, None, False),
        ("autocommit", 303, False, None, False),
        ("autocommit", 400, False, None, False),
        ("autocommit_include_redirect", 200, False, None, True),
        ("autocommit_include_redirect", 302, False, None, True),
        ("autocommit_include_redirect", 303, False, None, True),
        ("autocommit_include_redirect", 400, False, None, False),
        ("manual", 200, False, None, False),
        ("manual", 200, False, {200}, False),
        ("manual", 200, True, None, True),
    ],
)
def test_starlette_commit_mode_redirect_and_manual_rollback_behavior(
    tmp_path: Any,
    commit_mode: str | None,
    status_code: int,
    explicit_commit: bool,
    extra_commit: "set[int] | None",
    expect_persisted: bool,
) -> None:
    """Verify Starlette default autocommit, Inertia 302/303 redirect commit, and manual rollback."""
    import sqlite3

    database = str(tmp_path / "starlette_modes.db")
    conn = sqlite3.connect(database)
    conn.execute("CREATE TABLE items (value TEXT)")
    conn.commit()
    conn.close()

    starlette_cfg: dict[str, Any] = {}
    if commit_mode is not None:
        starlette_cfg["commit_mode"] = commit_mode
    if extra_commit is not None:
        starlette_cfg["extra_commit_statuses"] = extra_commit

    config = AiosqliteConfig(
        connection_config={"database": database}, extension_config={"starlette": starlette_cfg} if starlette_cfg else {}
    )

    async def write_route(request: Request) -> Response:
        session = await plugin.get_session_async(request)
        await session.execute("INSERT INTO items VALUES (:val)", {"val": "written"})
        if explicit_commit:
            await session.commit()
        return JSONResponse({"status": status_code}, status_code=status_code)

    app = Starlette(routes=[Route("/write", write_route, methods=["POST"])])
    plugin = SQLSpecPlugin(config, app)

    with TestClient(app) as client:
        resp = client.post("/write")
        assert resp.status_code == status_code

    reader = sqlite3.connect(database)
    try:
        rows = reader.execute("SELECT value FROM items").fetchall()
    finally:
        reader.close()
    assert rows == ([("written",)] if expect_persisted else [])


def test_starlette_plugin_cursor_config_and_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Starlette SQLSpecPlugin cursor configuration, env fallback, and conflict detection."""
    import datetime as dt

    monkeypatch.delenv("SQLSPEC_PAGINATION_CURSOR_SECRET", raising=False)
    monkeypatch.delenv("SECRET_KEY", raising=False)

    base = SQLSpecPlugin(AiosqliteConfig(connection_config={"database": ":memory:"}))
    assert base.pagination_cursor_secret is None
    assert base.pagination_cursor_ttl is None
    assert base.pagination_cursor_encrypted is None

    monkeypatch.setenv("SECRET_KEY", "flask-style-secret")
    monkeypatch.setenv("SQLSPEC_PAGINATION_CURSOR_SECRET", "primary-env-secret")
    from_env = SQLSpecPlugin(AiosqliteConfig(connection_config={"database": ":memory:"}))
    assert from_env.pagination_cursor_secret == "primary-env-secret"
    monkeypatch.setenv("SQLSPEC_PAGINATION_CURSOR_SECRET", "mutated-later")
    assert from_env.pagination_cursor_secret == "primary-env-secret"

    explicit = SQLSpecPlugin(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"starlette": {"pagination_cursor_ttl": 60, "pagination_cursor_encrypted": True}},
        ),
        pagination_cursor_secret="kwarg-secret",
        pagination_cursor_ttl=dt.timedelta(seconds=60),
        pagination_cursor_encrypted=True,
    )
    assert explicit.pagination_cursor_secret == "kwarg-secret"
    assert explicit.pagination_cursor_ttl == dt.timedelta(seconds=60)
    assert explicit.pagination_cursor_encrypted is True

    with pytest.raises(ImproperConfigurationError, match="pagination_cursor_secret"):
        SQLSpecPlugin([
            AiosqliteConfig(
                bind_key="one",
                connection_config={"database": ":memory:"},
                extension_config={"starlette": {"pagination_cursor_secret": b"abc"}},
            ),
            AiosqliteConfig(
                bind_key="two",
                connection_config={"database": ":memory:"},
                extension_config={"starlette": {"pagination_cursor_secret": "abc"}},
            ),
        ])

    with pytest.raises(ImproperConfigurationError, match="pagination_cursor_ttl"):
        SQLSpecPlugin(
            AiosqliteConfig(
                connection_config={"database": ":memory:"},
                extension_config={"starlette": {"pagination_cursor_ttl": 30}},
            ),
            pagination_cursor_ttl=60,
        )

    with pytest.raises(ImproperConfigurationError, match="pagination_cursor_encrypted"):
        SQLSpecPlugin(
            AiosqliteConfig(
                connection_config={"database": ":memory:"},
                extension_config={"starlette": {"pagination_cursor_encrypted": False}},
            ),
            pagination_cursor_encrypted=True,
        )


def test_starlette_middleware_skips_inactive_routing_states() -> None:
    """Default single-database configs do not populate precomputed routing states."""
    plain_config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={
            "starlette": {
                "session_key": "plain_session",
                "connection_key": "plain_connection",
                "pool_key": "plain_pool",
            }
        },
    )
    active_config = AiosqliteConfig(
        bind_key="active", connection_config={"database": ":memory:"}, replicas=[{"database": ":memory:"}]
    )
    app = Starlette()
    plugin = SQLSpecPlugin([plain_config, active_config], app)
    lease = SQLSpecTransactionMiddleware(app, plugin._config_states)  # pyright: ignore[reportPrivateUsage]
    assert len(lease._routing_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert lease._routing_states[0].config is active_config  # pyright: ignore[reportPrivateUsage]

    plain_app = Starlette()
    plain_plugin = SQLSpecPlugin(plain_config, plain_app)
    plain_lease = SQLSpecTransactionMiddleware(plain_app, plain_plugin._config_states)  # pyright: ignore[reportPrivateUsage]
    assert plain_lease._routing_states == ()  # pyright: ignore[reportPrivateUsage]

    async def index(_request: Request) -> JSONResponse:
        return JSONResponse({"has_scope": RoutingContext.current() is not None})

    plain_route_app = Starlette(routes=[Route("/index", index)])
    SQLSpecPlugin(AiosqliteConfig(connection_config={"database": ":memory:"}), plain_route_app)
    with TestClient(plain_route_app) as client:
        assert client.get("/index").json() == {"has_scope": False}


@pytest.mark.parametrize(("path", "expected_error"), [("/ok", False), ("/fail", True)])
def test_deferred_session_streams_close_with_request_outcome(
    monkeypatch: pytest.MonkeyPatch, path: str, expected_error: bool
) -> None:
    """Deferred request sessions close their streams with the request's own error outcome."""
    from sqlspec.utils.deferred_session import DeferredAsyncSession

    recorded: list[bool] = []

    async def record_close(self: Any, *, error: bool = False) -> None:
        recorded.append(error)

    monkeypatch.setattr(DeferredAsyncSession, "_close_open_streams", record_close)

    async def ok_route(request: Request) -> JSONResponse:
        plugin.get_session(request)
        return JSONResponse({"ok": True})

    async def fail_route(request: Request) -> JSONResponse:
        plugin.get_session(request)
        raise RuntimeError("boom")

    app = Starlette(routes=[Route("/ok", ok_route), Route("/fail", fail_route)])
    plugin = SQLSpecPlugin(AiosqliteConfig(connection_config={"database": ":memory:"}), app)

    async def dispatch_while_handling_error(scope: Any, receive: Any, send: Any) -> None:
        try:
            raise LookupError("handled by an outer layer")
        except LookupError:
            await app(scope, receive, send)

    with TestClient(dispatch_while_handling_error, raise_server_exceptions=False) as client:
        client.get(path)
    assert recorded == [expected_error]
