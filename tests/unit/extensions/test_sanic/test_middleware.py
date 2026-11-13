"""Tests for Sanic extension request middleware."""

from contextlib import AbstractContextManager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic import SQLSpecPlugin, set_request_tenant
from sqlspec.extensions.sanic._utils import has_context_value
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

pytestmark = pytest.mark.anyio


async def _touch_connections(plugin: Any, request: Any) -> None:
    """Check out every request connection, as a handler's first database access does."""
    for state in plugin._config_states:
        if not state.disable_di:
            await plugin.get_connection_async(request, state.session_key)


async def test_request_middleware_acquires_pooled_connection() -> None:
    """Request middleware should expose pooled connections on request.ctx."""
    connection = _make_connection()
    pool = object()
    config = _make_config(connection=connection)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=pool)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)

    assert request.ctx.db_connection is connection
    config.provide_connection.assert_called_once_with(pool)


async def test_manual_response_middleware_cleans_connection_without_transaction() -> None:
    """Manual mode should release connection state on lifecycle response without commit or rollback."""
    connection = _make_connection()
    config = _make_config(connection=connection)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())
    response = SimpleNamespace(status=200)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)
    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_not_awaited()
    connection.rollback.assert_not_awaited()
    assert not has_context_value(request.ctx, "db_connection")
    assert not has_context_value(request.ctx, "db_session_instance")


async def test_autocommit_response_commits_success_status() -> None:
    """Autocommit mode should commit 2xx responses on lifecycle response."""
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())
    response = SimpleNamespace(status=201)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)
    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_awaited_once_with()
    connection.rollback.assert_not_awaited()


async def test_autocommit_response_rolls_back_error_status() -> None:
    """Autocommit mode should rollback non-success responses on lifecycle response."""
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())
    response = SimpleNamespace(status=500)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)
    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_not_awaited()
    connection.rollback.assert_awaited_once_with()


async def test_autocommit_include_redirect_commits_redirect_status() -> None:
    """autocommit_include_redirect should commit 3xx responses on lifecycle response."""
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit_include_redirect")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())
    response = SimpleNamespace(status=302)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)
    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_awaited_once_with()
    connection.rollback.assert_not_awaited()


async def test_non_pooled_response_closes_connection() -> None:
    """Non-pooled configs should create and close one connection per request."""
    connection = _make_connection()
    config = _make_config(connection=connection, supports_connection_pooling=False)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=None)
    response = SimpleNamespace(status=200)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)
    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    config.create_connection.assert_awaited_once_with()
    connection.close.assert_awaited_once_with()
    assert not has_context_value(request.ctx, "db_connection")


async def test_request_middleware_wraps_sync_connection_manager() -> None:
    """Request middleware should support sync adapter connection managers."""
    connection = _make_connection()
    config = _make_config(connection=connection)
    manager = _SyncConnectionManager(connection)
    config.provide_connection = MagicMock(return_value=manager)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())
    response = SimpleNamespace(status=200)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    await _touch_connections(plugin, request)
    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    assert manager.entered == 1
    assert manager.exited == 1


def test_init_app_registers_request_middleware() -> None:
    """init_app should register request and response middleware when DI is enabled."""
    app = Sanic(f"SQLSpecMiddleware{uuid4().hex}")
    config = _make_config(connection=_make_connection())
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))

    plugin.init_app(app)

    assert len(app.request_middleware) == 1
    assert len(app.response_middleware) == 1


def test_disable_di_skips_request_middleware_registration() -> None:
    """disable_di should leave pool lifecycle intact but skip request management."""
    app = Sanic(f"SQLSpecMiddlewareDisabled{uuid4().hex}")
    config = _make_config(connection=_make_connection(), disable_di=True)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))

    plugin.init_app(app)

    assert len(app.request_middleware) == 0
    assert len(app.response_middleware) == 0
    assert "server.init.before" in app.signal_router.name_index


def _make_connection() -> MagicMock:
    connection = MagicMock()
    connection.commit = AsyncMock()
    connection.rollback = AsyncMock()
    connection.close = AsyncMock()
    return connection


def _make_config(
    *,
    connection: MagicMock,
    commit_mode: str = "manual",
    supports_connection_pooling: bool = True,
    disable_di: bool = False,
) -> MagicMock:
    config = MagicMock()
    config.supports_connection_pooling = supports_connection_pooling
    config.extension_config = {
        "sanic": {"commit_mode": commit_mode, "disable_di": disable_di, "enable_correlation_middleware": False}
    }
    config.provide_connection = MagicMock(return_value=_AsyncConnectionManager(connection))
    config.create_connection = AsyncMock(return_value=connection)
    config.statement_config = {}
    config.driver_features = {}
    config._prepare_driver.side_effect = lambda driver: driver
    return config


def _make_request(*, pool: object | None) -> SimpleNamespace:
    app_ctx = SimpleNamespace()
    if pool is not None:
        app_ctx.db_pool = pool
    return SimpleNamespace(
        app=SimpleNamespace(ctx=app_ctx),
        ctx=SimpleNamespace(
            db_session_instance=SimpleNamespace(
                observability=SimpleNamespace(is_idle=True), _detach_connection=lambda: None
            )
        ),
        headers={},
    )


class _AsyncConnectionManager:
    def __init__(self, connection: MagicMock) -> None:
        self.connection = connection
        self.entered = 0
        self.exited = 0

    async def __aenter__(self) -> MagicMock:
        self.entered += 1
        return self.connection

    async def __aexit__(self, *_: object) -> None:
        self.exited += 1


class _SyncConnectionManager(AbstractContextManager[MagicMock]):
    def __init__(self, connection: MagicMock) -> None:
        self.connection = connection
        self.entered = 0
        self.exited = 0

    def __enter__(self) -> MagicMock:
        self.entered += 1
        return self.connection

    def __exit__(self, *_: object) -> None:
        self.exited += 1


@pytest.mark.parametrize("failure", ["extractor", "acquire", "release", "complete"])
async def test_tenant_context_restores_after_sanic_failure(failure: str) -> None:
    from sqlspec.utils.tenant import TenantContext

    def extractor(request: object) -> str:
        if failure == "extractor":
            raise ValueError("auth")
        return "a"

    config = _make_config(connection=_make_connection())
    config.driver_features["tenant_strategy"] = object()
    config.extension_config["sanic"]["tenant_extractor"] = extractor
    if failure == "acquire":
        config.provide_connection.side_effect = ValueError("acquire")
    if failure == "release":
        config.provide_connection.return_value.__aexit__ = AsyncMock(side_effect=ValueError("release"))
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    plugin.init_app(Sanic("failure_" + uuid4().hex))
    request = _make_request(pool=object())
    with TenantContext.scope("outer"):
        await plugin._on_request(request)
        if failure in {"extractor", "acquire"}:
            with pytest.raises(ValueError):
                await plugin.get_connection_async(request)
            await plugin._on_lifecycle_response(request=request, response=SimpleNamespace(status=500))
        elif failure == "complete":
            await plugin.get_connection_async(request)
            await plugin._on_request_complete(request=request)
        else:
            await plugin.get_connection_async(request)
            with pytest.raises(ValueError):
                await plugin._on_lifecycle_response(request=request, response=SimpleNamespace(status=200))
        assert TenantContext.get_tenant_id() == "outer"


@pytest.mark.parametrize("status", [200, 500])
async def test_sanic_defers_tenant_connection_until_handler_sets_tenant(status: int) -> None:
    seen: list[TenantContextPayload | None] = []
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit")
    config.driver_features["tenant_strategy"] = object()
    config.driver_type = lambda *, connection, statement_config, driver_features: SimpleNamespace(
        connection=connection,
        statement_config=statement_config,
        driver_features=driver_features,
        observability=SimpleNamespace(is_idle=True),
        commit=connection.commit,
        rollback=connection.rollback,
        _detach_connection=lambda: None,
    )
    original = config.provide_connection

    def provide_connection(pool: object) -> object:
        seen.append(TenantContext.get())
        return original(pool)

    config.provide_connection = provide_connection
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    plugin.init_app(Sanic("deferred_" + uuid4().hex))

    untouched = _make_request(pool=object())
    response = SimpleNamespace(status=status)
    await plugin._on_request(untouched)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(untouched, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=untouched, response=response)  # pyright: ignore[reportPrivateUsage]
    assert seen == []
    connection.commit.assert_not_awaited()
    connection.rollback.assert_not_awaited()

    active = _make_request(pool=object())
    active.ctx = SimpleNamespace()
    await plugin._on_request(active)  # pyright: ignore[reportPrivateUsage]
    assert seen == []
    set_request_tenant(active, "acme", variables={"role": "editor"})
    session = await plugin.get_session_async(active)
    assert session.connection is connection
    assert seen == [TenantContextPayload("acme", {"role": "editor"})]
    await plugin._on_response(active, response)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_lifecycle_response(request=active, response=response)  # pyright: ignore[reportPrivateUsage]
    if status == 200:
        connection.commit.assert_awaited_once_with()
        connection.rollback.assert_not_awaited()
    else:
        connection.commit.assert_not_awaited()
        connection.rollback.assert_awaited_once_with()


async def test_sanic_supports_independent_per_config_tenant_extractors_and_direct_configs() -> None:
    seen: dict[str, TenantContextPayload | None] = {}
    conn_a = _make_connection()
    conn_b = _make_connection()
    cfg_a = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={
            "sanic": {
                "session_key": "alpha_db",
                "connection_key": "alpha_conn",
                "pool_key": "alpha_pool",
                "tenant_extractor": lambda request: request.headers.get("x-alpha"),
            }
        },
    )
    cfg_b = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={
            "sanic": {
                "session_key": "beta_db",
                "connection_key": "beta_conn",
                "pool_key": "beta_pool",
                "tenant_extractor": lambda request: request.headers.get("x-beta"),
            }
        },
    )
    cfg_a.driver_features["tenant_strategy"] = object()  # type: ignore[typeddict-unknown-key]
    cfg_b.driver_features["tenant_strategy"] = object()  # type: ignore[typeddict-unknown-key]

    def provide_a(_pool: object) -> object:
        seen["a"] = TenantContext.get()
        return _AsyncConnectionManager(conn_a)

    def provide_b(_pool: object) -> object:
        seen["b"] = TenantContext.get()
        return _AsyncConnectionManager(conn_b)

    cfg_a.provide_connection = provide_a  # type: ignore[method-assign]
    cfg_b.provide_connection = provide_b  # type: ignore[method-assign]

    single = SQLSpecPlugin(cfg_a)
    assert single.config == [cfg_a]
    assert single.get_config() is cfg_a

    plugin = SQLSpecPlugin([cfg_a, cfg_b])
    assert plugin.get_config("alpha_db") is cfg_a
    assert plugin.get_config("beta_db") is cfg_b
    with pytest.raises(KeyError):
        plugin.get_config()

    request = SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace(alpha_pool=object(), beta_pool=object())),
        ctx=SimpleNamespace(),
        headers={"x-alpha": "org-a"},
    )
    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    assert seen == {}
    await plugin.get_connection_async(request, "alpha_db")
    assert seen == {"a": TenantContextPayload("org-a")}
    set_request_tenant(request, "org-b", key="beta_db")
    resolved_conn = await plugin.get_connection_async(request, "beta_db")
    assert resolved_conn is conn_b
    assert seen["b"] == TenantContextPayload("org-b")
    await plugin._on_response(request, SimpleNamespace(status=200))  # pyright: ignore[reportPrivateUsage]


async def test_sanic_rejects_non_callable_tenant_extractor() -> None:
    config = _make_config(connection=_make_connection())
    config.driver_features["tenant_strategy"] = object()
    config.extension_config["sanic"]["tenant_extractor"] = "untrusted"
    with pytest.raises(ImproperConfigurationError, match="must be callable"):
        SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))


async def test_sanic_sync_get_connection_raises_when_tenant_connection_is_deferred() -> None:
    config = _make_config(connection=_make_connection())
    config.driver_features["tenant_strategy"] = object()
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace(db_pool=object())), ctx=SimpleNamespace(), headers={}
    )
    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(ImproperConfigurationError, match="get_connection_async"):
        plugin.get_connection(request)
    await plugin._on_response(request, SimpleNamespace(status=401))  # pyright: ignore[reportPrivateUsage]


def test_sanic_request_connection_is_checked_out_on_first_access_only(tmp_path: Any) -> None:
    import sqlite3

    from sanic import response

    from sqlspec.adapters.aiosqlite import AiosqliteConfig

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
        connection_config={"database": database}, extension_config={"sanic": {"commit_mode": "autocommit"}}
    )
    app = Sanic(f"SQLSpecLazy{uuid4().hex}")
    app.config.USE_UVLOOP = False
    plugin = SQLSpecPlugin(config, app)

    async def nodb(request: Any) -> Any:
        return response.json({"ok": True})

    async def write(request: Any, status: int) -> Any:
        session = await plugin.get_session_async(request)
        await session.begin()
        await session.execute("INSERT INTO items VALUES (:status)", {"status": status})
        assert await plugin.get_session_async(request) is session
        assert await plugin.get_connection_async(request) is session.connection
        return response.json({"ok": True}, status=status)

    app.add_route(nodb, "/nodb")
    app.add_route(write, "/write/<status:int>")
    try:
        assert app.test_client.get("/nodb")[1].status == 200
        assert checkouts == []
        assert app.test_client.get("/write/201")[1].status == 201
        assert checkouts == [1]
        assert app.test_client.get("/write/500")[1].status == 500
        assert checkouts == [1, 1]
    finally:
        Sanic.unregister_app(app)
    reader = sqlite3.connect(database)
    try:
        assert reader.execute("SELECT status FROM items").fetchall() == [(201,)]
    finally:
        reader.close()
