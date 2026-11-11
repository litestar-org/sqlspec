"""Tests for Sanic extension request middleware."""

from contextlib import AbstractContextManager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.extensions.sanic import SQLSpecPlugin, set_request_tenant
from sqlspec.extensions.sanic._utils import has_context_value
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

pytestmark = pytest.mark.anyio


async def test_request_middleware_acquires_pooled_connection() -> None:
    """Request middleware should expose pooled connections on request.ctx."""
    connection = _make_connection()
    pool = object()
    config = _make_config(connection=connection)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=pool)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

    assert request.ctx.db_connection is connection
    config.provide_connection.assert_called_once_with(pool)


async def test_manual_response_middleware_cleans_connection_without_transaction() -> None:
    """Manual mode should release connection state without commit or rollback."""
    connection = _make_connection()
    config = _make_config(connection=connection)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(request, SimpleNamespace(status=200))  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_not_awaited()
    connection.rollback.assert_not_awaited()
    assert not has_context_value(request.ctx, "db_connection")
    assert not has_context_value(request.ctx, "db_session_instance")


async def test_autocommit_response_commits_success_status() -> None:
    """Autocommit mode should commit 2xx responses."""
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(request, SimpleNamespace(status=201))  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_awaited_once_with()
    connection.rollback.assert_not_awaited()


async def test_autocommit_response_rolls_back_error_status() -> None:
    """Autocommit mode should rollback non-success responses."""
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(request, SimpleNamespace(status=500))  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_not_awaited()
    connection.rollback.assert_awaited_once_with()


async def test_autocommit_include_redirect_commits_redirect_status() -> None:
    """autocommit_include_redirect should commit 3xx responses."""
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit_include_redirect")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=object())

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(request, SimpleNamespace(status=302))  # pyright: ignore[reportPrivateUsage]

    connection.commit.assert_awaited_once_with()
    connection.rollback.assert_not_awaited()


async def test_non_pooled_response_closes_connection() -> None:
    """Non-pooled configs should create and close one connection per request."""
    connection = _make_connection()
    config = _make_config(connection=connection, supports_connection_pooling=False)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    request = _make_request(pool=None)

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(request, SimpleNamespace(status=200))  # pyright: ignore[reportPrivateUsage]

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

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(request, SimpleNamespace(status=200))  # pyright: ignore[reportPrivateUsage]

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
    return config


def _make_request(*, pool: object | None) -> SimpleNamespace:
    app_ctx = SimpleNamespace()
    if pool is not None:
        app_ctx.db_pool = pool
    return SimpleNamespace(
        app=SimpleNamespace(ctx=app_ctx), ctx=SimpleNamespace(db_session_instance=object()), headers={}
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


@pytest.mark.parametrize("identity", ["a", "uuid", "bypass", "none", "awaitable"])
async def test_tenant_extractor_binds_and_resets_sanic_request(identity: str) -> None:
    from uuid import UUID

    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    identities: dict[str, str | UUID | TenantContextPayload | None] = {
        "a": "a",
        "uuid": UUID(int=1),
        "bypass": TenantContextPayload.bypass(),
        "none": None,
        "awaitable": "a",
    }
    raw = identities[identity]
    seen = []
    config = _make_config(connection=_make_connection())

    async def async_extractor(request: object) -> str:
        return "a"

    config.extension_config["sanic"]["tenant_extractor"] = (
        async_extractor if identity == "awaitable" else lambda request: raw
    )
    original = config.provide_connection

    def provide_connection(pool: object) -> object:
        seen.append(TenantContext.get())
        return original(pool)

    config.provide_connection = provide_connection
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    plugin.init_app(Sanic("tenant_" + uuid4().hex))
    request = _make_request(pool=object())
    with TenantContext.scope("outer"):
        await plugin._on_request(request)
        await plugin._on_response(request, SimpleNamespace(status=200))
        await plugin._on_response(request, SimpleNamespace(status=200))
        assert TenantContext.get_tenant_id() == "outer"
    expected = None if raw is None else raw if isinstance(raw, TenantContextPayload) else TenantContextPayload(raw)
    assert seen == [expected]


@pytest.mark.parametrize("failure", ["extractor", "acquire", "release", "complete"])
async def test_tenant_context_restores_after_sanic_failure(failure: str) -> None:
    from sqlspec.utils.tenant import TenantContext

    def extractor(request: object) -> str:
        if failure == "extractor":
            raise ValueError("auth")
        return "a"

    config = _make_config(connection=_make_connection())
    config.extension_config["sanic"]["tenant_extractor"] = extractor
    if failure == "acquire":
        config.provide_connection.side_effect = ValueError("acquire")
    if failure == "release":
        config.provide_connection.return_value.__aexit__ = AsyncMock(side_effect=ValueError("release"))
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    plugin.init_app(Sanic("failure_" + uuid4().hex))
    request = _make_request(pool=object())
    with TenantContext.scope("outer"):
        if failure in {"extractor", "acquire"}:
            with pytest.raises(ValueError):
                await plugin._on_request(request)
        else:
            await plugin._on_request(request)
            if failure == "complete":
                await plugin._on_request_complete(request=request)
            else:
                with pytest.raises(ValueError):
                    await plugin._on_response(request, SimpleNamespace(status=200))
        assert TenantContext.get_tenant_id() == "outer"


@pytest.mark.parametrize("status", [200, 500])
async def test_sanic_defers_tenant_connection_until_handler_sets_tenant(status: int) -> None:
    seen: list[TenantContextPayload | None] = []
    connection = _make_connection()
    config = _make_config(connection=connection, commit_mode="autocommit")
    config.driver_features["tenant_strategy"] = object()
    config.driver_type = lambda *, connection, statement_config, driver_features: SimpleNamespace(
        connection=connection, statement_config=statement_config, driver_features=driver_features
    )
    original = config.provide_connection

    def provide_connection(pool: object) -> object:
        seen.append(TenantContext.get())
        return original(pool)

    config.provide_connection = provide_connection
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    plugin.init_app(Sanic("deferred_" + uuid4().hex))

    untouched = _make_request(pool=object())
    await plugin._on_request(untouched)  # pyright: ignore[reportPrivateUsage]
    await plugin._on_response(untouched, SimpleNamespace(status=status))  # pyright: ignore[reportPrivateUsage]
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
    await plugin._on_response(active, SimpleNamespace(status=status))  # pyright: ignore[reportPrivateUsage]
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
    assert seen == {"a": TenantContextPayload("org-a")}
    set_request_tenant(request, "org-b", key="beta_db")
    resolved_conn = await plugin.get_connection_async(request, "beta_db")
    assert resolved_conn is conn_b
    assert seen["b"] == TenantContextPayload("org-b")
    await plugin._on_response(request, SimpleNamespace(status=200))  # pyright: ignore[reportPrivateUsage]


async def test_sanic_rejects_non_callable_tenant_extractor() -> None:
    config = _make_config(connection=_make_connection())
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
    with pytest.raises(TenantContextError, match="get_session_async"):
        plugin.get_connection(request)
    await plugin._on_response(request, SimpleNamespace(status=401))  # pyright: ignore[reportPrivateUsage]
