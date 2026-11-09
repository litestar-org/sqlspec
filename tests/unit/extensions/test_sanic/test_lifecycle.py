"""Tests for Sanic extension lifecycle listeners."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.extensions.sanic._utils import has_context_value

pytestmark = pytest.mark.anyio


async def test_startup_creates_pool_on_app_context() -> None:
    """before_server_start should create configured pools on app.ctx."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace())

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]

    assert app.ctx.db_pool is pool
    config.create_pool.assert_awaited_once_with()


async def test_startup_is_idempotent_for_existing_pool() -> None:
    """before_server_start should not recreate a pool already present on app.ctx."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace(db_pool=pool))

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]

    assert app.ctx.db_pool is pool
    config.create_pool.assert_not_awaited()


async def test_shutdown_closes_pool_and_removes_app_context_value() -> None:
    """after_server_stop should close configured pools and remove app.ctx storage."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace(db_pool=pool))

    await plugin._after_server_stop(app)  # pyright: ignore[reportPrivateUsage]

    config.close_pool.assert_awaited_once_with()
    assert not has_context_value(app.ctx, "db_pool")


async def test_lifecycle_wraps_sync_pool_hooks() -> None:
    """Lifecycle listeners should support sync adapter pool hooks."""
    pool = object()
    config = _make_config(pool=pool)
    config.create_pool = MagicMock(return_value=pool)
    config.close_pool = MagicMock()
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace())

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]
    await plugin._after_server_stop(app)  # pyright: ignore[reportPrivateUsage]

    config.create_pool.assert_called_once_with()
    config.close_pool.assert_called_once_with()
    assert not has_context_value(app.ctx, "db_pool")


def test_init_app_registers_server_lifecycle_listeners() -> None:
    """init_app should register Sanic startup and shutdown listeners."""
    app = Sanic(f"SQLSpecLifecycle{uuid4().hex}")
    plugin = SQLSpecPlugin(MagicMock(configs={}))

    plugin.init_app(app)

    assert "server.init.before" in app.signal_router.name_index
    assert "server.shutdown.after" in app.signal_router.name_index


class _AsyncConnectionManagerFake:
    """Fake connection manager tracking exit arguments."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.exit_args: tuple[Any, ...] | None = None

    async def __aenter__(self) -> Any:
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.exit_args = (exc_type, exc_val, exc_tb)


def _make_config(*, pool: object) -> MagicMock:
    config = MagicMock()
    config.supports_connection_pooling = True
    config.extension_config = {"sanic": {}}
    config.create_pool = AsyncMock(return_value=pool)
    config.close_pool = AsyncMock()
    return config


def test_init_app_registers_lifecycle_exception_signal() -> None:
    """init_app should register http.lifecycle.exception signal listener."""
    app = Sanic(f"SQLSpecLifecycleSignal{uuid4().hex}")
    config = _make_config(pool=object())
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))

    plugin.init_app(app)

    assert "http.lifecycle.exception" in app.signal_router.name_index


async def test_on_lifecycle_exception_ignores_none_request() -> None:
    """_on_lifecycle_exception should handle None request without error."""
    plugin = SQLSpecPlugin(MagicMock(configs={}))
    await plugin._on_lifecycle_exception(None, ValueError("test"))


async def test_on_lifecycle_exception_records_exception() -> None:
    """_on_lifecycle_exception should store the exception in request context."""
    from sqlspec.extensions.sanic._utils import get_context_value

    plugin = SQLSpecPlugin(MagicMock(configs={}))
    request = SimpleNamespace(ctx=SimpleNamespace())
    exc = ValueError("boom")

    await plugin._on_lifecycle_exception(request, exc)

    assert get_context_value(request.ctx, "sqlspec_request_exception") is exc


async def test_release_request_connection_forwards_exception() -> None:
    """_release_request_connection should forward recorded exception to context manager exit."""
    from sqlspec.extensions.sanic._utils import set_context_value

    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    config_state = plugin._config_states[0]

    conn = MagicMock()
    cm = _AsyncConnectionManagerFake(conn)

    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)
    set_context_value(request.ctx, plugin._connection_manager_key(config_state), cm)

    try:
        raise ValueError("handler crash")
    except ValueError as e:
        exc = e
    set_context_value(request.ctx, "sqlspec_request_exception", exc)

    await plugin._release_request_connection(request, config_state)

    assert cm.exit_args is not None
    assert cm.exit_args[0] is ValueError
    assert cm.exit_args[1] is exc
    assert cm.exit_args[2] is not None


async def test_release_request_connection_clean_exit_without_exception() -> None:
    """_release_request_connection should pass None exit args when no exception occurred."""
    from sqlspec.extensions.sanic._utils import set_context_value

    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    config_state = plugin._config_states[0]

    conn = MagicMock()
    cm = _AsyncConnectionManagerFake(conn)

    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)
    set_context_value(request.ctx, plugin._connection_manager_key(config_state), cm)

    await plugin._release_request_connection(request, config_state)

    assert cm.exit_args == (None, None, None)


async def test_on_request_acquisition_failure_records_exception_and_releases() -> None:
    """_on_request should record acquisition exception and forward it when releasing earlier connections."""
    from sqlspec.extensions.sanic._utils import get_context_value

    conn1 = MagicMock()
    cm1 = _AsyncConnectionManagerFake(conn1)

    config1 = _make_config(pool=object())
    config1.provide_connection = MagicMock(return_value=cm1)
    config1.extension_config = {"sanic": {"connection_key": "conn1", "session_key": "sess1", "pool_key": "pool1"}}

    config2 = _make_config(pool=object())
    config2.provide_connection = MagicMock(side_effect=RuntimeError("pool 2 acquire failed"))
    config2.extension_config = {"sanic": {"connection_key": "conn2", "session_key": "sess2", "pool_key": "pool2"}}

    plugin = SQLSpecPlugin(MagicMock(configs={"db1": config1, "db2": config2}))

    request = SimpleNamespace(
        ctx=SimpleNamespace(), app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())), headers={}
    )

    with pytest.raises(RuntimeError) as exc_info:
        await plugin._on_request(request)

    assert "pool 2 acquire failed" in str(exc_info.value)
    assert get_context_value(request.ctx, "sqlspec_request_exception") is exc_info.value
    assert cm1.exit_args is not None
    assert cm1.exit_args[0] is RuntimeError
    assert cm1.exit_args[1] is exc_info.value


def test_init_app_registers_lifecycle_complete_signal() -> None:
    """init_app should register http.lifecycle.complete signal listener."""
    app = Sanic(f"SQLSpecLifecycleCompleteSignal{uuid4().hex}")
    config = _make_config(pool=object())
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))

    plugin.init_app(app)

    assert "http.lifecycle.complete" in app.signal_router.name_index


async def test_on_request_cancelled_error_mid_acquisition_releases_and_restores() -> None:
    """_on_request must catch CancelledError, release already-acquired connections, and restore contexts."""
    import asyncio

    from sqlspec.core.sqlcommenter import SQLCommenterContext
    from sqlspec.extensions.sanic._utils import get_context_value
    from sqlspec.utils.correlation import CorrelationContext

    conn1 = MagicMock()
    cm1 = _AsyncConnectionManagerFake(conn1)

    config1 = _make_config(pool=object())
    config1.provide_connection = MagicMock(return_value=cm1)
    config1.statement_config = SimpleNamespace(enable_sqlcommenter=True)
    config1.extension_config = {
        "sanic": {
            "connection_key": "conn1",
            "session_key": "sess1",
            "pool_key": "pool1",
            "enable_correlation_middleware": True,
            "enable_sqlcommenter_middleware": True,
        }
    }

    config2 = _make_config(pool=object())
    config2.provide_connection = MagicMock(side_effect=asyncio.CancelledError("cancelled mid acquire"))
    config2.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config2.extension_config = {"sanic": {"connection_key": "conn2", "session_key": "sess2", "pool_key": "pool2"}}

    plugin = SQLSpecPlugin(MagicMock(configs={"db1": config1, "db2": config2}))
    CorrelationContext.set("outer-cid")
    SQLCommenterContext.set({"framework": "outer"})
    try:
        conn_info = SimpleNamespace(ctx=SimpleNamespace())
        request = SimpleNamespace(
            ctx=SimpleNamespace(),
            conn_info=conn_info,
            app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())),
            headers={"x-request-id": "req-cid"},
            path="/items",
        )

        with pytest.raises(asyncio.CancelledError):
            await plugin._on_request(request)

        assert cm1.exit_args is not None
        assert cm1.exit_args[0] is asyncio.CancelledError
        assert isinstance(get_context_value(request.ctx, "sqlspec_request_exception"), asyncio.CancelledError)
        assert not has_context_value(request.ctx, "conn1")
        assert CorrelationContext.get() == "outer-cid"
        assert SQLCommenterContext.get() == {"framework": "outer"}
    finally:
        CorrelationContext.set(None)
        SQLCommenterContext.set(None)


async def test_on_request_complete_releases_residual_connection_on_cancellation_and_is_idempotent() -> None:
    """http.lifecycle.complete listener exits residual connections with CancelledError and is idempotent."""
    import asyncio

    from sqlspec.utils.correlation import CorrelationContext

    conn = MagicMock()
    conn.rollback = AsyncMock()
    cm = _AsyncConnectionManagerFake(conn)

    config = _make_config(pool=object())
    config.provide_connection = MagicMock(return_value=cm)
    config.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config.extension_config = {"sanic": {"commit_mode": "autocommit", "enable_correlation_middleware": True}}

    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    CorrelationContext.set("before-req")
    try:
        conn_info = SimpleNamespace(ctx=SimpleNamespace())
        request = SimpleNamespace(
            ctx=SimpleNamespace(),
            conn_info=conn_info,
            app=SimpleNamespace(ctx=SimpleNamespace(db_pool=object())),
            headers={"x-request-id": "aborted-cid"},
            path="/cancel",
        )

        await plugin._on_request(request)
        assert CorrelationContext.get() == "aborted-cid"

        await plugin._on_request_complete(conn_info=conn_info)

        conn.rollback.assert_awaited_once_with()
        assert cm.exit_args is not None
        assert cm.exit_args[0] is asyncio.CancelledError
        assert not has_context_value(request.ctx, "db_connection")
        assert CorrelationContext.get() == "before-req"

        cm2 = _AsyncConnectionManagerFake(MagicMock(commit=AsyncMock(), rollback=AsyncMock()))
        config.provide_connection = MagicMock(return_value=cm2)
        conn_info2 = SimpleNamespace(ctx=SimpleNamespace())
        request2 = SimpleNamespace(
            ctx=SimpleNamespace(),
            conn_info=conn_info2,
            app=SimpleNamespace(ctx=SimpleNamespace(db_pool=object())),
            headers={"x-request-id": "clean-cid"},
            path="/ok",
        )
        response2 = SimpleNamespace(status=200, headers={})

        await plugin._on_request(request2)
        await plugin._on_response(request2, response2)
        assert cm2.exit_args == (None, None, None)

        cm2.exit_args = ("sentinel", None, None)
        await plugin._on_request_complete(conn_info=conn_info2)
        assert cm2.exit_args == ("sentinel", None, None)
    finally:
        CorrelationContext.set(None)


async def test_finalize_request_connection_rolls_back_when_exception_mapped_to_200() -> None:
    """autocommit mode must roll back rather than commit when an exception was recorded despite 200 status."""
    from sqlspec.extensions.sanic._utils import set_context_value

    conn = MagicMock()
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    cm = _AsyncConnectionManagerFake(conn)

    config = _make_config(pool=object())
    config.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config.extension_config = {"sanic": {"commit_mode": "autocommit"}}
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    config_state = plugin._config_states[0]

    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)
    set_context_value(request.ctx, plugin._connection_manager_key(config_state), cm)
    err = ValueError("mapped to 200")
    await plugin._on_lifecycle_exception(request, err)

    response = SimpleNamespace(status=200, headers={})
    await plugin._finalize_request_connection(request, response, config_state)

    conn.commit.assert_not_awaited()
    conn.rollback.assert_awaited_once_with()
    assert cm.exit_args is not None
    assert cm.exit_args[0] is ValueError
    assert cm.exit_args[1] is err
