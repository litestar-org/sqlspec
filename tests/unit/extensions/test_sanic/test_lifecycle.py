"""Tests for Sanic extension lifecycle listeners."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.extensions.sanic._utils import get_or_create_session, has_context_value, set_context_value

pytestmark = pytest.mark.anyio


async def _touch_connections(plugin: Any, request: Any) -> None:
    """Check out every request connection, as a handler's first database access does."""
    for state in plugin._config_states:
        if not state.disable_di:
            await plugin.get_connection_async(request, state.session_key)


async def test_startup_creates_pool_on_app_context() -> None:
    """before_server_start should create configured pools on app.ctx."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace())

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]

    assert app.ctx.db_pool is pool
    config.create_pool.assert_awaited_once_with()


async def test_startup_is_idempotent_for_existing_pool() -> None:
    """before_server_start should not recreate a pool already present on app.ctx."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace(db_pool=pool))

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]

    assert app.ctx.db_pool is pool
    config.create_pool.assert_not_awaited()


async def test_shutdown_closes_pool_and_removes_app_context_value() -> None:
    """after_server_stop should close configured pools and remove app.ctx storage."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
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
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace())

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]
    await plugin._after_server_stop(app)  # pyright: ignore[reportPrivateUsage]

    config.create_pool.assert_called_once_with()
    config.close_pool.assert_called_once_with()
    assert not has_context_value(app.ctx, "db_pool")


def test_init_app_registers_server_lifecycle_listeners() -> None:
    """init_app should register Sanic startup and shutdown listeners."""
    app = Sanic(f"SQLSpecLifecycle{uuid4().hex}")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={}))

    plugin.init_app(app)

    assert "server.init.before" in app.signal_router.name_index
    assert "server.shutdown.after" in app.signal_router.name_index


class _AsyncConnectionManagerFake:
    """Fake connection manager tracking exit arguments."""

    def __init__(self, connection: Any, raise_on_exit: BaseException | None = None) -> None:
        self.connection = connection
        self.raise_on_exit = raise_on_exit
        self.exit_args: tuple[Any, ...] | None = None

    async def __aenter__(self) -> Any:
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.exit_args = (exc_type, exc_val, exc_tb)
        if self.raise_on_exit is not None:
            raise self.raise_on_exit


def _make_config(*, pool: object) -> MagicMock:
    config = MagicMock()
    config.driver_features = {}
    config.supports_connection_pooling = True
    config.extension_config = {"sanic": {}}
    config.create_pool = AsyncMock(return_value=pool)
    config.close_pool = AsyncMock()
    return config


def test_init_app_registers_lifecycle_exception_signal() -> None:
    """init_app should register http.lifecycle.exception signal listener."""
    app = Sanic(f"SQLSpecLifecycleSignal{uuid4().hex}")
    config = _make_config(pool=object())
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))

    plugin.init_app(app)

    assert "http.lifecycle.exception" in app.signal_router.name_index


async def test_on_lifecycle_exception_ignores_none_request() -> None:
    """_on_lifecycle_exception should handle None request without error."""
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={}))
    await plugin._on_lifecycle_exception(None, ValueError("test"))


async def test_on_lifecycle_exception_records_exception() -> None:
    """_on_lifecycle_exception should store the exception in request context."""
    from sqlspec.extensions.sanic._utils import get_context_value

    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={}))
    request = SimpleNamespace(ctx=SimpleNamespace())
    exc = ValueError("boom")

    await plugin._on_lifecycle_exception(request, exc)

    assert get_context_value(request.ctx, "sqlspec_request_exception") is exc


async def test_release_request_connection_forwards_exception() -> None:
    """_release_request_connection should forward recorded exception to context manager exit."""
    from sqlspec.extensions.sanic._utils import set_context_value

    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
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
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    config_state = plugin._config_states[0]

    conn = MagicMock()
    cm = _AsyncConnectionManagerFake(conn)

    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)
    set_context_value(request.ctx, plugin._connection_manager_key(config_state), cm)

    await plugin._release_request_connection(request, config_state)

    assert cm.exit_args == (None, None, None)


def test_init_app_registers_lifecycle_complete_signal() -> None:
    """init_app should register http.lifecycle.complete signal listener."""
    app = Sanic(f"SQLSpecLifecycleCompleteSignal{uuid4().hex}")
    config = _make_config(pool=object())
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))

    plugin.init_app(app)

    assert "http.lifecycle.complete" in app.signal_router.name_index


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

    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
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

        await _touch_connections(plugin, request)
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

        await _touch_connections(plugin, request2)
        await plugin._on_response(request2, response2)
        assert cm2.exit_args is None
        assert has_context_value(request2.ctx, "db_connection")

        await plugin._on_lifecycle_response(request=request2, response=response2)
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
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
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


async def test_finalize_request_connection_commit_failure_rolls_back_and_forwards_exception_to_exit() -> None:
    """Commit failure during autocommit finalization rolls back and passes exception to __aexit__."""
    import sqlite3

    from sqlspec.extensions.sanic._utils import set_context_value

    commit_err = sqlite3.IntegrityError("FOREIGN KEY constraint failed")
    conn = MagicMock()
    conn.commit = AsyncMock(side_effect=commit_err)
    conn.rollback = AsyncMock()
    cm = _AsyncConnectionManagerFake(conn)

    config = _make_config(pool=object())
    config.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config.extension_config = {"sanic": {"commit_mode": "autocommit"}}
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    config_state = plugin._config_states[0]

    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)
    set_context_value(request.ctx, plugin._connection_manager_key(config_state), cm)

    response = SimpleNamespace(status=200, headers={})
    with pytest.raises(sqlite3.IntegrityError) as exc_info:
        await plugin._finalize_request_connection(request, response, config_state)

    assert exc_info.value is commit_err
    conn.commit.assert_awaited_once_with()
    conn.rollback.assert_awaited_once_with()
    assert cm.exit_args is not None
    assert cm.exit_args[0] is sqlite3.IntegrityError
    assert cm.exit_args[1] is commit_err
    assert cm.exit_args[2] is not None
    assert not has_context_value(request.ctx, config_state.connection_key)


async def test_finalize_request_connection_rollback_cancellation_forwards_to_exit() -> None:
    """Rollback cancellation during autocommit finalization passes CancelledError to __aexit__."""
    import asyncio

    from sqlspec.extensions.sanic._utils import set_context_value

    cancel_err = asyncio.CancelledError("cancelled on rollback")
    conn = MagicMock()
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock(side_effect=cancel_err)
    cm = _AsyncConnectionManagerFake(conn)

    config = _make_config(pool=object())
    config.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config.extension_config = {"sanic": {"commit_mode": "autocommit"}}
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    config_state = plugin._config_states[0]

    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)
    set_context_value(request.ctx, plugin._connection_manager_key(config_state), cm)

    response = SimpleNamespace(status=500, headers={})
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await plugin._finalize_request_connection(request, response, config_state)

    assert exc_info.value is cancel_err
    conn.commit.assert_not_awaited()
    conn.rollback.assert_awaited_once_with()
    assert cm.exit_args is not None
    assert cm.exit_args[0] is asyncio.CancelledError
    assert cm.exit_args[1] is cancel_err


async def test_on_response_multi_db_isolates_failure_releases_sibling_and_restores_contexts() -> None:
    """_on_response isolates per-target failures so sibling connections and observability contexts release."""
    import asyncio

    from sqlspec.core.sqlcommenter import SQLCommenterContext
    from sqlspec.utils.correlation import CorrelationContext

    conn1 = MagicMock(commit=AsyncMock(), rollback=AsyncMock())
    cm1 = _AsyncConnectionManagerFake(conn1)
    conn2 = MagicMock(commit=AsyncMock(side_effect=RuntimeError("db2 commit boom")), rollback=AsyncMock())
    cm2 = _AsyncConnectionManagerFake(conn2)

    config1 = _make_config(pool=object())
    config1.provide_connection = MagicMock(return_value=cm1)
    config1.statement_config = SimpleNamespace(enable_sqlcommenter=True)
    config1.extension_config = {
        "sanic": {
            "commit_mode": "autocommit",
            "connection_key": "conn1",
            "session_key": "sess1",
            "pool_key": "pool1",
            "enable_correlation_middleware": True,
            "enable_sqlcommenter_middleware": True,
        }
    }

    config2 = _make_config(pool=object())
    config2.provide_connection = MagicMock(return_value=cm2)
    config2.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config2.extension_config = {
        "sanic": {"commit_mode": "autocommit", "connection_key": "conn2", "session_key": "sess2", "pool_key": "pool2"}
    }

    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"db1": config1, "db2": config2}))
    CorrelationContext.set("outer-cid")
    SQLCommenterContext.set({"framework": "outer"})
    try:
        conn_info = SimpleNamespace(ctx=SimpleNamespace())
        request = SimpleNamespace(
            ctx=SimpleNamespace(),
            conn_info=conn_info,
            app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())),
            headers={"x-request-id": "req-cid"},
            path="/multi",
        )
        response = SimpleNamespace(status=200, headers={})

        await plugin._on_request(request)

        await _touch_connections(plugin, request)
        await plugin._on_response(request, response)
        assert has_context_value(request.ctx, "conn1")
        assert has_context_value(request.ctx, "conn2")
        assert CorrelationContext.get() == "outer-cid"
        assert SQLCommenterContext.get() == {"framework": "outer"}

        with pytest.raises(RuntimeError, match="db2 commit boom"):
            await plugin._on_lifecycle_response(request=request, response=response)

        conn2.rollback.assert_awaited_once_with()
        assert cm2.exit_args is not None
        assert cm2.exit_args[0] is RuntimeError
        conn1.commit.assert_awaited_once_with()
        assert cm1.exit_args == (None, None, None)
        assert not has_context_value(request.ctx, "conn1")
        assert not has_context_value(request.ctx, "conn2")
        assert not has_context_value(conn_info.ctx, "_sqlspec_active_request")

        cm1_b = _AsyncConnectionManagerFake(
            MagicMock(commit=AsyncMock(side_effect=asyncio.CancelledError("db1 cancel")), rollback=AsyncMock())
        )
        cm2_b = _AsyncConnectionManagerFake(
            MagicMock(commit=AsyncMock(side_effect=RuntimeError("db2 error")), rollback=AsyncMock())
        )
        config1.provide_connection = MagicMock(return_value=cm1_b)
        config2.provide_connection = MagicMock(return_value=cm2_b)
        request_b = SimpleNamespace(
            ctx=SimpleNamespace(),
            conn_info=SimpleNamespace(ctx=SimpleNamespace()),
            app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())),
            headers={"x-request-id": "req-cid-2"},
            path="/multi-cancel",
        )
        resp_b = SimpleNamespace(status=200, headers={})
        await plugin._on_request(request_b)
        await _touch_connections(plugin, request_b)
        await plugin._on_response(request_b, resp_b)
        with pytest.raises(asyncio.CancelledError):
            await plugin._on_lifecycle_response(request=request_b, response=resp_b)
        assert cm1_b.exit_args is not None
        assert cm1_b.exit_args[0] is asyncio.CancelledError
    finally:
        CorrelationContext.set(None)
        SQLCommenterContext.set(None)


async def test_on_request_complete_multi_db_isolates_release_failure_and_restores_contexts() -> None:
    """_on_request_complete releases sibling residual connections and restores contexts if one exit raises."""
    import asyncio

    from sqlspec.utils.correlation import CorrelationContext

    conn1 = MagicMock(rollback=AsyncMock())
    cm1 = _AsyncConnectionManagerFake(conn1)
    conn2 = MagicMock(rollback=AsyncMock())
    cm2 = _AsyncConnectionManagerFake(conn2, raise_on_exit=RuntimeError("db2 abort exit boom"))

    config1 = _make_config(pool=object())
    config1.provide_connection = MagicMock(return_value=cm1)
    config1.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config1.extension_config = {
        "sanic": {
            "commit_mode": "autocommit",
            "connection_key": "conn1",
            "session_key": "sess1",
            "pool_key": "pool1",
            "enable_correlation_middleware": True,
        }
    }

    config2 = _make_config(pool=object())
    config2.provide_connection = MagicMock(return_value=cm2)
    config2.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config2.extension_config = {
        "sanic": {"commit_mode": "autocommit", "connection_key": "conn2", "session_key": "sess2", "pool_key": "pool2"}
    }

    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"db1": config1, "db2": config2}))
    CorrelationContext.set("outer-cid")
    try:
        conn_info = SimpleNamespace(ctx=SimpleNamespace())
        request = SimpleNamespace(
            ctx=SimpleNamespace(),
            conn_info=conn_info,
            app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())),
            headers={"x-request-id": "abort-cid"},
            path="/abort-multi",
        )

        await plugin._on_request(request)

        await _touch_connections(plugin, request)
        with pytest.raises(RuntimeError, match="db2 abort exit boom"):
            await plugin._on_request_complete(conn_info=conn_info)

        conn2.rollback.assert_awaited_once_with()
        conn1.rollback.assert_awaited_once_with()
        assert cm1.exit_args is not None
        assert cm1.exit_args[0] is asyncio.CancelledError
        assert not has_context_value(request.ctx, "conn1")
        assert not has_context_value(request.ctx, "conn2")
        assert CorrelationContext.get() == "outer-cid"
    finally:
        CorrelationContext.set(None)


async def test_on_request_complete_prefers_task_contextvar_over_shared_conn_info_for_http2_streams() -> None:
    """Multiplexed HTTP/2 stream tasks resolve their own _CURRENT_REQUEST before shared conn_info.ctx."""
    import asyncio

    from sqlspec.extensions.sanic._utils import get_context_value

    cm1 = _AsyncConnectionManagerFake(MagicMock(rollback=AsyncMock()))
    cm2 = _AsyncConnectionManagerFake(MagicMock(rollback=AsyncMock()))
    managers = iter([cm1, cm2])

    config = _make_config(pool=object())
    config.provide_connection = MagicMock(side_effect=lambda _pool: next(managers))
    config.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config.extension_config = {"sanic": {"commit_mode": "manual", "enable_correlation_middleware": False}}

    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": config}))
    shared_conn_info = SimpleNamespace(ctx=SimpleNamespace())
    app_ns = SimpleNamespace(ctx=SimpleNamespace(db_pool=object()))

    req1 = SimpleNamespace(ctx=SimpleNamespace(), conn_info=shared_conn_info, app=app_ns, headers={}, path="/s1")
    req2 = SimpleNamespace(ctx=SimpleNamespace(), conn_info=shared_conn_info, app=app_ns, headers={}, path="/s2")

    stream1_acquired = asyncio.Event()
    stream2_acquired = asyncio.Event()

    async def run_stream_1() -> None:
        await plugin._on_request(req1)
        await _touch_connections(plugin, req1)
        stream1_acquired.set()
        await stream2_acquired.wait()
        await plugin._on_request_complete(conn_info=shared_conn_info)

    async def run_stream_2() -> None:
        await stream1_acquired.wait()
        await plugin._on_request(req2)
        await _touch_connections(plugin, req2)
        stream2_acquired.set()

    await asyncio.gather(run_stream_1(), run_stream_2())

    assert cm1.exit_args is not None
    assert cm1.exit_args[0] is asyncio.CancelledError
    assert not has_context_value(req1.ctx, "db_connection")
    assert cm2.exit_args is None
    assert has_context_value(req2.ctx, "db_connection")
    assert get_context_value(shared_conn_info.ctx, "_sqlspec_active_request") is req2


async def test_on_request_complete_rollback_cancellation_still_releases_target_and_siblings() -> None:
    """_on_request_complete releases the cancelled target and sibling connections when rollback raises CancelledError."""
    import asyncio

    cancel_err = asyncio.CancelledError("rollback cancelled")
    conn1 = MagicMock(rollback=AsyncMock())
    cm1 = _AsyncConnectionManagerFake(conn1)
    conn2 = MagicMock(rollback=AsyncMock(side_effect=cancel_err))
    cm2 = _AsyncConnectionManagerFake(conn2)

    config1 = _make_config(pool=object())
    config1.provide_connection = MagicMock(return_value=cm1)
    config1.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config1.extension_config = {
        "sanic": {"commit_mode": "autocommit", "connection_key": "conn1", "session_key": "sess1", "pool_key": "pool1"}
    }

    config2 = _make_config(pool=object())
    config2.provide_connection = MagicMock(return_value=cm2)
    config2.statement_config = SimpleNamespace(enable_sqlcommenter=False)
    config2.extension_config = {
        "sanic": {"commit_mode": "autocommit", "connection_key": "conn2", "session_key": "sess2", "pool_key": "pool2"}
    }

    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"db1": config1, "db2": config2}))
    conn_info = SimpleNamespace(ctx=SimpleNamespace())
    request = SimpleNamespace(
        ctx=SimpleNamespace(),
        conn_info=conn_info,
        app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())),
        headers={},
        path="/abort-cancel",
    )

    await plugin._on_request(request)

    await _touch_connections(plugin, request)
    await plugin._on_lifecycle_exception(request, ValueError("handler failed"))

    with pytest.raises(asyncio.CancelledError, match="rollback cancelled"):
        await plugin._on_request_complete(request=request)

    assert cm2.exit_args is not None
    assert cm2.exit_args[0] is asyncio.CancelledError
    assert cm2.exit_args[1] is cancel_err
    assert cm2.exit_args[2] is not None
    assert cm1.exit_args is not None
    assert cm1.exit_args[0] is ValueError
    assert not has_context_value(request.ctx, "conn1")
    assert not has_context_value(request.ctx, "conn2")


async def test_failed_lazy_acquisition_publishes_nothing_and_finalization_releases_siblings() -> None:
    """A failed first access publishes no connection; finalization still releases acquired siblings."""
    from sqlspec.extensions.sanic._utils import has_context_value

    conn1 = MagicMock()
    cm1 = _AsyncConnectionManagerFake(conn1)
    config1 = _make_config(pool=object())
    config1.provide_connection = MagicMock(return_value=cm1)
    config1.extension_config = {"sanic": {"connection_key": "conn1", "session_key": "sess1", "pool_key": "pool1"}}
    config2 = _make_config(pool=object())
    config2.provide_connection = MagicMock(side_effect=RuntimeError("pool 2 acquire failed"))
    config2.extension_config = {"sanic": {"connection_key": "conn2", "session_key": "sess2", "pool_key": "pool2"}}
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"db1": config1, "db2": config2}))
    request = SimpleNamespace(
        ctx=SimpleNamespace(), app=SimpleNamespace(ctx=SimpleNamespace(pool1=object(), pool2=object())), headers={}
    )

    await plugin._on_request(request)
    assert await plugin.get_connection_async(request, "sess1") is conn1
    with pytest.raises(RuntimeError, match="pool 2 acquire failed"):
        await plugin.get_connection_async(request, "sess2")
    assert not has_context_value(request.ctx, "conn2")
    await plugin._on_lifecycle_response(request=request, response=SimpleNamespace(status=500))
    assert cm1.exit_args is not None
    assert not has_context_value(request.ctx, "conn1")


@pytest.mark.parametrize(
    ("status", "record_exc", "expected_op"), [(200, False, "commit"), (500, False, "rollback"), (200, True, "rollback")]
)
async def test_on_response_keeps_session_open_until_on_lifecycle_response_finalizes(
    status: int, record_exc: bool, expected_op: str
) -> None:
    """_on_response leaves connection and session active for streaming, and _on_lifecycle_response finalizes and detaches."""
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"sanic": {"commit_mode": "autocommit"}}
    )
    plugin = SQLSpecPlugin(config)
    conn = AsyncMock(_conn=None, autocommit=False, in_transaction=True)
    cm = _AsyncConnectionManagerFake(conn)
    config.provide_connection = MagicMock(return_value=cm)  # type: ignore[method-assign]

    request = SimpleNamespace(
        ctx=SimpleNamespace(),
        conn_info=SimpleNamespace(ctx=SimpleNamespace()),
        app=SimpleNamespace(ctx=SimpleNamespace(db_pool=object())),
        headers={},
        path="/stream",
    )
    response = SimpleNamespace(status=status, headers={})

    await plugin._on_request(request)
    session = await plugin.get_session_async(request)

    await plugin._on_response(request, response)
    assert has_context_value(request.ctx, "db_connection")
    assert has_context_value(request.ctx, "db_session_instance")
    assert session.connection is conn
    conn.commit.assert_not_awaited()
    conn.rollback.assert_not_awaited()

    if record_exc:
        await plugin._on_lifecycle_exception(request, RuntimeError("stream failed"))

    await plugin._on_lifecycle_response(request=request, response=response)
    if expected_op == "commit":
        conn.commit.assert_awaited_once()
        conn.rollback.assert_not_awaited()
    else:
        conn.rollback.assert_awaited_once()
        conn.commit.assert_not_awaited()
    assert not has_context_value(request.ctx, "db_connection")
    assert not has_context_value(request.ctx, "db_session_instance")
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        _ = session.connection.cursor


@pytest.mark.parametrize(
    ("adapter_kind", "commit_mode", "in_tx", "op", "expect_auto_begin", "expect_called"),
    [
        ("asyncpg", "autocommit", False, "commit", True, False),
        ("asyncpg", "autocommit", False, "rollback", True, False),
        ("asyncpg", "autocommit", True, "commit", True, True),
        ("asyncpg", "autocommit", True, "rollback", True, True),
        ("asyncpg", "autocommit_include_redirect", False, "commit", True, False),
        ("asyncpg", "manual", False, "commit", False, True),
        ("aiosqlite", "autocommit", False, "commit", False, True),
    ],
)
async def test_sanic_armed_and_unarmed_session_commit_and_rollback_targets(
    adapter_kind: str, commit_mode: str, in_tx: bool, op: str, expect_auto_begin: bool, expect_called: bool
) -> None:
    """Sanic arms _auto_begin_on_write on lazy-begin adapters and skips _commit_target/_rollback_target when idle."""
    config: Any = (
        AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/db"},
            extension_config={"sanic": {"commit_mode": commit_mode}},
        )
        if adapter_kind == "asyncpg"
        else AiosqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"sanic": {"commit_mode": commit_mode}}
        )
    )
    plugin = SQLSpecPlugin(config)
    config_state = plugin._config_states[0]
    tx = AsyncMock()
    if adapter_kind == "asyncpg":
        conn: Any = AsyncMock(is_in_transaction=MagicMock(return_value=in_tx))
    else:
        conn = AsyncMock(_conn=None, autocommit=False, in_transaction=in_tx)
    request = SimpleNamespace(ctx=SimpleNamespace())
    set_context_value(request.ctx, config_state.connection_key, conn)

    session = get_or_create_session(request, config_state)
    assert session._auto_begin_on_write is expect_auto_begin
    if adapter_kind == "asyncpg":
        session._transaction = tx

    if op == "commit":
        await plugin._commit_target(request, conn, config_state)
    else:
        await plugin._rollback_target(request, conn, config_state)

    target_mock = tx if adapter_kind == "asyncpg" else conn
    actual_called = (target_mock.commit.await_count > 0) if op == "commit" else (target_mock.rollback.await_count > 0)
    assert actual_called is expect_called
