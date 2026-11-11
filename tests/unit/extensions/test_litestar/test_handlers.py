"""Test handlers for SQLSpec Litestar extension."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar.constants import HTTP_RESPONSE_START
from litestar.datastructures import State

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.extensions.litestar.handlers import (
    autocommit_handler_maker,
    connection_provider_maker,
    lifespan_handler_maker,
    manual_handler_maker,
    pool_provider_maker,
    session_provider_maker,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from litestar.types import Message, Scope

    from sqlspec.config import DatabaseConfigProtocol

pytestmark = pytest.mark.anyio


class _AsyncContextManagerFake:
    """Fake async context manager tracking exit arguments."""

    def __init__(self, connection: Any, suppress: bool = False, raise_on_exit: Exception | None = None) -> None:
        self.connection = connection
        self.suppress = suppress
        self.raise_on_exit = raise_on_exit
        self.exit_args: tuple[Any, ...] | None = None

    async def __aenter__(self) -> Any:
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.exit_args = (exc_type, exc_val, exc_tb)
        if self.raise_on_exit is not None:
            raise self.raise_on_exit
        return self.suppress


class _SyncContextManagerFake:
    """Fake sync context manager tracking exit arguments."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.exit_args: tuple[Any, ...] | None = None

    def __enter__(self) -> Any:
        return self.connection

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.exit_args = (exc_type, exc_val, exc_tb)


def test_session_provider_documents_mypyc_blockers() -> None:
    """The session provider should document why handlers.py stays interpreted."""
    source = Path("sqlspec/extensions/litestar/handlers.py").read_text()

    assert "*args/**kwargs" in source
    assert "yield in async def" in source
    assert "@contextlib.asynccontextmanager" in source
    assert "__signature__/__annotations__" in source


async def test_async_manual_handler_closes_connection() -> None:
    """Test async manual handler closes connection on terminus event."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.close.assert_awaited_once()
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_async_manual_handler_ignores_non_terminus_events() -> None:
    """Test async manual handler ignores non-terminus events."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": "http.request"})

    await handler(message, scope)

    mock_connection.close.assert_not_awaited()
    assert get_sqlspec_scope_state(scope, connection_key) is mock_connection


async def test_async_autocommit_handler_commits_on_success() -> None:
    """Test async autocommit handler commits on 2xx status."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.commit.assert_awaited_once()
    mock_connection.rollback.assert_not_awaited()
    mock_connection.close.assert_awaited_once()


async def test_async_autocommit_handler_rolls_back_on_error() -> None:
    """Test async autocommit handler rolls back on 4xx/5xx status."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 500})

    await handler(message, scope)

    mock_connection.commit.assert_not_awaited()
    mock_connection.rollback.assert_awaited_once()
    mock_connection.close.assert_awaited_once()


async def test_async_autocommit_handler_with_redirect_commit() -> None:
    """Test async autocommit handler commits on 3xx when enabled."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key, commit_on_redirect=True)

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 301})

    await handler(message, scope)

    mock_connection.commit.assert_awaited_once()
    mock_connection.rollback.assert_not_awaited()


async def test_async_autocommit_handler_extra_commit_statuses() -> None:
    """Test async autocommit handler uses extra commit statuses."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key, extra_commit_statuses={418})

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 418})

    await handler(message, scope)

    mock_connection.commit.assert_awaited_once()
    mock_connection.rollback.assert_not_awaited()


async def test_async_autocommit_handler_raises_on_conflicting_statuses() -> None:
    """Test async autocommit handler raises error when status sets overlap."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        autocommit_handler_maker("test", extra_commit_statuses={418}, extra_rollback_statuses={418})

    assert "must not share" in str(exc_info.value)


async def test_async_lifespan_handler_creates_and_closes_pool() -> None:
    """Test async lifespan handler manages pool lifecycle."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    handler = lifespan_handler_maker(config, pool_key)

    mock_app = MagicMock()
    mock_app.state = {}
    mock_app.logger = None

    async with handler(mock_app):
        assert pool_key in mock_app.state
        pool = mock_app.state[pool_key]
        assert pool is not None

    assert pool_key not in mock_app.state


async def test_async_pool_provider_returns_pool() -> None:
    """Test async pool provider returns pool from state."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    provider = pool_provider_maker(config, pool_key)

    mock_pool = MagicMock()
    state = MagicMock()
    state.get.return_value = mock_pool
    scope = cast("Scope", {})

    result: Any = await provider(state, scope)

    assert result is mock_pool
    state.get.assert_called_once_with(pool_key)


async def test_async_pool_provider_raises_when_pool_missing() -> None:
    """Test async pool provider raises error when pool not in state."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    provider = pool_provider_maker(config, pool_key)

    state = MagicMock()
    state.get.return_value = None
    scope = cast("Scope", {})

    with pytest.raises(ImproperConfigurationError) as exc_info:
        await provider(state, scope)

    assert pool_key in str(exc_info.value)
    assert "not found in application state" in str(exc_info.value)


async def test_async_connection_provider_creates_connection() -> None:
    """Test async connection provider creates connection from pool."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"
    connection_key = "test_connection"

    provider = connection_provider_maker(config, pool_key, connection_key)

    try:
        pool = await config.create_pool()
        state = MagicMock()
        state.get.return_value = pool
        scope = cast("Scope", {})

        connection: Any
        async for connection in provider(state, scope):
            assert connection is not None
            assert get_sqlspec_scope_state(scope, connection_key) is connection
    finally:
        await config.close_pool()


async def test_async_connection_provider_raises_when_pool_missing() -> None:
    """Test async connection provider raises error when pool missing."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"
    connection_key = "test_connection"

    provider = connection_provider_maker(config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = None
    scope = cast("Scope", {})

    with pytest.raises(ImproperConfigurationError) as exc_info:
        async for _ in provider(state, scope):
            pass

    assert pool_key in str(exc_info.value)


async def test_sync_connection_provider_supports_context_manager() -> None:
    """Test sync connection provider wraps sync context managers."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"
    connection_key = "test_connection"

    provider = connection_provider_maker(config, pool_key, connection_key)

    pool = config.create_pool()
    state = MagicMock()
    state.get.return_value = pool
    scope = cast("Scope", {})

    try:
        async for connection in provider(state, scope):
            assert connection is not None
            assert get_sqlspec_scope_state(scope, connection_key) is connection
    finally:
        pool.close()

    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_async_session_provider_creates_session() -> None:
    """Test async session provider creates driver session."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    connection_key = "test_connection"

    provider = session_provider_maker(config, connection_key)

    mock_connection = AsyncMock()

    session: Any
    async for session in provider(mock_connection):
        assert session is not None
        assert session.connection is mock_connection


def test_handlers_use_ensure_async_unconditionally() -> None:
    """Test that unified handlers normalize sync and async callables via ensure_async_."""
    from pathlib import Path

    from sqlspec.extensions.litestar import handlers

    source = handlers.__file__
    assert source is not None

    content = Path(source).read_text()

    assert "from sqlspec.utils.sync_tools import ensure_async_" in content
    assert "is_async" not in content, "handlers should not branch on is_async"
    assert "await ensure_async_(connection.close)()" in content, "close should go through ensure_async_"
    assert "await ensure_async_(connection.commit)()" in content, "commit should go through ensure_async_"
    assert "await ensure_async_(connection.rollback)()" in content, "rollback should go through ensure_async_"


async def test_sync_manual_handler_closes_connection() -> None:
    """Test manual handler closes sync connections through ensure_async_."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_connection = MagicMock()
    mock_connection.close = MagicMock(return_value=None)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.close.assert_called_once()
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_sync_autocommit_handler_commits_on_success() -> None:
    """Test autocommit handler commits sync connections through ensure_async_."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_connection = MagicMock()
    mock_connection.commit = MagicMock(return_value=None)
    mock_connection.rollback = MagicMock(return_value=None)
    mock_connection.close = MagicMock(return_value=None)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.commit.assert_called_once()
    mock_connection.rollback.assert_not_called()
    mock_connection.close.assert_called_once()


async def test_sync_lifespan_handler_creates_and_closes_pool() -> None:
    """Test lifespan handler manages a sync config's pool lifecycle."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    handler = lifespan_handler_maker(config, pool_key)

    mock_app = MagicMock()
    mock_app.state = {}
    mock_app.logger = None

    async with handler(mock_app):
        assert pool_key in mock_app.state
        assert mock_app.state[pool_key] is not None

    assert pool_key not in mock_app.state


async def test_connection_provider_handover_on_http() -> None:
    """Connection provider hands over context manager when response has not started."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})

    gen = provider(state, scope)
    conn = await anext(gen)

    assert conn is mock_conn
    assert get_sqlspec_scope_state(scope, f"{connection_key}_provider_owned") is True
    assert get_sqlspec_scope_state(scope, connection_key) is mock_conn

    with pytest.raises(StopAsyncIteration):
        await anext(gen)

    assert cm_fake.exit_args is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is cm_fake
    assert get_sqlspec_scope_state(scope, connection_key) is mock_conn
    assert get_sqlspec_scope_state(scope, f"{connection_key}_provider_owned") is None


async def test_manual_handler_terminus_after_handover() -> None:
    """Manual before-send handler completes context manager exit after handover."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})

    gen = provider(state, scope)
    await anext(gen)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)

    handler = manual_handler_maker(connection_key)
    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    assert cm_fake.exit_args == (None, None, None)
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_connection_provider_websocket_clean_exit() -> None:
    """Websocket connection provider exits clean immediately upon generator end."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "websocket"})

    gen = provider(state, scope)
    await anext(gen)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)

    assert cm_fake.exit_args == (None, None, None)
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_connection_provider_response_already_started() -> None:
    """Connection provider exits immediately if response has already started."""
    from litestar.utils.scope.state import ScopeState

    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})
    ScopeState.from_scope(scope).response_started = True

    gen = provider(state, scope)
    await anext(gen)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)

    assert cm_fake.exit_args == (None, None, None)
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_connection_provider_body_error_forwards_exception() -> None:
    """Exceptions thrown into connection provider are forwarded to context exit."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})

    gen = provider(state, scope)
    await anext(gen)

    err = ValueError("boom")
    with pytest.raises(ValueError) as exc_info:
        await gen.athrow(err)

    assert exc_info.value is err
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is ValueError
    assert cm_fake.exit_args[1] is err
    assert cm_fake.exit_args[2] is not None
    assert get_sqlspec_scope_state(scope, connection_key) is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_provider_owned") is None


async def test_connection_provider_suppressing_context_propagates_exception() -> None:
    """Connection provider propagates exceptions even if context manager attempts suppression."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn, suppress=True)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})

    gen = provider(state, scope)
    await anext(gen)

    err = ValueError("cannot suppress")
    with pytest.raises(ValueError) as exc_info:
        await gen.athrow(err)

    assert exc_info.value is err
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[1] is err


async def test_connection_provider_sync_manager_receives_exception() -> None:
    """Sync context manager receives thrown exception wrapped via with_ensure_async_."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _SyncContextManagerFake(mock_conn)

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})

    gen = provider(state, scope)
    await anext(gen)

    err = ValueError("sync boom")
    with pytest.raises(ValueError) as exc_info:
        await gen.athrow(err)

    assert exc_info.value is err
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is ValueError
    assert cm_fake.exit_args[1] is err
    assert cm_fake.exit_args[2] is not None


async def test_connection_provider_exit_failure_propagates_after_cleanup() -> None:
    """Context manager exit failure propagates and scope keys are cleaned up."""
    connection_key = "test_connection"
    pool_key = "test_pool"
    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn, raise_on_exit=RuntimeError("exit crashed"))

    mock_config = MagicMock()
    mock_config.provide_connection.return_value = cm_fake

    provider: Any = connection_provider_maker(mock_config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = MagicMock()
    scope = cast("Scope", {"type": "http"})

    gen = provider(state, scope)
    await anext(gen)

    with pytest.raises(RuntimeError) as exc_info:
        await gen.athrow(ValueError("original"))

    assert "exit crashed" in str(exc_info.value)
    assert get_sqlspec_scope_state(scope, connection_key) is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_provider_owned") is None


async def test_manual_handler_forwards_on_demand_exception() -> None:
    """Manual handler forwards recorded request exception to context exit."""
    from sqlspec.extensions.litestar.handlers import record_request_exception

    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_conn = MagicMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_conn)
    set_sqlspec_scope_state(scope, f"{connection_key}_ctx", cm_fake)

    exc = RuntimeError("handler exploded")
    await record_request_exception(exc, scope)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 500})
    await handler(message, scope)

    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is RuntimeError
    assert cm_fake.exit_args[1] is exc
    assert get_sqlspec_scope_state(scope, connection_key) is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None


async def test_autocommit_handler_forwards_exception_after_rollback() -> None:
    """Autocommit handler rolls back on 500 and forwards recorded exception to context exit."""
    from sqlspec.extensions.litestar.handlers import record_request_exception

    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_conn = AsyncMock()
    mock_conn.commit = AsyncMock()
    mock_conn.rollback = AsyncMock()
    mock_conn.close = AsyncMock()

    cm_fake = _AsyncContextManagerFake(mock_conn)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_conn)
    set_sqlspec_scope_state(scope, f"{connection_key}_ctx", cm_fake)

    exc = ValueError("bad request state")
    await record_request_exception(exc, scope)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 500})
    await handler(message, scope)

    mock_conn.commit.assert_not_awaited()
    mock_conn.rollback.assert_awaited_once()
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is ValueError
    assert cm_fake.exit_args[1] is exc
    assert get_sqlspec_scope_state(scope, connection_key) is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None


def test_session_terminus_events_excludes_http_disconnect() -> None:
    """SESSION_TERMINUS_ASGI_EVENTS only contains ASGI send events observed by before_send."""
    from litestar.constants import HTTP_DISCONNECT, WEBSOCKET_CLOSE, WEBSOCKET_DISCONNECT

    from sqlspec.extensions.litestar.handlers import SESSION_TERMINUS_ASGI_EVENTS

    assert HTTP_DISCONNECT not in SESSION_TERMINUS_ASGI_EVENTS
    assert SESSION_TERMINUS_ASGI_EVENTS == {HTTP_RESPONSE_START, WEBSOCKET_DISCONNECT, WEBSOCKET_CLOSE}


async def test_autocommit_handler_rolls_back_when_exception_recorded_despite_200_status() -> None:
    """Autocommit handler must roll back rather than commit when an exception is mapped to HTTP 200."""
    from sqlspec.extensions.litestar.handlers import record_request_exception

    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key, extra_commit_statuses={418})

    mock_conn = AsyncMock()
    mock_conn.commit = AsyncMock()
    mock_conn.rollback = AsyncMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_conn)
    set_sqlspec_scope_state(scope, f"{connection_key}_ctx", cm_fake)

    exc = RuntimeError("mapped to 200 fallback")
    await record_request_exception(exc, scope)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})
    await handler(message, scope)

    mock_conn.commit.assert_not_awaited()
    mock_conn.rollback.assert_awaited_once()
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is RuntimeError
    assert cm_fake.exit_args[1] is exc


async def test_connection_cleanup_middleware_handles_aborts_and_post_start_sessions() -> None:
    """ConnectionCleanupMiddleware exits residual handed-over and post-start connections safely."""
    import asyncio

    from sqlspec.extensions.litestar.plugin import ConnectionCleanupMiddleware, _ConnectionCleanupTarget

    target = _ConnectionCleanupTarget(
        connection_key="db_connection",
        session_key="db_session",
        commit_mode="autocommit",
        extra_commit_statuses=frozenset(),
        extra_rollback_statuses=frozenset(),
    )

    conn_cancel = AsyncMock()
    cm_cancel = _AsyncContextManagerFake(conn_cancel)

    async def cancelling_app(scope: Any, _receive: Any, _send: Any) -> None:
        set_sqlspec_scope_state(scope, "db_connection", conn_cancel)
        set_sqlspec_scope_state(scope, "db_connection_ctx", cm_cancel)
        raise asyncio.CancelledError()

    middleware_cancel = ConnectionCleanupMiddleware(cancelling_app, targets=(target,))
    scope_cancel = cast("Scope", {"type": "http", "state": {}})
    with pytest.raises(asyncio.CancelledError):
        await middleware_cancel(scope_cancel, AsyncMock(), AsyncMock())

    conn_cancel.rollback.assert_awaited_once()
    assert cm_cancel.exit_args is not None
    assert cm_cancel.exit_args[0] is asyncio.CancelledError
    assert get_sqlspec_scope_state(scope_cancel, "db_connection") is None

    conn_silent = AsyncMock()
    cm_silent = _AsyncContextManagerFake(conn_silent)

    async def silent_abort_app(scope: Any, _receive: Any, _send: Any) -> None:
        set_sqlspec_scope_state(scope, "db_connection", conn_silent)
        set_sqlspec_scope_state(scope, "db_connection_ctx", cm_silent)

    middleware_silent = ConnectionCleanupMiddleware(silent_abort_app, targets=(target,))
    scope_silent = cast("Scope", {"type": "http", "state": {}})
    await middleware_silent(scope_silent, AsyncMock(), AsyncMock())

    conn_silent.rollback.assert_awaited_once()
    assert cm_silent.exit_args is not None
    assert cm_silent.exit_args[0] is asyncio.CancelledError
    assert get_sqlspec_scope_state(scope_silent, "db_connection") is None

    conn_stream = AsyncMock()
    cm_stream = _AsyncContextManagerFake(conn_stream)

    async def streaming_app(scope: Any, _receive: Any, send: Any) -> None:
        await send({"type": HTTP_RESPONSE_START, "status": 200, "headers": []})
        set_sqlspec_scope_state(scope, "db_connection", conn_stream)
        set_sqlspec_scope_state(scope, "db_connection_ctx", cm_stream)
        await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    middleware_stream = ConnectionCleanupMiddleware(streaming_app, targets=(target,))
    scope_stream = cast("Scope", {"type": "http", "state": {}})
    await middleware_stream(scope_stream, AsyncMock(), AsyncMock())

    conn_stream.commit.assert_awaited_once()
    conn_stream.rollback.assert_not_awaited()
    assert cm_stream.exit_args == (None, None, None)
    assert get_sqlspec_scope_state(scope_stream, "db_connection") is None


async def test_autocommit_handler_commit_failure_rolls_back_and_forwards_exception_to_exit() -> None:
    """Autocommit handler rolls back and forwards commit failure to context exit instead of None."""
    import sqlite3

    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    commit_err = sqlite3.IntegrityError("FOREIGN KEY constraint failed")
    mock_conn = AsyncMock()
    mock_conn.commit = AsyncMock(side_effect=commit_err)
    mock_conn.rollback = AsyncMock()
    cm_fake = _AsyncContextManagerFake(mock_conn)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_conn)
    set_sqlspec_scope_state(scope, f"{connection_key}_ctx", cm_fake)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})
    with pytest.raises(sqlite3.IntegrityError) as exc_info:
        await handler(message, scope)

    assert exc_info.value is commit_err
    mock_conn.commit.assert_awaited_once()
    mock_conn.rollback.assert_awaited_once()
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is sqlite3.IntegrityError
    assert cm_fake.exit_args[1] is commit_err
    assert cm_fake.exit_args[2] is not None
    assert get_sqlspec_scope_state(scope, connection_key) is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None


async def test_autocommit_handler_rollback_failure_forwards_exception_to_exit() -> None:
    """Autocommit handler forwards rollback cancellation or failure to context exit instead of None."""
    import asyncio

    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    cancel_err = asyncio.CancelledError("cancelled during rollback")
    mock_conn = AsyncMock()
    mock_conn.commit = AsyncMock()
    mock_conn.rollback = AsyncMock(side_effect=cancel_err)
    cm_fake = _AsyncContextManagerFake(mock_conn)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_conn)
    set_sqlspec_scope_state(scope, f"{connection_key}_ctx", cm_fake)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 500})
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await handler(message, scope)

    assert exc_info.value is cancel_err
    mock_conn.commit.assert_not_awaited()
    mock_conn.rollback.assert_awaited_once()
    assert cm_fake.exit_args is not None
    assert cm_fake.exit_args[0] is asyncio.CancelledError
    assert cm_fake.exit_args[1] is cancel_err
    assert get_sqlspec_scope_state(scope, connection_key) is None
    assert get_sqlspec_scope_state(scope, f"{connection_key}_ctx") is None


async def test_connection_cleanup_middleware_commit_failure_rolls_back_and_forwards_exception() -> None:
    """ConnectionCleanupMiddleware rolls back and passes commit exception to context exit."""
    from sqlspec.extensions.litestar.plugin import ConnectionCleanupMiddleware, _ConnectionCleanupTarget

    target = _ConnectionCleanupTarget(
        connection_key="db_connection",
        session_key="db_session",
        commit_mode="autocommit",
        extra_commit_statuses=frozenset(),
        extra_rollback_statuses=frozenset(),
    )

    commit_err = RuntimeError("post-start commit failed")
    conn_stream = AsyncMock()
    conn_stream.commit = AsyncMock(side_effect=commit_err)
    conn_stream.rollback = AsyncMock()
    cm_stream = _AsyncContextManagerFake(conn_stream)

    async def streaming_app(scope: Any, _receive: Any, send: Any) -> None:
        await send({"type": HTTP_RESPONSE_START, "status": 200, "headers": []})
        set_sqlspec_scope_state(scope, "db_connection", conn_stream)
        set_sqlspec_scope_state(scope, "db_connection_ctx", cm_stream)

    middleware = ConnectionCleanupMiddleware(streaming_app, targets=(target,))
    scope = cast("Scope", {"type": "http", "state": {}})

    with pytest.raises(RuntimeError) as exc_info:
        await middleware(scope, AsyncMock(), AsyncMock())

    assert exc_info.value is commit_err
    conn_stream.commit.assert_awaited_once()
    conn_stream.rollback.assert_awaited_once()
    assert cm_stream.exit_args is not None
    assert cm_stream.exit_args[0] is RuntimeError
    assert cm_stream.exit_args[1] is commit_err
    assert get_sqlspec_scope_state(scope, "db_connection") is None


async def test_connection_cleanup_middleware_isolates_multi_db_teardown() -> None:
    """ConnectionCleanupMiddleware releases all configured targets even when an earlier target fails."""
    import asyncio

    from sqlspec.extensions.litestar.plugin import ConnectionCleanupMiddleware, _ConnectionCleanupTarget

    target1 = _ConnectionCleanupTarget(
        connection_key="db1_connection",
        session_key="db1_session",
        commit_mode="autocommit",
        extra_commit_statuses=frozenset(),
        extra_rollback_statuses=frozenset(),
    )
    target2 = _ConnectionCleanupTarget(
        connection_key="db2_connection",
        session_key="db2_session",
        commit_mode="autocommit",
        extra_commit_statuses=frozenset(),
        extra_rollback_statuses=frozenset(),
    )

    conn1 = AsyncMock()
    cm1 = _AsyncContextManagerFake(conn1, raise_on_exit=RuntimeError("db1 exit boom"))
    conn2 = AsyncMock()
    cm2 = _AsyncContextManagerFake(conn2)

    async def two_db_app(scope: Any, _receive: Any, send: Any) -> None:
        await send({"type": HTTP_RESPONSE_START, "status": 200, "headers": []})
        set_sqlspec_scope_state(scope, "db1_connection", conn1)
        set_sqlspec_scope_state(scope, "db1_connection_ctx", cm1)
        set_sqlspec_scope_state(scope, "db2_connection", conn2)
        set_sqlspec_scope_state(scope, "db2_connection_ctx", cm2)

    middleware = ConnectionCleanupMiddleware(two_db_app, targets=(target1, target2))
    scope = cast("Scope", {"type": "http", "state": {}})

    with pytest.raises(RuntimeError, match="db1 exit boom"):
        await middleware(scope, AsyncMock(), AsyncMock())

    conn1.commit.assert_awaited_once()
    conn2.commit.assert_awaited_once()
    assert cm2.exit_args == (None, None, None)
    assert get_sqlspec_scope_state(scope, "db1_connection") is None
    assert get_sqlspec_scope_state(scope, "db2_connection") is None

    conn1_b = AsyncMock()
    cm1_b = _AsyncContextManagerFake(conn1_b, raise_on_exit=RuntimeError("db1 regular error"))
    conn2_b = AsyncMock()
    conn2_b.commit = AsyncMock(side_effect=asyncio.CancelledError("db2 cancelled"))
    cm2_b = _AsyncContextManagerFake(conn2_b)

    async def cancel_second_app(scope: Any, _receive: Any, send: Any) -> None:
        await send({"type": HTTP_RESPONSE_START, "status": 200, "headers": []})
        set_sqlspec_scope_state(scope, "db1_connection", conn1_b)
        set_sqlspec_scope_state(scope, "db1_connection_ctx", cm1_b)
        set_sqlspec_scope_state(scope, "db2_connection", conn2_b)
        set_sqlspec_scope_state(scope, "db2_connection_ctx", cm2_b)

    middleware_b = ConnectionCleanupMiddleware(cancel_second_app, targets=(target1, target2))
    scope_b = cast("Scope", {"type": "http", "state": {}})

    with pytest.raises(asyncio.CancelledError):
        await middleware_b(scope_b, AsyncMock(), AsyncMock())

    assert cm2_b.exit_args is not None
    assert cm2_b.exit_args[0] is asyncio.CancelledError
    assert get_sqlspec_scope_state(scope_b, "db1_connection") is None
    assert get_sqlspec_scope_state(scope_b, "db2_connection") is None


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("failure", [None, "enter", "exit", "body"])
async def test_tenant_provider_captures_payload_for_acquisition_and_checkin(
    is_async: bool, failure: str | None
) -> None:
    from contextlib import asynccontextmanager, contextmanager
    from types import SimpleNamespace

    from sqlspec.extensions.litestar.tenant import TenantResolutionSettings
    from sqlspec.utils.tenant import TenantContext

    events: list[tuple[str, str | None]] = []
    connection = SimpleNamespace()

    @contextmanager
    def sync_connection(pool: object) -> "Generator[Any, None, None]":
        events.append(("enter", TenantContext.get_tenant_id()))
        if failure == "enter":
            raise RuntimeError("entry")
        try:
            yield connection
        finally:
            events.append(("exit", TenantContext.get_tenant_id()))
            if failure == "exit":
                raise RuntimeError("checkin")

    @asynccontextmanager
    async def async_connection(pool: object) -> "AsyncGenerator[Any, None]":
        with sync_connection(pool) as conn:
            yield conn

    async def extractor(scope: object) -> str:
        return "acme"

    config = SimpleNamespace(
        supports_tenant_strategy=True,
        driver_features={"tenant_strategy": object()},
        provide_connection=async_connection if is_async else sync_connection,
    )
    scope = cast("Scope", {"type": "http"})
    provider = connection_provider_maker(
        cast("DatabaseConfigProtocol[Any, Any, Any]", config),
        "pool",
        "tenant_conn",
        tenant_settings=TenantResolutionSettings(tenant_extractor=extractor),
    )
    iterator = provider(State({"pool": object()}), scope)
    assert TenantContext.get() is None
    if failure == "enter":
        with pytest.raises(RuntimeError, match="entry"):
            await anext(iterator)
        assert events == [("enter", "acme")]
    else:
        assert await anext(iterator) is connection
        assert TenantContext.get() is None
        if failure == "body":
            error = ValueError("body")
            with pytest.raises(ValueError) as raised:
                await iterator.athrow(error)
            assert raised.value is error
        else:
            with pytest.raises(StopAsyncIteration):
                await anext(iterator)
            handler = manual_handler_maker("tenant_conn")
            if failure == "exit":
                with pytest.raises(RuntimeError, match="checkin"):
                    await handler(cast("Message", {"type": HTTP_RESPONSE_START, "status": 200}), scope)
            else:
                await handler(cast("Message", {"type": HTTP_RESPONSE_START, "status": 200}), scope)
        assert events == [("enter", "acme"), ("exit", "acme")]
    assert TenantContext.get() is None
    assert get_sqlspec_scope_state(scope, "tenant_conn_tenant_payload") is None


async def test_tenant_provider_preserves_existing_ambient_scope() -> None:
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from sqlspec.extensions.litestar.tenant import TenantResolutionSettings
    from sqlspec.utils.tenant import TenantContext

    @asynccontextmanager
    async def connection(pool: object) -> "AsyncGenerator[object, None]":
        assert TenantContext.get_tenant_id() == "outer"
        yield object()

    def extractor(scope: object) -> None:
        raise AssertionError("ambient scope must skip resolution")

    config = SimpleNamespace(
        supports_tenant_strategy=True, driver_features={"tenant_strategy": object()}, provide_connection=connection
    )
    scope = cast("Scope", {"type": "http"})
    provider = connection_provider_maker(
        cast("DatabaseConfigProtocol[Any, Any, Any]", config),
        "pool",
        "tenant_conn",
        tenant_settings=TenantResolutionSettings(tenant_extractor=extractor),
    )
    with TenantContext.scope("outer"):
        iterator = provider(State({"pool": object()}), scope)
        await anext(iterator)
        await iterator.aclose()
        assert TenantContext.get_tenant_id() == "outer"


@pytest.mark.parametrize("is_async", [False, True])
async def test_tenant_plugin_on_demand_connection(is_async: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.base import SQLSpec
    from sqlspec.extensions.litestar import SQLSpecPlugin
    from sqlspec.utils.tenant import TenantContext

    async def async_checkout(connection: object, payload: object) -> None:
        pass

    async def async_checkin(connection: object, state: object) -> None:
        pass

    from types import SimpleNamespace

    strategy = (
        SimpleNamespace(checkout=async_checkout, checkin=async_checkin)
        if is_async
        else SimpleNamespace(checkout=lambda connection, payload: None, checkin=lambda connection, state: None)
    )
    config_type = AiosqliteConfig if is_async else SqliteConfig
    monkeypatch.setattr(config_type, "supports_tenant_strategy", True)
    monkeypatch.setattr(config_type, "_create_tenant_lease", lambda self, **kwargs: None)
    config = config_type(
        driver_features={"tenant_strategy": strategy},
        extension_config={"litestar": {"tenant_extractor": lambda scope: "acme"}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)
    pool = await config.create_pool() if isinstance(config, AiosqliteConfig) else config.create_pool()
    scope = cast("Scope", {"type": "http"})
    state = State({"db_pool": pool})
    try:
        if is_async:
            connection = await plugin.provide_request_connection_async("db_connection", state, scope)
        else:
            connection = plugin.provide_request_connection_sync("db_connection", state, scope)
        assert TenantContext.get() is None
        assert get_sqlspec_scope_state(scope, "db_connection") is connection
        await manual_handler_maker("db_connection")(
            cast("Message", {"type": HTTP_RESPONSE_START, "status": 200}), scope
        )
        assert TenantContext.get() is None
    finally:
        if isinstance(config, AiosqliteConfig):
            await config.close_pool()
        else:
            config.close_pool()


async def test_tenant_sync_on_demand_closes_rejected_extractor_coroutine(monkeypatch: pytest.MonkeyPatch) -> None:
    import inspect
    from types import SimpleNamespace

    from sqlspec.base import SQLSpec
    from sqlspec.extensions.litestar import SQLSpecPlugin
    from sqlspec.utils.tenant import TenantContext

    async def extract() -> str:
        return "acme"

    coroutine = extract()
    monkeypatch.setattr(SqliteConfig, "supports_tenant_strategy", True)
    strategy = SimpleNamespace(checkout=lambda connection, payload: None, checkin=lambda connection, state: None)
    config = SqliteConfig(
        driver_features={"tenant_strategy": strategy},
        extension_config={"litestar": {"tenant_extractor": lambda scope: coroutine}},
    )
    spec = SQLSpec()
    spec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=spec)
    pool = config.create_pool()
    try:
        with pytest.raises(ImproperConfigurationError, match="synchronous tenant_extractor"):
            plugin.provide_request_connection_sync(
                "db_connection", State({"db_pool": pool}), cast("Scope", {"type": "http"})
            )
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
        assert TenantContext.get() is None
    finally:
        coroutine.close()
        config.close_pool()


@pytest.mark.parametrize("tenant_enabled", [False, True])
async def test_tenant_provider_handoff_rebinds_payload_in_each_owning_context(tenant_enabled: bool) -> None:
    import asyncio
    from contextlib import asynccontextmanager
    from contextvars import Context
    from types import SimpleNamespace

    from sqlspec.extensions.litestar.tenant import TenantResolutionSettings
    from sqlspec.utils.tenant import TenantContext

    events: list[tuple[str, str | None]] = []
    connection = object()

    @asynccontextmanager
    async def connection_context(pool: object) -> "AsyncGenerator[object, None]":
        events.append(("acquire", TenantContext.get_tenant_id()))
        try:
            yield connection
        finally:
            events.append(("checkin", TenantContext.get_tenant_id()))

    config = SimpleNamespace(
        supports_tenant_strategy=tenant_enabled,
        driver_features={"tenant_strategy": object()} if tenant_enabled else {},
        provide_connection=connection_context,
    )
    scope = cast("Scope", {"type": "http"})
    provider = connection_provider_maker(
        cast("DatabaseConfigProtocol[Any, Any, Any]", config),
        "pool",
        "db_connection",
        tenant_settings=TenantResolutionSettings(tenant_extractor=lambda scope: "acme"),
    )
    iterator = provider(State({"pool": object()}), scope)

    async def enter() -> object:
        if tenant_enabled:
            return await anext(iterator)
        with TenantContext.scope("child"):
            result = await anext(iterator)
            assert TenantContext.get_tenant_id() == "child"
            return result

    with TenantContext.scope("parent"):
        assert await asyncio.create_task(enter(), context=Context()) is connection
        assert TenantContext.get_tenant_id() == "parent"
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)
        assert TenantContext.get_tenant_id() == "parent"
        with TenantContext.scope("cleanup"):
            await manual_handler_maker("db_connection")(
                cast("Message", {"type": HTTP_RESPONSE_START, "status": 200}), scope
            )
            assert TenantContext.get_tenant_id() == "cleanup"
        assert TenantContext.get_tenant_id() == "parent"
    expected = "acme" if tenant_enabled else None
    assert events == [("acquire", expected), ("checkin", expected)]
