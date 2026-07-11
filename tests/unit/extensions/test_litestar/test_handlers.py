"""Test handlers for SQLSpec Litestar extension."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar.constants import HTTP_RESPONSE_START

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
    from litestar.types import Message, Scope

pytestmark = pytest.mark.anyio


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

    mock_pool = await config.create_pool()
    state = MagicMock()
    state.get.return_value = mock_pool
    scope = cast("Scope", {})

    connection: Any
    async for connection in provider(state, scope):
        assert connection is not None
        assert get_sqlspec_scope_state(scope, connection_key) is connection


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
