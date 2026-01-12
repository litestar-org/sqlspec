"""Test handlers for SQLSpec Litestar extension."""

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


async def test_async_manual_handler_closes_connection() -> None:
    """Test async manual handler closes connection on terminus event."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key, is_async=True)

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
    handler = manual_handler_maker(connection_key, is_async=True)

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
    handler = autocommit_handler_maker(connection_key, is_async=True)

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
    handler = autocommit_handler_maker(connection_key, is_async=True)

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
    handler = autocommit_handler_maker(connection_key, is_async=True, commit_on_redirect=True)

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
    handler = autocommit_handler_maker(connection_key, is_async=True, extra_commit_statuses={418})

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
        autocommit_handler_maker("test", is_async=True, extra_commit_statuses={418}, extra_rollback_statuses={418})

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


def test_handlers_conditionally_use_ensure_async() -> None:
    """Test that unified handlers module imports ensure_async_ and uses it conditionally."""
    from pathlib import Path

    from sqlspec.extensions.litestar import handlers

    source = handlers.__file__
    assert source is not None

    content = Path(source).read_text()

    assert "from sqlspec.utils.sync_tools import ensure_async_" in content
    assert "if is_async:" in content, "handlers should check is_async flag"
    assert "await connection.close()" in content, "async path should use direct await"
    assert "await ensure_async_(connection.close)()" in content, "sync path should use ensure_async_"
