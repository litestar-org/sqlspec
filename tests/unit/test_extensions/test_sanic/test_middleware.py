"""Unit tests for Sanic middleware."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams
from sqlspec.extensions.sanic._middleware import SessionMiddleware
from sqlspec.extensions.sanic.config import DatabaseConfig


@pytest.fixture
def sqlite_config():
    """Create a SQLite configuration for testing."""
    return SqliteConfig(pool_config=SqliteConnectionParams(database=":memory:"))


@pytest.fixture
def database_config(sqlite_config):
    """Create a database configuration for testing."""
    return DatabaseConfig(config=sqlite_config)


@pytest.fixture
def mock_request():
    """Create a mock Sanic request object."""
    request = Mock()
    request.id = "test-request-123"
    request.ctx = Mock()
    return request


@pytest.fixture
def mock_response():
    """Create a mock Sanic response object."""
    response = Mock()
    response.status = 200
    return response


@pytest.fixture
def session_middleware(database_config):
    """Create a SessionMiddleware instance for testing."""
    return SessionMiddleware(database_config)


class TestSessionMiddlewareInit:
    """Test SessionMiddleware initialization."""

    def test_middleware_initialization(self, database_config):
        """Test that middleware initializes with database config."""
        middleware = SessionMiddleware(database_config)

        assert middleware.database_config is database_config
        assert middleware._connection_key == database_config.connection_key
        assert middleware._session_key == database_config.session_key


class TestBeforeRequest:
    """Test before_request middleware functionality."""

    async def test_before_request_skips_existing_connection(self, session_middleware, mock_request):
        """Test that before_request skips setup if connection already exists."""
        # Set up existing connection
        setattr(mock_request.ctx, session_middleware._connection_key, Mock())

        await session_middleware.before_request(mock_request)

        # Should not have created additional connections
        # This test verifies the early return path

    @patch("sqlspec.extensions.sanic._middleware.ensure_async_")
    async def test_before_request_creates_connection_with_provider(self, mock_ensure_async, session_middleware, mock_request):
        """Test that before_request creates connection using provider."""
        # Set up mocks
        mock_connection = Mock()
        mock_connection_gen = AsyncMock()
        mock_connection_gen.__anext__ = AsyncMock(return_value=mock_connection)

        session_middleware.database_config.connection_provider = AsyncMock(return_value=mock_connection_gen)

        await session_middleware.before_request(mock_request)

        # Verify connection was set
        assert getattr(mock_request.ctx, session_middleware._connection_key) is mock_connection

        # Verify generator was stored for cleanup
        assert hasattr(mock_request.ctx, f"_{session_middleware._connection_key}_gen")

    @patch("sqlspec.extensions.sanic._middleware.ensure_async_")
    async def test_before_request_creates_connection_fallback(self, mock_ensure_async, session_middleware, mock_request):
        """Test that before_request creates connection using fallback method."""
        # Set up mocks for fallback path
        session_middleware.database_config.connection_provider = None

        mock_pool = Mock()
        mock_ensure_async.return_value = AsyncMock(return_value=mock_pool)

        mock_connection_cm = Mock()
        mock_connection_cm.__aenter__ = AsyncMock(return_value=Mock())
        session_middleware.database_config.config.provide_connection = Mock(return_value=mock_connection_cm)
        session_middleware.database_config.config.create_pool = Mock()

        await session_middleware.before_request(mock_request)

        # Verify pool creation was called
        mock_ensure_async.assert_called_once()

    async def test_before_request_creates_session_with_provider(self, session_middleware, mock_request):
        """Test that before_request creates session using session provider."""
        # Set up existing connection
        mock_connection = Mock()
        setattr(mock_request.ctx, session_middleware._connection_key, mock_connection)

        # Set up session provider
        mock_session = Mock()
        mock_session_gen = AsyncMock()
        mock_session_gen.__anext__ = AsyncMock(return_value=mock_session)

        session_middleware.database_config.session_provider = AsyncMock(return_value=mock_session_gen)

        await session_middleware.before_request(mock_request)

        # Verify session was created
        assert getattr(mock_request.ctx, session_middleware._session_key) is mock_session

    async def test_before_request_handles_exceptions(self, session_middleware, mock_request, caplog):
        """Test that before_request handles exceptions properly."""
        # Set up connection provider to raise exception
        session_middleware.database_config.connection_provider = AsyncMock(side_effect=Exception("Test error"))

        with pytest.raises(Exception, match="Test error"):
            await session_middleware.before_request(mock_request)


class TestAfterResponse:
    """Test after_response middleware functionality."""

    async def test_after_response_cleanup_session_generator(self, session_middleware, mock_request, mock_response):
        """Test that after_response cleans up session generator."""
        # Set up session generator
        mock_session_gen = AsyncMock()
        mock_session_gen.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
        setattr(mock_request.ctx, f"_{session_middleware._session_key}_gen", mock_session_gen)
        setattr(mock_request.ctx, session_middleware._session_key, Mock())

        await session_middleware.after_response(mock_request, mock_response)

        # Verify session generator cleanup was attempted
        mock_session_gen.__anext__.assert_called_once()

    async def test_after_response_transaction_commit_on_success(self, session_middleware, mock_request, mock_response):
        """Test that after_response commits transaction on successful response."""
        # Set up successful response
        mock_response.status = 200
        session_middleware.database_config.commit_mode = "autocommit"

        # Set up connection with commit method
        mock_connection = Mock()
        mock_connection.commit = AsyncMock()
        setattr(mock_request.ctx, session_middleware._connection_key, mock_connection)

        await session_middleware.after_response(mock_request, mock_response)

        # Verify commit was called
        mock_connection.commit.assert_called_once()

    async def test_after_response_transaction_rollback_on_error(self, session_middleware, mock_request, mock_response):
        """Test that after_response rolls back transaction on error response."""
        # Set up error response
        mock_response.status = 500
        session_middleware.database_config.commit_mode = "autocommit"

        # Set up connection with rollback method
        mock_connection = Mock()
        mock_connection.rollback = AsyncMock()
        setattr(mock_request.ctx, session_middleware._connection_key, mock_connection)

        await session_middleware.after_response(mock_request, mock_response)

        # Verify rollback was called
        mock_connection.rollback.assert_called_once()

    async def test_after_response_manual_commit_mode_skips_transaction(self, session_middleware, mock_request, mock_response):
        """Test that after_response skips transaction handling in manual mode."""
        # Set up manual commit mode
        session_middleware.database_config.commit_mode = "manual"

        # Set up connection
        mock_connection = Mock()
        mock_connection.commit = AsyncMock()
        mock_connection.rollback = AsyncMock()
        setattr(mock_request.ctx, session_middleware._connection_key, mock_connection)

        await session_middleware.after_response(mock_request, mock_response)

        # Verify neither commit nor rollback was called
        mock_connection.commit.assert_not_called()
        mock_connection.rollback.assert_not_called()

    async def test_after_response_connection_cleanup(self, session_middleware, mock_request, mock_response):
        """Test that after_response cleans up connection resources."""
        # Set up connection with close method
        mock_connection = Mock()
        mock_connection.close = AsyncMock()
        setattr(mock_request.ctx, session_middleware._connection_key, mock_connection)

        await session_middleware.after_response(mock_request, mock_response)

        # Verify connection was closed
        mock_connection.close.assert_called_once()

    async def test_after_response_handles_transaction_exceptions(self, session_middleware, mock_request, mock_response, caplog):
        """Test that after_response handles transaction exceptions gracefully."""
        session_middleware.database_config.commit_mode = "autocommit"
        mock_response.status = 200

        # Set up connection that raises exception on commit
        mock_connection = Mock()
        mock_connection.commit = AsyncMock(side_effect=Exception("Commit failed"))
        mock_connection.rollback = AsyncMock()
        setattr(mock_request.ctx, session_middleware._connection_key, mock_connection)

        # Should not raise exception
        await session_middleware.after_response(mock_request, mock_response)

        # Verify rollback was attempted after commit failure
        mock_connection.rollback.assert_called_once()


class TestShouldCommitTransaction:
    """Test _should_commit_transaction logic."""

    def test_manual_mode_never_commits(self, session_middleware):
        """Test that manual mode never commits automatically."""
        session_middleware.database_config.commit_mode = "manual"

        assert not session_middleware._should_commit_transaction(200)
        assert not session_middleware._should_commit_transaction(500)

    def test_autocommit_mode_commits_on_success(self, session_middleware):
        """Test that autocommit mode commits on successful status codes."""
        session_middleware.database_config.commit_mode = "autocommit"

        # Success codes should commit
        assert session_middleware._should_commit_transaction(200)
        assert session_middleware._should_commit_transaction(201)
        assert session_middleware._should_commit_transaction(299)

        # Redirect codes should not commit
        assert not session_middleware._should_commit_transaction(300)
        assert not session_middleware._should_commit_transaction(302)

        # Error codes should not commit
        assert not session_middleware._should_commit_transaction(400)
        assert not session_middleware._should_commit_transaction(500)

    def test_autocommit_include_redirect_mode(self, session_middleware):
        """Test that autocommit_include_redirect mode commits on success and redirect."""
        session_middleware.database_config.commit_mode = "autocommit_include_redirect"

        # Success and redirect codes should commit
        assert session_middleware._should_commit_transaction(200)
        assert session_middleware._should_commit_transaction(302)
        assert session_middleware._should_commit_transaction(399)

        # Error codes should not commit
        assert not session_middleware._should_commit_transaction(400)
        assert not session_middleware._should_commit_transaction(500)

    def test_extra_commit_statuses_override(self, session_middleware):
        """Test that extra_commit_statuses override default behavior."""
        session_middleware.database_config.commit_mode = "autocommit"
        session_middleware.database_config.extra_commit_statuses = {500, 503}

        # Error codes in extra_commit_statuses should commit
        assert session_middleware._should_commit_transaction(500)
        assert session_middleware._should_commit_transaction(503)

        # Other error codes should not commit
        assert not session_middleware._should_commit_transaction(404)

    def test_extra_rollback_statuses_override(self, session_middleware):
        """Test that extra_rollback_statuses override default behavior."""
        session_middleware.database_config.commit_mode = "autocommit"
        session_middleware.database_config.extra_rollback_statuses = {200, 201}

        # Success codes in extra_rollback_statuses should not commit
        assert not session_middleware._should_commit_transaction(200)
        assert not session_middleware._should_commit_transaction(201)

        # Other success codes should commit
        assert session_middleware._should_commit_transaction(202)
