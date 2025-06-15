"""Tests for sqlspec.base module."""

from __future__ import annotations

import asyncio
import atexit
import threading
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.base import SQLSpec
from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig

# Test Fixtures and Mock Classes


class MockDriver:
    """Mock driver class for testing."""

    def __init__(
        self, connection: Any = None, instrumentation_config: Any = None, default_row_type: Any = None
    ) -> None:
        self.connection = connection
        self.instrumentation_config = instrumentation_config
        self.default_row_type = default_row_type


class MockAsyncDriver:
    """Mock async driver class for testing."""

    def __init__(
        self, connection: Any = None, instrumentation_config: Any = None, default_row_type: Any = None
    ) -> None:
        self.connection = connection
        self.instrumentation_config = instrumentation_config
        self.default_row_type = default_row_type


class MockSyncConfig(NoPoolSyncConfig[Any, Any]):
    """Mock sync configuration for testing."""

    is_async = False
    supports_connection_pooling = False

    def __init__(self, name: str = "MockSync") -> None:
        self.name = name
        self.connection_instance = Mock()
        self.driver_instance = Mock()
        self.driver_type = MockDriver  # Add missing driver_type as a type
        self.default_row_type = dict  # Add default row type
        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"mock": True, "name": self.name}

    def create_connection(self) -> Mock:
        return self.connection_instance

    def provide_connection(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()

    def provide_session(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()


class MockAsyncConfig(NoPoolAsyncConfig[Any, Any]):
    """Mock async configuration for testing."""

    is_async = True
    supports_connection_pooling = False

    def __init__(self, name: str = "MockAsync") -> None:
        self.name = name
        self.connection_instance = Mock()  # Use Mock instead of AsyncMock for instances
        self.driver_instance = Mock()  # Use Mock instead of AsyncMock for instances
        self.driver_type = MockAsyncDriver  # Add missing driver_type as a type
        self.default_row_type = dict  # Add default row type
        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"mock": True, "name": self.name, "async": True}

    async def create_connection(self) -> Mock:
        return self.connection_instance

    def provide_connection(self, *args: Any, **kwargs: Any) -> Mock:
        # Return a Mock object that can be used as an async context manager
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=mock)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    def provide_session(self, *args: Any, **kwargs: Any) -> Mock:
        # Return a Mock object that can be used as an async context manager
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=mock)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock


class MockSyncPoolConfig(SyncDatabaseConfig[Any, Any, Any]):
    """Mock sync configuration with pooling for testing."""

    is_async = False
    supports_connection_pooling = True

    def __init__(self, name: str = "MockSyncPool") -> None:
        self.name = name
        self.connection_instance = Mock()
        self.driver_instance = Mock()
        self.pool_instance = Mock()
        self.driver_type = MockDriver  # Add missing driver_type as a type
        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"mock": True, "name": self.name, "pool": True}

    def create_connection(self) -> Mock:
        return self.connection_instance

    def provide_connection(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()

    def provide_session(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()

    def _create_pool(self) -> Mock:
        return self.pool_instance or Mock()  # Ensure we always return a Mock

    def _close_pool(self) -> None:
        pass


class MockAsyncPoolConfig(AsyncDatabaseConfig[Any, Any, Any]):
    """Mock async configuration with pooling for testing."""

    is_async = True
    supports_connection_pooling = True

    def __init__(self, name: str = "MockAsyncPool") -> None:
        self.name = name
        self.connection_instance = Mock()  # Use Mock instead of AsyncMock for instances
        self.driver_instance = Mock()  # Use Mock instead of AsyncMock for instances
        self.pool_instance = Mock()  # Use Mock instead of AsyncMock for instances
        self.driver_type = MockAsyncDriver  # Add missing driver_type as a type
        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"mock": True, "name": self.name, "pool": True, "async": True}

    async def create_connection(self) -> Mock:
        return self.connection_instance

    def provide_connection(self, *args: Any, **kwargs: Any) -> Mock:
        # Return a Mock object that can be used as an async context manager
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=mock)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    def provide_session(self, *args: Any, **kwargs: Any) -> Mock:
        # Return a Mock object that can be used as an async context manager
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=mock)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    async def _create_pool(self) -> Mock:
        return self.pool_instance or Mock()  # Ensure we always return a Mock

    async def _close_pool(self) -> None:
        pass


# Basic Initialization and Configuration Tests


def test_sqlspec_initialization() -> None:
    """Test SQLSpec basic initialization."""
    sqlspec = SQLSpec()
    assert isinstance(sqlspec._configs, dict)
    assert len(sqlspec._configs) == 0


def test_sqlspec_atexit_registration() -> None:
    """Test that SQLSpec registers cleanup on atexit."""
    with patch.object(atexit, "register") as mock_register:
        sqlspec = SQLSpec()
        mock_register.assert_called_once_with(sqlspec._cleanup_pools)


# Configuration Management Tests


def test_add_config_sync_basic() -> None:
    """Test adding basic sync configuration."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test_sync")

    result = sqlspec.add_config(config)
    assert result is MockSyncConfig
    assert MockSyncConfig in sqlspec._configs
    assert sqlspec._configs[MockSyncConfig] is config


def test_add_config_async_basic() -> None:
    """Test adding basic async configuration."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test_async")

    result = sqlspec.add_config(config)
    assert result is MockAsyncConfig
    assert MockAsyncConfig in sqlspec._configs
    assert sqlspec._configs[MockAsyncConfig] is config


def test_add_config_overwrite_warning() -> None:
    """Test that overwriting configuration logs warning."""
    sqlspec = SQLSpec()
    config1 = MockSyncConfig("first")
    config2 = MockSyncConfig("second")

    sqlspec.add_config(config1)

    with patch("sqlspec.base.logger") as mock_logger:
        sqlspec.add_config(config2)
        mock_logger.warning.assert_called_once()
        assert "already exists" in str(mock_logger.warning.call_args)


def test_add_config_logging() -> None:
    """Test that adding configuration logs appropriate information."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")

    with patch("sqlspec.base.logger") as mock_logger:
        sqlspec.add_config(config)
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "Added configuration" in str(call_args)


@pytest.mark.parametrize(
    ("config_class", "expected_async", "expected_pooling"),
    [
        (MockSyncConfig, False, False),
        (MockAsyncConfig, True, False),
        (MockSyncPoolConfig, False, True),
        (MockAsyncPoolConfig, True, True),
    ],
    ids=["sync_no_pool", "async_no_pool", "sync_with_pool", "async_with_pool"],
)
def test_add_config_various_types(config_class: type, expected_async: bool, expected_pooling: bool) -> None:
    """Test adding various configuration types."""
    sqlspec = SQLSpec()
    config = config_class()

    result = sqlspec.add_config(config)
    assert result is config_class
    assert config.is_async == expected_async
    assert config.supports_connection_pooling == expected_pooling


# Configuration Retrieval Tests


def test_get_config_success() -> None:
    """Test successful configuration retrieval."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    retrieved = sqlspec.get_config(MockSyncConfig)
    assert retrieved is config


def test_get_config_not_found() -> None:
    """Test configuration retrieval when not found."""
    sqlspec = SQLSpec()

    with pytest.raises(KeyError, match="No configuration found"):
        sqlspec.get_config(MockSyncConfig)


def test_get_config_error_logging() -> None:
    """Test that get_config logs errors appropriately."""
    sqlspec = SQLSpec()

    with patch("sqlspec.base.logger") as mock_logger:
        with pytest.raises(KeyError):
            sqlspec.get_config(MockSyncConfig)
        mock_logger.error.assert_called_once()


def test_get_config_success_logging() -> None:
    """Test that get_config logs successful retrieval."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch("sqlspec.base.logger") as mock_logger:
        sqlspec.get_config(MockSyncConfig)
        mock_logger.debug.assert_called_once()


# Connection Management Tests


def test_get_connection_sync_config_type() -> None:
    """Test getting connection with sync config type."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "create_connection") as mock_create:
        mock_create.return_value = Mock()
        connection = sqlspec.get_connection(MockSyncConfig)
        mock_create.assert_called_once()
        assert connection == mock_create.return_value


async def test_get_connection_async_config_type() -> None:
    """Test getting connection with async config type."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "create_connection") as mock_create:
        mock_create.return_value = AsyncMock()
        connection_awaitable = sqlspec.get_connection(MockAsyncConfig)
        await connection_awaitable
        mock_create.assert_called_once()


def test_get_connection_with_config_instance() -> None:
    """Test getting connection with config instance instead of type."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")

    with patch.object(config, "create_connection") as mock_create:
        mock_create.return_value = Mock()
        sqlspec.get_connection(config)
        mock_create.assert_called_once()


def test_get_connection_logging() -> None:
    """Test that get_connection logs appropriately."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch("sqlspec.base.logger") as mock_logger:
        with patch.object(config, "create_connection", return_value=Mock()):
            sqlspec.get_connection(MockSyncConfig)
            # Should have two debug calls: one from get_config, one from get_connection
            assert mock_logger.debug.call_count == 2
            # Check that both expected calls were made
            calls = mock_logger.debug.call_args_list
            assert any("Retrieved configuration" in str(call) for call in calls)
            assert any("Getting connection for config" in str(call) for call in calls)


# Session Management Tests


def test_get_session_sync_basic() -> None:
    """Test getting sync session."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    mock_connection = Mock()

    with patch.object(config, "create_connection", return_value=mock_connection):
        session = sqlspec.get_session(MockSyncConfig)
        # The session should be an instance of MockDriver
        assert isinstance(session, MockDriver)
        assert session.connection == mock_connection


async def test_get_session_async_basic() -> None:
    """Test getting async session."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test")
    sqlspec.add_config(config)

    mock_connection = AsyncMock()

    with patch.object(config, "create_connection", return_value=mock_connection):
        session_awaitable = sqlspec.get_session(MockAsyncConfig)
        session = await session_awaitable
        # The session should be an instance of MockAsyncDriver
        assert isinstance(session, MockAsyncDriver)
        assert session.connection == mock_connection


def test_get_session_with_instance() -> None:
    """Test getting session with config instance."""
    config = MockSyncConfig("test")
    sqlspec = SQLSpec()

    mock_connection = Mock()

    with patch.object(config, "create_connection", return_value=mock_connection):
        session = sqlspec.get_session(config)
        # The session should be an instance of MockDriver
        assert isinstance(session, MockDriver)
        assert session.connection == mock_connection


def test_get_session_driver_instantiation() -> None:
    """Test that get_session properly instantiates driver."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    mock_connection = Mock()
    mock_driver_class = Mock()
    mock_driver_instance = Mock()
    mock_driver_class.return_value = mock_driver_instance
    config.driver_type = mock_driver_class  # pyright: ignore

    with patch.object(config, "create_connection", return_value=mock_connection):
        session = sqlspec.get_session(MockSyncConfig)

        mock_driver_class.assert_called_once_with(connection=mock_connection, default_row_type=dict)
        assert session == mock_driver_instance


# Context Manager Tests


def test_provide_connection_sync() -> None:
    """Test provide_connection context manager for sync config."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    mock_cm = Mock()
    with patch.object(config, "provide_connection", return_value=mock_cm):
        result = sqlspec.provide_connection(MockSyncConfig)
        assert result == mock_cm
        config.provide_connection.assert_called_once_with()  # type: ignore[attr-defined]


def test_provide_connection_with_args() -> None:
    """Test provide_connection with arguments."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "provide_connection") as mock_provide:
        sqlspec.provide_connection(MockSyncConfig, "arg1", kwarg1="value1")
        mock_provide.assert_called_once_with("arg1", kwarg1="value1")


def test_provide_session_sync() -> None:
    """Test provide_session context manager for sync config."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    mock_cm = Mock()
    with patch.object(config, "provide_session", return_value=mock_cm):
        result = sqlspec.provide_session(MockSyncConfig)
        assert result == mock_cm
        config.provide_session.assert_called_once_with()  # type: ignore[attr-defined]


def test_provide_session_with_instance() -> None:
    """Test provide_session with config instance."""
    config = MockSyncConfig("test")
    sqlspec = SQLSpec()

    mock_cm = Mock()
    with patch.object(config, "provide_session", return_value=mock_cm):
        result = sqlspec.provide_session(config)
        assert result == mock_cm


def test_provide_session_logging() -> None:
    """Test provide_session logging."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch("sqlspec.base.logger") as mock_logger:
        with patch.object(config, "provide_session", return_value=Mock()):
            sqlspec.provide_session(MockSyncConfig)
            # Should have two debug calls: one from get_config, one from provide_session
            assert mock_logger.debug.call_count == 2
            # Check that both expected calls were made
            calls = mock_logger.debug.call_args_list
            assert any("Retrieved configuration" in str(call) for call in calls)
            assert any("Providing session context for config" in str(call) for call in calls)


# Pool Management Tests


def test_get_pool_no_pool_config() -> None:
    """Test get_pool with no-pool configuration."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    result = sqlspec.get_pool(MockSyncConfig)
    assert result is None


def test_get_pool_sync_with_pool() -> None:
    """Test get_pool with sync pooled configuration."""
    sqlspec = SQLSpec()
    config = MockSyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    mock_pool = Mock()
    with patch.object(config, "create_pool", return_value=mock_pool):
        result = sqlspec.get_pool(MockSyncPoolConfig)
        assert result == mock_pool
        config.create_pool.assert_called_once()  # type: ignore[attr-defined]


async def test_get_pool_async_with_pool() -> None:
    """Test get_pool with async pooled configuration."""
    sqlspec = SQLSpec()
    config = MockAsyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    mock_pool = AsyncMock()
    with patch.object(config, "create_pool", return_value=mock_pool):
        result_awaitable = sqlspec.get_pool(MockAsyncPoolConfig)
        result = await result_awaitable
        assert result == mock_pool
        config.create_pool.assert_called_once()  # type: ignore[attr-defined]


def test_get_pool_with_instance() -> None:
    """Test get_pool with config instance."""
    config = MockSyncPoolConfig("test")  # pyright: ignore
    sqlspec = SQLSpec()

    mock_pool = Mock()
    with patch.object(config, "create_pool", return_value=mock_pool):
        result = sqlspec.get_pool(config)
        assert result == mock_pool


def test_get_pool_logging() -> None:
    """Test get_pool logging for both pooled and non-pooled configs."""
    sqlspec = SQLSpec()

    # Test non-pooled config logging
    no_pool_config = MockSyncConfig("test")
    sqlspec.add_config(no_pool_config)

    with patch("sqlspec.base.logger") as mock_logger:
        sqlspec.get_pool(MockSyncConfig)
        mock_logger.debug.assert_called_with("Config %s does not support connection pooling", "MockSyncConfig")


def test_close_pool_no_pool_config() -> None:
    """Test close_pool with no-pool configuration."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    result = sqlspec.close_pool(MockSyncConfig)
    assert result is None


def test_close_pool_sync_with_pool() -> None:
    """Test close_pool with sync pooled configuration."""
    sqlspec = SQLSpec()
    config = MockSyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    with patch.object(config, "close_pool") as mock_close:
        sqlspec.close_pool(MockSyncPoolConfig)
        mock_close.assert_called_once()


async def test_close_pool_async_with_pool() -> None:
    """Test close_pool with async pooled configuration."""
    sqlspec = SQLSpec()
    config = MockAsyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    # Create a proper async mock that doesn't return a coroutine
    mock_close = AsyncMock()

    with patch.object(config, "close_pool", mock_close):
        result_awaitable = sqlspec.close_pool(MockAsyncPoolConfig)
        await result_awaitable
        mock_close.assert_called_once()


# Cleanup and Resource Management Tests


def test_cleanup_pools_empty() -> None:
    """Test cleanup with no configurations."""
    sqlspec = SQLSpec()

    with patch("sqlspec.base.logger") as mock_logger:
        sqlspec._cleanup_pools()
        mock_logger.info.assert_called_with("Pool cleanup completed. Cleaned %d pools", 0)


def test_cleanup_pools_sync_configs() -> None:
    """Test cleanup with sync configurations."""
    sqlspec = SQLSpec()
    config = MockSyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    with patch.object(config, "close_pool") as mock_close:
        with patch("sqlspec.base.logger") as mock_logger:
            sqlspec._cleanup_pools()
            mock_close.assert_called_once()

            # Check that info and debug logs were called
            info_calls = [call for call in mock_logger.info.call_args_list if "Pool cleanup completed" in str(call)]
            assert len(info_calls) == 1


def test_cleanup_pools_async_configs() -> None:
    """Test cleanup with async configurations."""
    sqlspec = SQLSpec()
    config = MockAsyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    # Track calls
    close_pool_called = False

    async def mock_close_pool() -> None:
        nonlocal close_pool_called
        close_pool_called = True

    # Mock close_pool to return our coroutine function (not the coroutine itself)
    with patch.object(config, "close_pool", mock_close_pool):
        # Mock asyncio.run to actually run the coroutine
        def mock_run_impl(coro: Any) -> Any:
            # Create a new event loop and run the coroutine
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()

        with patch("asyncio.run", side_effect=mock_run_impl) as mock_run:
            with patch("asyncio.get_running_loop") as mock_get_loop:
                mock_get_loop.side_effect = RuntimeError("No running loop")

                sqlspec._cleanup_pools()
                mock_run.assert_called_once()
                assert close_pool_called


def test_cleanup_pools_exception_handling() -> None:
    """Test cleanup handles exceptions gracefully."""
    sqlspec = SQLSpec()
    config = MockSyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    with patch.object(config, "close_pool", side_effect=Exception("Pool error")):
        with patch("sqlspec.base.logger") as mock_logger:
            sqlspec._cleanup_pools()

            warning_calls = [
                call for call in mock_logger.warning.call_args_list if "Failed to clean up pool" in str(call)
            ]
            assert len(warning_calls) == 1


def test_cleanup_pools_running_event_loop() -> None:
    """Test cleanup with running event loop."""
    sqlspec = SQLSpec()
    config = MockAsyncPoolConfig("test")  # pyright: ignore
    sqlspec.add_config(config)

    mock_loop = Mock()
    mock_loop.is_running.return_value = True

    # Create a proper coroutine function that returns None
    async def mock_close_pool() -> None:
        return None

    # Create the coroutine but don't await it yet
    close_pool_coro = mock_close_pool()

    with patch.object(config, "close_pool", return_value=close_pool_coro):
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            with patch("asyncio.ensure_future") as mock_ensure_future:
                sqlspec._cleanup_pools()
                # Check that ensure_future was called with a coroutine
                mock_ensure_future.assert_called_once()
                args, kwargs = mock_ensure_future.call_args
                assert kwargs.get("loop") == mock_loop
                # The first argument should be a coroutine
                import inspect

                assert inspect.iscoroutine(args[0])

    # Clean up the coroutine to avoid warnings
    close_pool_coro.close()


def test_cleanup_pools_clears_configs() -> None:
    """Test that cleanup clears the configs dictionary."""
    sqlspec = SQLSpec()
    config1 = MockSyncConfig("test1")
    config2 = MockSyncPoolConfig("test2")  # pyright: ignore

    sqlspec.add_config(config1)
    sqlspec.add_config(config2)

    assert len(sqlspec._configs) == 2

    sqlspec._cleanup_pools()

    assert len(sqlspec._configs) == 0


# Thread Safety Tests


def test_thread_safety_add_config() -> None:
    """Test thread safety of add_config method."""
    sqlspec = SQLSpec()
    results = []

    def add_config_worker(config_id: int) -> None:
        # Create a unique config class for each thread to avoid overwrites
        class UniqueConfig(MockSyncConfig):
            pass

        config = UniqueConfig(f"test_{config_id}")
        result = sqlspec.add_config(config)
        results.append((config_id, result))

    threads = [threading.Thread(target=add_config_worker, args=(i,)) for i in range(5)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(results) == 5
    assert len(sqlspec._configs) == 5


def test_thread_safety_get_config() -> None:
    """Test thread safety of get_config method."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    results: list[MockSyncConfig | Exception] = []

    def get_config_worker() -> None:
        try:
            retrieved = sqlspec.get_config(MockSyncConfig)
            results.append(retrieved)
        except Exception as e:
            results.append(e)

    threads = [threading.Thread(target=get_config_worker) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(results) == 10
    assert all(result is config for result in results if isinstance(result, MockSyncConfig))


# Error Handling and Edge Cases


def test_get_config_with_none() -> None:
    """Test get_config behavior with None parameter."""
    sqlspec = SQLSpec()

    with pytest.raises(KeyError):
        sqlspec.get_config(None)  # type: ignore[call-overload]


def test_configuration_replacement() -> None:
    """Test replacing existing configuration."""
    sqlspec = SQLSpec()
    config1 = MockSyncConfig("first")
    config2 = MockSyncConfig("second")

    # Add first config
    sqlspec.add_config(config1)
    retrieved1 = sqlspec.get_config(MockSyncConfig)
    assert retrieved1 is config1

    # Replace with second config
    sqlspec.add_config(config2)
    retrieved2 = sqlspec.get_config(MockSyncConfig)
    assert retrieved2 is config2
    assert retrieved2 is not config1


def test_mixed_config_types() -> None:
    """Test SQLSpec with mixed sync/async configurations."""
    sqlspec = SQLSpec()

    sync_config = MockSyncConfig("sync")
    async_config = MockAsyncConfig("async")
    sync_pool_config = MockSyncPoolConfig("sync_pool")  # pyright: ignore
    async_pool_config = MockAsyncPoolConfig("async_pool")  # pyright: ignore

    sqlspec.add_config(sync_config)
    sqlspec.add_config(async_config)
    sqlspec.add_config(sync_pool_config)
    sqlspec.add_config(async_pool_config)

    assert len(sqlspec._configs) == 4
    assert sqlspec.get_config(MockSyncConfig) is sync_config
    assert sqlspec.get_config(MockAsyncConfig) is async_config
    assert sqlspec.get_config(MockSyncPoolConfig) is sync_pool_config
    assert sqlspec.get_config(MockAsyncPoolConfig) is async_pool_config


# Performance and Stress Tests


def test_large_number_of_configs() -> None:
    """Test SQLSpec with large number of configurations."""
    sqlspec = SQLSpec()
    configs = []

    # Create unique config classes
    for i in range(100):
        class_name = f"MockConfig{i}"
        config_class = type(class_name, (MockSyncConfig,), {})
        config = config_class(f"test_{i}")
        configs.append((config_class, config))
        sqlspec.add_config(config)

    assert len(sqlspec._configs) == 100

    # Verify all configs can be retrieved
    for config_class, original_config in configs:
        retrieved = sqlspec.get_config(config_class)  # type: ignore[var-annotated]
        assert retrieved is original_config


@pytest.mark.asyncio
async def test_concurrent_async_operations() -> None:
    """Test concurrent async operations."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test")
    sqlspec.add_config(config)

    async def get_session_worker() -> Any:
        return await sqlspec.get_session(MockAsyncConfig)

    # Run multiple concurrent operations
    results = await asyncio.gather(*[get_session_worker() for _ in range(10)])

    # All results should be MockAsyncDriver instances
    assert len(results) == 10
    for result in results:
        assert isinstance(result, MockAsyncDriver)


def test_instrumentation_config_integration() -> None:
    """Test SQLSpec with custom instrumentation configs."""
    sqlspec = SQLSpec()

    config = MockSyncConfig("test")

    sqlspec.add_config(config)


# Integration with Atexit


def test_atexit_cleanup_integration() -> None:
    """Test that atexit cleanup is properly registered and executed."""
    with patch("atexit.register") as mock_register:
        sqlspec = SQLSpec()

        # Verify registration
        mock_register.assert_called_once_with(sqlspec._cleanup_pools)

        # Add some configs
        config1 = MockSyncPoolConfig("test1")  # pyright: ignore
        config2 = MockAsyncPoolConfig("test2")  # pyright: ignore
        sqlspec.add_config(config1)
        sqlspec.add_config(config2)

        # Manually trigger cleanup (simulating atexit)
        cleanup_func = mock_register.call_args[0][0]

        with patch.object(config1, "close_pool") as mock_close1, patch.object(config2, "close_pool") as mock_close2:
            with patch("asyncio.run"):
                cleanup_func()

                mock_close1.assert_called_once()
                mock_close2.assert_called_once()
