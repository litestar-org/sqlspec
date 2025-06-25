"""Unit tests for sqlspec.base module."""

import asyncio
import atexit
import threading
from typing import Any, Optional
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.base import SQLSpec
from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig


# Mock implementation classes for testing
class MockConnection:
    """Mock connection for testing."""

    def __init__(self, name: "str" = "mock_connection") -> None:
        self.name = name
        self.closed = False


class MockDriver:
    """Mock driver for testing."""

    def __init__(
        self,
        connection: "MockConnection",
        config: "Optional[Any]" = None,
        default_row_type: "Optional[type[Any]]" = None,
    ) -> None:
        self.connection = connection
        self.config = config
        self.default_row_type = default_row_type or dict


class MockAsyncDriver:
    """Mock async driver for testing."""

    def __init__(
        self,
        connection: "MockConnection",
        config: "Optional[Any]" = None,
        default_row_type: "Optional[type[Any]]" = None,
    ) -> None:
        self.connection = connection
        self.config = config
        self.default_row_type = default_row_type or dict


class MockSyncConfig(NoPoolSyncConfig["MockConnection", "MockDriver"]):  # type: ignore[type-var]
    """Mock sync config without pooling."""

    driver_type = MockDriver
    is_async = False
    supports_connection_pooling = False

    def __init__(self, name: "str" = "mock_sync") -> None:
        super().__init__()
        self.name = name
        self._connection = MockConnection(name)
        self.default_row_type = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"name": self.name, "type": "sync"}

    def create_connection(self) -> "MockConnection":
        return self._connection

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Any":
        mock = Mock()
        mock.__enter__ = Mock(return_value=self._connection)
        mock.__exit__ = Mock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Any":
        driver = self.driver_type(self._connection, None, self.default_row_type)
        mock = Mock()
        mock.__enter__ = Mock(return_value=driver)
        mock.__exit__ = Mock(return_value=None)
        return mock


class MockAsyncConfig(NoPoolAsyncConfig["MockConnection", "MockAsyncDriver"]):  # type: ignore[type-var]
    """Mock async config without pooling."""

    driver_type = MockAsyncDriver
    is_async = True
    supports_connection_pooling = False

    def __init__(self, name: "str" = "mock_async") -> None:
        super().__init__()
        self.name = name
        self._connection = MockConnection(name)
        self.default_row_type = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"name": self.name, "type": "async"}

    async def create_connection(self) -> "MockConnection":
        return self._connection

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Any":
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=self._connection)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Any":
        driver = self.driver_type(self._connection, default_row_type=self.default_row_type)
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=driver)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock


class MockPool:
    """Mock connection pool."""

    def __init__(self, name: "str" = "mock_pool") -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class MockSyncPoolConfig(SyncDatabaseConfig["MockConnection", "MockPool", "MockDriver"]):  # type: ignore[type-var]
    """Mock sync config with pooling."""

    driver_type = MockDriver  # pyright: ignore
    is_async = False
    supports_connection_pooling = True

    def __init__(self, name: "str" = "mock_sync_pool") -> None:
        super().__init__()
        self.name = name
        self._connection = MockConnection(name)
        self._pool: Optional[MockPool] = None
        self.default_row_type = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"name": self.name, "type": "sync_pool"}

    def create_connection(self) -> "MockConnection":
        return self._connection

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Any":
        mock = Mock()
        mock.__enter__ = Mock(return_value=self._connection)
        mock.__exit__ = Mock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Any":
        driver = self.driver_type(self._connection, None, self.default_row_type)
        mock = Mock()
        mock.__enter__ = Mock(return_value=driver)
        mock.__exit__ = Mock(return_value=None)
        return mock

    def _create_pool(self) -> "MockPool":
        self._pool = MockPool(self.name)
        return self._pool

    def _close_pool(self) -> None:
        if self._pool:
            self._pool.close()


class MockAsyncPoolConfig(AsyncDatabaseConfig["MockConnection", "MockPool", "MockAsyncDriver"]):  # type: ignore[type-var]
    """Mock async config with pooling."""

    driver_type = MockAsyncDriver
    is_async = True
    supports_connection_pooling = True

    def __init__(self, name: "str" = "mock_async_pool") -> None:
        super().__init__()
        self.name = name
        self._connection = MockConnection(name)
        self._pool: Optional[MockPool] = None
        self.default_row_type = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"name": self.name, "type": "async_pool"}

    async def create_connection(self) -> "MockConnection":
        return self._connection

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Any":
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=self._connection)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Any":
        driver = self.driver_type(self._connection, default_row_type=self.default_row_type)
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=driver)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    async def _create_pool(self) -> "MockPool":
        self._pool = MockPool(self.name)
        return self._pool

    async def _close_pool(self) -> None:
        if self._pool:
            self._pool.close()


def test_sqlspec_initialization() -> None:
    """Test SQLSpec initialization."""
    with patch.object(atexit, "register") as mock_register:
        sqlspec = SQLSpec()
        assert isinstance(sqlspec._configs, dict)
        assert len(sqlspec._configs) == 0
        mock_register.assert_called_once_with(sqlspec._cleanup_pools)


@pytest.mark.parametrize(
    "config_class,config_name,expected_type",
    [
        (MockSyncConfig, "sync_test", MockSyncConfig),
        (MockAsyncConfig, "async_test", MockAsyncConfig),
        (MockSyncPoolConfig, "sync_pool_test", MockSyncPoolConfig),
        (MockAsyncPoolConfig, "async_pool_test", MockAsyncPoolConfig),
    ],
)
def test_add_config(config_class: "type", config_name: "str", expected_type: "type") -> None:
    """Test adding various configuration types."""
    sqlspec = SQLSpec()
    config = config_class(config_name)

    result = sqlspec.add_config(config)
    assert result is expected_type
    assert expected_type in sqlspec._configs
    assert sqlspec._configs[expected_type] is config


def test_add_config_overwrite() -> None:
    """Test overwriting existing configuration logs warning."""
    sqlspec = SQLSpec()
    config1 = MockSyncConfig("first")
    config2 = MockSyncConfig("second")

    sqlspec.add_config(config1)

    with patch("sqlspec.base.logger") as mock_logger:
        sqlspec.add_config(config2)
        mock_logger.warning.assert_called_once()
        assert MockSyncConfig in sqlspec._configs
        assert sqlspec._configs[MockSyncConfig] is config2


@pytest.mark.parametrize(
    "config_class,error_match",
    [
        (MockSyncConfig, r"No configuration found for.*MockSyncConfig"),
        (MockAsyncConfig, r"No configuration found for.*MockAsyncConfig"),
    ],
)
def test_get_config_not_found(config_class: "type", error_match: "str") -> None:
    """Test configuration retrieval when not found."""
    sqlspec = SQLSpec()

    with patch("sqlspec.base.logger") as mock_logger:
        with pytest.raises(KeyError, match=error_match):
            sqlspec.get_config(config_class)
        mock_logger.error.assert_called_once()


def test_get_config_success() -> None:
    """Test successful configuration retrieval."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch("sqlspec.base.logger") as mock_logger:
        retrieved = sqlspec.get_config(MockSyncConfig)
        assert retrieved is config
        mock_logger.debug.assert_called_once()


@pytest.mark.parametrize("use_instance", [True, False], ids=["with_instance", "with_type"])
def test_get_connection_sync(use_instance: bool) -> None:
    """Test getting sync connection."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")

    if not use_instance:
        sqlspec.add_config(config)
        config_or_type = MockSyncConfig
    else:
        config_or_type = config

    with patch.object(config, "create_connection") as mock_create:
        mock_create.return_value = MockConnection("test_conn")
        connection = sqlspec.get_connection(config_or_type)  # type: ignore[type-var]
        mock_create.assert_called_once()
        assert isinstance(connection, MockConnection)


@pytest.mark.asyncio
async def test_get_connection_async() -> None:
    """Test getting async connection."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "create_connection") as mock_create:
        mock_create.return_value = MockConnection("test_conn")
        connection = await sqlspec.get_connection(MockAsyncConfig)  # type: ignore[arg-type]
        mock_create.assert_called_once()
        assert isinstance(connection, MockConnection)


@pytest.mark.parametrize("use_instance", [True, False], ids=["with_instance", "with_type"])
def test_get_session_sync(use_instance: bool) -> None:
    """Test getting sync session."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")

    if not use_instance:
        sqlspec.add_config(config)
        config_or_type = MockSyncConfig
    else:
        config_or_type = config

    session = sqlspec.get_session(config_or_type)  # type: ignore[type-var]
    assert isinstance(session, MockDriver)
    assert isinstance(session.connection, MockConnection)


@pytest.mark.asyncio
async def test_get_session_async() -> None:
    """Test getting async session."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test")
    sqlspec.add_config(config)

    session = await sqlspec.get_session(MockAsyncConfig)  # type: ignore[arg-type]
    assert isinstance(session, MockAsyncDriver)
    assert isinstance(session.connection, MockConnection)


@pytest.mark.parametrize(
    "config_class,has_pool",
    [(MockSyncConfig, False), (MockAsyncConfig, False), (MockSyncPoolConfig, True), (MockAsyncPoolConfig, True)],
)
def test_get_pool_sync(config_class: "type", has_pool: bool) -> None:
    """Test getting pool from various config types."""
    sqlspec = SQLSpec()
    config = config_class("test")
    sqlspec.add_config(config)

    if config_class == MockAsyncPoolConfig:
        # Skip async test here, handled separately
        return

    result = sqlspec.get_pool(config_class)

    if has_pool:
        assert isinstance(result, MockPool)
    else:
        assert result is None


@pytest.mark.asyncio
async def test_get_pool_async() -> None:
    """Test getting async pool."""
    sqlspec = SQLSpec()
    config = MockAsyncPoolConfig("test")
    sqlspec.add_config(config)

    result = await sqlspec.get_pool(MockAsyncPoolConfig)  # type: ignore[arg-type,misc]
    assert isinstance(result, MockPool)


def test_provide_connection() -> None:
    """Test provide_connection context manager."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "provide_connection") as mock_provide:
        mock_cm = Mock()
        mock_provide.return_value = mock_cm

        result = sqlspec.provide_connection(MockSyncConfig, "arg1", kwarg1="value1")  # type: ignore[arg-type,type-var]
        assert result == mock_cm
        mock_provide.assert_called_once_with("arg1", kwarg1="value1")


def test_provide_session() -> None:
    """Test provide_session context manager."""
    sqlspec = SQLSpec()
    config = MockSyncConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "provide_session") as mock_provide:
        mock_cm = Mock()
        mock_provide.return_value = mock_cm

        result = sqlspec.provide_session(MockSyncConfig, "arg1", kwarg1="value1")  # type: ignore[arg-type,type-var]
        assert result == mock_cm
        mock_provide.assert_called_once_with("arg1", kwarg1="value1")


@pytest.mark.parametrize(
    "config_classes",
    [
        [],  # No configs
        [MockSyncConfig],  # Single sync config
        [MockSyncPoolConfig],  # Single sync pool config
        [MockSyncConfig, MockSyncPoolConfig],  # Mixed sync configs
    ],
)
def test_cleanup_pools_sync(config_classes: "list[type]") -> None:
    """Test cleanup pools with various sync configurations."""
    sqlspec = SQLSpec()
    configs = []

    for config_class in config_classes:
        config = config_class(f"test_{config_class.__name__}")
        sqlspec.add_config(config)
        configs.append(config)

    with patch("sqlspec.base.logger") as mock_logger:
        # Patch close_pool for pooled configs
        close_pool_mocks = []
        for config in configs:
            if hasattr(config, "close_pool") and config.supports_connection_pooling:
                mock_close = Mock()
                patch.object(config, "close_pool", mock_close).start()
                close_pool_mocks.append(mock_close)

        sqlspec._cleanup_pools()

        # Verify close_pool was called for pooled configs
        for mock_close in close_pool_mocks:
            mock_close.assert_called_once()

        # Verify cleanup completed log
        info_calls = [call for call in mock_logger.info.call_args_list if "Pool cleanup completed" in str(call)]
        assert len(info_calls) == 1

        # Verify configs were cleared
        assert len(sqlspec._configs) == 0


def test_cleanup_pools_async() -> None:
    """Test cleanup pools with async configurations."""
    sqlspec = SQLSpec()
    config = MockAsyncPoolConfig("test")
    sqlspec.add_config(config)

    async def mock_close_pool() -> None:
        pass

    with patch.object(config, "close_pool", mock_close_pool):
        with patch("asyncio.run") as mock_run:
            with patch("asyncio.get_running_loop", side_effect=RuntimeError):
                sqlspec._cleanup_pools()
                mock_run.assert_called_once()


def test_cleanup_pools_exception_handling() -> None:
    """Test cleanup handles exceptions gracefully."""
    sqlspec = SQLSpec()
    config = MockSyncPoolConfig("test")
    sqlspec.add_config(config)

    with patch.object(config, "close_pool", side_effect=Exception("Pool error")):
        with patch("sqlspec.base.logger") as mock_logger:
            sqlspec._cleanup_pools()

            warning_calls = [
                call for call in mock_logger.warning.call_args_list if "Failed to clean up pool" in str(call)
            ]
            assert len(warning_calls) == 1


def test_thread_safety() -> None:
    """Test thread safety of configuration operations."""
    sqlspec = SQLSpec()
    results = []
    errors = []

    def worker(worker_id: int) -> None:
        try:
            # Create unique config class per thread
            config_class = type(f"ThreadConfig{worker_id}", (MockSyncConfig,), {})
            config = config_class(f"thread_{worker_id}")

            # Add config
            sqlspec.add_config(config)

            # Get config
            retrieved: Any = sqlspec.get_config(config_class)
            results.append((worker_id, retrieved))
        except Exception as e:
            errors.append((worker_id, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(errors) == 0
    assert len(results) == 10
    assert len(sqlspec._configs) == 10


@pytest.mark.asyncio
async def test_concurrent_async_operations() -> None:
    """Test concurrent async operations."""
    sqlspec = SQLSpec()
    config = MockAsyncConfig("test")
    sqlspec.add_config(config)

    async def get_session_worker(worker_id: int) -> "tuple[int, Any]":
        session = await sqlspec.get_session(MockAsyncConfig)  # type: ignore[arg-type]
        return worker_id, session

    results = await asyncio.gather(*[get_session_worker(i) for i in range(10)])

    assert len(results) == 10
    for worker_id, session in results:
        assert isinstance(session, MockAsyncDriver)
