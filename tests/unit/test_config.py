"""Tests for sqlspec.config module."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.config import (
    AsyncDatabaseConfig,
    DatabaseConfigProtocol,
    GenericPoolConfig,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    SyncDatabaseConfig,
)
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL
from sqlspec.typing import DictRow

# Test Fixtures and Mock Classes


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, name: str = "mock_connection") -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class MockAsyncConnection:
    """Mock async connection for testing."""

    def __init__(self, name: str = "mock_async_connection") -> None:
        self.name = name
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class MockPool:
    """Mock pool for testing."""

    def __init__(self, name: str = "mock_pool") -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class MockAsyncPool:
    """Mock async pool for testing."""

    def __init__(self, name: str = "mock_async_pool") -> None:
        self.name = name
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class MockDriver(SyncDriverAdapterProtocol[MockConnection, DictRow]):
    """Mock driver for testing."""

    dialect = "mock"
    parameter_style = ParameterStyle.QMARK

    def __init__(self, connection: MockConnection, default_row_type: Any = None) -> None:
        super().__init__(connection, None, default_row_type)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    def _execute_statement(self, statement: SQL, connection: MockConnection | None = None, **kwargs: Any) -> Any:
        return Mock()

    def _wrap_select_result(self, statement: SQL, result: Any, schema_type: type | None = None, **kwargs: Any) -> Mock:
        return Mock()

    def _wrap_execute_result(self, statement: SQL, result: Any, **kwargs: Any) -> Mock:
        return Mock()

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Mock execute method."""
        return Mock()

    def execute_many(self, *args: Any, **kwargs: Any) -> Any:
        """Mock execute_many method."""
        return Mock()


class MockAsyncDriver(AsyncDriverAdapterProtocol[MockAsyncConnection, DictRow]):
    """Mock async driver for testing."""

    dialect = "mock_async"
    parameter_style = ParameterStyle.QMARK

    def __init__(self, connection: MockAsyncConnection, default_row_type: Any = None) -> None:
        super().__init__(connection, None, default_row_type)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    async def _execute_statement(
        self, statement: SQL, connection: MockAsyncConnection | None = None, **kwargs: Any
    ) -> Any:
        return AsyncMock()

    async def _wrap_select_result(
        self, statement: SQL, result: Any, schema_type: type | None = None, **kwargs: Any
    ) -> AsyncMock:
        return AsyncMock()

    async def _wrap_execute_result(self, statement: SQL, result: Any, **kwargs: Any) -> AsyncMock:
        return AsyncMock()

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Mock async execute method."""
        return AsyncMock()

    async def execute_many(self, *args: Any, **kwargs: Any) -> Any:
        """Mock async execute_many method."""
        return AsyncMock()


class MockSyncConfig(NoPoolSyncConfig[MockConnection, MockDriver]):
    """Mock sync configuration without pooling."""

    connection_type = MockConnection
    driver_type = MockDriver

    def __init__(self, connection_params: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.connection_params = connection_params or {}

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return dict(self.connection_params)  # Return a copy to ensure immutability

    def create_connection(self) -> MockConnection:
        return MockConnection("test_connection")

    def provide_connection(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()

    def provide_session(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()


class MockAsyncConfig(NoPoolAsyncConfig[MockAsyncConnection, MockAsyncDriver]):
    """Mock async configuration without pooling."""

    connection_type = MockAsyncConnection
    driver_type = MockAsyncDriver

    def __init__(self, connection_params: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.connection_params = connection_params or {}

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return dict(self.connection_params)  # Return a copy to ensure immutability

    async def create_connection(self) -> MockAsyncConnection:
        return MockAsyncConnection("test_async_connection")

    def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncMock:
        return AsyncMock()

    def provide_session(self, *args: Any, **kwargs: Any) -> AsyncMock:
        return AsyncMock()


class MockSyncPoolConfig(SyncDatabaseConfig[MockConnection, MockPool, MockDriver]):
    """Mock sync configuration with pooling."""

    connection_type = MockConnection
    driver_type = MockDriver

    def __init__(self, connection_params: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.connection_params = connection_params or {}
        self._pool: MockPool | None = None

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return dict(self.connection_params)  # Return a copy to ensure immutability

    def create_connection(self) -> MockConnection:
        return MockConnection("test_connection_from_pool")

    def provide_connection(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()

    def provide_session(self, *args: Any, **kwargs: Any) -> Mock:
        return Mock()

    def _create_pool(self) -> MockPool:
        self._pool = MockPool("test_pool")
        return self._pool

    def _close_pool(self) -> None:
        if self._pool:
            self._pool.close()
            self._pool = None


class MockAsyncPoolConfig(AsyncDatabaseConfig[MockAsyncConnection, MockAsyncPool, MockAsyncDriver]):
    """Mock async configuration with pooling."""

    connection_type = MockAsyncConnection
    driver_type = MockAsyncDriver

    def __init__(self, connection_params: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.connection_params = connection_params or {}
        self._pool: MockAsyncPool | None = None

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return dict(self.connection_params)  # Return a copy to ensure immutability

    async def create_connection(self) -> MockAsyncConnection:
        return MockAsyncConnection("test_async_connection_from_pool")

    def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncMock:
        return AsyncMock()

    def provide_session(self, *args: Any, **kwargs: Any) -> AsyncMock:
        return AsyncMock()

    async def _create_pool(self) -> MockAsyncPool:
        self._pool = MockAsyncPool("test_async_pool")
        return self._pool

    async def _close_pool(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None


def test_no_pool_sync_config_basic() -> None:
    """Test basic NoPoolSyncConfig functionality."""
    config = MockSyncConfig()

    assert config.is_async is False
    assert config.supports_connection_pooling is False
    assert config.pool_instance is None


def test_no_pool_sync_config_connection_creation() -> None:
    """Test NoPoolSyncConfig connection creation."""
    config = MockSyncConfig()
    connection = config.create_connection()

    assert isinstance(connection, MockConnection)
    assert connection.name == "test_connection"


def test_no_pool_sync_config_pool_operations() -> None:
    """Test NoPoolSyncConfig pool operations return None."""
    config = MockSyncConfig()

    assert config.create_pool() is None  # type: ignore[func-returns-value]
    assert config.close_pool() is None  # type: ignore[func-returns-value]
    assert config.provide_pool() is None  # type: ignore[func-returns-value]


def test_no_pool_async_config_basic() -> None:
    """Test basic NoPoolAsyncConfig functionality."""
    config = MockAsyncConfig()

    assert config.is_async is True
    assert config.supports_connection_pooling is False
    assert config.is_async is True
    assert config.supports_connection_pooling is False
    assert config.pool_instance is None


async def test_no_pool_async_config_connection_creation() -> None:
    """Test NoPoolAsyncConfig connection creation."""
    config = MockAsyncConfig()
    connection = await config.create_connection()

    assert isinstance(connection, MockAsyncConnection)
    assert connection.name == "test_async_connection"


async def test_no_pool_async_config_pool_operations() -> None:
    """Test NoPoolAsyncConfig pool operations return None."""
    config = MockAsyncConfig()

    assert await config.create_pool() is None  # type: ignore[func-returns-value]
    assert await config.close_pool() is None  # type: ignore[func-returns-value]
    assert config.provide_pool() is None  # type: ignore[func-returns-value]


def test_sync_database_config_basic() -> None:
    """Test basic SyncDatabaseConfig functionality."""
    config = MockSyncPoolConfig()

    assert config.is_async is False
    assert config.supports_connection_pooling is True


def test_sync_database_config_pool_creation() -> None:
    """Test SyncDatabaseConfig pool creation."""
    config = MockSyncPoolConfig()

    with patch("sqlspec.config.logger") as mock_logger:
        pool = config.create_pool()

        assert isinstance(pool, MockPool)
        assert pool.name == "test_pool"

        # Check logging
        mock_logger.info.assert_called()
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("Creating database connection pool" in call for call in log_calls)


def test_sync_database_config_pool_closure() -> None:
    """Test SyncDatabaseConfig pool closure."""
    config = MockSyncPoolConfig()

    # Create pool first
    pool = config.create_pool()
    assert not pool.closed

    with patch("sqlspec.config.logger") as mock_logger:
        config.close_pool()

        assert pool.closed

        # Check logging
        mock_logger.info.assert_called()  # type: ignore[unreachable]
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("Closing database connection pool" in call for call in log_calls)


def test_sync_database_config_pool_metrics() -> None:
    """Test SyncDatabaseConfig with pool metrics."""
    config = MockSyncPoolConfig()

    # Mock metrics
    mock_counter = Mock()
    mock_gauge = Mock()

    # Patch the isinstance calls in the config module to return True for our mocks
    with patch("sqlspec.config.isinstance") as mock_isinstance:
        mock_isinstance.return_value = True

        # Test pool creation with metrics
        pool = config.create_pool()
        assert isinstance(pool, MockPool)

        mock_counter.labels.assert_called()
        mock_gauge.labels.assert_called()


# AsyncDatabaseConfig Tests


def test_async_database_config_basic() -> None:
    """Test basic AsyncDatabaseConfig functionality."""
    config = MockAsyncPoolConfig()

    assert config.is_async is True
    assert config.supports_connection_pooling is True


async def test_async_database_config_pool_creation() -> None:
    """Test AsyncDatabaseConfig pool creation."""
    config = MockAsyncPoolConfig()

    with patch("sqlspec.config.logger") as mock_logger:
        pool = await config.create_pool()

        assert isinstance(pool, MockAsyncPool)
        assert pool.name == "test_async_pool"

        # Check logging
        mock_logger.info.assert_called()
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("Creating async database connection pool" in call for call in log_calls)


async def test_async_database_config_pool_closure() -> None:
    """Test AsyncDatabaseConfig pool closure."""
    config = MockAsyncPoolConfig()

    # Create pool first
    pool = await config.create_pool()
    assert not pool.closed

    with patch("sqlspec.config.logger") as mock_logger:
        await config.close_pool()

        assert pool.closed

        # Check logging
        mock_logger.info.assert_called()  # type: ignore[unreachable]
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("Closing async database connection pool" in call for call in log_calls)


async def test_async_database_config_concurrent_pool_operations() -> None:
    """Test AsyncDatabaseConfig with concurrent pool operations."""
    config = MockAsyncPoolConfig()

    # Test concurrent pool creation (should be safe)
    pools = await asyncio.gather(config.create_pool(), config.create_pool(), config.create_pool())

    assert len(pools) == 3
    for pool in pools:
        assert isinstance(pool, MockAsyncPool)


# GenericPoolConfig Tests


def test_generic_pool_config_basic() -> None:
    """Test basic GenericPoolConfig functionality."""
    config = GenericPoolConfig()
    assert isinstance(config, GenericPoolConfig)


def test_generic_pool_config_as_dataclass() -> None:
    """Test GenericPoolConfig behaves as a dataclass."""
    config = GenericPoolConfig()

    # Test that we can create a new instance with replace
    new_config = replace(config)
    assert isinstance(new_config, GenericPoolConfig)
    assert new_config is not config


# DatabaseConfigProtocol Tests


def test_database_config_protocol_hash() -> None:
    """Test DatabaseConfigProtocol hash implementation."""
    config1 = MockSyncConfig()
    config2 = MockSyncConfig()

    # Hash should be based on object id
    assert hash(config1) == id(config1)
    assert hash(config2) == id(config2)
    assert hash(config1) != hash(config2)


def test_database_config_protocol_properties() -> None:
    """Test DatabaseConfigProtocol property access."""
    sync_config = MockSyncConfig()
    async_config = MockAsyncConfig()
    sync_pool_config = MockSyncPoolConfig()
    async_pool_config = MockAsyncPoolConfig()

    assert sync_config.is_async is False
    assert async_config.is_async is True
    assert sync_pool_config.is_async is False
    assert async_pool_config.is_async is True

    assert sync_config.supports_connection_pooling is False
    assert async_config.supports_connection_pooling is False
    assert sync_pool_config.supports_connection_pooling is True
    assert async_pool_config.supports_connection_pooling is True


@pytest.mark.parametrize(
    ("config_class", "expected_async", "expected_pooling"),
    [
        (MockSyncConfig, False, False),
        (MockAsyncConfig, True, False),
        (MockSyncPoolConfig, False, True),
        (MockAsyncPoolConfig, True, True),
    ],
    ids=["sync_no_pool", "async_no_pool", "sync_pool", "async_pool"],
)
def test_database_config_protocol_subclasses(config_class: type, expected_async: bool, expected_pooling: bool) -> None:
    """Test various DatabaseConfigProtocol subclasses."""
    config = config_class()

    assert config.is_async == expected_async
    assert config.supports_connection_pooling == expected_pooling
    assert isinstance(config, DatabaseConfigProtocol)


# Configuration Dictionary Tests


def test_config_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    connection_params = {"host": "localhost", "port": 5432, "database": "test_db", "user": "test_user"}

    config = MockSyncConfig(connection_params)
    assert config.connection_config_dict == connection_params


def test_config_connection_config_dict_empty() -> None:
    """Test connection_config_dict with empty parameters."""
    config = MockSyncConfig()
    assert config.connection_config_dict == {}


def test_config_connection_config_dict_modification() -> None:
    """Test that connection_config_dict modifications don't affect original."""
    original_params = {"host": "localhost", "port": 5432}
    config = MockSyncConfig(original_params)

    config_dict = config.connection_config_dict
    config_dict["new_key"] = "new_value"

    # Original should not be modified
    assert "new_key" not in original_params
    assert "new_key" not in config.connection_params


# Error Handling Tests


def test_sync_config_create_pool_exception_handling() -> None:
    """Test SyncDatabaseConfig pool creation exception handling."""
    config = MockSyncPoolConfig()

    with patch.object(config, "_create_pool", side_effect=Exception("Pool creation failed")):
        with pytest.raises(Exception, match="Pool creation failed"):
            config.create_pool()


async def test_async_config_create_pool_exception_handling() -> None:
    """Test AsyncDatabaseConfig pool creation exception handling."""
    config = MockAsyncPoolConfig()

    with patch.object(config, "_create_pool", side_effect=Exception("Async pool creation failed")):
        with pytest.raises(Exception, match="Async pool creation failed"):
            await config.create_pool()


def test_sync_config_close_pool_exception_handling() -> None:
    """Test SyncDatabaseConfig pool closure exception handling."""
    config = MockSyncPoolConfig()

    with patch.object(config, "_close_pool", side_effect=Exception("Pool closure failed")):
        with pytest.raises(Exception, match="Pool closure failed"):
            config.close_pool()


async def test_async_config_close_pool_exception_handling() -> None:
    """Test AsyncDatabaseConfig pool closure exception handling."""
    config = MockAsyncPoolConfig()

    with patch.object(config, "_close_pool", side_effect=Exception("Async pool closure failed")):
        with pytest.raises(Exception, match="Async pool closure failed"):
            await config.close_pool()


async def test_config_concurrent_pool_creation_and_closure() -> None:
    """Test concurrent pool creation and closure operations."""
    config = MockAsyncPoolConfig()

    async def create_and_close() -> MockAsyncPool:
        pool = await config.create_pool()
        await config.close_pool()
        return pool

    # Run multiple concurrent create/close cycles
    results = await asyncio.gather(*[create_and_close() for _ in range(10)])
    assert len(results) == 10


def test_config_type_validation() -> None:
    """Test configuration type relationships."""
    sync_config = MockSyncConfig()
    async_config = MockAsyncConfig()

    # Test type hierarchy
    assert isinstance(sync_config, DatabaseConfigProtocol)
    assert isinstance(async_config, DatabaseConfigProtocol)
    assert isinstance(sync_config, NoPoolSyncConfig)
    assert isinstance(async_config, NoPoolAsyncConfig)

    # Test that sync and async configs are different
    assert type(sync_config) is not type(async_config)  # type: ignore[comparison-overlap]
    assert sync_config.is_async != async_config.is_async
