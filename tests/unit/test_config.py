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
    InstrumentationConfig,
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

    def __init__(
        self, connection: MockConnection, instrumentation_config: Any = None, default_row_type: Any = None
    ) -> None:
        super().__init__(connection, None, instrumentation_config, default_row_type)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    def _execute_statement(
        self,
        statement: SQL,
        connection: MockConnection | None = None,
        **kwargs: Any,
    ) -> Any:
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

    def __init__(
        self, connection: MockAsyncConnection, instrumentation_config: Any = None, default_row_type: Any = None
    ) -> None:
        super().__init__(connection, None, instrumentation_config, default_row_type)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    async def _execute_statement(
        self,
        statement: SQL,
        connection: MockAsyncConnection | None = None,
        **kwargs: Any,
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


# InstrumentationConfig Tests


def test_instrumentation_config_default_values() -> None:
    """Test InstrumentationConfig default values."""
    config = InstrumentationConfig()

    assert config.log_queries is True
    assert config.log_runtime is True
    assert config.log_parameters is False
    assert config.log_results_count is True
    assert config.log_pool_operations is True
    assert config.enable_opentelemetry is False
    assert config.enable_prometheus is False
    assert config.slow_query_threshold_ms == 1000.0
    assert config.slow_pool_operation_ms == 5000.0
    assert config.service_name == "sqlspec"
    assert config.custom_tags == {}
    assert config.prometheus_latency_buckets is None


def test_instrumentation_config_custom_values() -> None:
    """Test InstrumentationConfig with custom values."""
    custom_tags = {"env": "test", "version": "1.0"}
    custom_buckets = [0.1, 0.5, 1.0, 5.0]

    config = InstrumentationConfig(
        log_queries=False,
        log_runtime=False,
        log_parameters=True,
        log_results_count=False,
        log_pool_operations=False,
        enable_opentelemetry=True,
        enable_prometheus=True,
        slow_query_threshold_ms=500.0,
        slow_pool_operation_ms=2000.0,
        service_name="custom_service",
        custom_tags=custom_tags,
        prometheus_latency_buckets=custom_buckets,
    )

    assert config.log_queries is False
    assert config.log_runtime is False
    assert config.log_parameters is True
    assert config.log_results_count is False
    assert config.log_pool_operations is False
    assert config.enable_opentelemetry is True
    assert config.enable_prometheus is True
    assert config.slow_query_threshold_ms == 500.0
    assert config.slow_pool_operation_ms == 2000.0
    assert config.service_name == "custom_service"
    assert config.custom_tags == custom_tags
    assert config.prometheus_latency_buckets == custom_buckets


@pytest.mark.parametrize(
    ("threshold", "expected"),
    [
        (0.0, 0.0),
        (100.0, 100.0),
        (1000.0, 1000.0),
        (5000.0, 5000.0),
        (float("inf"), float("inf")),
    ],
    ids=["zero", "small", "default", "large", "infinite"],
)
def test_instrumentation_config_threshold_values(threshold: float, expected: float) -> None:
    """Test InstrumentationConfig with various threshold values."""
    config = InstrumentationConfig(slow_query_threshold_ms=threshold)
    assert config.slow_query_threshold_ms == expected


def test_instrumentation_config_custom_tags_immutability() -> None:
    """Test that custom_tags dict is properly isolated."""
    original_tags = {"env": "test"}
    config = InstrumentationConfig(custom_tags=original_tags)

    # Modifying original should not affect config
    original_tags["new_key"] = "new_value"
    assert "new_key" not in config.custom_tags

    # Modifying config tags should not affect original
    config.custom_tags["config_key"] = "config_value"
    assert "config_key" not in original_tags


def test_instrumentation_config_prometheus_buckets() -> None:
    """Test InstrumentationConfig with various prometheus bucket configurations."""
    # Test with None (default)
    config1 = InstrumentationConfig()
    assert config1.prometheus_latency_buckets is None

    # Test with custom buckets
    custom_buckets = [0.001, 0.01, 0.1, 1.0, 10.0]
    config2 = InstrumentationConfig(prometheus_latency_buckets=custom_buckets)
    assert config2.prometheus_latency_buckets == custom_buckets

    # Test with empty list
    config3 = InstrumentationConfig(prometheus_latency_buckets=[])
    assert config3.prometheus_latency_buckets == []


# NoPoolSyncConfig Tests


def test_no_pool_sync_config_basic() -> None:
    """Test basic NoPoolSyncConfig functionality."""
    config = MockSyncConfig()

    assert config.__is_async__ is False
    assert config.__supports_connection_pooling__ is False
    assert config.is_async is False
    assert config.support_connection_pooling is False
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


def test_no_pool_sync_config_instrumentation() -> None:
    """Test NoPoolSyncConfig instrumentation integration."""
    custom_instrumentation = InstrumentationConfig(
        service_name="test_service",
        enable_opentelemetry=True,
    )

    config = MockSyncConfig()
    config.instrumentation = custom_instrumentation

    assert config.instrumentation.service_name == "test_service"
    assert config.instrumentation.enable_opentelemetry is True


def test_no_pool_sync_config_instrument_sync_operation() -> None:
    """Test NoPoolSyncConfig sync operation instrumentation."""
    config = MockSyncConfig()
    mock_func = Mock(return_value="test_result")

    result = config.instrument_sync_operation(
        "test_operation",
        "database",
        {},
        mock_func,
        config,
        "arg1",
        kwarg1="value1",
    )

    assert result == "test_result"
    mock_func.assert_called_once_with(config, "arg1", kwarg1="value1")


def test_no_pool_sync_config_instrument_sync_operation_with_exception() -> None:
    """Test NoPoolSyncConfig sync operation instrumentation with exception."""
    config = MockSyncConfig()
    mock_func = Mock(side_effect=ValueError("Test error"))

    with pytest.raises(ValueError, match="Test error"):
        config.instrument_sync_operation(
            "test_operation",
            "database",
            {},
            mock_func,
            config,
        )


# NoPoolAsyncConfig Tests


def test_no_pool_async_config_basic() -> None:
    """Test basic NoPoolAsyncConfig functionality."""
    config = MockAsyncConfig()

    assert config.__is_async__ is True
    assert config.__supports_connection_pooling__ is False
    assert config.is_async is True
    assert config.support_connection_pooling is False
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


async def test_no_pool_async_config_instrument_async_operation() -> None:
    """Test NoPoolAsyncConfig async operation instrumentation."""
    config = MockAsyncConfig()
    mock_func = AsyncMock(return_value="async_result")

    result = await config.instrument_async_operation(
        "test_async_operation",
        "database",
        {},
        mock_func,
        config,
        "arg1",
        kwarg1="value1",
    )

    assert result == "async_result"
    mock_func.assert_called_once_with(config, "arg1", kwarg1="value1")


async def test_no_pool_async_config_instrument_async_operation_with_exception() -> None:
    """Test NoPoolAsyncConfig async operation instrumentation with exception."""
    config = MockAsyncConfig()
    mock_func = AsyncMock(side_effect=RuntimeError("Async test error"))

    with pytest.raises(RuntimeError, match="Async test error"):
        await config.instrument_async_operation(
            "test_async_operation",
            "database",
            {},
            mock_func,
            config,
        )


# SyncDatabaseConfig Tests


def test_sync_database_config_basic() -> None:
    """Test basic SyncDatabaseConfig functionality."""
    config = MockSyncPoolConfig()

    assert config.__is_async__ is False
    assert config.__supports_connection_pooling__ is True
    assert config.is_async is False
    assert config.support_connection_pooling is True


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
    config._pool_metrics = {
        "pool_operations": mock_counter,
        "pool_connections": mock_gauge,
    }

    # Patch the isinstance calls in the config module to return True for our mocks
    with patch("sqlspec.config.isinstance") as mock_isinstance:
        mock_isinstance.return_value = True

        # Test pool creation with metrics
        pool = config.create_pool()
        assert isinstance(pool, MockPool)

        mock_counter.labels.assert_called()
        mock_gauge.labels.assert_called()


def test_sync_database_config_instrumentation_logging() -> None:
    """Test SyncDatabaseConfig with different instrumentation settings."""
    # Test with logging disabled
    config = MockSyncPoolConfig()
    config.instrumentation = InstrumentationConfig(log_pool_operations=False)

    with patch("sqlspec.config.logger") as mock_logger:
        config.create_pool()

        # Should not log when disabled
        mock_logger.info.assert_not_called()


# AsyncDatabaseConfig Tests


def test_async_database_config_basic() -> None:
    """Test basic AsyncDatabaseConfig functionality."""
    config = MockAsyncPoolConfig()

    assert config.__is_async__ is True
    assert config.__supports_connection_pooling__ is True
    assert config.is_async is True
    assert config.support_connection_pooling is True


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


async def test_async_database_config_pool_metrics() -> None:
    """Test AsyncDatabaseConfig with pool metrics."""
    config = MockAsyncPoolConfig()

    # Mock metrics
    mock_counter = Mock()
    mock_gauge = Mock()
    config._pool_metrics = {
        "pool_operations": mock_counter,
        "pool_connections": mock_gauge,
    }

    # Patch the isinstance calls in the config module to return True for our mocks
    with patch("sqlspec.config.isinstance") as mock_isinstance:
        mock_isinstance.return_value = True

        # Test pool creation with metrics
        pool = await config.create_pool()
        assert isinstance(pool, MockAsyncPool)

        mock_counter.labels.assert_called()
        mock_gauge.labels.assert_called()


async def test_async_database_config_concurrent_pool_operations() -> None:
    """Test AsyncDatabaseConfig with concurrent pool operations."""
    config = MockAsyncPoolConfig()

    # Test concurrent pool creation (should be safe)
    pools = await asyncio.gather(
        config.create_pool(),
        config.create_pool(),
        config.create_pool(),
    )

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

    assert sync_config.support_connection_pooling is False
    assert async_config.support_connection_pooling is False
    assert sync_pool_config.support_connection_pooling is True
    assert async_pool_config.support_connection_pooling is True


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
    assert config.support_connection_pooling == expected_pooling
    assert isinstance(config, DatabaseConfigProtocol)


# Configuration Dictionary Tests


def test_config_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    connection_params = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
    }

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


# Instrumentation Integration Tests


def test_config_instrumentation_operation_success() -> None:
    """Test configuration instrumentation for successful operations."""
    config = MockSyncConfig()
    config.instrumentation = InstrumentationConfig(log_queries=True)

    mock_func = Mock(return_value="success")

    with patch("sqlspec.config.logger") as mock_logger:
        result = config.instrument_sync_operation(
            "test_operation",
            "database",
            {"custom_tag": "value"},
            mock_func,
            config,
        )

        assert result == "success"
        mock_logger.info.assert_called()


async def test_async_config_instrumentation_operation_success() -> None:
    """Test async configuration instrumentation for successful operations."""
    config = MockAsyncConfig()
    config.instrumentation = InstrumentationConfig(log_queries=True)

    mock_func = AsyncMock(return_value="async_success")

    with patch("sqlspec.config.logger") as mock_logger:
        result = await config.instrument_async_operation(
            "test_async_operation",
            "database",
            {"custom_tag": "value"},
            mock_func,
            config,
        )

        assert result == "async_success"
        mock_logger.info.assert_called()


def test_config_instrumentation_logging_disabled() -> None:
    """Test configuration instrumentation with logging disabled."""
    config = MockSyncConfig()
    config.instrumentation = InstrumentationConfig(log_queries=False)

    mock_func = Mock(return_value="success")

    with patch("sqlspec.config.logger") as mock_logger:
        result = config.instrument_sync_operation(
            "test_operation",
            "database",
            {},
            mock_func,
            config,
        )

        assert result == "success"
        mock_logger.info.assert_not_called()


def test_config_instrumentation_exception_logging() -> None:
    """Test configuration instrumentation exception logging."""
    config = MockSyncConfig()
    config.instrumentation = InstrumentationConfig(log_queries=True)

    mock_func = Mock(side_effect=ValueError("Test exception"))

    with patch("sqlspec.config.logger") as mock_logger:
        with pytest.raises(ValueError, match="Test exception"):
            config.instrument_sync_operation(
                "test_operation",
                "database",
                {},
                mock_func,
                config,
            )

        mock_logger.exception.assert_called()


# Performance and Stress Tests


def test_config_large_custom_tags() -> None:
    """Test InstrumentationConfig with large number of custom tags."""
    large_tags = {f"tag_{i}": f"value_{i}" for i in range(1000)}

    config = InstrumentationConfig(custom_tags=large_tags)
    assert len(config.custom_tags) == 1000
    assert config.custom_tags["tag_500"] == "value_500"


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


def test_config_instrumentation_performance() -> None:
    """Test instrumentation performance with many operations."""
    config = MockSyncConfig()
    mock_func = Mock(return_value="result")

    # Run many instrumented operations
    for i in range(1000):
        result = config.instrument_sync_operation(
            f"operation_{i}",
            "database",
            {"iteration": i},
            mock_func,
            config,
        )
        assert result == "result"

    assert mock_func.call_count == 1000


# Integration Tests


def test_config_with_real_instrumentation_config() -> None:
    """Test configuration with realistic instrumentation settings."""
    instrumentation = InstrumentationConfig(
        log_queries=True,
        log_runtime=True,
        log_parameters=True,
        enable_opentelemetry=True,
        enable_prometheus=True,
        service_name="integration_test",
        custom_tags={"env": "test", "component": "database"},
        slow_query_threshold_ms=100.0,
    )

    config = MockSyncPoolConfig()
    config.instrumentation = instrumentation

    assert config.instrumentation.service_name == "integration_test"
    assert config.instrumentation.custom_tags["env"] == "test"
    assert config.instrumentation.slow_query_threshold_ms == 100.0


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
