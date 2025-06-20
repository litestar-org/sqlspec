"""Unit tests for sqlspec.config module."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.config import (
    AsyncDatabaseConfig,
    GenericPoolConfig,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    SyncDatabaseConfig,
)
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager


# Mock implementations for testing
class MockConnection:
    """Mock database connection."""

    def __init__(self, name: "str" = "mock") -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class MockPool:
    """Mock connection pool."""

    def __init__(self, name: "str" = "mock_pool") -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class MockSyncDriver(SyncDriverAdapterProtocol["MockConnection", "dict[str, Any]"]):
    """Mock sync driver."""

    dialect = "mock"

    def __init__(self, connection: "MockConnection", default_row_type: "type[Any]" = dict) -> None:
        super().__init__(connection=connection, config=None, default_row_type=default_row_type)

    def _execute_statement(
        self, statement: "Any", connection: "Optional[MockConnection]" = None, **kwargs: "Any"
    ) -> "Any":
        return {"rows": [], "rowcount": 0}

    def _execute(self, sql: "str", parameters: "Any", connection: "MockConnection", **kwargs: "Any") -> "Any":
        return {"rows": [], "rowcount": 0}

    def _execute_many(self, sql: "str", parameters: "Any", connection: "MockConnection", **kwargs: "Any") -> "Any":
        return {"rows": [], "rowcount": 0}

    def _execute_script(self, sql: "str", connection: "MockConnection", **kwargs: "Any") -> "Any":
        return {"rows": [], "rowcount": 0}

    def _wrap_select_result(
        self, statement: "Any", result: "Any", schema_type: "Optional[type[Any]]" = None, **kwargs: "Any"
    ) -> "Any":
        return Mock(rows=result.get("rows", []), row_count=result.get("rowcount", 0))

    def _wrap_execute_result(self, statement: "Any", result: "Any", **kwargs: "Any") -> "Any":
        return Mock(affected_count=result.get("rowcount", 0), last_insert_id=None)


class MockAsyncDriver(AsyncDriverAdapterProtocol["MockConnection", "dict[str, Any]"]):
    """Mock async driver."""

    dialect = "mock"

    def __init__(self, connection: "MockConnection", default_row_type: "type[Any]" = dict) -> None:
        super().__init__(connection=connection, config=None, default_row_type=default_row_type)

    async def _execute_statement(
        self, statement: "Any", connection: "Optional[MockConnection]" = None, **kwargs: "Any"
    ) -> "Any":
        return {"rows": [], "rowcount": 0}

    async def _execute(self, sql: "str", parameters: "Any", connection: "MockConnection", **kwargs: "Any") -> "Any":
        return {"rows": [], "rowcount": 0}

    async def _execute_many(
        self, sql: "str", parameters: "Any", connection: "MockConnection", **kwargs: "Any"
    ) -> "Any":
        return {"rows": [], "rowcount": 0}

    async def _execute_script(self, sql: "str", connection: "MockConnection", **kwargs: "Any") -> "Any":
        return {"rows": [], "rowcount": 0}

    async def _wrap_select_result(
        self, statement: "Any", result: "Any", schema_type: "Optional[type[Any]]" = None, **kwargs: "Any"
    ) -> "Any":
        return Mock(rows=result.get("rows", []), row_count=result.get("rowcount", 0))

    async def _wrap_execute_result(self, statement: "Any", result: "Any", **kwargs: "Any") -> "Any":
        return Mock(affected_count=result.get("rowcount", 0), last_insert_id=None)


# Test GenericPoolConfig
def test_generic_pool_config() -> None:
    """Test GenericPoolConfig is a simple dataclass."""
    config = GenericPoolConfig()
    assert isinstance(config, GenericPoolConfig)


# Concrete config implementations for testing
@dataclass
class MockSyncTestConfig(NoPoolSyncConfig["MockConnection", "MockSyncDriver"]):
    """Mock sync config without pooling for testing."""

    driver_type: "type[MockSyncDriver]" = MockSyncDriver
    connection_type: "type[MockConnection]" = MockConnection
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("qmark", "named")
    preferred_parameter_style: "ClassVar[str]" = "qmark"
    default_row_type: "type[Any]" = dict

    def __hash__(self) -> int:
        return id(self)

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"type": "sync", "pooling": False}

    def create_connection(self) -> "MockConnection":
        return MockConnection("sync")

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AbstractContextManager[MockConnection]":
        mock = Mock()
        mock.__enter__ = Mock(return_value=MockConnection("sync"))
        mock.__exit__ = Mock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "AbstractContextManager[MockSyncDriver]":
        conn = MockConnection("sync")
        driver = self.driver_type(conn, default_row_type=self.default_row_type)
        mock = Mock()
        mock.__enter__ = Mock(return_value=driver)
        mock.__exit__ = Mock(return_value=None)
        return mock


@dataclass(eq=False)
class MockAsyncTestConfig(NoPoolAsyncConfig["MockConnection", "MockAsyncDriver"]):
    """Mock async config without pooling for testing."""

    driver_type: "type[MockAsyncDriver]" = MockAsyncDriver
    connection_type: "type[MockConnection]" = MockConnection
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("numeric",)
    preferred_parameter_style: "ClassVar[str]" = "numeric"
    default_row_type: "type[Any]" = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"type": "async", "pooling": False}

    async def create_connection(self) -> "MockConnection":
        return MockConnection("async")

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AbstractAsyncContextManager[MockConnection]":
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=MockConnection("async"))
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "AbstractAsyncContextManager[MockAsyncDriver]":
        conn = MockConnection("async")
        driver = self.driver_type(conn, default_row_type=self.default_row_type)
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=driver)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock


@dataclass(eq=False)
class MockSyncPoolTestConfig(SyncDatabaseConfig["MockConnection", "MockPool", "MockSyncDriver"]):
    """Mock sync config with pooling for testing."""

    driver_type: "type[MockSyncDriver]" = MockSyncDriver
    connection_type: "type[MockConnection]" = MockConnection
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("qmark",)
    preferred_parameter_style: "ClassVar[str]" = "qmark"
    default_row_type: "type[Any]" = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"type": "sync", "pooling": True}

    def create_connection(self) -> "MockConnection":
        return MockConnection("sync_pool")

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AbstractContextManager[MockConnection]":
        mock = Mock()
        mock.__enter__ = Mock(return_value=MockConnection("sync_pool"))
        mock.__exit__ = Mock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "AbstractContextManager[MockSyncDriver]":
        conn = MockConnection("sync_pool")
        driver = self.driver_type(conn, default_row_type=self.default_row_type)
        mock = Mock()
        mock.__enter__ = Mock(return_value=driver)
        mock.__exit__ = Mock(return_value=None)
        return mock

    def _create_pool(self) -> "MockPool":
        return MockPool("sync")

    def _close_pool(self) -> None:
        if self.pool_instance:
            self.pool_instance.close()


@dataclass(eq=False)
class MockAsyncPoolTestConfig(AsyncDatabaseConfig["MockConnection", "MockPool", "MockAsyncDriver"]):
    """Mock async config with pooling for testing."""

    driver_type: "type[MockAsyncDriver]" = MockAsyncDriver
    connection_type: "type[MockConnection]" = MockConnection
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("numeric",)
    preferred_parameter_style: "ClassVar[str]" = "numeric"
    default_row_type: "type[Any]" = dict

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        return {"type": "async", "pooling": True}

    async def create_connection(self) -> "MockConnection":
        return MockConnection("async_pool")

    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AbstractAsyncContextManager[MockConnection]":
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=MockConnection("async_pool"))
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    def provide_session(self, *args: "Any", **kwargs: "Any") -> "AbstractAsyncContextManager[MockAsyncDriver]":
        conn = MockConnection("async_pool")
        driver = self.driver_type(conn, default_row_type=self.default_row_type)
        mock = Mock()
        mock.__aenter__ = AsyncMock(return_value=driver)
        mock.__aexit__ = AsyncMock(return_value=None)
        return mock

    async def _create_pool(self) -> "MockPool":
        return MockPool("async")

    async def _close_pool(self) -> None:
        if self.pool_instance:
            self.pool_instance.close()


# Test base protocol functionality
def test_database_config_protocol_hash() -> None:
    """Test DatabaseConfigProtocol hashing uses object ID."""
    config1 = MockSyncTestConfig()
    config2 = MockSyncTestConfig()

    # Different objects should have different hashes
    assert hash(config1) != hash(config2)
    assert hash(config1) == id(config1)
    assert hash(config2) == id(config2)


def test_database_config_dialect_property() -> None:
    """Test dialect property lazy loading."""
    config = MockSyncTestConfig()

    # Initial state - dialect not loaded
    assert config._dialect is None

    # Access dialect - should load from driver
    dialect = config.dialect
    assert dialect == "mock"
    assert config._dialect == "mock"

    # Subsequent access should use cached value
    dialect2 = config.dialect
    assert dialect2 == "mock"


# Test parameter style configuration
def test_sync_config_parameter_styles() -> None:
    """Test sync config parameter style attributes."""
    config = MockSyncTestConfig()
    assert config.supported_parameter_styles == ("qmark", "named")
    assert config.preferred_parameter_style == "qmark"


def test_async_config_parameter_styles() -> None:
    """Test async config parameter style attributes."""
    config = MockAsyncTestConfig()
    assert config.supported_parameter_styles == ("numeric",)
    assert config.preferred_parameter_style == "numeric"


# Test NoPoolSyncConfig behavior
def test_no_pool_sync_config_pool_methods() -> None:
    """Test NoPoolSyncConfig pool methods return None."""
    config = MockSyncTestConfig()

    config.create_pool()  # Should not raise
    config.close_pool()  # Should not raise
    config.provide_pool()  # Should not raise
    assert config.pool_instance is None


# Test NoPoolAsyncConfig behavior
@pytest.mark.asyncio
async def test_no_pool_async_config_pool_methods() -> None:
    """Test NoPoolAsyncConfig pool methods return None."""
    config = MockAsyncTestConfig()

    await config.create_pool()  # Should not raise
    await config.close_pool()  # Should not raise
    config.provide_pool()  # Should not raise
    assert config.pool_instance is None


# Test SyncDatabaseConfig pool management
def test_sync_pool_config_lifecycle() -> None:
    """Test sync pool config pool lifecycle."""
    config = MockSyncPoolTestConfig()

    # Initially no pool
    assert config.pool_instance is None

    # Create pool
    pool = config.create_pool()
    assert isinstance(pool, MockPool)
    assert not pool.closed
    assert config.pool_instance is pool

    # Create pool again returns same instance
    pool2 = config.create_pool()
    assert pool2 is pool

    # Close pool
    config.close_pool()
    assert pool.closed


# Test AsyncDatabaseConfig pool management
@pytest.mark.asyncio
async def test_async_pool_config_lifecycle() -> None:
    """Test async pool config pool lifecycle."""
    config = MockAsyncPoolTestConfig()

    # Initially no pool
    assert config.pool_instance is None

    # Create pool
    pool = await config.create_pool()
    assert isinstance(pool, MockPool)
    assert not pool.closed
    assert config.pool_instance is pool

    # Create pool again returns same instance
    pool2 = await config.create_pool()
    assert pool2 is pool

    # Close pool
    await config.close_pool()
    assert pool.closed


# Test provide_pool methods
def test_sync_pool_config_provide_pool() -> None:
    """Test sync pool config provide_pool creates pool if needed."""
    config = MockSyncPoolTestConfig()

    # Initially no pool
    assert config.pool_instance is None

    # provide_pool creates pool
    pool = config.provide_pool()
    assert isinstance(pool, MockPool)
    assert config.pool_instance is pool

    # Second call returns same pool
    pool2 = config.provide_pool()
    assert pool2 is pool


@pytest.mark.asyncio
async def test_async_pool_config_provide_pool() -> None:
    """Test async pool config provide_pool creates pool if needed."""
    config = MockAsyncPoolTestConfig()

    # Initially no pool
    assert config.pool_instance is None

    # provide_pool creates pool
    pool = await config.provide_pool()
    assert isinstance(pool, MockPool)
    assert config.pool_instance is pool

    # Second call returns same pool
    pool2 = await config.provide_pool()
    assert pool2 is pool


# Test connection and session context managers
@pytest.mark.parametrize("config_class", [MockSyncTestConfig, MockSyncPoolTestConfig], ids=["no_pool", "with_pool"])
def test_sync_provide_connection(config_class: "type") -> None:
    """Test sync config provide_connection context manager."""
    config = config_class()

    with config.provide_connection() as conn:
        assert isinstance(conn, MockConnection)
        assert not conn.closed


@pytest.mark.parametrize("config_class", [MockAsyncTestConfig, MockAsyncPoolTestConfig], ids=["no_pool", "with_pool"])
@pytest.mark.asyncio
async def test_async_provide_connection(config_class: "type") -> None:
    """Test async config provide_connection context manager."""
    config = config_class()

    async with config.provide_connection() as conn:
        assert isinstance(conn, MockConnection)
        assert not conn.closed


@pytest.mark.parametrize(
    "config_class,driver_class",
    [(MockSyncTestConfig, MockSyncDriver), (MockSyncPoolTestConfig, MockSyncDriver)],
    ids=["no_pool", "with_pool"],
)
def test_sync_provide_session(config_class: "type", driver_class: "type") -> None:
    """Test sync config provide_session context manager."""
    config = config_class()

    with config.provide_session() as driver:
        assert isinstance(driver, driver_class)
        assert isinstance(driver.connection, MockConnection)


@pytest.mark.parametrize(
    "config_class,driver_class",
    [(MockAsyncTestConfig, MockAsyncDriver), (MockAsyncPoolTestConfig, MockAsyncDriver)],
    ids=["no_pool", "with_pool"],
)
@pytest.mark.asyncio
async def test_async_provide_session(config_class: "type", driver_class: "type") -> None:
    """Test async config provide_session context manager."""
    config = config_class()

    async with config.provide_session() as driver:
        assert isinstance(driver, driver_class)
        assert isinstance(driver.connection, MockConnection)


# Test default row type
@pytest.mark.parametrize("row_type", [dict, list, tuple])
def test_config_default_row_type(row_type: "type") -> None:
    """Test configuration with different default row types."""
    config = MockSyncTestConfig()
    config.default_row_type = row_type

    with config.provide_session() as driver:
        assert driver.default_row_type == row_type


# Test connection_config_dict property
@pytest.mark.parametrize(
    "config_class,expected_dict",
    [
        (MockSyncTestConfig, {"type": "sync", "pooling": False}),
        (MockAsyncTestConfig, {"type": "async", "pooling": False}),
        (MockSyncPoolTestConfig, {"type": "sync", "pooling": True}),
        (MockAsyncPoolTestConfig, {"type": "async", "pooling": True}),
    ],
)
def test_connection_config_dict(config_class: "type", expected_dict: "dict[str, Any]") -> None:
    """Test connection_config_dict property returns expected values."""
    config = config_class()
    assert config.connection_config_dict == expected_dict


# Test is_async and supports_connection_pooling class variables
@pytest.mark.parametrize(
    "config_class,expected_async,expected_pooling",
    [
        (MockSyncTestConfig, False, False),
        (MockAsyncTestConfig, True, False),
        (MockSyncPoolTestConfig, False, True),
        (MockAsyncPoolTestConfig, True, True),
    ],
)
def test_config_class_variables(config_class: "type", expected_async: bool, expected_pooling: bool) -> None:
    """Test config class variables are set correctly."""
    config = config_class()
    assert config.is_async == expected_async
    assert config.supports_connection_pooling == expected_pooling


# Test native support flags (all default to False)
def test_native_support_flags() -> None:
    """Test native support flags default to False."""
    config = MockSyncTestConfig()

    assert config.supports_native_arrow_import is False
    assert config.supports_native_arrow_export is False
    assert config.supports_native_parquet_import is False
    assert config.supports_native_parquet_export is False
