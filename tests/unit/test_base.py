from collections.abc import AsyncGenerator, Generator
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Annotated, Any

import pytest

from sqlspec.base import NoPoolAsyncConfig, NoPoolSyncConfig, SQLSpec, SyncDatabaseConfig


class MockConnection:
    """Mock database connection for testing."""

    def close(self) -> None:
        pass


class MockAsyncConnection:
    """Mock async database connection for testing."""

    async def close(self) -> None:
        pass


class MockPool:
    """Mock connection pool for testing."""

    def close(self) -> None:
        pass


class MockAsyncPool:
    """Mock async connection pool for testing."""

    async def close(self) -> None:
        pass


@dataclass
class MockDatabaseConfig(SyncDatabaseConfig[MockConnection, MockPool, Any]):
    """Mock database configuration that supports pooling."""

    def create_connection(self) -> MockConnection:
        return MockConnection()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}

    def create_pool(self) -> MockPool:
        return MockPool()

    def close_pool(self) -> None:
        pass

    def provide_pool(self, *args: Any, **kwargs: Any) -> AbstractContextManager[MockPool]:
        @contextmanager
        def _provide_pool() -> Generator[MockPool, None, None]:
            pool = self.create_pool()
            try:
                yield pool
            finally:
                pool.close()

        return _provide_pool()

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()


class MockNonPoolConfig(NoPoolSyncConfig[MockConnection, Any]):
    """Mock database configuration that doesn't support pooling."""

    def create_connection(self) -> MockConnection:
        return MockConnection()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    def close_pool(self) -> None:
        pass

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Generator[MockConnection, None, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}


class MockAsyncNonPoolConfig(NoPoolAsyncConfig[MockAsyncConnection, Any]):
    """Mock database configuration that doesn't support pooling."""

    def create_connection(self) -> MockAsyncConnection:
        return MockAsyncConnection()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[MockAsyncConnection, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()

    async def close_pool(self) -> None:
        pass

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[MockAsyncConnection, None]:
        connection = self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}


@pytest.fixture(scope="session")
def sql_spec() -> SQLSpec:
    """Create a SQLSpec instance for testing.

    Returns:
        A SQLSpec instance.
    """
    return SQLSpec()


@pytest.fixture(scope="session")
def pool_config() -> MockDatabaseConfig:
    """Create a mock database configuration that supports pooling.

    Returns:
        A MockDatabaseConfig instance.
    """
    return MockDatabaseConfig()


@pytest.fixture(scope="session")
def non_pool_config() -> MockNonPoolConfig:
    """Create a mock database configuration that doesn't support pooling.

    Returns:
        A MockNonPoolConfig instance.
    """
    return MockNonPoolConfig()


@pytest.fixture(scope="session")
def async_non_pool_config() -> MockAsyncNonPoolConfig:
    """Create a mock async database configuration that doesn't support pooling.

    Returns:
        A MockAsyncNonPoolConfig instance.
    """
    return MockAsyncNonPoolConfig()


def test_add_config(sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test adding configurations."""
    main_db_with_a_pool = sql_spec.add_config(pool_config)
    db_config = main_db_with_a_pool()
    assert isinstance(db_config, MockDatabaseConfig)

    non_pool_type = sql_spec.add_config(non_pool_config)
    instance = non_pool_type()
    assert isinstance(instance, MockNonPoolConfig)


def test_get_config(sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test retrieving configurations."""
    pool_type = sql_spec.add_config(pool_config)
    retrieved_config = sql_spec.get_config(pool_type)
    assert isinstance(retrieved_config, MockDatabaseConfig)

    non_pool_type = sql_spec.add_config(non_pool_config)
    retrieved_non_pool = sql_spec.get_config(non_pool_type)
    assert isinstance(retrieved_non_pool, MockNonPoolConfig)


def test_get_nonexistent_config(sql_spec: SQLSpec) -> None:
    """Test retrieving non-existent configuration."""
    fake_type = Annotated[MockDatabaseConfig, MockConnection, MockPool]
    with pytest.raises(KeyError):
        sql_spec.get_config(fake_type)  # pyright: ignore[reportCallIssue,reportArgumentType]


def test_get_connection(sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test creating connections."""
    pool_type = sql_spec.add_config(pool_config)
    connection = sql_spec.get_connection(pool_type)
    assert isinstance(connection, MockConnection)

    non_pool_type = sql_spec.add_config(non_pool_config)
    non_pool_connection = sql_spec.get_connection(non_pool_type)
    assert isinstance(non_pool_connection, MockConnection)


def test_get_pool(sql_spec: SQLSpec, pool_config: MockDatabaseConfig) -> None:
    """Test creating pools."""
    pool_type = sql_spec.add_config(pool_config)
    pool = sql_spec.get_pool(pool_type)
    assert isinstance(pool, MockPool)


def test_config_properties(pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test configuration properties."""
    assert pool_config.is_async is False
    assert pool_config.support_connection_pooling is True
    assert non_pool_config.is_async is False
    assert non_pool_config.support_connection_pooling is False


def test_connection_context(pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test connection context manager."""
    with pool_config.provide_connection() as conn:
        assert isinstance(conn, MockConnection)

    with non_pool_config.provide_connection() as conn:
        assert isinstance(conn, MockConnection)


def test_pool_context(pool_config: MockDatabaseConfig) -> None:
    """Test pool context manager."""
    with pool_config.provide_pool() as pool:
        assert isinstance(pool, MockPool)


def test_connection_config_dict(pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig) -> None:
    """Test connection configuration dictionary."""
    assert pool_config.connection_config_dict == {"host": "localhost", "port": 5432}
    assert non_pool_config.connection_config_dict == {"host": "localhost", "port": 5432}


def test_multiple_configs(
    sql_spec: SQLSpec, pool_config: MockDatabaseConfig, non_pool_config: MockNonPoolConfig
) -> None:
    """Test managing multiple configurations simultaneously."""
    # Add multiple configurations
    pool_type = sql_spec.add_config(pool_config)
    non_pool_type = sql_spec.add_config(non_pool_config)
    second_pool_config = MockDatabaseConfig()
    second_pool_type = sql_spec.add_config(second_pool_config)

    # Test retrieving each configuration
    assert isinstance(sql_spec.get_config(pool_type), MockDatabaseConfig)
    assert isinstance(sql_spec.get_config(second_pool_type), MockDatabaseConfig)
    assert isinstance(sql_spec.get_config(non_pool_type), MockNonPoolConfig)

    # Test that configurations are distinct
    assert sql_spec.get_config(second_pool_type) is second_pool_config

    # Test connections from different configs
    pool_conn = sql_spec.get_connection(pool_type)
    non_pool_conn = sql_spec.get_connection(non_pool_type)
    second_pool_conn = sql_spec.get_connection(second_pool_type)

    assert isinstance(pool_conn, MockConnection)
    assert isinstance(non_pool_conn, MockConnection)
    assert isinstance(second_pool_conn, MockConnection)

    # Test pools from pooled configs
    pool1 = sql_spec.get_pool(pool_type)
    pool2 = sql_spec.get_pool(second_pool_type)

    assert isinstance(pool1, MockPool)
    assert isinstance(pool2, MockPool)  # type: ignore[unreachable]
    assert pool1 is not pool2


def test_pool_methods(non_pool_config: MockNonPoolConfig) -> None:
    """Test that pool methods return None."""
    assert non_pool_config.support_connection_pooling is False
    assert non_pool_config.is_async is False
    assert non_pool_config.create_pool() is None  # type: ignore[func-returns-value]
