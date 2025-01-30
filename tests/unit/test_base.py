from collections.abc import AsyncGenerator, Generator
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Annotated, Any

import pytest

from sqlspec.base import ConfigManager, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig


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
class MockDatabaseConfig(SyncDatabaseConfig[MockConnection, MockPool]):
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

    def provide_pool(self, *args: Any, **kwargs: Any) -> AbstractContextManager[MockPool]:
        @contextmanager
        def _provide_pool() -> Generator[MockPool, None, None]:
            pool = self.create_pool()
            try:
                yield pool
            finally:
                pool.close()

        return _provide_pool()


class MockNonPoolConfig(NoPoolSyncConfig[MockConnection]):
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

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}


class MockAsyncNonPoolConfig(NoPoolAsyncConfig[MockAsyncConnection]):
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

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {"host": "localhost", "port": 5432}


class TestConfigManager:
    """Test cases for ConfigManager."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.config_manager = ConfigManager()
        self.pool_config = MockDatabaseConfig()
        self.non_pool_config = MockNonPoolConfig()

    def test_add_config(self) -> None:
        """Test adding configurations."""
        main_db_with_a_pool = self.config_manager.add_config(self.pool_config)
        db_config = main_db_with_a_pool()
        assert isinstance(db_config, MockDatabaseConfig)

        non_pool_type = self.config_manager.add_config(self.non_pool_config)
        instance = non_pool_type()
        assert isinstance(instance, MockNonPoolConfig)

    def test_get_config(self) -> None:
        """Test retrieving configurations."""
        pool_type = self.config_manager.add_config(self.pool_config)
        retrieved_config = self.config_manager.get_config(pool_type)
        assert isinstance(retrieved_config, MockDatabaseConfig)

        non_pool_type = self.config_manager.add_config(self.non_pool_config)
        retrieved_non_pool = self.config_manager.get_config(non_pool_type)
        assert isinstance(retrieved_non_pool, MockNonPoolConfig)

    def test_get_nonexistent_config(self) -> None:
        """Test retrieving non-existent configuration."""
        fake_type = Annotated[MockDatabaseConfig, MockConnection, MockPool]
        with pytest.raises(KeyError):
            self.config_manager.get_config(fake_type)  # pyright: ignore[reportCallIssue,reportArgumentType]

    def test_get_connection(self) -> None:
        """Test creating connections."""
        pool_type = self.config_manager.add_config(self.pool_config)
        connection = self.config_manager.get_connection(pool_type)
        assert isinstance(connection, MockConnection)

        non_pool_type = self.config_manager.add_config(self.non_pool_config)
        non_pool_connection = self.config_manager.get_connection(non_pool_type)
        assert isinstance(non_pool_connection, MockConnection)

    def test_get_pool(self) -> None:
        """Test creating pools."""
        pool_type = self.config_manager.add_config(self.pool_config)
        pool = self.config_manager.get_pool(pool_type)
        assert isinstance(pool, MockPool)

    def test_config_properties(self) -> None:
        """Test configuration properties."""
        assert self.pool_config.is_async is False
        assert self.pool_config.support_connection_pooling is True
        assert self.non_pool_config.is_async is False
        assert self.non_pool_config.support_connection_pooling is False

    def test_connection_context(self) -> None:
        """Test connection context manager."""
        with self.pool_config.provide_connection() as conn:
            assert isinstance(conn, MockConnection)

        with self.non_pool_config.provide_connection() as conn:
            assert isinstance(conn, MockConnection)

    def test_pool_context(self) -> None:
        """Test pool context manager."""
        with self.pool_config.provide_pool() as pool:
            assert isinstance(pool, MockPool)

    def test_connection_config_dict(self) -> None:
        """Test connection configuration dictionary."""
        assert self.pool_config.connection_config_dict == {"host": "localhost", "port": 5432}
        assert self.non_pool_config.connection_config_dict == {"host": "localhost", "port": 5432}

    def test_multiple_configs(self) -> None:
        """Test managing multiple configurations simultaneously."""
        # Add multiple configurations
        pool_type = self.config_manager.add_config(self.pool_config)
        non_pool_type = self.config_manager.add_config(self.non_pool_config)
        second_pool_config = MockDatabaseConfig()
        second_pool_type = self.config_manager.add_config(second_pool_config)

        # Test retrieving each configuration
        assert isinstance(self.config_manager.get_config(pool_type), MockDatabaseConfig)
        assert isinstance(self.config_manager.get_config(second_pool_type), MockDatabaseConfig)
        assert isinstance(self.config_manager.get_config(non_pool_type), MockNonPoolConfig)

        # Test that configurations are distinct
        assert self.config_manager.get_config(second_pool_type) is second_pool_config

        # Test connections from different configs
        pool_conn = self.config_manager.get_connection(pool_type)
        non_pool_conn = self.config_manager.get_connection(non_pool_type)
        second_pool_conn = self.config_manager.get_connection(second_pool_type)

        assert isinstance(pool_conn, MockConnection)
        assert isinstance(non_pool_conn, MockConnection)
        assert isinstance(second_pool_conn, MockConnection)

        # Test pools from pooled configs
        pool1 = self.config_manager.get_pool(pool_type)
        pool2 = self.config_manager.get_pool(second_pool_type)

        assert isinstance(pool1, MockPool)
        assert isinstance(pool2, MockPool)  # type: ignore[unreachable]
        assert pool1 is not pool2


class TestNoPoolConfig:
    """Test cases for NoPoolConfig."""

    def test_pool_methods(self) -> None:
        """Test that pool methods return None."""
        config = MockNonPoolConfig()
        assert config.support_connection_pooling is False
        assert config.is_async is False
        assert config.create_pool() is None  # type: ignore[func-returns-value]
