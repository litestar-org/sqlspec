from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Generator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar, Union, overload

__all__ = (
    "AsyncDatabaseConfig",
    "DatabaseConfigProtocol",
    "GenericPoolConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SyncDatabaseConfig",
)

ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")
AsyncConfigT = TypeVar("AsyncConfigT", bound="Union[AsyncDatabaseConfig[Any, Any], NoPoolAsyncConfig[Any]]")
SyncConfigT = TypeVar("SyncConfigT", bound="Union[SyncDatabaseConfig[Any, Any], NoPoolSyncConfig[Any]]")


@dataclass
class DatabaseConfigProtocol(Generic[ConnectionT, PoolT], ABC):
    """Protocol defining the interface for database configurations."""

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    @abstractmethod
    def create_connection(self) -> Union[ConnectionT, Awaitable[ConnectionT]]:
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self, *args: Any, **kwargs: Any
    ) -> Union[
        Generator[ConnectionT, None, None],
        AsyncGenerator[ConnectionT, None],
        AbstractContextManager[ConnectionT],
        AbstractAsyncContextManager[ConnectionT],
    ]:
        """Provide a database connection context manager."""
        raise NotImplementedError

    @property
    @abstractmethod
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        raise NotImplementedError

    @abstractmethod
    def create_pool(self) -> Union[PoolT, Awaitable[PoolT]]:
        """Create and return connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self, *args: Any, **kwargs: Any
    ) -> Union[PoolT, Awaitable[PoolT], AbstractContextManager[PoolT], AbstractAsyncContextManager[PoolT]]:
        """Provide pool instance."""
        raise NotImplementedError

    @property
    def is_async(self) -> bool:
        """Return whether the configuration is for an async database."""
        return self.__is_async__

    @property
    def support_connection_pooling(self) -> bool:
        """Return whether the configuration supports connection pooling."""
        return self.__supports_connection_pooling__


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None]):
    """Base class for a sync database configurations that do not implement a pool."""

    __is_async__ = False
    __supports_connection_pooling__ = False

    def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""
        return

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""
        return


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None]):
    """Base class for an async database configurations that do not implement a pool."""

    __is_async__ = True
    __supports_connection_pooling__ = False

    def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""
        return

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""
        return


class ConfigManager:
    """Type-safe configuration manager with literal inference."""

    def __init__(self) -> None:
        self._configs: dict[type, DatabaseConfigProtocol[Any, Any]] = {}

    @overload
    def add_config(self, config: SyncConfigT) -> type[SyncConfigT]: ...

    @overload
    def add_config(self, config: AsyncConfigT) -> type[AsyncConfigT]: ...

    def add_config(self, config: DatabaseConfigProtocol[ConnectionT, PoolT]) -> type:
        """Add a new configuration to the manager."""
        config_type = type(config)
        self._configs[config_type] = config
        return config_type

    def get_config(
        self, name: type[DatabaseConfigProtocol[ConnectionT, PoolT]]
    ) -> DatabaseConfigProtocol[ConnectionT, PoolT]:
        """Retrieve a configuration by its type."""
        config = self._configs.get(name)
        if not config:
            raise KeyError(f"No configuration found for {name}")
        return config

    def get_connection(
        self, name: type[DatabaseConfigProtocol[ConnectionT, PoolT]]
    ) -> Union[ConnectionT, Awaitable[ConnectionT]]:
        """Create and return a connection from the specified configuration."""
        config = self.get_config(name)
        return config.create_connection()

    def get_pool(self, name: type[DatabaseConfigProtocol[ConnectionT, PoolT]]) -> Union[PoolT, Awaitable[PoolT]]:
        """Create and return a connection pool from the specified configuration."""
        config = self.get_config(name)
        if not config.support_connection_pooling:
            raise TypeError(f"Configuration does not support pooling: {name}")
        return config.create_pool()


@dataclass
class GenericPoolConfig:
    """Generic Database Pool Configuration."""


@dataclass
class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT]):
    """Generic Sync Database Configuration."""

    __is_async__ = False
    __supports_connection_pooling__ = True


@dataclass
class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT]):
    """Generic Async Database Configuration."""

    __is_async__ = True
    __supports_connection_pooling__ = True
