from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Generator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, Union

__all__ = (
    "DatabaseConfigProtocol",
    "GenericPoolConfig",
    "NoPoolConfig",
)

ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")


@dataclass
class DatabaseConfigProtocol(Generic[ConnectionT, PoolT], ABC):
    """Protocol defining the interface for database configurations."""

    __is_async__: bool = field(default=False, init=False)
    __supports_connection_pooling__: bool = field(default=False, init=False)

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


class NoPoolConfig(DatabaseConfigProtocol[ConnectionT, None]):
    """Base class for database configurations that do not implement a pool."""

    def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""


@dataclass
class GenericPoolConfig:
    """Generic Database Pool Configuration."""


@dataclass
class GenericDatabaseConfig:
    """Generic Database Configuration."""
