# ruff: noqa: PLR6301
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Generator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, Generic, Optional, TypeVar, Union, cast, overload

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
ConfigT = TypeVar(
    "ConfigT",
    bound="Union[Union[AsyncDatabaseConfig[Any, Any], NoPoolAsyncConfig[Any]], SyncDatabaseConfig[Any, Any], NoPoolSyncConfig[Any]]",
)


@dataclass
class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT]):
    """Protocol defining the interface for database configurations."""

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False
    pool_instance: Union[PoolT, None] = None

    def __hash__(self) -> int:
        return id(self)

    @abstractmethod
    def create_connection(self) -> Union[ConnectionT, Awaitable[ConnectionT]]:
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self,
        *args: Any,
        **kwargs: Any,
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
    def close_pool(self) -> Optional[Awaitable[None]]:
        """Terminate the connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self,
        *args: Any,
        **kwargs: Any,
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
    pool_instance: None = None

    def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""
        return

    def close_pool(self) -> None:
        return

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""
        return


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None]):
    """Base class for an async database configurations that do not implement a pool."""

    __is_async__ = True
    __supports_connection_pooling__ = False
    pool_instance: None = None

    async def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""
        return

    async def close_pool(self) -> None:
        return

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""
        return


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


class ConfigManager:
    """Type-safe configuration manager with literal inference."""

    __slots__ = ("_configs",)

    def __init__(self) -> None:
        self._configs: dict[Any, DatabaseConfigProtocol[Any, Any]] = {}

    @overload
    def add_config(self, config: SyncConfigT) -> type[SyncConfigT]: ...

    @overload
    def add_config(self, config: AsyncConfigT) -> type[AsyncConfigT]: ...

    def add_config(
        self,
        config: Union[
            SyncConfigT,
            AsyncConfigT,
        ],
    ) -> Union[Annotated[type[SyncConfigT], int], Annotated[type[AsyncConfigT], int]]:  # pyright: ignore[reportInvalidTypeVarUse]
        """Add a new configuration to the manager.

        Returns:
            A unique type key that can be used to retrieve the configuration later.
        """
        key = Annotated[type(config), id(config)]  # type: ignore[valid-type]
        self._configs[key] = config
        return key  # type: ignore[return-value]  # pyright: ignore[reportReturnType]

    @overload
    def get_config(self, name: type[SyncConfigT]) -> SyncConfigT: ...

    @overload
    def get_config(self, name: type[AsyncConfigT]) -> AsyncConfigT: ...

    def get_config(
        self,
        name: Union[type[DatabaseConfigProtocol[ConnectionT, PoolT]], Any],
    ) -> DatabaseConfigProtocol[ConnectionT, PoolT]:
        """Retrieve a configuration by its type.

        Returns:
            DatabaseConfigProtocol: The configuration instance for the given type.

        Raises:
            KeyError: If no configuration is found for the given type.
        """
        config = self._configs.get(name)
        if not config:
            msg = f"No configuration found for {name}"
            raise KeyError(msg)
        return config

    @overload
    def get_connection(
        self,
        name: Union[
            type[NoPoolSyncConfig[ConnectionT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT]],  # pyright: ignore[reportInvalidTypeVarUse]
        ],
    ) -> ConnectionT: ...

    @overload
    def get_connection(
        self,
        name: Union[
            type[NoPoolAsyncConfig[ConnectionT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT]],  # pyright: ignore[reportInvalidTypeVarUse]
        ],
    ) -> Awaitable[ConnectionT]: ...

    def get_connection(
        self,
        name: Union[
            type[NoPoolSyncConfig[ConnectionT]],
            type[NoPoolAsyncConfig[ConnectionT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT]],
        ],
    ) -> Union[ConnectionT, Awaitable[ConnectionT]]:
        """Create and return a connection from the specified configuration.

        Args:
            name: The configuration type to use for creating the connection.

        Returns:
            Either a connection instance or an awaitable that resolves to a connection,
            depending on whether the configuration is sync or async.
        """
        config = self.get_config(name)
        return config.create_connection()

    @overload
    def get_pool(self, name: type[Union[NoPoolSyncConfig[ConnectionT], NoPoolAsyncConfig[ConnectionT]]]) -> None: ...  # pyright: ignore[reportInvalidTypeVarUse]

    @overload
    def get_pool(self, name: type[SyncDatabaseConfig[ConnectionT, PoolT]]) -> type[PoolT]: ...  # pyright: ignore[reportInvalidTypeVarUse]

    @overload
    def get_pool(self, name: type[AsyncDatabaseConfig[ConnectionT, PoolT]]) -> Awaitable[type[PoolT]]: ...  # pyright: ignore[reportInvalidTypeVarUse]

    def get_pool(
        self,
        name: Union[
            type[NoPoolSyncConfig[ConnectionT]],
            type[NoPoolAsyncConfig[ConnectionT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT]],
        ],
    ) -> Union[type[PoolT], Awaitable[type[PoolT]], None]:
        """Create and return a connection pool from the specified configuration.

        Args:
            name: The configuration type to use for creating the pool.

        Returns:
            Either a pool instance, an awaitable that resolves to a pool instance, or None
            if the configuration does not support connection pooling.
        """
        config = self.get_config(name)
        if config.support_connection_pooling:
            return cast("Union[type[PoolT], Awaitable[type[PoolT]]]", config.create_pool())
        return None

    def close_pool(
        self,
        name: Union[
            type[NoPoolSyncConfig[ConnectionT]],
            type[NoPoolAsyncConfig[ConnectionT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT]],
        ],
    ) -> Optional[Awaitable[None]]:
        """Close the connection pool for the specified configuration.

        Args:
            name: The configuration type whose pool to close.

        Returns:
            An awaitable if the configuration is async, otherwise None.
        """
        config = self.get_config(name)
        if config.support_connection_pooling:
            return config.close_pool()
        return None
