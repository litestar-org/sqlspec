from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union

from asyncpg import Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.pool import PoolConnectionProxy

from sqlspec._serialization import decode_json, encode_json
from sqlspec.adapters.asyncpg.driver import AsyncpgConnection, AsyncpgDriver
from sqlspec.base import AsyncDatabaseConfig, GenericPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop  # pyright: ignore[reportAttributeAccessIssue]
    from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine

    from asyncpg.connection import Connection
    from asyncpg.pool import Pool


__all__ = (
    "AsyncpgConfig",
    "AsyncpgPoolConfig",
)


T = TypeVar("T")


@dataclass
class AsyncpgPoolConfig(GenericPoolConfig):
    """Configuration for Asyncpg's :class:`Pool <asyncpg.pool.Pool>`.

    For details see: https://magicstack.github.io/asyncpg/current/api/index.html#connection-pools
    """

    dsn: str
    """Connection arguments specified using as a single string in the following format: ``postgres://user:pass@host:port/database?option=value``
    """
    connect_kwargs: "Optional[Union[dict[Any, Any], EmptyType]]" = Empty
    """A dictionary of arguments which will be passed directly to the ``connect()`` method as keyword arguments.
    """
    connection_class: "Optional[Union[type[Connection], EmptyType]]" = Empty  # pyright: ignore[reportMissingTypeArgument]
    """The class to use for connections. Must be a subclass of Connection
    """
    record_class: "Union[type[Record], EmptyType]" = Empty
    """If specified, the class to use for records returned by queries on the connections in this pool. Must be a subclass of Record."""

    min_size: "Union[int, EmptyType]" = Empty
    """The number of connections to keep open inside the connection pool."""
    max_size: "Union[int, EmptyType]" = Empty
    """The number of connections to allow in connection pool "overflow", that is connections that can be opened above
    and beyond the pool_size setting, which defaults to 10."""

    max_queries: "Union[int, EmptyType]" = Empty
    """Number of queries after a connection is closed and replaced with a new connection.
    """
    max_inactive_connection_lifetime: "Union[float, EmptyType]" = Empty
    """Number of seconds after which inactive connections in the pool will be closed. Pass 0 to disable this mechanism."""

    setup: "Union[Coroutine[None, type[Connection], Any], EmptyType]" = Empty  # pyright: ignore[reportMissingTypeArgument]
    """A coroutine to prepare a connection right before it is returned from Pool.acquire(). An example use case would be to automatically set up notifications listeners for all connections of a pool."""
    init: "Union[Coroutine[None, type[Connection], Any], EmptyType]" = Empty  # pyright: ignore[reportMissingTypeArgument]
    """A coroutine to prepare a connection right before it is returned from Pool.acquire(). An example use case would be to automatically set up notifications listeners for all connections of a pool."""

    loop: "Union[AbstractEventLoop, EmptyType]" = Empty
    """An asyncio event loop instance. If None, the default event loop will be used."""


@dataclass
class AsyncpgConfig(AsyncDatabaseConfig["AsyncpgConnection", "Pool", "AsyncpgDriver"]):  # pyright: ignore[reportMissingTypeArgument]
    """Asyncpg Configuration."""

    pool_config: "Optional[AsyncpgPoolConfig]" = field(default=None)
    """Asyncpg Pool configuration"""
    json_deserializer: "Callable[[str], Any]" = field(hash=False, default=decode_json)
    """For dialects that support the :class:`JSON <sqlalchemy.types.JSON>` datatype, this is a Python callable that will
    convert a JSON string to a Python object. By default, this is set to SQLSpec's
    :attr:`decode_json() <sqlspec._serialization.decode_json>` function."""
    json_serializer: "Callable[[Any], str]" = field(hash=False, default=encode_json)
    """For dialects that support the JSON datatype, this is a Python callable that will render a given object as JSON.
    By default, SQLSpec's :attr:`encode_json() <sqlspec._serialization.encode_json>` is used."""
    connection_type: "type[AsyncpgConnection]" = field(
        hash=False,
        init=False,
        default_factory=lambda: PoolConnectionProxy,  # type: ignore[assignment]
    )
    """Type of the connection object"""
    driver_type: "type[AsyncpgDriver]" = field(hash=False, init=False, default_factory=lambda: AsyncpgDriver)  # type: ignore[type-abstract,unused-ignore]
    """Type of the driver object"""
    pool_instance: "Optional[Pool[Any]]" = field(hash=False, default=None)
    """The connection pool instance. If set, this will be used instead of creating a new pool."""

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the asyncpg.connect function.

        Raises:
            ImproperConfigurationError: If the connection configuration is not provided.
        """
        if self.pool_config:
            connect_dict: dict[str, Any] = {}

            # Add dsn if available
            if hasattr(self.pool_config, "dsn"):
                connect_dict["dsn"] = self.pool_config.dsn

            # Add any connect_kwargs if available
            if (
                hasattr(self.pool_config, "connect_kwargs")
                and self.pool_config.connect_kwargs is not Empty
                and isinstance(self.pool_config.connect_kwargs, dict)
            ):
                connect_dict.update(dict(self.pool_config.connect_kwargs.items()))

            return connect_dict
        msg = "You must provide a 'pool_config' for this adapter."
        raise ImproperConfigurationError(msg)

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Return the pool configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the Asyncpg :func:`create_pool <asyncpg.pool.create_pool>`
            function.

        Raises:
            ImproperConfigurationError: If no pool_config is provided but a pool_instance is set.
        """
        if self.pool_config:
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                exclude={"pool_instance", "driver_type", "connection_type"},
                convert_nested=False,
            )
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> "Pool":  # pyright: ignore[reportMissingTypeArgument,reportUnknownParameterType]
        """Return a pool. If none exists yet, create one.

        Returns:
            Getter that returns the pool instance used by the plugin.

        Raises:
            ImproperConfigurationError: If neither pool_config nor pool_instance are provided,
                or if the pool could not be configured.
        """
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        pool_config = self.pool_config_dict
        self.pool_instance = await asyncpg_create_pool(**pool_config)
        if self.pool_instance is None:  # pyright: ignore[reportUnnecessaryComparison]
            msg = "Could not configure the 'pool_instance'. Please check your configuration."  # type: ignore[unreachable]
            raise ImproperConfigurationError(msg)
        return self.pool_instance

    def provide_pool(self, *args: "Any", **kwargs: "Any") -> "Awaitable[Pool]":  # pyright: ignore[reportMissingTypeArgument,reportUnknownParameterType]
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return self.create_pool()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

    async def create_connection(self) -> "AsyncpgConnection":
        """Create and return a new asyncpg connection from the pool.

        Returns:
            A Connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be created.
        """
        try:
            pool = await self.provide_pool()
            return await pool.acquire()
        except Exception as e:
            msg = f"Could not configure the asyncpg connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @asynccontextmanager
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[AsyncpgConnection, None]":  # pyright: ignore[reportMissingTypeArgument,reportUnknownParameterType]
        """Create a connection instance.

        Yields:
            A connection instance.
        """
        db_pool = await self.provide_pool(*args, **kwargs)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        async with db_pool.acquire() as connection:  # pyright: ignore[reportUnknownVariableType]
            yield connection

    async def close_pool(self) -> None:
        """Close the pool."""
        if self.pool_instance is not None:
            await self.pool_instance.close()
            self.pool_instance = None

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AsyncpgDriver, None]":
        """Create and provide a database session.

        Yields:
            A Aiosqlite driver instance.


        """
        async with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)
