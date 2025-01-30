from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, Union

from asyncpg import Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import Connection
from asyncpg.pool import Pool, PoolConnectionProxy
from typing_extensions import TypeAlias

from sqlspec._serialization import decode_json, encode_json
from sqlspec.base import AsyncDatabaseConfig, GenericPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop  # pyright: ignore[reportAttributeAccessIssue]
    from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine


__all__ = (
    "AsyncPgConfig",
    "AsyncPgPoolConfig",
)


T = TypeVar("T")

PgConnection: TypeAlias = Union[Connection, PoolConnectionProxy]


@dataclass
class AsyncPgPoolConfig(GenericPoolConfig):
    """Configuration for Asyncpg's :class:`Pool <asyncpg.pool.Pool>`.

    For details see: https://magicstack.github.io/asyncpg/current/api/index.html#connection-pools
    """

    dsn: str
    """Connection arguments specified using as a single string in the following format: ``postgres://user:pass@host:port/database?option=value``
    """
    connect_kwargs: dict[Any, Any] | None | EmptyType = Empty
    """A dictionary of arguments which will be passed directly to the ``connect()`` method as keyword arguments.
    """
    connection_class: type[Connection] | None | EmptyType = Empty
    """The class to use for connections. Must be a subclass of Connection
    """
    record_class: type[Record] | EmptyType = Empty
    """If specified, the class to use for records returned by queries on the connections in this pool. Must be a subclass of Record."""

    min_size: int | EmptyType = Empty
    """The number of connections to keep open inside the connection pool."""
    max_size: int | EmptyType = Empty
    """The number of connections to allow in connection pool “overflow”, that is connections that can be opened above
    and beyond the pool_size setting, which defaults to 10."""

    max_queries: int | EmptyType = Empty
    """Number of queries after a connection is closed and replaced with a new connection.
    """
    max_inactive_connection_lifetime: float | EmptyType = Empty
    """Number of seconds after which inactive connections in the pool will be closed. Pass 0 to disable this mechanism."""

    setup: Coroutine[None, type[Connection], Any] | EmptyType = Empty
    """A coroutine to prepare a connection right before it is returned from Pool.acquire(). An example use case would be to automatically set up notifications listeners for all connections of a pool."""
    init: Coroutine[None, type[Connection], Any] | EmptyType = Empty
    """A coroutine to prepare a connection right before it is returned from Pool.acquire(). An example use case would be to automatically set up notifications listeners for all connections of a pool."""

    loop: AbstractEventLoop | EmptyType = Empty
    """An asyncio event loop instance. If None, the default event loop will be used."""


@dataclass
class AsyncPgConfig(AsyncDatabaseConfig[PgConnection, Pool]):
    """Asyncpg Configuration."""

    pool_config: AsyncPgPoolConfig | None = None
    """Asyncpg Pool configuration"""
    json_deserializer: Callable[[str], Any] = decode_json
    """For dialects that support the :class:`JSON <sqlalchemy.types.JSON>` datatype, this is a Python callable that will
    convert a JSON string to a Python object. By default, this is set to SQLSpec's
    :attr:`decode_json() <sqlspec._serialization.decode_json>` function."""
    json_serializer: Callable[[Any], str] = encode_json
    """For dialects that support the JSON datatype, this is a Python callable that will render a given object as JSON.
    By default, SQLSpec's :attr:`encode_json() <sqlspec._serialization.encode_json>` is used."""
    pool_instance: Pool | None = None
    """Optional pool to use.

    If set, the plugin will use the provided pool rather than instantiate one.
    """

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the Asyncpg :func:`create_pool <asyncpg.pool.create_pool>`
            function.
        """
        if self.pool_config:
            return dataclass_to_dict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> Pool:
        """Return a pool. If none exists yet, create one.

        Returns:
            Getter that returns the pool instance used by the plugin.
        """
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        pool_config = self.pool_config_dict
        self.pool_instance = await asyncpg_create_pool(**pool_config)
        if self.pool_instance is None:
            msg = "Could not configure the 'pool_instance'. Please check your configuration."
            raise ImproperConfigurationError(
                msg,
            )
        return self.pool_instance

    def provide_pool(self, *args: Any, **kwargs: Any) -> Awaitable[Pool]:
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return self.create_pool()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[PoolConnectionProxy, None]:
        """Create a connection instance.

        Returns:
            A connection instance.
        """
        db_pool = await self.provide_pool(*args, **kwargs)
        async with db_pool.acquire() as connection:
            yield connection
