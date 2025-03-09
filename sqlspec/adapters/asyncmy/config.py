from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, TypeVar, Union

from asyncmy.connection import Connection  # pyright: ignore[reportUnknownVariableType]
from asyncmy.pool import Pool  # pyright: ignore[reportUnknownVariableType]

from sqlspec.base import AsyncDatabaseConfig, GenericPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import Any

    from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore[reportUnknownVariableType]

__all__ = (
    "AsyncMyConfig",
    "AsyncmyPoolConfig",
)


T = TypeVar("T")


@dataclass
class AsyncmyPoolConfig(GenericPoolConfig):
    """Configuration for Asyncmy's connection pool.

    This class provides configuration options for Asyncmy database connection pools.

    For details see: https://github.com/long2ice/asyncmy
    """

    host: "Union[str, EmptyType]" = Empty
    """Host where the database server is located."""

    user: "Union[str, EmptyType]" = Empty
    """The username used to authenticate with the database."""

    password: "Union[str, EmptyType]" = Empty
    """The password used to authenticate with the database."""

    database: "Union[str, EmptyType]" = Empty
    """The database name to use."""

    port: "Union[int, EmptyType]" = Empty
    """The TCP/IP port of the MySQL server. Must be an integer."""

    unix_socket: "Union[str, EmptyType]" = Empty
    """The location of the Unix socket file."""

    charset: "Union[str, EmptyType]" = Empty
    """The character set to use for the connection."""

    connect_timeout: "Union[float, EmptyType]" = Empty
    """Timeout before throwing an error when connecting."""

    read_default_file: "Union[str, EmptyType]" = Empty
    """MySQL configuration file to read."""

    read_default_group: "Union[str, EmptyType]" = Empty
    """Group to read from the configuration file."""

    autocommit: "Union[bool, EmptyType]" = Empty
    """If True, autocommit mode will be enabled."""

    local_infile: "Union[bool, EmptyType]" = Empty
    """If True, enables LOAD LOCAL INFILE."""

    ssl: "Union[dict[str, Any], bool, EmptyType]" = Empty
    """If present, a dictionary of SSL connection parameters, or just True."""

    sql_mode: "Union[str, EmptyType]" = Empty
    """Default SQL_MODE to use."""

    init_command: "Union[str, EmptyType]" = Empty
    """Initial SQL statement to execute once connected."""

    cursor_class: "Union[type[Union[Cursor, DictCursor]], EmptyType]" = Empty
    """Custom cursor class to use."""

    minsize: "Union[int, EmptyType]" = Empty
    """Minimum number of connections to keep in the pool."""

    maxsize: "Union[int, EmptyType]" = Empty
    """Maximum number of connections allowed in the pool."""

    echo: "Union[bool, EmptyType]" = Empty
    """If True, logging will be enabled for all SQL statements."""

    pool_recycle: "Union[int, EmptyType]" = Empty
    """Number of seconds after which a connection is recycled."""

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Return the pool configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the Asyncmy create_pool function.
        """
        return dataclass_to_dict(self, exclude_empty=True, convert_nested=False)


@dataclass
class AsyncMyConfig(AsyncDatabaseConfig[Connection, Pool]):
    """Asyncmy Configuration."""

    __is_async__ = True
    __supports_connection_pooling__ = True

    pool_config: "Optional[AsyncmyPoolConfig]" = None
    """Asyncmy Pool configuration"""

    pool_instance: "Optional[Pool]" = None  # pyright: ignore[reportUnknownVariableType]
    """Optional pool to use.

    If set, the plugin will use the provided pool rather than instantiate one.
    """

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Return the pool configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the Asyncmy create_pool function.

        Raises:
            ImproperConfigurationError: If the pool configuration is not provided.
        """
        if self.pool_config:
            return dataclass_to_dict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> "Pool":  # pyright: ignore[reportUnknownParameterType]
        """Return a pool. If none exists yet, create one.

        Returns:
            Getter that returns the pool instance used by the plugin.

        Raises:
            ImproperConfigurationError: If the pool could not be created.
        """
        if self.pool_instance is not None:  # pyright: ignore[reportUnknownMemberType]
            return self.pool_instance  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        try:
            import asyncmy  # pyright: ignore[reportMissingTypeStubs]

            self.pool_instance = await asyncmy.create_pool(**self.pool_config_dict)  # pyright: ignore[reportUnknownMemberType]
        except Exception as e:
            msg = f"Could not configure the Asyncmy pool. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e
        else:
            return self.pool_instance  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]

    async def provide_pool(self, *args: "Any", **kwargs: "Any") -> "Pool":  # pyright: ignore[reportUnknownParameterType]
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return await self.create_pool()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

    @asynccontextmanager
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[Connection, None]":  # pyright: ignore[reportUnknownParameterType]
        """Create and provide a database connection.

        Yields:
            An Asyncmy connection instance.

        """
        pool = await self.provide_pool(*args, **kwargs)  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
        async with pool.acquire() as connection:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            yield connection  # pyright: ignore[reportUnknownMemberType]
