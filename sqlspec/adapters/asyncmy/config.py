from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from asyncmy.connection import Connection
from asyncmy.pool import Pool

from sqlspec.base import AsyncDatabaseConfig, GenericPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import Any

    from asyncmy.cursors import Cursor, DictCursor

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

    host: str | EmptyType = Empty
    """Host where the database server is located."""

    user: str | EmptyType = Empty
    """The username used to authenticate with the database."""

    password: str | EmptyType = Empty
    """The password used to authenticate with the database."""

    database: str | EmptyType = Empty
    """The database name to use."""

    port: int | EmptyType = Empty
    """The TCP/IP port of the MySQL server. Must be an integer."""

    unix_socket: str | EmptyType = Empty
    """The location of the Unix socket file."""

    charset: str | EmptyType = Empty
    """The character set to use for the connection."""

    connect_timeout: float | EmptyType = Empty
    """Timeout before throwing an error when connecting."""

    read_default_file: str | EmptyType = Empty
    """MySQL configuration file to read."""

    read_default_group: str | EmptyType = Empty
    """Group to read from the configuration file."""

    autocommit: bool | EmptyType = Empty
    """If True, autocommit mode will be enabled."""

    local_infile: bool | EmptyType = Empty
    """If True, enables LOAD LOCAL INFILE."""

    ssl: dict[str, Any] | bool | EmptyType = Empty
    """If present, a dictionary of SSL connection parameters, or just True."""

    sql_mode: str | EmptyType = Empty
    """Default SQL_MODE to use."""

    init_command: str | EmptyType = Empty
    """Initial SQL statement to execute once connected."""

    cursor_class: type[Cursor] | type[DictCursor] | EmptyType = Empty
    """Custom cursor class to use."""

    minsize: int | EmptyType = Empty
    """Minimum number of connections to keep in the pool."""

    maxsize: int | EmptyType = Empty
    """Maximum number of connections allowed in the pool."""

    echo: bool | EmptyType = Empty
    """If True, logging will be enabled for all SQL statements."""

    pool_recycle: int | EmptyType = Empty
    """Number of seconds after which a connection is recycled."""

    @property
    def pool_config_dict(self) -> dict[str, Any]:
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

    pool_config: AsyncmyPoolConfig | None = None
    """Asyncmy Pool configuration"""

    pool_instance: Pool | None = None
    """Optional pool to use.

    If set, the plugin will use the provided pool rather than instantiate one.
    """

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the Asyncmy create_pool function.
        """
        if self.pool_config:
            return dataclass_to_dict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> Pool:
        """Return a pool. If none exists yet, create one.

        Returns:
            Getter that returns the pool instance used by the plugin.

        Raises:
            ImproperConfigurationError: If the pool could not be created.
        """
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        try:
            import asyncmy

            self.pool_instance = await asyncmy.create_pool(**self.pool_config_dict)
            return self.pool_instance
        except Exception as e:
            msg = f"Could not configure the Asyncmy pool. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    async def provide_pool(self, *args: Any, **kwargs: Any) -> Pool:
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return await self.create_pool()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[Connection, None]:
        """Create and provide a database connection.

        Yields:
            An Asyncmy connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        pool = await self.provide_pool(*args, **kwargs)
        async with pool.acquire() as connection:
            yield connection
