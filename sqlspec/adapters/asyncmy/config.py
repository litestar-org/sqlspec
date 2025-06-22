"""Asyncmy database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

import asyncmy

from sqlspec.adapters.asyncmy.driver import AsyncmyConnection, AsyncmyDriver
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor
    from asyncmy.pool import Pool
    from sqlglot.dialects.dialect import DialectType


__all__ = ("CONNECTION_FIELDS", "POOL_FIELDS", "AsyncmyConfig")

logger = logging.getLogger(__name__)

CONNECTION_FIELDS = frozenset(
    {
        "host",
        "user",
        "password",
        "database",
        "port",
        "unix_socket",
        "charset",
        "connect_timeout",
        "read_default_file",
        "read_default_group",
        "autocommit",
        "local_infile",
        "ssl",
        "sql_mode",
        "init_command",
        "cursor_class",
    }
)

POOL_FIELDS = CONNECTION_FIELDS.union({"minsize", "maxsize", "echo", "pool_recycle"})


class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "Pool", AsyncmyDriver]):  # pyright: ignore
    """Configuration for Asyncmy database connections with direct field-based configuration."""

    __slots__ = (
        "_dialect",
        "autocommit",
        "charset",
        "connect_timeout",
        "cursor_class",
        "database",
        "default_row_type",
        "echo",
        "extras",
        "host",
        "init_command",
        "local_infile",
        "maxsize",
        "minsize",
        "password",
        "pool_instance",
        "pool_recycle",
        "port",
        "read_default_file",
        "read_default_group",
        "sql_mode",
        "ssl",
        "statement_config",
        "unix_socket",
        "user",
    )

    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = True
    driver_type: type[AsyncmyDriver] = AsyncmyDriver
    connection_type: type[AsyncmyConnection] = AsyncmyConnection  # pyright: ignore

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("pyformat_positional",)
    """AsyncMy only supports %s (pyformat_positional) parameter style."""

    preferred_parameter_style: ClassVar[str] = "pyformat_positional"
    """AsyncMy's native parameter style is %s (pyformat_positional)."""

    def __init__(
        self,
        statement_config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        # Connection parameters
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        port: Optional[int] = None,
        unix_socket: Optional[str] = None,
        charset: Optional[str] = None,
        connect_timeout: Optional[float] = None,
        read_default_file: Optional[str] = None,
        read_default_group: Optional[str] = None,
        autocommit: Optional[bool] = None,
        local_infile: Optional[bool] = None,
        ssl: Optional[Any] = None,
        sql_mode: Optional[str] = None,
        init_command: Optional[str] = None,
        cursor_class: Optional[Union["type[Cursor]", "type[DictCursor]"]] = None,
        # Pool parameters
        minsize: Optional[int] = None,
        maxsize: Optional[int] = None,
        echo: Optional[bool] = None,
        pool_recycle: Optional[int] = None,
        pool_instance: Optional["Pool"] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Asyncmy configuration.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            host: Host where the database server is located
            user: The username used to authenticate with the database
            password: The password used to authenticate with the database
            database: The database name to use
            port: The TCP/IP port of the MySQL server
            unix_socket: The location of the Unix socket file
            charset: The character set to use for the connection
            connect_timeout: Timeout before throwing an error when connecting
            read_default_file: MySQL configuration file to read
            read_default_group: Group to read from the configuration file
            autocommit: If True, autocommit mode will be enabled
            local_infile: If True, enables LOAD LOCAL INFILE
            ssl: SSL connection parameters or boolean
            sql_mode: Default SQL_MODE to use
            init_command: Initial SQL statement to execute once connected
            cursor_class: Custom cursor class to use
            minsize: Minimum number of connections to keep in the pool
            maxsize: Maximum number of connections allowed in the pool
            echo: If True, logging will be enabled for all SQL statements
            pool_recycle: Number of seconds after which a connection is recycled
            pool_instance: Existing connection pool instance to use
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.unix_socket = unix_socket
        self.charset = charset
        self.connect_timeout = connect_timeout
        self.read_default_file = read_default_file
        self.read_default_group = read_default_group
        self.autocommit = autocommit
        self.local_infile = local_infile
        self.ssl = ssl
        self.sql_mode = sql_mode
        self.init_command = init_command
        self.cursor_class = cursor_class

        # Store pool parameters as instance attributes
        self.minsize = minsize
        self.maxsize = maxsize
        self.echo = echo
        self.pool_recycle = pool_recycle
        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.pool_instance: Optional[Pool] = pool_instance
        self._dialect: DialectType = None

        super().__init__()  # pyright: ignore

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for asyncmy.connect().

        This method filters out pool-specific parameters that are not valid for asyncmy.connect().
        """
        # Gather non-None connection parameters
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Add connection-specific extras (not pool-specific ones)
        config.update(self.extras)

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the full pool configuration as a dict for asyncmy.create_pool().

        Returns:
            A dictionary containing all pool configuration parameters.
        """
        # Gather non-None parameters from all fields (connection + pool)
        config = {
            field: getattr(self, field)
            for field in POOL_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras parameters
        config.update(self.extras)

        return config

    async def _create_pool(self) -> "Pool":  # pyright: ignore
        """Create the actual async connection pool."""
        return await asyncmy.create_pool(**self.pool_config_dict)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> AsyncmyConnection:  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            An Asyncmy connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return await self.pool_instance.acquire()  # pyright: ignore

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncmyConnection, None]:  # pyright: ignore
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Asyncmy connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        async with self.pool_instance.acquire() as connection:  # pyright: ignore
            yield connection

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncmyDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncmyDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(connection=connection, config=statement_config)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool":  # pyright: ignore
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
