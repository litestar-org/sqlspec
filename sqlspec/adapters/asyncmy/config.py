"""Asyncmy database configuration using TypedDict for better maintainability."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from asyncmy.connection import Connection, Pool
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor
    from asyncmy.pool import Pool

__all__ = ("AsyncmyConfig", "AsyncmyConnectionConfig", "AsyncmyPoolConfig")


class AsyncmyConnectionConfig(TypedDict, total=False):
    """Asyncmy connection configuration as TypedDict.

    Basic connection parameters for asyncmy.connect().
    Based on asyncmy and PyMySQL documentation.
    """

    host: NotRequired[str]
    """Host where the database server is located."""

    user: NotRequired[str]
    """The username used to authenticate with the database."""

    password: NotRequired[str]
    """The password used to authenticate with the database."""

    database: NotRequired[str]
    """The database name to use."""

    port: NotRequired[int]
    """The TCP/IP port of the MySQL server."""

    unix_socket: NotRequired[str]
    """The location of the Unix socket file."""

    charset: NotRequired[str]
    """The character set to use for the connection."""

    connect_timeout: NotRequired[float]
    """Timeout before throwing an error when connecting."""

    read_default_file: NotRequired[str]
    """MySQL configuration file to read."""

    read_default_group: NotRequired[str]
    """Group to read from the configuration file."""

    autocommit: NotRequired[bool]
    """If True, autocommit mode will be enabled."""

    local_infile: NotRequired[bool]
    """If True, enables LOAD LOCAL INFILE."""

    ssl: NotRequired[Any]
    """SSL connection parameters or boolean."""

    sql_mode: NotRequired[str]
    """Default SQL_MODE to use."""

    init_command: NotRequired[str]
    """Initial SQL statement to execute once connected."""

    cursor_class: NotRequired["type[Union[Cursor, DictCursor]]"]
    """Custom cursor class to use."""


class AsyncmyPoolConfig(TypedDict, total=False):
    """Asyncmy pool configuration as TypedDict.

    All parameters for asyncmy.create_pool() including connection parameters.
    Based on asyncmy documentation.
    """

    # Connection parameters (inherit from connection config)
    host: NotRequired[str]
    """Host where the database server is located."""

    user: NotRequired[str]
    """The username used to authenticate with the database."""

    password: NotRequired[str]
    """The password used to authenticate with the database."""

    database: NotRequired[str]
    """The database name to use."""

    port: NotRequired[int]
    """The TCP/IP port of the MySQL server."""

    unix_socket: NotRequired[str]
    """The location of the Unix socket file."""

    charset: NotRequired[str]
    """The character set to use for the connection."""

    connect_timeout: NotRequired[float]
    """Timeout before throwing an error when connecting."""

    read_default_file: NotRequired[str]
    """MySQL configuration file to read."""

    read_default_group: NotRequired[str]
    """Group to read from the configuration file."""

    autocommit: NotRequired[bool]
    """If True, autocommit mode will be enabled."""

    local_infile: NotRequired[bool]
    """If True, enables LOAD LOCAL INFILE."""

    ssl: NotRequired[Any]
    """SSL connection parameters or boolean."""

    sql_mode: NotRequired[str]
    """Default SQL_MODE to use."""

    init_command: NotRequired[str]
    """Initial SQL statement to execute once connected."""

    cursor_class: NotRequired["type[Union[Cursor, DictCursor]]"]
    """Custom cursor class to use."""

    # Pool-specific parameters
    minsize: NotRequired[int]
    """Minimum number of connections to keep in the pool."""

    maxsize: NotRequired[int]
    """Maximum number of connections allowed in the pool."""

    echo: NotRequired[bool]
    """If True, logging will be enabled for all SQL statements."""

    pool_recycle: NotRequired[int]
    """Number of seconds after which a connection is recycled."""


class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "Pool", AsyncmyDriver]):  # pyright: ignore
    """Configuration for Asyncmy database connections using TypedDict."""

    __is_async__: ClassVar[bool] = True
    __supports_connection_pooling__: ClassVar[bool] = True

    def __init__(
        self,
        pool_config: AsyncmyPoolConfig,
        connection_config: Optional[AsyncmyConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
    ) -> None:
        """Initialize Asyncmy configuration.

        Args:
            pool_config: Asyncmy pool parameters
            connection_config: Basic connection parameters (optional, can be included in pool_config)
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
        """
        self.pool_config = pool_config
        self.connection_config = connection_config or {}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),
        )

    @property
    def connection_type(self) -> type[AsyncmyConnection]:  # type: ignore[override]
        """Return the connection type."""
        return Connection

    @property
    def driver_type(self) -> type[AsyncmyDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AsyncmyDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        # Merge connection_config into pool_config, with pool_config taking precedence
        merged_config = {**self.connection_config, **self.pool_config}
        config = {k: v for k, v in merged_config.items() if v is not Empty}

        # Validate essential connection info
        has_host = config.get("host") is not None
        has_socket = config.get("unix_socket") is not None

        if not (has_host or has_socket):
            msg = f"Asyncmy configuration requires either 'host' or 'unix_socket' in pool_config. Current config: {config}"
            raise ImproperConfigurationError(msg)

        return config

    async def _create_pool_impl(self) -> "Pool":  # pyright: ignore
        """Create the actual async connection pool."""
        import asyncmy

        pool_args = self.connection_config_dict
        return await asyncmy.create_pool(**pool_args)

    async def _close_pool_impl(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> AsyncmyConnection:  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            An Asyncmy connection instance.
        """
        async with instrument_operation_async(self, "asyncmy_create_connection", "database"):
            import asyncmy

            config = {k: v for k, v in self.connection_config.items() if v is not Empty}
            return await asyncmy.connect(**config)

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncmyConnection, None]:  # pyright: ignore
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Asyncmy connection instance.
        """
        if self.pool_instance:
            async with self.pool_instance.acquire() as connection:
                yield connection
        else:
            connection = await self.create_connection()
            try:
                yield connection
            finally:
                await connection.close()

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
            yield self.driver_type(
                connection=connection,
                config=self.statement_config,
                instrumentation_config=self.instrumentation,
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool":  # pyright: ignore
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
