"""AsyncPG database configuration using TypedDict for better maintainability."""

from collections.abc import AsyncGenerator, Awaitable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict

from asyncpg import Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.pool import PoolConnectionProxy
from typing_extensions import NotRequired

from sqlspec._serialization import decode_json, encode_json
from sqlspec.adapters.asyncpg.driver import AsyncpgConnection, AsyncpgDriver
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_async

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import Callable

    from asyncpg.pool import Pool


__all__ = ("AsyncpgConfig", "AsyncpgConnectionConfig", "AsyncpgPoolConfig")


class AsyncpgConnectionConfig(TypedDict, total=False):
    """AsyncPG connection configuration as TypedDict.

    Basic connection parameters for asyncpg.connect().
    """

    dsn: NotRequired[str]
    """Connection DSN string."""

    host: NotRequired[str]
    """Database server host."""

    port: NotRequired[int]
    """Database server port."""

    user: NotRequired[str]
    """Database user."""

    password: NotRequired[str]
    """Database password."""

    database: NotRequired[str]
    """Database name."""

    connect_timeout: NotRequired[float]
    """Connection timeout in seconds."""

    command_timeout: NotRequired[float]
    """Command timeout in seconds."""

    server_settings: NotRequired[dict[str, str]]
    """Server settings to apply on connection."""


class AsyncpgPoolConfig(TypedDict, total=False):
    """AsyncPG pool configuration as TypedDict.

    All parameters for asyncpg.create_pool() including connection parameters.
    """

    # Connection parameters (inherit from connection config)
    dsn: NotRequired[str]
    """Connection DSN string."""

    host: NotRequired[str]
    """Database server host."""

    port: NotRequired[int]
    """Database server port."""

    user: NotRequired[str]
    """Database user."""

    password: NotRequired[str]
    """Database password."""

    database: NotRequired[str]
    """Database name."""

    connect_timeout: NotRequired[float]
    """Connection timeout in seconds."""

    command_timeout: NotRequired[float]
    """Command timeout in seconds."""

    server_settings: NotRequired[dict[str, str]]
    """Server settings to apply on connection."""

    # Pool-specific parameters
    min_size: NotRequired[int]
    """Minimum number of connections in the pool."""

    max_size: NotRequired[int]
    """Maximum number of connections in the pool."""

    max_queries: NotRequired[int]
    """Maximum queries per connection before recycling."""

    max_inactive_connection_lifetime: NotRequired[float]
    """Maximum lifetime for inactive connections (seconds)."""

    setup: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    """Async callable to setup a new connection."""

    init: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    """Async callable to initialize a new connection (alias for setup)."""

    loop: NotRequired["AbstractEventLoop"]
    """Asyncio event loop."""

    connection_class: NotRequired[type["AsyncpgConnection"]]
    """Custom connection class."""

    record_class: NotRequired[type[Record]]
    """Custom record class."""


class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    __is_async__: ClassVar[bool] = True
    __supports_connection_pooling__: ClassVar[bool] = True

    def __init__(
        self,
        pool_config: AsyncpgPoolConfig,
        connection_config: Optional[AsyncpgConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
        json_serializer: "Callable[[Any], str]" = encode_json,
        json_deserializer: "Callable[[str], Any]" = decode_json,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            pool_config: AsyncPG pool parameters
            connection_config: Basic connection parameters (optional, can be included in pool_config)
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            json_serializer: JSON serialization function
            json_deserializer: JSON deserialization function
        """
        self.pool_config = pool_config
        self.connection_config = connection_config or {}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.json_serializer = json_serializer
        self.json_deserializer = json_deserializer

        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),
        )

    @property
    def connection_type(self) -> type[AsyncpgConnection]:  # type: ignore[override]
        """Return the connection type."""
        return PoolConnectionProxy  # type: ignore[return-value]

    @property
    def driver_type(self) -> type[AsyncpgDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AsyncpgDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        # For asyncpg, connection config comes from pool config
        config = {k: v for k, v in self.pool_config.items() if v is not Empty}

        # Add basic connection config if provided separately
        if self.connection_config:
            basic_config = {k: v for k, v in self.connection_config.items() if v is not Empty}
            config.update(basic_config)

        # Validate essential connection info
        has_dsn = config.get("dsn") is not None
        has_host = config.get("host") is not None

        if not (has_dsn or has_host):
            msg = f"AsyncPG configuration requires either 'dsn' or 'host' in pool_config. Current config: {config}"
            raise ImproperConfigurationError(msg)

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict."""
        return {k: v for k, v in self.pool_config.items() if v is not Empty}

    @instrument_async(operation_type="pool_lifecycle")
    async def create_pool(self) -> "Pool[Record]":
        """Create and return an AsyncPG connection pool."""
        if self.pool_instance is not None:
            return self.pool_instance

        pool_args = self.pool_config_dict
        self.pool_instance = await asyncpg_create_pool(**pool_args)
        return self.pool_instance

    @instrument_async(operation_type="connection")
    async def create_connection(self) -> AsyncpgConnection:
        """Create and return an AsyncPG connection from the pool."""
        try:
            pool = await self.create_pool()
            conn = await pool.acquire()
            return conn
        except Exception as e:
            msg = f"Could not acquire asyncpg connection from pool. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncpgConnection, None]:
        """Provide an AsyncPG connection context manager."""
        pool = await self.create_pool()
        connection: Optional[AsyncpgConnection] = None
        try:
            connection = await pool.acquire()
            yield connection
        finally:
            if connection is not None:
                await pool.release(connection)

    @instrument_async(operation_type="pool_lifecycle")
    async def close_pool(self) -> None:
        """Close the AsyncPG connection pool."""
        if self.pool_instance is not None:
            await self.pool_instance.close()
            self.pool_instance = None

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncpgDriver, None]:
        """Provide an AsyncPG driver session context manager."""
        async with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(
                connection=connection,
                config=self.statement_config,
                instrumentation_config=self.instrumentation,
            )
