"""AsyncPG database configuration using TypedDict for better maintainability."""

import logging
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
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import Callable

    from asyncpg.pool import Pool


__all__ = ("AsyncpgConfig", "AsyncpgConnectionConfig", "AsyncpgPoolConfig")

logger = logging.getLogger("sqlspec")


class AsyncpgConnectionConfig(TypedDict, total=False):
    """AsyncPG connection configuration as TypedDict.

    Basic connection parameters for asyncpg.connect().
    Based on latest AsyncPG documentation.
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

    ssl: NotRequired[Any]
    """SSL configuration (True, False, or SSLContext)."""

    passfile: NotRequired[str]
    """Path to password file."""

    direct_tls: NotRequired[bool]
    """Use direct TLS connection."""

    connect_timeout: NotRequired[float]
    """Connection timeout in seconds."""

    command_timeout: NotRequired[float]
    """Command timeout in seconds."""

    statement_cache_size: NotRequired[int]
    """Statement cache size."""

    max_cached_statement_lifetime: NotRequired[int]
    """Maximum cached statement lifetime in seconds."""

    max_cacheable_statement_size: NotRequired[int]
    """Maximum size of cacheable statements in bytes."""

    server_settings: NotRequired[dict[str, str]]
    """Server settings to apply on connection."""


class AsyncpgPoolConfig(AsyncpgConnectionConfig, total=False):
    """AsyncPG pool configuration as TypedDict.

    All parameters for asyncpg.create_pool() including connection parameters.
    Based on latest AsyncPG documentation.
    """

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

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[AsyncpgDriver]] = AsyncpgDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("numeric",)
    """AsyncPG only supports $1, $2, ... (numeric) parameter style."""

    preferred_parameter_style: ClassVar[str] = "numeric"
    """AsyncPG's native parameter style is $1, $2, ... (numeric)."""

    def __init__(
        self,
        pool_config: AsyncpgPoolConfig,
        connection_config: Optional[AsyncpgConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
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
        return PoolConnectionProxy

    @property
    def driver_type(self) -> type[AsyncpgDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AsyncpgDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for asyncpg.connect().

        This method filters out pool-specific parameters that are not valid for asyncpg.connect().
        """
        return {k: v for k, v in self.connection_config.items() if v is not Empty}

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the full pool configuration as a dict for asyncpg.create_pool().

        Returns:
            A dictionary containing all pool configuration parameters.
        """
        return {k: v for k, v in self.pool_config.items() if v is not Empty}

    async def _create_pool(self) -> "Pool[Record]":
        """Create the actual async connection pool."""
        pool_args = self.pool_config_dict
        return await asyncpg_create_pool(**pool_args)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> AsyncpgConnection:
        """Create a single async connection (not from pool).

        Returns:
            An AsyncPG connection instance.
        """
        async with instrument_operation_async(self, "asyncpg_create_connection", "database"):
            import asyncpg

            config = self.connection_config_dict
            return await asyncpg.connect(**config)  # type: ignore[no-any-return]

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncpgConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncPG connection instance.
        """
        if self.pool_instance:
            connection: Optional[AsyncpgConnection] = None
            try:
                connection = await self.pool_instance.acquire()
                yield connection
            finally:
                if connection is not None:
                    await self.pool_instance.release(connection)  # type: ignore[arg-type]
        else:
            connection = await self.create_connection()
            try:
                yield connection
            finally:
                await connection.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncpgDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncpgDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                from dataclasses import replace

                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(
                connection=connection,
                config=statement_config,
                instrumentation_config=self.instrumentation,
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool[Record]":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
