"""AsyncPG database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from asyncpg import Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.pool import PoolConnectionProxy

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


__all__ = ("CONNECTION_FIELDS", "POOL_FIELDS", "AsyncpgConfig")

logger = logging.getLogger("sqlspec")

CONNECTION_FIELDS = {
    "dsn",
    "host",
    "port",
    "user",
    "password",
    "database",
    "ssl",
    "passfile",
    "direct_tls",
    "connect_timeout",
    "command_timeout",
    "statement_cache_size",
    "max_cached_statement_lifetime",
    "max_cacheable_statement_size",
    "server_settings",
}
POOL_FIELDS = CONNECTION_FIELDS.union(
    {
        "min_size",
        "max_size",
        "max_queries",
        "max_inactive_connection_lifetime",
        "setup",
        "init",
        "loop",
        "connection_class",
        "record_class",
    }
)


class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    __slots__ = (
        "command_timeout",
        "connect_timeout",
        "connection_class",
        "database",
        "default_row_type",
        "direct_tls",
        "dsn",
        "extras",
        "host",
        "init",
        "json_deserializer",
        "json_serializer",
        "loop",
        "max_cacheable_statement_size",
        "max_cached_statement_lifetime",
        "max_inactive_connection_lifetime",
        "max_queries",
        "max_size",
        "min_size",
        "passfile",
        "password",
        "port",
        "record_class",
        "server_settings",
        "setup",
        "ssl",
        "statement_cache_size",
        "statement_config",
        "user",
    )

    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[AsyncpgDriver]] = AsyncpgDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("numeric",)
    """AsyncPG only supports $1, $2, ... (numeric) parameter style."""

    preferred_parameter_style: ClassVar[str] = "numeric"
    """AsyncPG's native parameter style is $1, $2, ... (numeric)."""

    def __init__(
        self,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        json_serializer: "Callable[[Any], str]" = encode_json,
        json_deserializer: "Callable[[str], Any]" = decode_json,
        # Connection parameters
        dsn: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        ssl: Optional[Any] = None,
        passfile: Optional[str] = None,
        direct_tls: Optional[bool] = None,
        connect_timeout: Optional[float] = None,
        command_timeout: Optional[float] = None,
        statement_cache_size: Optional[int] = None,
        max_cached_statement_lifetime: Optional[int] = None,
        max_cacheable_statement_size: Optional[int] = None,
        server_settings: Optional[dict[str, str]] = None,
        # Pool parameters
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        max_queries: Optional[int] = None,
        max_inactive_connection_lifetime: Optional[float] = None,
        setup: Optional["Callable[[AsyncpgConnection], Awaitable[None]]"] = None,
        init: Optional["Callable[[AsyncpgConnection], Awaitable[None]]"] = None,
        loop: Optional["AbstractEventLoop"] = None,
        connection_class: Optional[type["AsyncpgConnection"]] = None,
        record_class: Optional[type[Record]] = None,
        # User-defined extras
        extras: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            json_serializer: JSON serialization function
            json_deserializer: JSON deserialization function
            dsn: Connection DSN string
            host: Database server host
            port: Database server port
            user: Database user
            password: Database password
            database: Database name
            ssl: SSL configuration (True, False, or SSLContext)
            passfile: Path to password file
            direct_tls: Use direct TLS connection
            connect_timeout: Connection timeout in seconds
            command_timeout: Command timeout in seconds
            statement_cache_size: Statement cache size
            max_cached_statement_lifetime: Maximum cached statement lifetime in seconds
            max_cacheable_statement_size: Maximum size of cacheable statements in bytes
            server_settings: Server settings to apply on connection
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            max_queries: Maximum queries per connection before recycling
            max_inactive_connection_lifetime: Maximum lifetime for inactive connections (seconds)
            setup: Async callable to setup a new connection
            init: Async callable to initialize a new connection (alias for setup)
            loop: Asyncio event loop
            connection_class: Custom connection class
            record_class: Custom record class
            extras: Additional connection parameters not explicitly defined
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.dsn = dsn
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.ssl = ssl
        self.passfile = passfile
        self.direct_tls = direct_tls
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.statement_cache_size = statement_cache_size
        self.max_cached_statement_lifetime = max_cached_statement_lifetime
        self.max_cacheable_statement_size = max_cacheable_statement_size
        self.server_settings = server_settings

        # Store pool parameters as instance attributes
        self.min_size = min_size
        self.max_size = max_size
        self.max_queries = max_queries
        self.max_inactive_connection_lifetime = max_inactive_connection_lifetime
        self.setup = setup
        self.init = init
        self.loop = loop
        self.connection_class = connection_class
        self.record_class = record_class

        # Handle extras and additional kwargs
        self.extras = extras or {}
        self.extras.update(kwargs)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.json_serializer = json_serializer
        self.json_deserializer = json_deserializer

        super().__init__(instrumentation=instrumentation or InstrumentationConfig())

    @property
    def connection_type(self) -> type[AsyncpgConnection]:  # type: ignore[override]
        """Return the connection type."""
        return PoolConnectionProxy

    @property
    def driver_type(self) -> type[AsyncpgDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AsyncpgDriver

    @classmethod
    def from_pool_config(
        cls,
        pool_config: dict[str, Any],
        connection_config: Optional[dict[str, Any]] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        json_serializer: "Callable[[Any], str]" = encode_json,
        json_deserializer: "Callable[[str], Any]" = decode_json,
    ) -> "AsyncpgConfig":
        """Create config from old-style pool_config and connection_config dicts for backward compatibility.

        Args:
            pool_config: Dictionary with pool and connection parameters
            connection_config: Dictionary with additional connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            json_serializer: JSON serialization function
            json_deserializer: JSON deserialization function

        Returns:
            AsyncpgConfig instance
        """
        # Merge connection_config into pool_config (pool_config takes precedence)
        merged_config = {}
        if connection_config:
            merged_config.update(connection_config)
        merged_config.update(pool_config)

        # Create config with all parameters
        return cls(
            statement_config=statement_config,
            instrumentation=instrumentation,
            default_row_type=default_row_type,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
            **merged_config,  # All connection and pool parameters go to direct fields or extras
        )

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for asyncpg.connect().

        This method filters out pool-specific parameters that are not valid for asyncpg.connect().
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
        """Return the full pool configuration as a dict for asyncpg.create_pool().

        Returns:
            A dictionary containing all pool configuration parameters.
        """
        # All AsyncPG parameter names (connection + pool)
        config = {
            field: getattr(self, field)
            for field in POOL_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras parameters
        config.update(self.extras)

        return config

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
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            yield self.driver_type(
                connection=connection, config=statement_config, instrumentation_config=self.instrumentation
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool[Record]":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
