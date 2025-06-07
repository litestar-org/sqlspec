"""Psycopg database configuration using TypedDict for better maintainability."""

import contextlib
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict

from psycopg import AsyncConnection, Connection, connect
from psycopg.rows import DictRow as PsycopgDictRow
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncConnection,
    PsycopgAsyncDriver,
    PsycopgSyncConnection,
    PsycopgSyncDriver,
)
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig, SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

logger = logging.getLogger("sqlspec.adapters.psycopg")

__all__ = (
    "PsycopgAsyncConfig",
    "PsycopgConfig",
    "PsycopgConnectionConfig",
    "PsycopgPoolConfig",
    "PsycopgSyncConfig",
)


class PsycopgConnectionConfig(TypedDict, total=False):
    """Psycopg connection configuration as TypedDict.

    Basic connection parameters for psycopg.connect().
    """

    conninfo: NotRequired[str]
    """Connection string in libpq format."""

    host: NotRequired[str]
    """Database server host."""

    port: NotRequired[int]
    """Database server port."""

    user: NotRequired[str]
    """Database user."""

    password: NotRequired[str]
    """Database password."""

    dbname: NotRequired[str]
    """Database name."""

    connect_timeout: NotRequired[float]
    """Connection timeout in seconds."""

    options: NotRequired[str]
    """Command-line options to send to the server."""

    application_name: NotRequired[str]
    """Application name for logging and statistics."""

    sslmode: NotRequired[str]
    """SSL mode (disable, prefer, require, etc.)."""

    sslcert: NotRequired[str]
    """SSL client certificate file."""

    sslkey: NotRequired[str]
    """SSL client private key file."""

    sslrootcert: NotRequired[str]
    """SSL root certificate file."""

    autocommit: NotRequired[bool]
    """Enable autocommit mode."""


class PsycopgPoolConfig(TypedDict, total=False):
    """Psycopg pool configuration as TypedDict.

    All parameters for psycopg_pool.ConnectionPool() and AsyncConnectionPool().
    Inherits connection parameters and adds pool-specific settings.
    """

    # Connection parameters (inherit from connection config)
    conninfo: NotRequired[str]
    """Connection string in libpq format."""

    host: NotRequired[str]
    """Database server host."""

    port: NotRequired[int]
    """Database server port."""

    user: NotRequired[str]
    """Database user."""

    password: NotRequired[str]
    """Database password."""

    dbname: NotRequired[str]
    """Database name."""

    connect_timeout: NotRequired[float]
    """Connection timeout in seconds."""

    options: NotRequired[str]
    """Command-line options to send to the server."""

    application_name: NotRequired[str]
    """Application name for logging and statistics."""

    sslmode: NotRequired[str]
    """SSL mode (disable, prefer, require, etc.)."""

    sslcert: NotRequired[str]
    """SSL client certificate file."""

    sslkey: NotRequired[str]
    """SSL client private key file."""

    sslrootcert: NotRequired[str]
    """SSL root certificate file."""

    autocommit: NotRequired[bool]
    """Enable autocommit mode."""

    # Pool-specific parameters
    min_size: NotRequired[int]
    """Minimum number of connections in the pool."""

    max_size: NotRequired[int]
    """Maximum number of connections in the pool."""

    name: NotRequired[str]
    """Name of the connection pool."""

    timeout: NotRequired[float]
    """Timeout for acquiring connections."""

    max_waiting: NotRequired[int]
    """Maximum number of waiting clients."""

    max_lifetime: NotRequired[float]
    """Maximum connection lifetime."""

    max_idle: NotRequired[float]
    """Maximum idle time for connections."""

    reconnect_timeout: NotRequired[float]
    """Time between reconnection attempts."""

    num_workers: NotRequired[int]
    """Number of background workers."""

    configure: NotRequired["Callable[[Connection[Any]], None]"]
    """Callback to configure new connections."""

    kwargs: NotRequired[dict[str, Any]]
    """Additional connection parameters."""


class PsycopgSyncConfig(SyncDatabaseConfig[PsycopgSyncConnection, ConnectionPool, PsycopgSyncDriver]):
    """Configuration for Psycopg synchronous database connections using TypedDict."""

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[PsycopgSyncDriver]] = PsycopgSyncDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("pyformat_positional", "pyformat_named")
    """Psycopg supports %s (positional) and %(name)s (named) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "pyformat_positional"
    """Psycopg's native parameter style is %s (pyformat positional)."""

    def __init__(
        self,
        pool_config: "PsycopgPoolConfig",
        connection_config: "Optional[PsycopgConnectionConfig]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        instrumentation: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize Psycopg synchronous configuration.

        Args:
            pool_config: Psycopg pool parameters
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
    def connection_type(self) -> "type[PsycopgSyncConnection]":  # type: ignore[override]
        """Return the connection type."""
        return Connection  # pyright: ignore

    @property
    def driver_type(self) -> "type[PsycopgSyncDriver]":  # type: ignore[override]
        """Return the driver type."""
        return PsycopgSyncDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        merged_config = {**self.connection_config, **self.pool_config}
        return {k: v for k, v in merged_config.items() if v is not Empty}

    def _create_pool(self) -> "ConnectionPool":
        """Create the actual connection pool."""
        if self.instrumentation.log_pool_operations:
            logger.info("Creating Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            pool = ConnectionPool(**self.connection_config_dict)
            if self.instrumentation.log_pool_operations:
                logger.info("Psycopg connection pool created successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception("Failed to create Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)})
            raise
        return pool

    def _close_pool(self) -> None:
        """Close the actual connection pool."""
        if not self.pool_instance:
            return

        if self.instrumentation.log_pool_operations:
            logger.info("Closing Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            self.pool_instance.close()
            if self.instrumentation.log_pool_operations:
                logger.info("Psycopg connection pool closed successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception("Failed to close Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)})
            raise

    def create_connection(self) -> "PsycopgSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            A psycopg Connection instance configured with DictRow.
        """
        conn_dict = {k: v for k, v in self.connection_config.items() if v is not Empty}
        conn_dict["row_factory"] = PsycopgDictRow
        return connect(**conn_dict)  # type: ignore[arg-type]

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[PsycopgSyncConnection, None, None]":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psycopg Connection instance.
        """
        if self.pool_instance:
            with self.pool_instance.connection() as conn:
                yield conn  # type: ignore[misc]
        else:
            conn = self.create_connection()  # type: ignore[assignment]
            try:
                yield conn  # type: ignore[misc]
            finally:
                conn.close()

    @contextlib.contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[PsycopgSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgSyncDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as conn:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                from dataclasses import replace

                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            driver = self.driver_type(
                connection=conn,
                config=statement_config,
                instrumentation_config=self.instrumentation,
            )
            yield driver

    def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = self.create_pool()
        return self.pool_instance


class PsycopgAsyncConfig(AsyncDatabaseConfig[PsycopgAsyncConnection, AsyncConnectionPool, PsycopgAsyncDriver]):
    """Configuration for Psycopg asynchronous database connections using TypedDict."""

    __is_async__: ClassVar[bool] = True
    __supports_connection_pooling__: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[PsycopgAsyncDriver]] = PsycopgAsyncDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("pyformat_positional", "pyformat_named")
    """Psycopg supports %s (pyformat_positional) and %(name)s (pyformat_named) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "pyformat_positional"
    """Psycopg's preferred parameter style is %s (pyformat_positional)."""

    def __init__(
        self,
        pool_config: "PsycopgPoolConfig",
        connection_config: "Optional[PsycopgConnectionConfig]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        instrumentation: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize Psycopg asynchronous configuration.

        Args:
            pool_config: Psycopg pool parameters
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
    def connection_type(self) -> "type[PsycopgAsyncConnection]":  # type: ignore[override]
        """Return the connection type."""
        from psycopg import AsyncConnection

        return AsyncConnection  # pyright: ignore

    @property
    def driver_type(self) -> "type[PsycopgAsyncDriver]":  # type: ignore[override]
        """Return the driver type."""
        return PsycopgAsyncDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        merged_config = {**self.connection_config, **self.pool_config}
        return {k: v for k, v in merged_config.items() if v is not Empty}

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""
        if self.instrumentation.log_pool_operations:
            logger.info("Creating async Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            pool = AsyncConnectionPool(**self.connection_config_dict)
            await pool.open()
            if self.instrumentation.log_pool_operations:
                logger.info("Async Psycopg connection pool created successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception(
                "Failed to create async Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)}
            )
            raise
        return pool

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.pool_instance:
            return

        if self.instrumentation.log_pool_operations:
            logger.info("Closing async Psycopg connection pool", extra={"adapter": "psycopg"})

        try:
            await self.pool_instance.close()
            if self.instrumentation.log_pool_operations:
                logger.info("Async Psycopg connection pool closed successfully", extra={"adapter": "psycopg"})
        except Exception as e:
            logger.exception(
                "Failed to close async Psycopg connection pool", extra={"adapter": "psycopg", "error": str(e)}
            )
            raise

    async def create_connection(self) -> "PsycopgAsyncConnection":  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            A psycopg AsyncConnection instance configured with DictRow.
        """

        conn_dict: dict[str, Any] = {k: v for k, v in self.connection_config.items() if v is not Empty}
        conn_dict["row_factory"] = PsycopgDictRow
        return await AsyncConnection.connect(**conn_dict)  # pyright: ignore

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[PsycopgAsyncConnection, None]":  # pyright: ignore
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psycopg AsyncConnection instance.
        """
        if self.pool_instance:
            async with self.pool_instance.connection() as conn:
                yield conn  # type: ignore[misc]
        else:
            conn = await self.create_connection()  # type: ignore[assignment]
            try:
                yield conn  # type: ignore[misc]
            finally:
                await conn.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[PsycopgAsyncDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgAsyncDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as conn:
            # Create statement config with parameter style info if not already set
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                from dataclasses import replace

                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            driver = self.driver_type(
                connection=conn,
                config=statement_config,
                instrumentation_config=self.instrumentation,
            )
            yield driver

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance


# Legacy alias for backwards compatibility (uses sync version by default)
PsycopgConfig = PsycopgSyncConfig
