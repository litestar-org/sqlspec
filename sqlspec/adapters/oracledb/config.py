"""OracleDB database configuration using TypedDict for better maintainability."""

import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict

import oracledb
from typing_extensions import NotRequired

from sqlspec.adapters.oracledb.driver import (
    OracleAsyncConnection,
    OracleAsyncDriver,
    OracleSyncConnection,
    OracleSyncDriver,
)
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig, SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from oracledb import AuthMode
    from oracledb.pool import AsyncConnectionPool, ConnectionPool


__all__ = (
    "OracleAsyncConfig",
    "OracleConnectionConfig",
    "OraclePoolConfig",
    "OracleSyncConfig",
)

logger = logging.getLogger(__name__)


class OracleConnectionConfig(TypedDict, total=False):
    """Oracle connection configuration as TypedDict.

    Basic connection parameters for oracledb.connect().
    """

    dsn: NotRequired[str]
    """Connection string for the database."""

    user: NotRequired[str]
    """Username for database authentication."""

    password: NotRequired[str]
    """Password for database authentication."""

    host: NotRequired[str]
    """Database server hostname."""

    port: NotRequired[int]
    """Database server port number."""

    service_name: NotRequired[str]
    """Oracle service name."""

    sid: NotRequired[str]
    """Oracle System ID (SID)."""

    wallet_location: NotRequired[str]
    """Location of Oracle Wallet."""

    wallet_password: NotRequired[str]
    """Password for accessing Oracle Wallet."""

    config_dir: NotRequired[str]
    """Directory containing Oracle configuration files."""

    tcp_connect_timeout: NotRequired[float]
    """Timeout for establishing TCP connections."""

    retry_count: NotRequired[int]
    """Number of attempts to connect."""

    retry_delay: NotRequired[int]
    """Time in seconds between connection attempts."""

    mode: NotRequired["AuthMode"]
    """Session mode (SYSDBA, SYSOPER, etc.)."""

    events: NotRequired[bool]
    """If True, enables Oracle events for FAN and RLB."""

    edition: NotRequired[str]
    """Edition name for edition-based redefinition."""


class OraclePoolConfig(TypedDict, total=False):
    """Oracle pool configuration as TypedDict.

    All parameters for oracledb.create_pool() and oracledb.create_pool_async().
    Inherits connection parameters and adds pool-specific settings.
    """

    # Connection parameters (inherit from connection config)
    dsn: NotRequired[str]
    """Connection string for the database."""

    user: NotRequired[str]
    """Username for database authentication."""

    password: NotRequired[str]
    """Password for database authentication."""

    host: NotRequired[str]
    """Database server hostname."""

    port: NotRequired[int]
    """Database server port number."""

    service_name: NotRequired[str]
    """Oracle service name."""

    sid: NotRequired[str]
    """Oracle System ID (SID)."""

    wallet_location: NotRequired[str]
    """Location of Oracle Wallet."""

    wallet_password: NotRequired[str]
    """Password for accessing Oracle Wallet."""

    config_dir: NotRequired[str]
    """Directory containing Oracle configuration files."""

    tcp_connect_timeout: NotRequired[float]
    """Timeout for establishing TCP connections."""

    retry_count: NotRequired[int]
    """Number of attempts to connect."""

    retry_delay: NotRequired[int]
    """Time in seconds between connection attempts."""

    mode: NotRequired["AuthMode"]
    """Session mode (SYSDBA, SYSOPER, etc.)."""

    events: NotRequired[bool]
    """If True, enables Oracle events for FAN and RLB."""

    edition: NotRequired[str]
    """Edition name for edition-based redefinition."""

    # Pool-specific parameters
    min: NotRequired[int]
    """Minimum number of connections in the pool."""

    max: NotRequired[int]
    """Maximum number of connections in the pool."""

    increment: NotRequired[int]
    """Number of connections to create when pool needs to grow."""

    threaded: NotRequired[bool]
    """Whether the pool should be threaded."""

    getmode: NotRequired[int]
    """How connections are returned from the pool."""

    homogeneous: NotRequired[bool]
    """Whether all connections use the same credentials."""

    timeout: NotRequired[int]
    """Time in seconds after which idle connections are closed."""

    wait_timeout: NotRequired[int]
    """Time in seconds to wait for an available connection."""

    max_lifetime_session: NotRequired[int]
    """Maximum time in seconds that a connection can remain in the pool."""

    session_callback: NotRequired["Callable[[Any, Any], None]"]
    """Callback function called when a connection is returned to the pool."""

    max_sessions_per_shard: NotRequired[int]
    """Maximum number of sessions per shard."""

    soda_metadata_cache: NotRequired[bool]
    """Whether to enable SODA metadata caching."""

    ping_interval: NotRequired[int]
    """Interval for pinging pooled connections."""


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "ConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections using TypedDict."""

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("named_colon", "numeric")
    """OracleDB supports :name (named_colon) and :1 (numeric) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "named_colon"
    """OracleDB's preferred parameter style is :name (named_colon)."""

    def __init__(
        self,
        pool_config: "OraclePoolConfig",
        connection_config: "Optional[OracleConnectionConfig]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        instrumentation: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            pool_config: Oracle pool parameters
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
    def connection_type(self) -> type[OracleSyncConnection]:  # type: ignore[override]
        """Return the connection type."""

        return oracledb.Connection

    @property
    def driver_type(self) -> type[OracleSyncDriver]:  # type: ignore[override]
        """Return the driver type."""
        return OracleSyncDriver

    def _create_pool(self) -> "ConnectionPool":
        """Create the actual connection pool."""

        return oracledb.create_pool(**self.connection_config_dict)

    def _close_pool(self) -> None:
        """Close the actual connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> OracleSyncConnection:
        """Create a single connection (not from pool).

        Returns:
            An Oracle Connection instance.
        """
        with instrument_operation(self, "oracle_create_connection", "database"):
            import oracledb

            return oracledb.connect(**{k: v for k, v in self.connection_config.items() if v is not Empty})

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[OracleSyncConnection, None, None]":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Oracle Connection instance.
        """
        if self.pool_instance:
            conn = self.pool_instance.acquire()
            try:
                yield conn
            finally:
                self.pool_instance.release(conn)
        else:
            conn = self.create_connection()
            try:
                yield conn
            finally:
                conn.close()

    @contextlib.contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[OracleSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An OracleSyncDriver instance.
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

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        merged_config = {**self.connection_config, **self.pool_config}
        return {k: v for k, v in merged_config.items() if v is not Empty}


class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "AsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections using TypedDict."""

    __is_async__: ClassVar[bool] = True
    __supports_connection_pooling__: ClassVar[bool] = True

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("named_colon", "numeric")
    """OracleDB supports :name (named_colon) and :1 (numeric) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "named_colon"
    """OracleDB's preferred parameter style is :name (named_colon)."""

    def __init__(
        self,
        pool_config: "OraclePoolConfig",
        connection_config: "Optional[OracleConnectionConfig]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        instrumentation: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            pool_config: Oracle pool parameters
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
    def connection_type(self) -> type[OracleAsyncConnection]:  # type: ignore[override]
        """Return the connection type."""

        return oracledb.AsyncConnection

    @property
    def driver_type(self) -> type[OracleAsyncDriver]:  # type: ignore[override]
        """Return the driver type."""
        return OracleAsyncDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        # Merge connection_config into pool_config, with pool_config taking precedence
        merged_config = {**self.connection_config, **self.pool_config}
        return {k: v for k, v in merged_config.items() if v is not Empty}

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""

        # Note: oracledb.create_pool_async returns a pool directly, not a coroutine
        return oracledb.create_pool_async(**self.connection_config_dict)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        async with instrument_operation_async(self, "oracle_async_create_connection", "database"):
            import oracledb

            return await oracledb.connect_async(**{k: v for k, v in self.connection_config.items() if v is not Empty})  # type: ignore[no-any-return]

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[OracleAsyncConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance:
            conn = await self.pool_instance.acquire()
            try:
                yield conn
            finally:
                await self.pool_instance.release(conn)
        else:
            conn = await self.create_connection()
            try:
                yield conn
            finally:
                await conn.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[OracleAsyncDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An OracleAsyncDriver instance.
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
