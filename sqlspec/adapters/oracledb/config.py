"""OracleDB database configuration with direct field-based configuration."""

import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional, cast

import oracledb

from sqlspec.adapters.oracledb.driver import (
    OracleAsyncConnection,
    OracleAsyncDriver,
    OracleSyncConnection,
    OracleSyncDriver,
)
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from oracledb import AuthMode
    from oracledb.pool import AsyncConnectionPool, ConnectionPool
    from sqlglot.dialects.dialect import DialectType


__all__ = ("CONNECTION_FIELDS", "POOL_FIELDS", "OracleAsyncConfig", "OracleSyncConfig")

logger = logging.getLogger(__name__)

CONNECTION_FIELDS = frozenset(
    {
        "dsn",
        "user",
        "password",
        "host",
        "port",
        "service_name",
        "sid",
        "wallet_location",
        "wallet_password",
        "config_dir",
        "tcp_connect_timeout",
        "retry_count",
        "retry_delay",
        "mode",
        "events",
        "edition",
    }
)

POOL_FIELDS = CONNECTION_FIELDS.union(
    {
        "min",
        "max",
        "increment",
        "threaded",
        "getmode",
        "homogeneous",
        "timeout",
        "wait_timeout",
        "max_lifetime_session",
        "session_callback",
        "max_sessions_per_shard",
        "soda_metadata_cache",
        "ping_interval",
    }
)


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "ConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections with direct field-based configuration."""

    __slots__ = (
        "_dialect",
        "config_dir",
        "default_row_type",
        "dsn",
        "edition",
        "events",
        "extras",
        "getmode",
        "homogeneous",
        "host",
        "increment",
        "max",
        "max_lifetime_session",
        "max_sessions_per_shard",
        "min",
        "mode",
        "password",
        "ping_interval",
        "pool_instance",
        "port",
        "retry_count",
        "retry_delay",
        "service_name",
        "session_callback",
        "sid",
        "soda_metadata_cache",
        "statement_config",
        "tcp_connect_timeout",
        "threaded",
        "timeout",
        "user",
        "wait_timeout",
        "wallet_location",
        "wallet_password",
    )

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = True

    driver_type: type[OracleSyncDriver] = OracleSyncDriver
    connection_type: type[OracleSyncConnection] = OracleSyncConnection

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("named_colon", "positional_colon")
    """OracleDB supports :name (named_colon) and :1 (positional_colon) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "named_colon"
    """OracleDB's preferred parameter style is :name (named_colon)."""

    def __init__(
        self,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        # Connection parameters
        dsn: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        service_name: Optional[str] = None,
        sid: Optional[str] = None,
        wallet_location: Optional[str] = None,
        wallet_password: Optional[str] = None,
        config_dir: Optional[str] = None,
        tcp_connect_timeout: Optional[float] = None,
        retry_count: Optional[int] = None,
        retry_delay: Optional[int] = None,
        mode: Optional["AuthMode"] = None,
        events: Optional[bool] = None,
        edition: Optional[str] = None,
        # Pool parameters
        min: Optional[int] = None,
        max: Optional[int] = None,
        increment: Optional[int] = None,
        threaded: Optional[bool] = None,
        getmode: Optional[int] = None,
        homogeneous: Optional[bool] = None,
        timeout: Optional[int] = None,
        wait_timeout: Optional[int] = None,
        max_lifetime_session: Optional[int] = None,
        session_callback: Optional["Callable[[Any, Any], None]"] = None,
        max_sessions_per_shard: Optional[int] = None,
        soda_metadata_cache: Optional[bool] = None,
        ping_interval: Optional[int] = None,
        pool_instance: Optional["ConnectionPool"] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            dsn: Connection string for the database
            user: Username for database authentication
            password: Password for database authentication
            host: Database server hostname
            port: Database server port number
            service_name: Oracle service name
            sid: Oracle System ID (SID)
            wallet_location: Location of Oracle Wallet
            wallet_password: Password for accessing Oracle Wallet
            config_dir: Directory containing Oracle configuration files
            tcp_connect_timeout: Timeout for establishing TCP connections
            retry_count: Number of attempts to connect
            retry_delay: Time in seconds between connection attempts
            mode: Session mode (SYSDBA, SYSOPER, etc.)
            events: If True, enables Oracle events for FAN and RLB
            edition: Edition name for edition-based redefinition
            min: Minimum number of connections in the pool
            max: Maximum number of connections in the pool
            increment: Number of connections to create when pool needs to grow
            threaded: Whether the pool should be threaded
            getmode: How connections are returned from the pool
            homogeneous: Whether all connections use the same credentials
            timeout: Time in seconds after which idle connections are closed
            wait_timeout: Time in seconds to wait for an available connection
            max_lifetime_session: Maximum time in seconds that a connection can remain in the pool
            session_callback: Callback function called when a connection is returned to the pool
            max_sessions_per_shard: Maximum number of sessions per shard
            soda_metadata_cache: Whether to enable SODA metadata caching
            ping_interval: Interval for pinging pooled connections
            pool_instance: Optional existing connection pool instance
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.dsn = dsn
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.sid = sid
        self.wallet_location = wallet_location
        self.wallet_password = wallet_password
        self.config_dir = config_dir
        self.tcp_connect_timeout = tcp_connect_timeout
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.mode = mode
        self.events = events
        self.edition = edition

        # Store pool parameters as instance attributes
        self.min = min
        self.max = max
        self.increment = increment
        self.threaded = threaded
        self.getmode = getmode
        self.homogeneous = homogeneous
        self.timeout = timeout
        self.wait_timeout = wait_timeout
        self.max_lifetime_session = max_lifetime_session
        self.session_callback = session_callback
        self.max_sessions_per_shard = max_sessions_per_shard
        self.soda_metadata_cache = soda_metadata_cache
        self.ping_interval = ping_interval

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.pool_instance = pool_instance
        self._dialect: DialectType = None

        super().__init__()

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
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return self.pool_instance.acquire()

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[OracleSyncConnection, None, None]":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Oracle Connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        conn = self.pool_instance.acquire()
        try:
            yield conn
        finally:
            self.pool_instance.release(conn)

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
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            driver = self.driver_type(connection=conn, config=statement_config)
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
        """Return the connection configuration as a dict for Oracle operations.

        Returns all configuration parameters merged together.
        """
        # Gather non-None parameters from all fields (connection + pool)
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras parameters
        config.update(self.extras)

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict for Oracle operations.

        Returns all configuration parameters merged together.
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


class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "AsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections with direct field-based configuration."""

    __slots__ = (
        "_dialect",
        "config_dir",
        "default_row_type",
        "dsn",
        "edition",
        "events",
        "extras",
        "getmode",
        "homogeneous",
        "host",
        "increment",
        "max",
        "max_lifetime_session",
        "max_sessions_per_shard",
        "min",
        "mode",
        "password",
        "ping_interval",
        "pool_instance",
        "port",
        "retry_count",
        "retry_delay",
        "service_name",
        "session_callback",
        "sid",
        "soda_metadata_cache",
        "statement_config",
        "tcp_connect_timeout",
        "threaded",
        "timeout",
        "user",
        "wait_timeout",
        "wallet_location",
        "wallet_password",
    )

    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = True

    connection_type: type[OracleAsyncConnection] = OracleAsyncConnection
    driver_type: type[OracleAsyncDriver] = OracleAsyncDriver

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("named_colon", "positional_colon")
    """OracleDB supports :name (named_colon) and :1 (positional_colon) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "named_colon"
    """OracleDB's preferred parameter style is :name (named_colon)."""

    def __init__(
        self,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        # Connection parameters
        dsn: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        service_name: Optional[str] = None,
        sid: Optional[str] = None,
        wallet_location: Optional[str] = None,
        wallet_password: Optional[str] = None,
        config_dir: Optional[str] = None,
        tcp_connect_timeout: Optional[float] = None,
        retry_count: Optional[int] = None,
        retry_delay: Optional[int] = None,
        mode: Optional["AuthMode"] = None,
        events: Optional[bool] = None,
        edition: Optional[str] = None,
        # Pool parameters
        min: Optional[int] = None,
        max: Optional[int] = None,
        increment: Optional[int] = None,
        threaded: Optional[bool] = None,
        getmode: Optional[int] = None,
        homogeneous: Optional[bool] = None,
        timeout: Optional[int] = None,
        wait_timeout: Optional[int] = None,
        max_lifetime_session: Optional[int] = None,
        session_callback: Optional["Callable[[Any, Any], None]"] = None,
        max_sessions_per_shard: Optional[int] = None,
        soda_metadata_cache: Optional[bool] = None,
        ping_interval: Optional[int] = None,
        pool_instance: Optional["AsyncConnectionPool"] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            dsn: Connection string for the database
            user: Username for database authentication
            password: Password for database authentication
            host: Database server hostname
            port: Database server port number
            service_name: Oracle service name
            sid: Oracle System ID (SID)
            wallet_location: Location of Oracle Wallet
            wallet_password: Password for accessing Oracle Wallet
            config_dir: Directory containing Oracle configuration files
            tcp_connect_timeout: Timeout for establishing TCP connections
            retry_count: Number of attempts to connect
            retry_delay: Time in seconds between connection attempts
            mode: Session mode (SYSDBA, SYSOPER, etc.)
            events: If True, enables Oracle events for FAN and RLB
            edition: Edition name for edition-based redefinition
            min: Minimum number of connections in the pool
            max: Maximum number of connections in the pool
            increment: Number of connections to create when pool needs to grow
            threaded: Whether the pool should be threaded
            getmode: How connections are returned from the pool
            homogeneous: Whether all connections use the same credentials
            timeout: Time in seconds after which idle connections are closed
            wait_timeout: Time in seconds to wait for an available connection
            max_lifetime_session: Maximum time in seconds that a connection can remain in the pool
            session_callback: Callback function called when a connection is returned to the pool
            max_sessions_per_shard: Maximum number of sessions per shard
            soda_metadata_cache: Whether to enable SODA metadata caching
            ping_interval: Interval for pinging pooled connections
            pool_instance: Optional existing async connection pool instance
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.dsn = dsn
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.sid = sid
        self.wallet_location = wallet_location
        self.wallet_password = wallet_password
        self.config_dir = config_dir
        self.tcp_connect_timeout = tcp_connect_timeout
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.mode = mode
        self.events = events
        self.edition = edition

        # Store pool parameters as instance attributes
        self.min = min
        self.max = max
        self.increment = increment
        self.threaded = threaded
        self.getmode = getmode
        self.homogeneous = homogeneous
        self.timeout = timeout
        self.wait_timeout = wait_timeout
        self.max_lifetime_session = max_lifetime_session
        self.session_callback = session_callback
        self.max_sessions_per_shard = max_sessions_per_shard
        self.soda_metadata_cache = soda_metadata_cache
        self.ping_interval = ping_interval

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.pool_instance: Optional[AsyncConnectionPool] = pool_instance
        self._dialect: DialectType = None

        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for Oracle async operations.

        Returns all configuration parameters merged together.
        """
        # Gather non-None parameters
        config = {field: getattr(self, field) for field in CONNECTION_FIELDS if getattr(self, field, None) is not None}

        # Merge extras parameters
        config.update(self.extras)

        return config

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for Oracle async operations.

        Returns all configuration parameters merged together.
        """
        # Gather non-None parameters
        config = {field: getattr(self, field) for field in POOL_FIELDS if getattr(self, field, None) is not None}

        # Merge extras parameters
        config.update(self.extras)

        return config

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""

        return oracledb.create_pool_async(**self.pool_config_dict)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return cast("OracleAsyncConnection", await self.pool_instance.acquire())

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[OracleAsyncConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        conn = await self.pool_instance.acquire()
        try:
            yield conn
        finally:
            await self.pool_instance.release(conn)

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
                statement_config = replace(
                    statement_config,
                    allowed_parameter_styles=self.supported_parameter_styles,
                    target_parameter_style=self.preferred_parameter_style,
                )

            driver = self.driver_type(connection=conn, config=statement_config)
            yield driver

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
