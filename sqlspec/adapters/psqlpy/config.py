"""Psqlpy database configuration using TypedDict for better maintainability."""

import inspect
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict

import psqlpy
from psqlpy import Connection, ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psqlpy.driver import PsqlpyConnection, PsqlpyDriver
from sqlspec.config import AsyncDatabaseConfig, InstrumentationConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger("sqlspec.adapters.psqlpy")

__all__ = (
    "PsqlpyConfig",
    "PsqlpyConnectionConfig",
    "PsqlpyPoolConfig",
)


class PsqlpyConnectionConfig(TypedDict, total=False):
    """Psqlpy connection configuration as TypedDict.

    Basic connection parameters for psqlpy Connection.
    """

    dsn: NotRequired[str]
    """DSN of the PostgreSQL database."""

    username: NotRequired[str]
    """Username of the user in the PostgreSQL."""

    password: NotRequired[str]
    """Password of the user in the PostgreSQL."""

    db_name: NotRequired[str]
    """Name of the database in PostgreSQL."""

    host: NotRequired[str]
    """Host of the PostgreSQL (use for single host)."""

    port: NotRequired[int]
    """Port of the PostgreSQL (use for single host)."""

    connect_timeout_sec: NotRequired[int]
    """The time limit in seconds applied to each socket-level connection attempt."""

    connect_timeout_nanosec: NotRequired[int]
    """Nanoseconds for connection timeout, can be used only with connect_timeout_sec."""

    tcp_user_timeout_sec: NotRequired[int]
    """The time limit that transmitted data may remain unacknowledged before a connection is forcibly closed."""

    tcp_user_timeout_nanosec: NotRequired[int]
    """Nanoseconds for tcp_user_timeout, can be used only with tcp_user_timeout_sec."""

    keepalives: NotRequired[bool]
    """Controls the use of TCP keepalive. Defaults to True (on)."""

    keepalives_idle_sec: NotRequired[int]
    """The number of seconds of inactivity after which a keepalive message is sent to the server."""

    keepalives_idle_nanosec: NotRequired[int]
    """Nanoseconds for keepalives_idle_sec."""

    keepalives_interval_sec: NotRequired[int]
    """The time interval between TCP keepalive probes."""

    keepalives_interval_nanosec: NotRequired[int]
    """Nanoseconds for keepalives_interval_sec."""

    keepalives_retries: NotRequired[int]
    """The maximum number of TCP keepalive probes that will be sent before dropping a connection."""

    ssl_mode: NotRequired[str]
    """SSL mode (disable, prefer, require, verify-ca, verify-full)."""

    ca_file: NotRequired[str]
    """Path to ca_file for SSL."""

    target_session_attrs: NotRequired[str]
    """Specifies requirements of the session (e.g., 'read-write', 'read-only', 'primary', 'standby')."""

    options: NotRequired[str]
    """Command line options used to configure the server."""

    application_name: NotRequired[str]
    """Sets the application_name parameter on the server."""

    client_encoding: NotRequired[str]
    """Sets the client_encoding parameter."""

    gssencmode: NotRequired[str]
    """GSS encryption mode (disable, prefer, require)."""

    sslnegotiation: NotRequired[str]
    """SSL negotiation mode (postgres, direct)."""

    sslcompression: NotRequired[bool]
    """Whether to use SSL compression."""

    sslcert: NotRequired[str]
    """Client SSL certificate file."""

    sslkey: NotRequired[str]
    """Client SSL private key file."""

    sslpassword: NotRequired[str]
    """Password for the SSL private key."""

    sslrootcert: NotRequired[str]
    """SSL root certificate file."""

    sslcrl: NotRequired[str]
    """SSL certificate revocation list file."""

    require_auth: NotRequired[str]
    """Authentication method requirements."""

    channel_binding: NotRequired[str]
    """Channel binding preference (disable, prefer, require)."""

    krbsrvname: NotRequired[str]
    """Kerberos service name."""

    gsslib: NotRequired[str]
    """GSS library to use."""

    gssdelegation: NotRequired[bool]
    """Forward GSS credentials to server."""

    service: NotRequired[str]
    """Service name for additional parameters."""


class PsqlpyPoolConfig(TypedDict, total=False):
    """Psqlpy pool configuration as TypedDict.

    All parameters for psqlpy ConnectionPool.
    Inherits connection parameters and adds pool-specific settings.
    """

    # Connection parameters (inherit from connection config)
    dsn: NotRequired[str]
    """DSN of the PostgreSQL database."""

    username: NotRequired[str]
    """Username of the user in the PostgreSQL."""

    password: NotRequired[str]
    """Password of the user in the PostgreSQL."""

    db_name: NotRequired[str]
    """Name of the database in PostgreSQL."""

    host: NotRequired[str]
    """Host of the PostgreSQL (use for single host)."""

    port: NotRequired[int]
    """Port of the PostgreSQL (use for single host)."""

    hosts: NotRequired[list[str]]
    """List of hosts of the PostgreSQL (use for multiple hosts)."""

    ports: NotRequired[list[int]]
    """List of ports of the PostgreSQL (use for multiple hosts)."""

    connect_timeout_sec: NotRequired[int]
    """The time limit in seconds applied to each socket-level connection attempt."""

    connect_timeout_nanosec: NotRequired[int]
    """Nanoseconds for connection timeout, can be used only with connect_timeout_sec."""

    tcp_user_timeout_sec: NotRequired[int]
    """The time limit that transmitted data may remain unacknowledged before a connection is forcibly closed."""

    tcp_user_timeout_nanosec: NotRequired[int]
    """Nanoseconds for tcp_user_timeout, can be used only with tcp_user_timeout_sec."""

    keepalives: NotRequired[bool]
    """Controls the use of TCP keepalive. Defaults to True (on)."""

    keepalives_idle_sec: NotRequired[int]
    """The number of seconds of inactivity after which a keepalive message is sent to the server."""

    keepalives_idle_nanosec: NotRequired[int]
    """Nanoseconds for keepalives_idle_sec."""

    keepalives_interval_sec: NotRequired[int]
    """The time interval between TCP keepalive probes."""

    keepalives_interval_nanosec: NotRequired[int]
    """Nanoseconds for keepalives_interval_sec."""

    keepalives_retries: NotRequired[int]
    """The maximum number of TCP keepalive probes that will be sent before dropping a connection."""

    ssl_mode: NotRequired[str]
    """SSL mode (disable, prefer, require, verify-ca, verify-full)."""

    ca_file: NotRequired[str]
    """Path to ca_file for SSL."""

    target_session_attrs: NotRequired[str]
    """Specifies requirements of the session (e.g., 'read-write', 'read-only', 'primary', 'standby')."""

    options: NotRequired[str]
    """Command line options used to configure the server."""

    application_name: NotRequired[str]
    """Sets the application_name parameter on the server."""

    client_encoding: NotRequired[str]
    """Sets the client_encoding parameter."""

    gssencmode: NotRequired[str]
    """GSS encryption mode (disable, prefer, require)."""

    sslnegotiation: NotRequired[str]
    """SSL negotiation mode (postgres, direct)."""

    sslcompression: NotRequired[bool]
    """Whether to use SSL compression."""

    sslcert: NotRequired[str]
    """Client SSL certificate file."""

    sslkey: NotRequired[str]
    """Client SSL private key file."""

    sslpassword: NotRequired[str]
    """Password for the SSL private key."""

    sslrootcert: NotRequired[str]
    """SSL root certificate file."""

    sslcrl: NotRequired[str]
    """SSL certificate revocation list file."""

    require_auth: NotRequired[str]
    """Authentication method requirements."""

    channel_binding: NotRequired[str]
    """Channel binding preference (disable, prefer, require)."""

    krbsrvname: NotRequired[str]
    """Kerberos service name."""

    gsslib: NotRequired[str]
    """GSS library to use."""

    gssdelegation: NotRequired[bool]
    """Forward GSS credentials to server."""

    service: NotRequired[str]
    """Service name for additional parameters."""

    load_balance_hosts: NotRequired[str]
    """Controls the order in which the client tries to connect to the available hosts and addresses ('disable' or 'random')."""

    conn_recycling_method: NotRequired[str]
    """How a connection is recycled."""

    # Pool-specific parameters
    max_db_pool_size: NotRequired[int]
    """Maximum size of the connection pool. Defaults to 10."""

    configure: NotRequired["Callable[[Connection], None]"]
    """Callback to configure new connections."""


class PsqlpyConfig(AsyncDatabaseConfig[PsqlpyConnection, ConnectionPool, PsqlpyDriver]):
    """Configuration for Psqlpy asynchronous database connections using TypedDict."""

    __is_async__: ClassVar[bool] = True
    __supports_connection_pooling__: ClassVar[bool] = True

    def __init__(
        self,
        pool_config: PsqlpyPoolConfig,
        connection_config: Optional[PsqlpyConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        """Initialize Psqlpy asynchronous configuration.

        Args:
            pool_config: Psqlpy pool parameters
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
    def connection_type(self) -> type[PsqlpyConnection]:  # type: ignore[override]
        """Return the connection type."""
        return Connection

    @property
    def driver_type(self) -> type[PsqlpyDriver]:  # type: ignore[override]
        """Return the driver type."""
        return PsqlpyDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict, with validation for required parameters."""
        config = {k: v for k, v in self.connection_config.items() if v is not Empty}
        try:
            valid_params = set(inspect.signature(psqlpy.connect).parameters)
        except Exception:
            valid_params = set()
        extra_keys = set(config) - valid_params
        if extra_keys:
            logger.debug(
                "Psqlpy config received extra/unrecognized parameters: %s. These will be ignored and not passed to the driver.",
                list(extra_keys),
            )
        return {k: v for k, v in config.items() if k in valid_params}

    async def _create_pool_impl(self) -> ConnectionPool:
        """Create the actual async connection pool."""
        if self.instrumentation.log_pool_operations:
            logger.info("Creating psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            pool = ConnectionPool(**self.connection_config_dict)
            if self.instrumentation.log_pool_operations:
                logger.info("Psqlpy connection pool created successfully", extra={"adapter": "psqlpy"})
        except Exception as e:
            logger.exception("Failed to create psqlpy connection pool", extra={"adapter": "psqlpy", "error": str(e)})
            raise
        return pool

    async def _close_pool_impl(self) -> None:
        """Close the actual async connection pool."""
        if not self.pool_instance:
            return

        if self.instrumentation.log_pool_operations:
            logger.info("Closing psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            self.pool_instance.close()
            if self.instrumentation.log_pool_operations:
                logger.info("Psqlpy connection pool closed successfully", extra={"adapter": "psqlpy"})
        except Exception as e:
            logger.exception("Failed to close psqlpy connection pool", extra={"adapter": "psqlpy", "error": str(e)})
            raise

    async def create_connection(self) -> PsqlpyConnection:
        """Create a single async connection (not from pool).

        Returns:
            A psqlpy Connection instance.
        """
        conn_dict = {k: v for k, v in self.connection_config.items() if v is not Empty}
        # psqlpy Connection constructor is synchronous, not async
        return Connection(**conn_dict)

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[PsqlpyConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psqlpy Connection instance.
        """
        if self.pool_instance:
            async with self.pool_instance.acquire() as conn:
                yield conn
        else:
            conn = await self.create_connection()
            try:
                yield conn
            finally:
                if conn is not None:
                    conn.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[PsqlpyDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsqlpyDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as conn:
            driver = self.driver_type(
                connection=conn,
                config=self.statement_config,
                instrumentation_config=self.instrumentation,
            )
            yield driver

    async def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
