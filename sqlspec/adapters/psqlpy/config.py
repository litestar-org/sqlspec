"""Psqlpy database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from psqlpy import ConnectionPool

from sqlspec.adapters.psqlpy.driver import PsqlpyConnection, PsqlpyDriver
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger("sqlspec.adapters.psqlpy")

CONNECTION_FIELDS = frozenset(
    {
        "dsn",
        "username",
        "password",
        "db_name",
        "host",
        "port",
        "connect_timeout_sec",
        "connect_timeout_nanosec",
        "tcp_user_timeout_sec",
        "tcp_user_timeout_nanosec",
        "keepalives",
        "keepalives_idle_sec",
        "keepalives_idle_nanosec",
        "keepalives_interval_sec",
        "keepalives_interval_nanosec",
        "keepalives_retries",
        "ssl_mode",
        "ca_file",
        "target_session_attrs",
        "options",
        "application_name",
        "client_encoding",
        "gssencmode",
        "sslnegotiation",
        "sslcompression",
        "sslcert",
        "sslkey",
        "sslpassword",
        "sslrootcert",
        "sslcrl",
        "require_auth",
        "channel_binding",
        "krbsrvname",
        "gsslib",
        "gssdelegation",
        "service",
        "load_balance_hosts",
    }
)

POOL_FIELDS = CONNECTION_FIELDS.union({"hosts", "ports", "conn_recycling_method", "max_db_pool_size", "configure"})

__all__ = ("CONNECTION_FIELDS", "POOL_FIELDS", "PsqlpyConfig")


class PsqlpyConfig(AsyncDatabaseConfig[PsqlpyConnection, ConnectionPool, PsqlpyDriver]):
    """Configuration for Psqlpy asynchronous database connections with direct field-based configuration."""

    __slots__ = (
        "_dialect",
        "application_name",
        "ca_file",
        "channel_binding",
        "client_encoding",
        "configure",
        "conn_recycling_method",
        "connect_timeout_nanosec",
        "connect_timeout_sec",
        "db_name",
        "default_row_type",
        "dsn",
        "extras",
        "gssdelegation",
        "gssencmode",
        "gsslib",
        "host",
        "hosts",
        "keepalives",
        "keepalives_idle_nanosec",
        "keepalives_idle_sec",
        "keepalives_interval_nanosec",
        "keepalives_interval_sec",
        "keepalives_retries",
        "krbsrvname",
        "load_balance_hosts",
        "max_db_pool_size",
        "options",
        "password",
        "pool_instance",
        "port",
        "ports",
        "require_auth",
        "service",
        "ssl_mode",
        "sslcert",
        "sslcompression",
        "sslcrl",
        "sslkey",
        "sslnegotiation",
        "sslpassword",
        "sslrootcert",
        "statement_config",
        "target_session_attrs",
        "tcp_user_timeout_nanosec",
        "tcp_user_timeout_sec",
        "username",
    )

    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = True

    driver_type: type[PsqlpyDriver] = PsqlpyDriver
    connection_type: type[PsqlpyConnection] = PsqlpyConnection
    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("numeric",)
    """Psqlpy only supports $1, $2, ... (numeric) parameter style."""

    preferred_parameter_style: ClassVar[str] = "numeric"
    """Psqlpy's native parameter style is $1, $2, ... (numeric)."""

    def __init__(
        self,
        statement_config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        # Connection parameters
        dsn: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        db_name: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        hosts: Optional[list[str]] = None,
        ports: Optional[list[int]] = None,
        connect_timeout_sec: Optional[int] = None,
        connect_timeout_nanosec: Optional[int] = None,
        tcp_user_timeout_sec: Optional[int] = None,
        tcp_user_timeout_nanosec: Optional[int] = None,
        keepalives: Optional[bool] = None,
        keepalives_idle_sec: Optional[int] = None,
        keepalives_idle_nanosec: Optional[int] = None,
        keepalives_interval_sec: Optional[int] = None,
        keepalives_interval_nanosec: Optional[int] = None,
        keepalives_retries: Optional[int] = None,
        ssl_mode: Optional[str] = None,
        ca_file: Optional[str] = None,
        target_session_attrs: Optional[str] = None,
        options: Optional[str] = None,
        application_name: Optional[str] = None,
        client_encoding: Optional[str] = None,
        gssencmode: Optional[str] = None,
        sslnegotiation: Optional[str] = None,
        sslcompression: Optional[bool] = None,
        sslcert: Optional[str] = None,
        sslkey: Optional[str] = None,
        sslpassword: Optional[str] = None,
        sslrootcert: Optional[str] = None,
        sslcrl: Optional[str] = None,
        require_auth: Optional[str] = None,
        channel_binding: Optional[str] = None,
        krbsrvname: Optional[str] = None,
        gsslib: Optional[str] = None,
        gssdelegation: Optional[bool] = None,
        service: Optional[str] = None,
        load_balance_hosts: Optional[str] = None,
        # Pool parameters
        conn_recycling_method: Optional[str] = None,
        max_db_pool_size: Optional[int] = None,
        configure: Optional["Callable[[ConnectionPool], None]"] = None,
        pool_instance: Optional[ConnectionPool] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Psqlpy asynchronous configuration.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            dsn: DSN of the PostgreSQL database
            username: Username of the user in the PostgreSQL
            password: Password of the user in the PostgreSQL
            db_name: Name of the database in PostgreSQL
            host: Host of the PostgreSQL (use for single host)
            port: Port of the PostgreSQL (use for single host)
            hosts: List of hosts of the PostgreSQL (use for multiple hosts)
            ports: List of ports of the PostgreSQL (use for multiple hosts)
            connect_timeout_sec: The time limit in seconds applied to each socket-level connection attempt
            connect_timeout_nanosec: Nanoseconds for connection timeout, can be used only with connect_timeout_sec
            tcp_user_timeout_sec: The time limit that transmitted data may remain unacknowledged before a connection is forcibly closed
            tcp_user_timeout_nanosec: Nanoseconds for tcp_user_timeout, can be used only with tcp_user_timeout_sec
            keepalives: Controls the use of TCP keepalive. Defaults to True (on)
            keepalives_idle_sec: The number of seconds of inactivity after which a keepalive message is sent to the server
            keepalives_idle_nanosec: Nanoseconds for keepalives_idle_sec
            keepalives_interval_sec: The time interval between TCP keepalive probes
            keepalives_interval_nanosec: Nanoseconds for keepalives_interval_sec
            keepalives_retries: The maximum number of TCP keepalive probes that will be sent before dropping a connection
            ssl_mode: SSL mode (disable, prefer, require, verify-ca, verify-full)
            ca_file: Path to ca_file for SSL
            target_session_attrs: Specifies requirements of the session (e.g., 'read-write', 'read-only', 'primary', 'standby')
            options: Command line options used to configure the server
            application_name: Sets the application_name parameter on the server
            client_encoding: Sets the client_encoding parameter
            gssencmode: GSS encryption mode (disable, prefer, require)
            sslnegotiation: SSL negotiation mode (postgres, direct)
            sslcompression: Whether to use SSL compression
            sslcert: Client SSL certificate file
            sslkey: Client SSL private key file
            sslpassword: Password for the SSL private key
            sslrootcert: SSL root certificate file
            sslcrl: SSL certificate revocation list file
            require_auth: Authentication method requirements
            channel_binding: Channel binding preference (disable, prefer, require)
            krbsrvname: Kerberos service name
            gsslib: GSS library to use
            gssdelegation: Forward GSS credentials to server
            service: Service name for additional parameters
            load_balance_hosts: Controls the order in which the client tries to connect to the available hosts and addresses ('disable' or 'random')
            conn_recycling_method: How a connection is recycled
            max_db_pool_size: Maximum size of the connection pool. Defaults to 10
            configure: Callback to configure new connections
            pool_instance: Existing connection pool instance to use
            **kwargs: Additional parameters (stored in extras)
        """
        # Store connection parameters as instance attributes
        self.dsn = dsn
        self.username = username
        self.password = password
        self.db_name = db_name
        self.host = host
        self.port = port
        self.hosts = hosts
        self.ports = ports
        self.connect_timeout_sec = connect_timeout_sec
        self.connect_timeout_nanosec = connect_timeout_nanosec
        self.tcp_user_timeout_sec = tcp_user_timeout_sec
        self.tcp_user_timeout_nanosec = tcp_user_timeout_nanosec
        self.keepalives = keepalives
        self.keepalives_idle_sec = keepalives_idle_sec
        self.keepalives_idle_nanosec = keepalives_idle_nanosec
        self.keepalives_interval_sec = keepalives_interval_sec
        self.keepalives_interval_nanosec = keepalives_interval_nanosec
        self.keepalives_retries = keepalives_retries
        self.ssl_mode = ssl_mode
        self.ca_file = ca_file
        self.target_session_attrs = target_session_attrs
        self.options = options
        self.application_name = application_name
        self.client_encoding = client_encoding
        self.gssencmode = gssencmode
        self.sslnegotiation = sslnegotiation
        self.sslcompression = sslcompression
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.sslpassword = sslpassword
        self.sslrootcert = sslrootcert
        self.sslcrl = sslcrl
        self.require_auth = require_auth
        self.channel_binding = channel_binding
        self.krbsrvname = krbsrvname
        self.gsslib = gsslib
        self.gssdelegation = gssdelegation
        self.service = service
        self.load_balance_hosts = load_balance_hosts

        # Store pool parameters as instance attributes
        self.conn_recycling_method = conn_recycling_method
        self.max_db_pool_size = max_db_pool_size
        self.configure = configure

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.pool_instance: Optional[ConnectionPool] = pool_instance
        self._dialect: DialectType = None

        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for psqlpy.Connection.

        This method filters out pool-specific parameters that are not valid for psqlpy.Connection.
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
        """Return the full pool configuration as a dict for psqlpy.ConnectionPool.

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

    async def _create_pool(self) -> ConnectionPool:
        """Create the actual async connection pool."""
        logger.info("Creating psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            config = self.pool_config_dict
            pool = ConnectionPool(**config)  # pyright: ignore
            logger.info("Psqlpy connection pool created successfully", extra={"adapter": "psqlpy"})
        except Exception as e:
            logger.exception("Failed to create psqlpy connection pool", extra={"adapter": "psqlpy", "error": str(e)})
            raise
        return pool

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.pool_instance:
            return

        logger.info("Closing psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            self.pool_instance.close()
            logger.info("Psqlpy connection pool closed successfully", extra={"adapter": "psqlpy"})
        except Exception as e:
            logger.exception("Failed to close psqlpy connection pool", extra={"adapter": "psqlpy", "error": str(e)})
            raise

    async def create_connection(self) -> PsqlpyConnection:
        """Create a single async connection (not from pool).

        Returns:
            A psqlpy Connection instance.
        """
        # Ensure pool exists
        if not self.pool_instance:
            self.pool_instance = await self._create_pool()

        # Get connection from pool
        return await self.pool_instance.connection()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[PsqlpyConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psqlpy Connection instance.
        """
        # Ensure pool exists
        if not self.pool_instance:
            self.pool_instance = await self._create_pool()

        async with self.pool_instance.acquire() as conn:
            yield conn

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

    async def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance
