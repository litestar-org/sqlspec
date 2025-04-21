"""Configuration for the psqlpy PostgreSQL adapter."""

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from psqlpy import Connection, ConnectionPool

from sqlspec.adapters.psqlpy.driver import PsqlpyConnection, PsqlpyDriver
from sqlspec.base import AsyncDatabaseConfig, GenericPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable


__all__ = (
    "PsqlpyConfig",
    "PsqlpyPoolConfig",
)


@dataclass
class PsqlpyPoolConfig(GenericPoolConfig):
    """Configuration for psqlpy connection pool.

    Ref: https://psqlpy-python.github.io/components/connection_pool.html#all-available-connectionpool-parameters
    """

    dsn: Optional[Union[str, EmptyType]] = Empty
    """DSN of the PostgreSQL."""
    # Required connection parameters
    username: Optional[Union[str, EmptyType]] = Empty
    """Username of the user in the PostgreSQL."""
    password: Optional[Union[str, EmptyType]] = Empty
    """Password of the user in the PostgreSQL."""
    db_name: Optional[Union[str, EmptyType]] = Empty
    """Name of the database in PostgreSQL."""

    # Single or Multi-host parameters (mutually exclusive)
    host: Optional[Union[str, EmptyType]] = Empty
    """Host of the PostgreSQL (use for single host)."""
    port: Optional[Union[int, EmptyType]] = Empty
    """Port of the PostgreSQL (use for single host)."""
    hosts: Optional[Union[list[str], EmptyType]] = Empty
    """List of hosts of the PostgreSQL (use for multiple hosts)."""
    ports: Optional[Union[list[int], EmptyType]] = Empty
    """List of ports of the PostgreSQL (use for multiple hosts)."""

    # Pool size
    max_db_pool_size: int = 10
    """Maximum size of the connection pool. Defaults to 10."""

    # Optional timeouts
    connect_timeout_sec: Optional[Union[int, EmptyType]] = Empty
    """The time limit in seconds applied to each socket-level connection attempt."""
    connect_timeout_nanosec: Optional[Union[int, EmptyType]] = Empty
    """Nanoseconds for connection timeout, can be used only with `connect_timeout_sec`."""
    tcp_user_timeout_sec: Optional[Union[int, EmptyType]] = Empty
    """The time limit that transmitted data may remain unacknowledged before a connection is forcibly closed."""
    tcp_user_timeout_nanosec: Optional[Union[int, EmptyType]] = Empty
    """Nanoseconds for tcp_user_timeout, can be used only with `tcp_user_timeout_sec`."""

    # Optional keepalives
    keepalives: bool = True
    """Controls the use of TCP keepalive. Defaults to True (on)."""
    keepalives_idle_sec: Optional[Union[int, EmptyType]] = Empty
    """The number of seconds of inactivity after which a keepalive message is sent to the server."""
    keepalives_idle_nanosec: Optional[Union[int, EmptyType]] = Empty
    """Nanoseconds for keepalives_idle_sec."""
    keepalives_interval_sec: Optional[Union[int, EmptyType]] = Empty
    """The time interval between TCP keepalive probes."""
    keepalives_interval_nanosec: Optional[Union[int, EmptyType]] = Empty
    """Nanoseconds for keepalives_interval_sec."""
    keepalives_retries: Optional[Union[int, EmptyType]] = Empty
    """The maximum number of TCP keepalive probes that will be sent before dropping a connection."""

    # Other optional parameters
    load_balance_hosts: Optional[Union[str, EmptyType]] = Empty
    """Controls the order in which the client tries to connect to the available hosts and addresses ('disable' or 'random')."""
    conn_recycling_method: Optional[Union[str, EmptyType]] = Empty
    """How a connection is recycled."""
    ssl_mode: Optional[Union[str, EmptyType]] = Empty
    """SSL mode."""
    ca_file: Optional[Union[str, EmptyType]] = Empty
    """Path to ca_file for SSL."""
    target_session_attrs: Optional[Union[str, EmptyType]] = Empty
    """Specifies requirements of the session (e.g., 'read-write')."""
    options: Optional[Union[str, EmptyType]] = Empty
    """Command line options used to configure the server."""
    application_name: Optional[Union[str, EmptyType]] = Empty
    """Sets the application_name parameter on the server."""


@dataclass
class PsqlpyConfig(AsyncDatabaseConfig[PsqlpyConnection, ConnectionPool, PsqlpyDriver]):
    """Configuration for psqlpy database connections, managing a connection pool.

    This configuration class wraps `PsqlpyPoolConfig` and manages the lifecycle
    of a `psqlpy.ConnectionPool`.
    """

    pool_config: Optional[PsqlpyPoolConfig] = field(default=None)
    """Psqlpy Pool configuration"""
    driver_type: type[PsqlpyDriver] = field(default=PsqlpyDriver, init=False, hash=False)
    """Type of the driver object"""
    connection_type: type[PsqlpyConnection] = field(default=PsqlpyConnection, init=False, hash=False)
    """Type of the connection object"""
    pool_instance: Optional[ConnectionPool] = field(default=None, hash=False)
    """The connection pool instance. If set, this will be used instead of creating a new pool."""

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the minimal connection configuration as a dict for standalone use.

        Returns:
            A string keyed dict of config kwargs for a psqlpy.Connection.

        Raises:
            ImproperConfigurationError: If essential connection parameters are missing.
        """
        if self.pool_config:
            # Exclude pool-specific keys and internal metadata
            pool_specific_keys = {
                "max_db_pool_size",
                "load_balance_hosts",
                "conn_recycling_method",
                "pool_instance",
                "connection_type",
                "driver_type",
            }
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude_none=True,
                exclude=pool_specific_keys,
            )
        msg = "You must provide a 'pool_config' for this adapter."
        raise ImproperConfigurationError(msg)

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Return the pool configuration as a dict.

        Raises:
            ImproperConfigurationError: If no pool_config is provided but a pool_instance

        Returns:
            A string keyed dict of config kwargs for creating a psqlpy pool.
        """
        if self.pool_config:
            # Extract the config from the pool_config
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude_none=True,
                exclude={"pool_instance", "connection_type", "driver_type"},
            )

        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> "ConnectionPool":
        """Return a pool. If none exists yet, create one.

        Ensures that the pool is initialized and returns the instance.

        Returns:
            The pool instance used by the plugin.

        Raises:
            ImproperConfigurationError: If the pool could not be configured.
        """
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        # pool_config is guaranteed to exist due to __post_init__
        try:
            # psqlpy ConnectionPool doesn't have an explicit async connect/startup method
            # It creates connections on demand.
            self.pool_instance = ConnectionPool(**self.pool_config_dict)
        except Exception as e:
            msg = f"Could not configure the 'pool_instance'. Error: {e!s}. Please check your configuration."
            raise ImproperConfigurationError(msg) from e

        return self.pool_instance

    def provide_pool(self, *args: "Any", **kwargs: "Any") -> "Awaitable[ConnectionPool]":
        """Create or return the pool instance.

        Returns:
            An awaitable resolving to the Pool instance.
        """

        async def _create() -> "ConnectionPool":
            return await self.create_pool()

        return _create()

    def create_connection(self) -> "Awaitable[PsqlpyConnection]":
        """Create and return a new, standalone psqlpy connection using the configured parameters.

        Returns:
            An awaitable that resolves to a new Connection instance.
        """

        async def _create() -> "Connection":
            try:
                async with self.provide_connection() as conn:
                    return conn
            except Exception as e:
                msg = f"Could not configure the psqlpy connection. Error: {e!s}"
                raise ImproperConfigurationError(msg) from e

        return _create()

    @asynccontextmanager
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[PsqlpyConnection, None]":
        """Acquire a connection from the pool.

        Yields:
            A connection instance managed by the pool.
        """
        db_pool = await self.provide_pool(*args, **kwargs)
        async with db_pool.acquire() as conn:
            yield conn

    def close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance is not None:
            # psqlpy pool close is synchronous
            self.pool_instance.close()
            self.pool_instance = None

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[PsqlpyDriver, None]":
        """Create and provide a database session using a pooled connection.

        Yields:
            A Psqlpy driver instance wrapping a pooled connection.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)
