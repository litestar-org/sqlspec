from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from psycopg_pool import ConnectionPool

from sqlspec.adapters.psycopg.config._common import PsycopgGenericPoolConfig
from sqlspec.adapters.psycopg.driver import PsycopgSyncConnection, PsycopgSyncDriver
from sqlspec.base import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator


__all__ = (
    "PsycopgSyncConfig",
    "PsycopgSyncPoolConfig",
)


@dataclass
class PsycopgSyncPoolConfig(PsycopgGenericPoolConfig[PsycopgSyncConnection, ConnectionPool]):
    """Sync Psycopg Pool Config"""


@dataclass
class PsycopgSyncConfig(SyncDatabaseConfig[PsycopgSyncConnection, ConnectionPool, PsycopgSyncDriver]):
    """Sync Psycopg database Configuration.
    This class provides the base configuration for Psycopg database connections, extending
    the generic database configuration with Psycopg-specific settings.([1](https://www.psycopg.org/psycopg3/docs/api/connections.html))

    The configuration supports all standard Psycopg connection parameters and can be used
    with both synchronous and asynchronous connections.([2](https://www.psycopg.org/psycopg3/docs/api/connections.html))
    """

    pool_config: "Optional[PsycopgSyncPoolConfig]" = None
    """Psycopg Pool configuration"""
    pool_instance: "Optional[ConnectionPool]" = None
    """Optional pool to use"""
    connection_type: "type[PsycopgSyncConnection]" = field(init=False, default_factory=lambda: PsycopgSyncConnection)  # type: ignore[assignment]
    """Type of the connection object"""
    driver_type: "type[PsycopgSyncDriver]" = field(init=False, default_factory=lambda: PsycopgSyncDriver)  # type: ignore[type-abstract,unused-ignore]
    """Type of the driver object"""

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the psycopg.connect function.

        Raises:
            ImproperConfigurationError: If the connection configuration is not provided.
        """
        if self.pool_config:
            # Filter out pool-specific parameters
            pool_only_params = {
                "min_size",
                "max_size",
                "name",
                "timeout",
                "reconnect_timeout",
                "max_idle",
                "max_lifetime",
            }
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude=pool_only_params.union({"pool_instance", "connection_type", "driver_type", "open"}),
            )
        msg = "You must provide a 'pool_config' for this adapter."
        raise ImproperConfigurationError(msg)

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Return the pool configuration as a dict.

        Raises:
            ImproperConfigurationError: If pool_config is not provided and instead pool_instance is used.
        """
        if self.pool_config:
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude={"pool_instance", "connection_type", "driver_type", "open"},
            )
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    def create_connection(self) -> "PsycopgSyncConnection":
        """Create and return a new psycopg connection from the pool.

        Returns:
            A Connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be created.
        """
        try:
            pool = self.provide_pool()
            return pool.getconn()
        except Exception as e:
            msg = f"Could not configure the Psycopg connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    def create_pool(self) -> "ConnectionPool":
        """Create and return a connection pool.

        Returns:
            ConnectionPool: The configured connection pool instance.

        Raises:
            ImproperConfigurationError: If neither pool_config nor pool_instance is provided,
                or if the pool could not be configured.
        """
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        pool_config = self.pool_config_dict
        self.pool_instance = ConnectionPool(open=False, **pool_config)
        if self.pool_instance is None:  # pyright: ignore[reportUnnecessaryComparison]
            msg = "Could not configure the 'pool_instance'. Please check your configuration."  # type: ignore[unreachable]
            raise ImproperConfigurationError(msg)
        self.pool_instance.open()
        return self.pool_instance

    def provide_pool(self, *args: "Any", **kwargs: "Any") -> "ConnectionPool":
        """Create and return a connection pool.

        Returns:
            ConnectionPool: The configured connection pool instance.
        """
        return self.create_pool()

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[PsycopgSyncConnection, None, None]":
        """Create and provide a database connection.

        Yields:
            PsycopgSyncConnection: A database connection from the pool.
        """
        pool = self.provide_pool(*args, **kwargs)
        with pool, pool.connection() as connection:
            yield connection

    @contextmanager
    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Generator[PsycopgSyncDriver, None, None]":
        """Create and provide a database session.

        Yields:
            PsycopgSyncDriver: A driver instance with an active connection.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)

    def close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance is not None:
            self.pool_instance.close()
            self.pool_instance = None
