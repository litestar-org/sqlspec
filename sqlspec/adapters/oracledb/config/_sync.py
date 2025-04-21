from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from oracledb import create_pool as oracledb_create_pool  # pyright: ignore[reportUnknownVariableType]

from sqlspec.adapters.oracledb.config._common import OracleGenericPoolConfig
from sqlspec.adapters.oracledb.driver import OracleSyncConnection, OracleSyncDriver
from sqlspec.base import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator

    from oracledb.pool import ConnectionPool


__all__ = (
    "OracleSyncConfig",
    "OracleSyncPoolConfig",
)


@dataclass
class OracleSyncPoolConfig(OracleGenericPoolConfig["OracleSyncConnection", "ConnectionPool"]):
    """Sync Oracle Pool Config"""


@dataclass
class OracleSyncConfig(SyncDatabaseConfig["OracleSyncConnection", "ConnectionPool", "OracleSyncDriver"]):
    """Oracle Sync database Configuration.

    This class provides the base configuration for Oracle database connections, extending
    the generic database configuration with Oracle-specific settings. It supports both
    thin and thick modes of the python-oracledb driver.([1](https://python-oracledb.readthedocs.io/en/latest/index.html))

    The configuration supports all standard Oracle connection parameters and can be used
    with both synchronous and asynchronous connections. It includes support for features
    like Oracle Wallet, external authentication, connection pooling, and advanced security
    options.([2](https://python-oracledb.readthedocs.io/en/latest/user_guide/tuning.html))
    """

    pool_config: "Optional[OracleSyncPoolConfig]" = None
    """Oracle Pool configuration"""
    pool_instance: "Optional[ConnectionPool]" = None
    """Optional pool to use.

    If set, the plugin will use the provided pool rather than instantiate one.
    """
    connection_type: "type[OracleSyncConnection]" = field(init=False, default_factory=lambda: OracleSyncConnection)  # pyright: ignore
    """Connection class to use.

    Defaults to :class:`Connection`.
    """
    driver_type: "type[OracleSyncDriver]" = field(init=False, default_factory=lambda: OracleSyncDriver)  # type: ignore[type-abstract,unused-ignore]
    """Driver class to use.

    Defaults to :class:`OracleSyncDriver`.
    """

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the oracledb.connect function.

        Raises:
            ImproperConfigurationError: If the connection configuration is not provided.
        """
        if self.pool_config:
            # Filter out pool-specific parameters
            pool_only_params = {
                "min",
                "max",
                "increment",
                "timeout",
                "wait_timeout",
                "max_lifetime_session",
                "session_callback",
            }
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude=pool_only_params.union({"pool_instance", "connection_type", "driver_type"}),
            )
        msg = "You must provide a 'pool_config' for this adapter."
        raise ImproperConfigurationError(msg)

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Return the pool configuration as a dict.

        Raises:
            ImproperConfigurationError: If no pool_config is provided but a pool_instance

        Returns:
            A string keyed dict of config kwargs for the Asyncpg :func:`create_pool <oracledb.pool.create_pool>`
            function.
        """
        if self.pool_config:
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude={"pool_instance", "connection_type", "driver_type"},
            )
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    def create_connection(self) -> "OracleSyncConnection":
        """Create and return a new oracledb connection from the pool.

        Returns:
            A Connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be created.
        """
        try:
            pool = self.provide_pool()
            return pool.acquire()
        except Exception as e:
            msg = f"Could not configure the Oracle connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    def create_pool(self) -> "ConnectionPool":
        """Return a pool. If none exists yet, create one.

        Raises:
            ImproperConfigurationError: If neither pool_config nor pool_instance is provided,
                or if the pool could not be configured.

        Returns:
            Getter that returns the pool instance used by the plugin.
        """
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        pool_config = self.pool_config_dict
        self.pool_instance = oracledb_create_pool(**pool_config)
        if self.pool_instance is None:  # pyright: ignore[reportUnnecessaryComparison]
            msg = "Could not configure the 'pool_instance'. Please check your configuration."  # type: ignore[unreachable]
            raise ImproperConfigurationError(msg)
        return self.pool_instance

    def provide_pool(self, *args: "Any", **kwargs: "Any") -> "ConnectionPool":
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return self.create_pool()

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[OracleSyncConnection, None, None]":
        """Create a connection instance.

        Yields:
            Connection: A connection instance from the pool.
        """
        db_pool = self.provide_pool(*args, **kwargs)
        with db_pool.acquire() as connection:  # pyright: ignore[reportUnknownMemberType]
            yield connection

    @contextmanager
    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Generator[OracleSyncDriver, None, None]":
        """Create and provide a database session.

        Yields:
            OracleSyncDriver: A driver instance with an active connection.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)

    def close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance is not None:
            self.pool_instance.close()
            self.pool_instance = None
