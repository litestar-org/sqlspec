from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, cast

from oracledb import create_pool_async as oracledb_create_pool  # pyright: ignore[reportUnknownVariableType]

from sqlspec.adapters.oracledb.config._common import OracleGenericPoolConfig
from sqlspec.adapters.oracledb.driver import OracleAsyncConnection, OracleAsyncDriver
from sqlspec.base import AsyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from oracledb.pool import AsyncConnectionPool


__all__ = (
    "OracleAsyncConfig",
    "OracleAsyncPoolConfig",
)


@dataclass
class OracleAsyncPoolConfig(OracleGenericPoolConfig["OracleAsyncConnection", "AsyncConnectionPool"]):
    """Async Oracle Pool Config"""


@dataclass
class OracleAsyncConfig(AsyncDatabaseConfig["OracleAsyncConnection", "AsyncConnectionPool", "OracleAsyncDriver"]):
    """Oracle Async database Configuration.

    This class provides the base configuration for Oracle database connections, extending
    the generic database configuration with Oracle-specific settings. It supports both
    thin and thick modes of the python-oracledb driver.([1](https://python-oracledb.readthedocs.io/en/latest/index.html))

    The configuration supports all standard Oracle connection parameters and can be used
    with both synchronous and asynchronous connections. It includes support for features
    like Oracle Wallet, external authentication, connection pooling, and advanced security
    options.([2](https://python-oracledb.readthedocs.io/en/latest/user_guide/tuning.html))
    """

    pool_config: "Optional[OracleAsyncPoolConfig]" = None
    """Oracle Pool configuration"""
    pool_instance: "Optional[AsyncConnectionPool]" = None
    """Optional pool to use.

    If set, the plugin will use the provided pool rather than instantiate one.
    """
    connection_type: "type[OracleAsyncConnection]" = field(init=False, default_factory=lambda: OracleAsyncConnection)
    """Connection class to use.

    Defaults to :class:`AsyncConnection`.
    """
    driver_type: "type[OracleAsyncDriver]" = field(init=False, default_factory=lambda: OracleAsyncDriver)  # type: ignore[type-abstract,unused-ignore]
    """Driver class to use.

    Defaults to :class:`OracleAsyncDriver`.
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
        if self.pool_config is not None:
            return dataclass_to_dict(
                self.pool_config,
                exclude_empty=True,
                convert_nested=False,
                exclude={"pool_instance", "connection_type", "driver_type"},
            )
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_connection(self) -> "OracleAsyncConnection":
        """Create and return a new oracledb async connection from the pool.

        Returns:
            An AsyncConnection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be created.
        """
        try:
            pool = await self.provide_pool()
            return cast("OracleAsyncConnection", await pool.acquire())  # type: ignore[no-any-return,unused-ignore]
        except Exception as e:
            msg = f"Could not configure the Oracle async connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    async def create_pool(self) -> "AsyncConnectionPool":
        """Return a pool. If none exists yet, create one.

        Raises:
            ImproperConfigurationError: If neither pool_config nor pool_instance are provided,
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

    def provide_pool(self, *args: "Any", **kwargs: "Any") -> "Awaitable[AsyncConnectionPool]":
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return self.create_pool()

    @asynccontextmanager
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[OracleAsyncConnection, None]":
        """Create a connection instance.

        Yields:
            AsyncConnection: A connection instance.
        """
        db_pool = await self.provide_pool(*args, **kwargs)
        async with db_pool.acquire() as connection:  # pyright: ignore[reportUnknownMemberType]
            yield connection

    @asynccontextmanager
    async def provide_session(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[OracleAsyncDriver, None]":
        """Create and provide a database session.

        Yields:
            OracleAsyncDriver: A driver instance with an active connection.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)

    async def close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance is not None:
            await self.pool_instance.close()
            self.pool_instance = None
