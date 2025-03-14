from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from oracledb import create_pool_async as oracledb_create_pool  # pyright: ignore[reportUnknownVariableType]
from oracledb.connection import AsyncConnection
from oracledb.pool import AsyncConnectionPool

from sqlspec.adapters.oracledb.config._common import (
    OracleGenericPoolConfig,
)
from sqlspec.base import AsyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable


__all__ = (
    "OracleAsyncDatabaseConfig",
    "OracleAsyncPoolConfig",
)


@dataclass
class OracleAsyncPoolConfig(OracleGenericPoolConfig[AsyncConnection, AsyncConnectionPool]):
    """Async Oracle Pool Config"""


@dataclass
class OracleAsyncDatabaseConfig(AsyncDatabaseConfig[AsyncConnection, AsyncConnectionPool]):
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
                self.pool_config, exclude_empty=True, convert_nested=False, exclude={"pool_instance"}
            )
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

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
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[AsyncConnection, None]":
        """Create a connection instance.

        Yields:
            AsyncConnection: A connection instance.
        """
        db_pool = await self.provide_pool(*args, **kwargs)
        async with db_pool.acquire() as connection:  # pyright: ignore[reportUnknownMemberType]
            yield connection
