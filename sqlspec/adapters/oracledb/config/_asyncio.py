from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from oracledb import create_pool_async as oracledb_create_pool
from oracledb.connection import AsyncConnection
from oracledb.pool import AsyncConnectionPool

from sqlspec.adapters.oracledb.config._common import (
    OracleGenericDatabaseConfig,
    OracleGenericPoolConfig,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable
    from typing import Any

__all__ = (
    "OracleAsyncDatabaseConfig",
    "OracleAsyncPoolConfig",
)


@dataclass
class OracleAsyncPoolConfig(OracleGenericPoolConfig[AsyncConnectionPool, AsyncConnection]):
    """Async Oracle Pool Config"""


@dataclass
class OracleAsyncDatabaseConfig(OracleGenericDatabaseConfig[AsyncConnectionPool, AsyncConnection]):
    """Async Oracle database Configuration."""

    pool_config: OracleAsyncPoolConfig | None = None
    """Oracle Pool configuration"""
    pool_instance: AsyncConnectionPool | None = None
    """Optional pool to use.

    If set, the plugin will use the provided pool rather than instantiate one.
    """

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the Asyncpg :func:`create_pool <oracledb.pool.create_pool>`
            function.
        """
        if self.pool_config is not None:
            return dataclass_to_dict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> AsyncConnectionPool:
        """Return a pool. If none exists yet, create one.

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
        if self.pool_instance is None:
            msg = "Could not configure the 'pool_instance'. Please check your configuration."
            raise ImproperConfigurationError(msg)
        return self.pool_instance

    @asynccontextmanager
    async def lifespan(self, *args: Any, **kwargs: Any) -> AsyncGenerator[None, None]:
        db_pool = await self.create_pool()
        try:
            yield
        finally:
            await db_pool.close(force=True)

    def provide_pool(self, *args: Any, **kwargs: Any) -> Awaitable[AsyncConnectionPool]:
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return self.create_pool()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncConnection, None]:
        """Create a connection instance.

        Returns:
            A connection instance.
        """
        db_pool = await self.provide_pool(*args, **kwargs)
        async with db_pool.acquire() as connection:
            yield connection
