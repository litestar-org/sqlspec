from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from sqlspec.adapters.psycopg.config._common import (
    GenericPsycopgDatabaseConfig,
    GenericPsycopgPoolConfig,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.dataclass import simple_asdict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable
    from typing import Any


__all__ = (
    "PsycopgAsyncDatabaseConfig",
    "PsycopgAsyncPoolConfig",
)


@dataclass
class PsycopgAsyncPoolConfig(GenericPsycopgPoolConfig[AsyncConnectionPool, AsyncConnection]):
    """Async Psycopg Pool Config"""


@dataclass
class PsycopgAsyncDatabaseConfig(GenericPsycopgDatabaseConfig[AsyncConnectionPool, AsyncConnection]):
    """Async Psycopg database Configuration."""

    pool_config: PsycopgAsyncPoolConfig | None = None
    """Psycopg Pool configuration"""
    pool_instance: AsyncConnectionPool | None = None
    """Optional pool to use"""

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict."""
        if self.pool_config:
            return simple_asdict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    async def create_pool(self) -> AsyncConnectionPool:
        """Create and return a connection pool."""
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        pool_config = self.pool_config_dict
        self.pool_instance = AsyncConnectionPool(**pool_config)
        if self.pool_instance is None:
            msg = "Could not configure the 'pool_instance'. Please check your configuration."
            raise ImproperConfigurationError(msg)
        return self.pool_instance

    @asynccontextmanager
    async def lifespan(self, *args: Any, **kwargs: Any) -> AsyncGenerator[None, None]:
        """Manage the lifecycle of the connection pool."""
        pool = await self.create_pool()
        try:
            yield
        finally:
            await pool.close()

    def provide_pool(self, *args: Any, **kwargs: Any) -> Awaitable[AsyncConnectionPool]:
        """Create and return a connection pool."""
        return self.create_pool()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[AsyncConnection, None]:
        """Create and provide a database connection."""
        pool = await self.provide_pool(*args, **kwargs)
        async with pool.connection() as connection:
            yield connection
