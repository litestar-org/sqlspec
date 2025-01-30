from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from psycopg import Connection
from psycopg_pool import ConnectionPool

from sqlspec.adapters.psycopg.config._common import PsycoPgGenericPoolConfig
from sqlspec.base import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any


__all__ = (
    "PsycoPgSyncDatabaseConfig",
    "PsycoPgSyncPoolConfig",
)


@dataclass
class PsycoPgSyncPoolConfig(PsycoPgGenericPoolConfig[Connection, ConnectionPool]):
    """Sync Psycopg Pool Config"""


@dataclass
class PsycoPgSyncDatabaseConfig(SyncDatabaseConfig[Connection, ConnectionPool]):
    """Sync Psycopg database Configuration.
    This class provides the base configuration for Psycopg database connections, extending
    the generic database configuration with Psycopg-specific settings.([1](https://www.psycopg.org/psycopg3/docs/api/connections.html))

    The configuration supports all standard Psycopg connection parameters and can be used
    with both synchronous and asynchronous connections.([2](https://www.psycopg.org/psycopg3/docs/api/connections.html))
    """

    pool_config: PsycoPgSyncPoolConfig | None = None
    """Psycopg Pool configuration"""
    pool_instance: ConnectionPool | None = None
    """Optional pool to use"""

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Return the pool configuration as a dict."""
        if self.pool_config:
            return dataclass_to_dict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    def create_pool(self) -> ConnectionPool:
        """Create and return a connection pool."""
        if self.pool_instance is not None:
            return self.pool_instance

        if self.pool_config is None:
            msg = "One of 'pool_config' or 'pool_instance' must be provided."
            raise ImproperConfigurationError(msg)

        pool_config = self.pool_config_dict
        self.pool_instance = ConnectionPool(**pool_config)
        if self.pool_instance is None:  # pyright: ignore[reportUnnecessaryComparison]
            msg = "Could not configure the 'pool_instance'. Please check your configuration."  # type: ignore[unreachable]
            raise ImproperConfigurationError(msg)
        return self.pool_instance

    def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
        """Create and return a connection pool."""
        return self.create_pool()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[Connection, None, None]:
        """Create and provide a database connection."""
        pool = self.provide_pool(*args, **kwargs)
        with pool.connection() as connection:
            yield connection
