from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from oracledb import create_pool as oracledb_create_pool
from oracledb.connection import Connection
from oracledb.pool import ConnectionPool

from sqlspec.adapters.oracledb.config._common import (
    GenericOracleDatabaseConfig,
    GenericOraclePoolConfig,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.dataclass import simple_asdict

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any

__all__ = (
    "SyncOracleDatabaseConfig",
    "SyncOraclePoolConfig",
)


@dataclass
class SyncOraclePoolConfig(GenericOraclePoolConfig[ConnectionPool, Connection]):
    """Sync Oracle Pool Config"""


@dataclass
class SyncOracleDatabaseConfig(GenericOracleDatabaseConfig[ConnectionPool, Connection]):
    """Oracle database Configuration."""

    pool_config: SyncOraclePoolConfig | None = None
    """Oracle Pool configuration"""
    pool_instance: ConnectionPool | None = None
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
        if self.pool_config:
            return simple_asdict(self.pool_config, exclude_empty=True, convert_nested=False)
        msg = "'pool_config' methods can not be used when a 'pool_instance' is provided."
        raise ImproperConfigurationError(msg)

    def create_pool(self) -> ConnectionPool:
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

    def lifespan(self, *args: Any, **kwargs) -> Generator[None, None, None]:
        db_pool = self.create_pool()
        try:
            yield
        finally:
            db_pool.close()

    def provide_pool(self, *args: Any, **kwargs) -> ConnectionPool:
        """Create a pool instance.

        Returns:
            A Pool instance.
        """
        return self.create_pool()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[Connection, None, None]:
        """Create a connection instance.

        Returns:
            A connection instance.
        """
        db_pool = self.provide_pool(*args, **kwargs)
        with db_pool.acquire() as connection:
            yield connection