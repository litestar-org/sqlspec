"""SQLite database configuration with thread-local connections."""

import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._types import SqliteConnection
from sqlspec.adapters.sqlite.driver import SqliteCursor, SqliteDriver, sqlite_statement_config
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool
from sqlspec.config import SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.core.statement import StatementConfig


class SqliteConnectionParams(TypedDict, total=False):
    """SQLite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: "NotRequired[Optional[str]]"
    check_same_thread: NotRequired[bool]
    factory: "NotRequired[Optional[type[SqliteConnection]]]"
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


__all__ = ("SqliteConfig", "SqliteConnectionParams")


class SqliteConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with thread-local connections."""

    driver_type: "ClassVar[type[SqliteDriver]]" = SqliteDriver
    connection_type: "ClassVar[type[SqliteConnection]]" = SqliteConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[SqliteConnectionParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[SqliteConnectionPool]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            pool_config: Configuration parameters including connection settings
            pool_instance: Pre-created pool instance
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
        """
        if pool_config is None:
            pool_config = {}
        if "database" not in pool_config or pool_config["database"] == ":memory:":
            pool_config["database"] = f"file:memory_{uuid.uuid4().hex}?mode=memory&cache=private"
            pool_config["uri"] = True

        super().__init__(
            pool_instance=pool_instance,
            pool_config=cast("dict[str, Any]", pool_config),
            migration_config=migration_config,
            statement_config=statement_config or sqlite_statement_config,
            driver_features=driver_features or {},
        )

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""

        excluded_keys = {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        return {k: v for k, v in self.pool_config.items() if v is not None and k not in excluded_keys}

    def _create_pool(self) -> SqliteConnectionPool:
        """Create connection pool from configuration."""
        config_dict = self._get_connection_config_dict()

        return SqliteConnectionPool(connection_parameters=config_dict, **self.pool_config)

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> SqliteConnection:
        """Get a SQLite connection from the pool.

        Returns:
            SqliteConnection: A connection from the pool
        """
        pool = self.provide_pool()
        return pool.acquire()

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[SqliteConnection, None, None]":
        """Provide a SQLite connection context manager.

        Yields:
            SqliteConnection: A thread-local connection
        """
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(
        self, *args: "Any", statement_config: "Optional[StatementConfig]" = None, **kwargs: "Any"
    ) -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session.

        Yields:
            SqliteDriver: A driver instance with thread-local connection
        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection=connection, statement_config=statement_config or self.statement_config)

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for SQLite types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({"SqliteConnection": SqliteConnection, "SqliteCursor": SqliteCursor})
        return namespace
