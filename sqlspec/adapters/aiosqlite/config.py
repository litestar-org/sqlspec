"""Aiosqlite database configuration with optimized connection management."""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict

from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite.driver import AiosqliteCursor, AiosqliteDriver, aiosqlite_statement_config
from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool
from sqlspec.config import AsyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.core.statement import StatementConfig

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams", "AiosqlitePoolParams")

logger = logging.getLogger(__name__)

# Core PRAGMAs for SQLite performance optimization
WAL_PRAGMA_SQL: Final[str] = "PRAGMA journal_mode = WAL"
FOREIGN_KEYS_SQL: Final[str] = "PRAGMA foreign_keys = ON"
SYNC_NORMAL_SQL: Final[str] = "PRAGMA synchronous = NORMAL"
BUSY_TIMEOUT_SQL: Final[str] = "PRAGMA busy_timeout = 5000"  # 5 seconds


class AiosqliteConnectionParams(TypedDict, total=False):
    """aiosqlite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


class AiosqlitePoolParams(AiosqliteConnectionParams, total=False):
    """Pool parameters for aiosqlite, extending connection parameters."""

    pool_size: NotRequired[int]
    acquisition_timeout: NotRequired[float]
    idle_timeout: NotRequired[float]
    operation_timeout: NotRequired[float]


class AiosqliteConfig(AsyncDatabaseConfig):
    """Database configuration for AioSQLite engine."""

    driver_type: ClassVar[type[AiosqliteDriver]] = AiosqliteDriver
    cursor_type: ClassVar[type[AiosqliteCursor]] = AiosqliteCursor

    def __init__(
        self,
        *,
        pool_instance: "Optional[AiosqliteConnectionPool]" = None,
        pool_config: "Optional[AiosqlitePoolParams]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize AioSQLite configuration.

        Args:
            pool_instance: Optional pre-configured connection pool instance.
            pool_config: Optional pool configuration (AiosqlitePoolParams).
            migration_config: Optional migration configuration.
            statement_config: Optional statement configuration.
            **kwargs: Additional connection parameters.
        """
        pool_params: dict[str, Any] = dict(pool_config) if pool_config else {}
        pool_params.update(kwargs)

        # Extract pool-specific parameters
        pool_size = pool_params.pop("pool_size", 5)
        acquisition_timeout = pool_params.pop("acquisition_timeout", 30.0)
        idle_timeout = pool_params.pop("idle_timeout", 24 * 60 * 60)
        operation_timeout = pool_params.pop("operation_timeout", 10.0)

        # Make :memory: databases shared for multi-connection access
        if pool_params.get("database") == ":memory:":
            pool_params["database"] = "file::memory:?cache=shared"
            pool_params["uri"] = True

        super().__init__(
            pool_config=pool_params,
            pool_instance=pool_instance,
            migration_config=migration_config or {},
            statement_config=statement_config or aiosqlite_statement_config,
        )

        self._connection_parameters = self._parse_connection_parameters(pool_params)

        if pool_instance is None:
            self.pool_instance: AiosqliteConnectionPool = AiosqliteConnectionPool(
                connection_parameters=self._connection_parameters,
                pool_size=pool_size,
                acquisition_timeout=acquisition_timeout,
                idle_timeout=idle_timeout,
                operation_timeout=operation_timeout,
            )

    def _parse_connection_parameters(self, params: "dict[str, Any]") -> "dict[str, Any]":
        """Parse connection parameters for AioSQLite.

        Args:
            params: Connection parameters dict.

        Returns:
            Processed connection parameters dict.
        """
        result = params.copy()

        if "database" not in result:
            # Default to in-memory database
            result["database"] = ":memory:"

        # Convert regular :memory: to shared memory for multi-connection access
        if result.get("database") == ":memory:":
            result["database"] = "file::memory:?cache=shared"
            result["uri"] = True

        for pool_param in [
            "pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds",
            "pool_size", "acquisition_timeout", "idle_timeout", "operation_timeout"
        ]:
            result.pop(pool_param, None)

        return result

    @asynccontextmanager
    async def provide_connection(self) -> "AsyncGenerator[AiosqliteConnection, None]":
        """Provide a database connection.

        Yields:
            AiosqliteConnection: Database connection instance.
        """
        async with self.pool_instance.get_connection() as connection:
            yield connection

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AsyncGenerator[AiosqliteDriver, None]":
        """Provide an async database session.

        Args:
            *args: Additional positional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            AiosqliteDriver: Database session instance.
        """
        _ = args, kwargs
        effective_statement_config = statement_config or self.statement_config
        async with self.pool_instance.get_connection() as connection:
            session = self.driver_type(connection, statement_config=effective_statement_config)
            try:
                yield session
            finally:
                pass

    async def close(self) -> None:
        """Close the connection manager."""
        if self.pool_instance:
            await self.pool_instance.close()

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration dictionary.

        Returns:
            Connection parameters for creating connections.
        """
        return self._connection_parameters.copy()

    async def _create_pool(self) -> "AiosqliteConnectionPool":
        """Create the connection manager instance.

        Returns:
            AiosqliteConnectionPool: The connection manager instance.
        """
        if self.pool_instance is None:
            self.pool_instance = AiosqliteConnectionPool(self._connection_parameters)
        return self.pool_instance

    async def _close_pool(self) -> None:
        """Close the connection manager."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def close_pool(self) -> None:
        """Close the connection pool (delegates to _close_pool)."""
        await self._close_pool()
