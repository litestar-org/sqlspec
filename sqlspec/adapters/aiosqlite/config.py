"""Aiosqlite database configuration with optimized connection management."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict

import aiosqlite
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite.driver import AiosqliteCursor, AiosqliteDriver, aiosqlite_statement_config
from sqlspec.config import AsyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.core.statement import StatementConfig

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams", "AiosqliteConnectionPool")

logger = logging.getLogger(__name__)

# Core PRAGMAs for SQLite performance optimization
WAL_PRAGMA_SQL: Final[str] = "PRAGMA journal_mode = WAL"
FOREIGN_KEYS_SQL: Final[str] = "PRAGMA foreign_keys = ON"
SYNC_NORMAL_SQL: Final[str] = "PRAGMA synchronous = NORMAL"
BUSY_TIMEOUT_SQL: Final[str] = "PRAGMA busy_timeout = 5000"  # 5 seconds


class AiosqliteConnectionPool:
    """Connection pool for Aiosqlite using a single shared connection approach.

    Uses a single shared connection per database file since aiosqlite internally
    handles queuing and serialization of operations.
    """

    __slots__ = ("_closed", "_connection", "_connection_parameters", "_lock")

    def __init__(self, connection_parameters: "dict[str, Any]") -> None:
        """Initialize connection manager.

        Args:
            connection_parameters: SQLite connection parameters
        """
        self._connection: Optional[AiosqliteConnection] = None
        self._connection_parameters = connection_parameters
        self._lock = asyncio.Lock()
        self._closed = False

    async def _ensure_connection(self) -> "AiosqliteConnection":
        """Ensure we have a valid connection, creating one if needed."""
        async with self._lock:
            if self._connection is None or self._closed:
                self._connection = await aiosqlite.connect(**self._connection_parameters)

                await self._connection.execute(WAL_PRAGMA_SQL)
                await self._connection.execute(FOREIGN_KEYS_SQL)
                await self._connection.execute(SYNC_NORMAL_SQL)
                await self._connection.execute(BUSY_TIMEOUT_SQL)
                await self._connection.commit()

                self._closed = False
                logger.debug("Created new aiosqlite connection")

            return self._connection

    @asynccontextmanager
    async def get_connection(self) -> "AsyncGenerator[AiosqliteConnection, None]":
        """Get the shared connection.

        Yields:
            The shared Aiosqlite connection instance.
        """
        connection = await self._ensure_connection()
        yield connection

    async def close(self) -> None:
        """Close the shared connection."""
        async with self._lock:
            if self._connection is not None and not self._closed:
                await self._connection.close()
                self._connection = None
                self._closed = True
                logger.debug("Closed aiosqlite connection")

    def size(self) -> int:
        """Get connection count."""
        return 0 if self._closed or self._connection is None else 1

    def checked_out(self) -> int:
        """Get number of checked out connections."""
        return 0

    async def acquire(self) -> "AiosqliteConnection":
        """Get the shared connection directly.

        Returns:
            The shared connection instance.
        """
        return await self._ensure_connection()

    async def release(self, connection: "AiosqliteConnection") -> None:
        """No-op release for compatibility.

        Args:
            connection: Connection to release (ignored)
        """
        _ = connection


class AiosqliteConnectionParams(TypedDict, total=False):
    """aiosqlite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


class AiosqliteConfig(AsyncDatabaseConfig):
    """Database configuration for AioSQLite engine."""

    driver_type: ClassVar[type[AiosqliteDriver]] = AiosqliteDriver
    cursor_type: ClassVar[type[AiosqliteCursor]] = AiosqliteCursor

    def __init__(
        self,
        *,
        pool_instance: "Optional[AiosqliteConnectionPool]" = None,
        pool_config: "Optional[dict[str, Any]]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize AioSQLite configuration.

        Args:
            pool_instance: Optional pre-configured connection pool instance.
            pool_config: Optional pool configuration dict (AiosqliteConnectionParams).
            migration_config: Optional migration configuration.
            statement_config: Optional statement configuration.
            **kwargs: Additional connection parameters.
        """
        connection_params = {}
        if pool_config:
            connection_params.update(pool_config)
        connection_params.update(kwargs)

        super().__init__(
            pool_config=connection_params,
            pool_instance=pool_instance,
            migration_config=migration_config or {},
            statement_config=statement_config or aiosqlite_statement_config,
        )

        self._connection_parameters = self._parse_connection_parameters(connection_params)

        if pool_instance is None:
            self.pool_instance: AiosqliteConnectionPool = AiosqliteConnectionPool(self._connection_parameters)

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

        for pool_param in ["pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds"]:
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
