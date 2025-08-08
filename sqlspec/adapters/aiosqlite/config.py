"""Aiosqlite database configuration with connection pooling."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict, Union

import aiosqlite
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
from sqlspec.adapters.aiosqlite.driver import AiosqliteCursor, AiosqliteDriver, aiosqlite_statement_config
from sqlspec.config import AsyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.core.statement import StatementConfig

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams", "AiosqliteConnectionPool", "AiosqlitePoolParams")

logger = logging.getLogger(__name__)

DEFAULT_MIN_POOL: Final[int] = 1
DEFAULT_MAX_POOL: Final[int] = 5
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 86400
# Core PRAGMAs only - simplified from 6 to 3 based on consensus
WAL_PRAGMA_SQL: Final[str] = "PRAGMA journal_mode = WAL"
FOREIGN_KEYS_SQL: Final[str] = "PRAGMA foreign_keys = ON"
SYNC_NORMAL_SQL: Final[str] = "PRAGMA synchronous = NORMAL"


class AiosqliteConnectionPool:
    """Simplified async connection pool for Aiosqlite.

    Uses a basic asyncio.Queue for connection pooling with minimal complexity.
    Focuses on the three core PRAGMAs for performance optimization.
    """

    __slots__ = ("_connection_parameters", "_connection_times", "_lock", "_max_pool", "_pool", "_recycle", "_timeout")

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        pool_min_size: int = DEFAULT_MIN_POOL,
        pool_max_size: int = DEFAULT_MAX_POOL,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle_seconds: int = POOL_RECYCLE,
    ) -> None:
        """Initialize simplified async connection pool.

        Args:
            connection_parameters: SQLite connection parameters
            pool_min_size: Minimum pool size (ignored, kept for compatibility)
            pool_max_size: Maximum pool size
            pool_timeout: Timeout for acquiring connections
            pool_recycle_seconds: Connection recycle time
        """
        self._pool: asyncio.Queue[AiosqliteConnection] = asyncio.Queue(maxsize=pool_max_size)
        self._lock = asyncio.Lock()
        self._max_pool = pool_max_size
        self._timeout = pool_timeout
        self._recycle = pool_recycle_seconds
        self._connection_parameters = connection_parameters
        self._connection_times: dict[int, float] = {}

    async def _create_connection(self) -> "AiosqliteConnection":
        """Create a new Aiosqlite connection with core optimizations."""
        connection = await aiosqlite.connect(**self._connection_parameters)

        # Apply only the 3 core PRAGMAs based on consensus
        await connection.execute(WAL_PRAGMA_SQL)
        await connection.execute(FOREIGN_KEYS_SQL)
        await connection.execute(SYNC_NORMAL_SQL)
        await connection.commit()

        # Track connection creation time for recycling
        conn_id = id(connection)
        async with self._lock:
            self._connection_times[conn_id] = time.time()

        return connection

    def _should_recycle(self, connection: "AiosqliteConnection") -> bool:
        """Check if connection should be recycled based on age."""
        conn_id = id(connection)
        created_at = self._connection_times.get(conn_id)
        if created_at is None:
            return True
        return (time.time() - created_at) > self._recycle

    async def _is_connection_alive(self, connection: "AiosqliteConnection") -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            await connection.execute("SELECT 1")
        except Exception:
            return False
        else:
            return True

    @asynccontextmanager
    async def get_connection(self) -> "AsyncGenerator[AiosqliteConnection, None]":
        """Get a connection from the pool.

        Yields:
            An Aiosqlite connection instance.
        """
        connection = None

        try:
            # Try to get existing connection from pool
            try:
                connection = self._pool.get_nowait()
                # Check if connection needs recycling
                if self._should_recycle(connection) or not await self._is_connection_alive(connection):
                    conn_id = id(connection)
                    with suppress(Exception):
                        await connection.close()
                    async with self._lock:
                        self._connection_times.pop(conn_id, None)
                    connection = None
            except asyncio.QueueEmpty:
                connection = None

            # Create new connection if needed
            if connection is None:
                connection = await self._create_connection()

            yield connection

        finally:
            if connection is not None:
                # Validate connection before returning to pool
                if await self._is_connection_alive(connection):
                    try:
                        self._pool.put_nowait(connection)
                    except asyncio.QueueFull:
                        # Pool is full, close the connection
                        with suppress(Exception):
                            await connection.close()
                        conn_id = id(connection)
                        async with self._lock:
                            self._connection_times.pop(conn_id, None)
                else:
                    # Connection is dead, close it
                    with suppress(Exception):
                        await connection.close()
                    conn_id = id(connection)
                    async with self._lock:
                        self._connection_times.pop(conn_id, None)

    async def close(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            connection = self._pool.get_nowait()
            with suppress(Exception):
                await connection.close()

        async with self._lock:
            self._connection_times.clear()

    def size(self) -> int:
        """Get current pool size."""
        return self._pool.qsize()

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0 for simplified pool)."""
        return 0

    async def acquire(self) -> "AiosqliteConnection":
        """Acquire a connection from the pool without a context manager.

        This method gets a connection from the pool that must be manually
        returned using the release() method.

        Returns:
            AiosqliteConnection: A connection from the pool
        """
        connection = None

        try:
            # Try to get existing connection from pool with timeout
            try:
                connection = await asyncio.wait_for(self._pool.get(), timeout=self._timeout)
                # Check if connection needs recycling
                if self._should_recycle(connection) or not await self._is_connection_alive(connection):
                    conn_id = id(connection)
                    with suppress(Exception):
                        await connection.close()
                    async with self._lock:
                        self._connection_times.pop(conn_id, None)
                    connection = None
            except asyncio.QueueEmpty:
                connection = None
            except asyncio.TimeoutError:
                connection = None

            # Create new connection if needed
            if connection is None:
                connection = await self._create_connection()

        except Exception:
            # If we got a connection but failed somewhere, return it to pool
            if connection is not None:
                with suppress(asyncio.QueueFull):
                    self._pool.put_nowait(connection)
            raise

        return connection

    async def release(self, connection: "AiosqliteConnection") -> None:
        """Release a connection back to the pool.

        Args:
            connection: The connection to release
        """
        if connection is not None:
            # Validate connection before returning to pool
            if await self._is_connection_alive(connection):
                try:
                    self._pool.put_nowait(connection)
                except asyncio.QueueFull:
                    # Pool is full, close the connection
                    with suppress(Exception):
                        await connection.close()
                    conn_id = id(connection)
                    async with self._lock:
                        self._connection_times.pop(conn_id, None)
            else:
                # Connection is dead, close it
                with suppress(Exception):
                    await connection.close()
                conn_id = id(connection)
                async with self._lock:
                    self._connection_times.pop(conn_id, None)


class AiosqliteConnectionParams(TypedDict, total=False):
    """aiosqlite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
    extra: "NotRequired[dict[str, Any]]"


class AiosqlitePoolParams(AiosqliteConnectionParams, total=False):
    """Complete pool configuration for AioSQLite adapter.

    Combines standardized pool parameters with AioSQLite-specific connection parameters.
    """

    # Standardized pool parameters (consistent across ALL adapters)
    pool_min_size: NotRequired[int]
    pool_max_size: NotRequired[int]
    pool_timeout: NotRequired[float]
    pool_recycle_seconds: NotRequired[int]


class AiosqliteConfig(AsyncDatabaseConfig[AiosqliteConnection, AiosqliteConnectionPool, AiosqliteDriver]):
    """Configuration for Aiosqlite database connections with connection pooling.

    Implements async connection pooling for improved performance.
    """

    driver_type: "ClassVar[type[AiosqliteDriver]]" = AiosqliteDriver
    connection_type: "ClassVar[type[AiosqliteConnection]]" = AiosqliteConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[AiosqlitePoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[AiosqliteConnectionPool]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
    ) -> None:
        """Initialize Aiosqlite configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict) including connection parameters
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration
            pool_instance: Pre-created pool instance

        """
        if pool_config is None:
            pool_config = {}
        if "database" not in pool_config or pool_config["database"] == ":memory:":
            pool_config["database"] = "file::memory:?cache=shared"

        super().__init__(
            pool_config=dict(pool_config),
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or aiosqlite_statement_config,
            driver_features={},
        )

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""
        return {
            k: v
            for k, v in self.pool_config.items()
            if v is not None
            and k not in {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        }

    def _get_pool_config_dict(self) -> "dict[str, Any]":
        """Get pool configuration as plain dict for pool creation."""
        return {
            k: v
            for k, v in self.pool_config.items()
            if v is not None and k in {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds"}
        }

    async def _create_pool(self) -> "AiosqliteConnectionPool":
        """Create the Aiosqlite connection pool."""
        config_dict = self._get_connection_config_dict()
        pool_config = self._get_pool_config_dict()
        return AiosqliteConnectionPool(connection_parameters=config_dict, **pool_config)

    async def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> "AiosqliteConnection":
        """Get an Aiosqlite connection from the pool.

        This method ensures the pool is created and returns a connection
        from the pool. The connection is checked out from the pool and must
        be properly managed by the caller.

        Returns:
            AiosqliteConnection: A connection from the pool

        Note:
            For automatic connection management, prefer using provide_connection()
            or provide_session() which handle returning connections to the pool.
            The caller is responsible for returning the connection to the pool
            using pool.release(connection) when done.
        """
        pool = await self.provide_pool()
        return await pool.acquire()

    @asynccontextmanager
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[AiosqliteConnection, None]":
        """Provide a pooled async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An Aiosqlite connection instance.
        """
        pool = await self.provide_pool()
        async with pool.get_connection() as connection:
            yield connection

    @asynccontextmanager
    async def provide_session(
        self, *args: "Any", statement_config: "Optional[StatementConfig]" = None, **kwargs: "Any"
    ) -> "AsyncGenerator[AiosqliteDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            An AiosqliteDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            # Use shared config or user-provided config or instance default
            final_statement_config = statement_config or self.statement_config
            yield self.driver_type(connection=connection, statement_config=final_statement_config)

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for Aiosqlite types.

        This provides all Aiosqlite-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({"AiosqliteConnection": AiosqliteConnection, "AiosqliteCursor": AiosqliteCursor})
        return namespace
