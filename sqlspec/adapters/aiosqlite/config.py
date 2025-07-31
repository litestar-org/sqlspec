"""Aiosqlite database configuration with connection pooling."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict, Union

import aiosqlite
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
from sqlspec.adapters.aiosqlite.driver import AiosqliteCursor, AiosqliteDriver
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.statement.sql import StatementConfig

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams", "AiosqliteConnectionPool", "AiosqlitePoolParams")

logger = logging.getLogger(__name__)

# Performance constants for Aiosqlite pooling optimization
DEFAULT_MIN_POOL: Final[int] = 5
DEFAULT_MAX_POOL: Final[int] = 20
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 3600  # 1 hour
WAL_PRAGMA_SQL: Final[str] = "PRAGMA journal_mode = WAL"
FOREIGN_KEYS_SQL: Final[str] = "PRAGMA foreign_keys = ON"
SYNC_NORMAL_SQL: Final[str] = "PRAGMA synchronous = NORMAL"
CACHE_SIZE_SQL: Final[str] = "PRAGMA cache_size = -64000"  # 64MB cache


class AiosqliteConnectionPool:
    """Async connection pool for Aiosqlite with performance optimizations.

    Implements async connection pooling to achieve performance improvements
    similar to the sync SQLite pool.
    """

    __slots__ = (
        "_checked_out",
        "_connection_params",
        "_connection_times",
        "_created_connections",
        "_lock",
        "_max_pool",
        "_min_pool",
        "_pool",
        "_recycle",
        "_semaphore",
        "_timeout",
    )

    def __init__(
        self,
        connection_params: "dict[str, Any]",
        min_pool: int = DEFAULT_MIN_POOL,
        max_pool: int = DEFAULT_MAX_POOL,
        timeout: float = POOL_TIMEOUT,
        recycle: int = POOL_RECYCLE,
    ) -> None:
        self._pool: "asyncio.Queue[AiosqliteConnection]" = asyncio.Queue(maxsize=max_pool)  # noqa: UP037
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max_pool)
        self._min_pool = min_pool
        self._max_pool = max_pool
        self._timeout = timeout
        self._recycle = recycle
        self._connection_params = connection_params
        self._created_connections = 0
        self._checked_out = 0
        self._connection_times: "dict[int, float]" = {}  # noqa: UP037

    async def _create_connection(self) -> "AiosqliteConnection":
        """Create a new Aiosqlite connection with performance optimizations."""
        connection = await aiosqlite.connect(**self._connection_params)

        # Apply SQLite performance optimizations
        await connection.execute(WAL_PRAGMA_SQL)
        await connection.execute(FOREIGN_KEYS_SQL)
        await connection.execute(SYNC_NORMAL_SQL)
        await connection.execute(CACHE_SIZE_SQL)
        await connection.execute("PRAGMA temp_store = MEMORY")
        await connection.execute("PRAGMA mmap_size = 268435456")
        await connection.commit()

        # Track creation time
        conn_id = id(connection)
        async with self._lock:
            self._created_connections += 1
            self._connection_times[conn_id] = time.time()

        return connection

    async def initialize(self) -> None:
        """Pre-populate the pool with minimum connections."""
        # Skip pre-population for now to avoid initialization hangs
        # connections_to_create = min(self._min_pool, self._max_pool)
        # for _ in range(connections_to_create):
        #     if self._pool.full():
        #         break
        #     conn = await self._create_connection()
        #     await self._pool.put(conn)

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
        acquired = False

        try:
            acquired = await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout)
            try:
                connection = self._pool.get_nowait()
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

            async with self._lock:
                self._checked_out += 1

            yield connection

        except asyncio.TimeoutError:
            msg = f"Connection pool timeout after {self._timeout} seconds"
            raise RuntimeError(msg) from None

        finally:
            if connection is not None:
                async with self._lock:
                    self._checked_out -= 1

                # Validate connection before returning to pool
                if await self._is_connection_alive(connection):
                    try:
                        self._pool.put_nowait(connection)
                    except asyncio.QueueFull:
                        with suppress(Exception):
                            await connection.close()
                else:
                    # Connection is dead, close it
                    with suppress(Exception):
                        await connection.close()

            if acquired:
                self._semaphore.release()

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
        """Get number of checked out connections."""
        return self._checked_out


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
            pool_config: Pool configuration parameters (TypedDict or dict) including connection params
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration
            pool_instance: Pre-created pool instance

        """
        # Convert to dict and extract configuration
        pool_config_dict: dict[str, Any] = dict(pool_config) if pool_config else {}

        # Set defaults for connection config
        if not any(key in pool_config_dict for key in ["database"]):
            pool_config_dict["database"] = ":memory:"

        # Remaining config is connection configuration
        self.connection_config = pool_config_dict
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        # Note: aiosqlite doesn't properly support shared memory databases with cache=shared
        # so we leave memory databases as :memory: for compatibility

        super().__init__(pool_instance=pool_instance, migration_config=migration_config)

        # Use provided StatementConfig or None to let driver set its own defaults
        self.statement_config = statement_config

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""
        config: "dict[str, Any]" = dict(self.connection_config)  # noqa: UP037
        config.pop("extra", None)
        return {k: v for k, v in config.items() if v is not None}

    async def _create_pool(self) -> "AiosqliteConnectionPool":
        """Create the Aiosqlite connection pool."""
        # Get connection parameters and extract pool configuration
        config_dict = dict(self.connection_config)

        # Extract pool parameters with unified names
        min_pool = config_dict.pop("pool_min_size", DEFAULT_MIN_POOL)
        max_pool = config_dict.pop("pool_max_size", DEFAULT_MAX_POOL)
        timeout = config_dict.pop("pool_timeout", POOL_TIMEOUT)
        recycle = config_dict.pop("pool_recycle_seconds", POOL_RECYCLE)

        # Remaining is connection configuration
        connection_params = {k: v for k, v in config_dict.items() if v is not None}
        connection_params.pop("extra", None)

        pool = AiosqliteConnectionPool(
            connection_params=connection_params, min_pool=min_pool, max_pool=max_pool, timeout=timeout, recycle=recycle
        )
        await pool.initialize()
        return pool

    def _is_memory_database(self, database: str) -> bool:
        """Check if the database is an in-memory database.

        Args:
            database: Database path or connection string

        Returns:
            True if this is an in-memory database
        """
        if not database:
            return True

        # Standard :memory: database
        if database == ":memory:":
            return True

        # Check for URI-style memory database but NOT shared cache
        return "file::memory:" in database and "cache=shared" not in database

    def _convert_to_shared_memory(self) -> None:
        """Convert in-memory database to shared memory for connection pooling.

        Automatically converts :memory: and file::memory: databases to
        file::memory:?cache=shared format to enable safe connection pooling.
        """
        database = self.connection_config.get("database", ":memory:")

        if database in {":memory:", ""}:
            self.connection_config["database"] = "file::memory:?cache=shared"
            self.connection_config["uri"] = True
        elif "file::memory:" in database and "cache=shared" not in database:
            # Add cache=shared to existing file::memory: URI
            separator = "&" if "?" in database else "?"
            self.connection_config["database"] = f"{database}{separator}cache=shared"
            self.connection_config["uri"] = True

    async def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            await self.pool_instance.close()

    async def create_connection(self) -> "AiosqliteConnection":
        """Create a single async connection (bypasses pool).

        Returns:
            An Aiosqlite connection instance.
        """
        try:
            config = self._get_connection_config_dict()
            return await aiosqlite.connect(**config)
        except Exception as e:
            msg = f"Could not configure the Aiosqlite connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

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
            # Use provided statement_config or instance default
            config = statement_config if statement_config is not None else self.statement_config
            yield self.driver_type(connection=connection, statement_config=config)

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
