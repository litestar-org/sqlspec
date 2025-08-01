"""SQLite database configuration with QueuePool-like connection pooling."""

import sqlite3
import threading
import time
from contextlib import contextmanager, suppress
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict, Union, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._types import SqliteConnection
from sqlspec.adapters.sqlite.driver import SqliteCursor, SqliteDriver, sqlite_statement_config
from sqlspec.config import SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.statement.sql import StatementConfig


DEFAULT_MIN_POOL: Final[int] = 5
DEFAULT_MAX_POOL: Final[int] = 20
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 3600  # 1 hour
WAL_PRAGMA_SQL: Final[str] = "PRAGMA journal_mode = WAL"
FOREIGN_KEYS_SQL: Final[str] = "PRAGMA foreign_keys = ON"
SYNC_NORMAL_SQL: Final[str] = "PRAGMA synchronous = NORMAL"
CACHE_SIZE_SQL: Final[str] = "PRAGMA cache_size = -64000"  # 64MB cache


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
    extra: "NotRequired[dict[str, Any]]"


class SqlitePoolParams(SqliteConnectionParams, total=False):
    """Complete pool configuration for SQLite adapter.

    Combines standardized pool parameters with SQLite-specific connection parameters.
    """

    # Standardized pool parameters (consistent across ALL adapters)
    pool_min_size: NotRequired[int]
    pool_max_size: NotRequired[int]
    pool_timeout: NotRequired[float]
    pool_recycle_seconds: NotRequired[int]


class SqliteDriverFeatures(TypedDict, total=False):
    json_serializer: str


__all__ = ("SqliteConfig", "SqliteConnectionParams", "SqliteConnectionPool", "SqlitePoolParams", "sqlite3")


class SqliteConnectionPool:
    """QueuePool-like connection pool for SQLite with performance optimizations.

    Implements connection pooling similar to SQLAlchemy's QueuePool to achieve
    the 4.5x performance improvement (2k → 9k TPS) mentioned in benchmarks.
    """

    __slots__ = (
        "_checked_out",
        "_connection_params",
        "_connection_times",
        "_created_connections",
        "_lock",
        "_max_overflow",
        "_max_pool_size",
        "_min_pool_size",
        "_pool",
        "_recycle",
        "_timeout",
    )

    def __init__(
        self,
        connection_params: "dict[str, Any]",
        pool_min_size: int = DEFAULT_MIN_POOL,
        pool_max_size: int = DEFAULT_MAX_POOL,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle_seconds: int = POOL_RECYCLE,
    ) -> None:
        """Initialize the connection pool."""
        self._pool: "Queue[SqliteConnection]" = Queue(maxsize=pool_max_size)  # noqa: UP037
        self._lock = threading.RLock()
        self._min_pool_size = pool_min_size
        self._max_pool_size = pool_max_size
        self._timeout = pool_timeout
        self._recycle = pool_recycle_seconds
        self._connection_params = connection_params
        self._created_connections = 0
        self._checked_out = 0
        self._connection_times: "dict[int, float]" = {}  # noqa: UP037

        # Pre-populate core pool
        try:
            for _ in range(pool_min_size):
                conn = self._create_connection()
                self._pool.put_nowait(conn)
        except Full:
            pass  # Pool is full, stop creating connections

    def _create_connection(self) -> SqliteConnection:
        """Create a new SQLite connection with performance optimizations."""
        connection = sqlite3.connect(**self._connection_params)
        connection.row_factory = sqlite3.Row

        # Apply SQLAlchemy-style performance optimizations
        connection.execute(WAL_PRAGMA_SQL)  # WAL mode for concurrent reads
        connection.execute(FOREIGN_KEYS_SQL)  # Enable foreign keys
        connection.execute(SYNC_NORMAL_SQL)  # Faster than FULL sync
        connection.execute(CACHE_SIZE_SQL)  # 64MB cache
        connection.execute("PRAGMA temp_store = MEMORY")  # In-memory temp tables
        connection.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap

        # Track creation time for recycling using connection id
        conn_id = id(connection)
        with self._lock:
            self._created_connections += 1
            self._connection_times[conn_id] = time.time()

        return connection  # type: ignore[no-any-return]

    def _should_recycle(self, connection: SqliteConnection) -> bool:
        """Check if connection should be recycled based on age."""
        conn_id = id(connection)
        created_at = self._connection_times.get(conn_id)
        if created_at is None:
            return True  # Recycle if no creation time tracked

        return (time.time() - created_at) > self._recycle

    def _is_connection_alive(self, connection: SqliteConnection) -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            cursor = connection.execute("SELECT 1")
            cursor.close()
        except Exception:
            return False
        else:
            return True

    @contextmanager
    def get_connection(self) -> "Generator[SqliteConnection, None, None]":
        """Get a connection from the pool with automatic return.

        Yields:
            SqliteConnection: A connection from the pool.
        """
        connection = None
        try:
            # Try to get existing connection
            try:
                connection = self._pool.get(timeout=self._timeout)

                # Check if connection should be recycled
                if self._should_recycle(connection):
                    conn_id = id(connection)
                    with suppress(Exception):
                        connection.close()
                    # Clean up connection time tracking
                    with self._lock:
                        self._connection_times.pop(conn_id, None)
                    connection = None

            except Empty:
                # Pool is empty, check if we can create new connection
                with self._lock:
                    if self._checked_out < self._max_pool_size:
                        connection = None  # Will create new one below
                    else:
                        try:
                            connection = self._pool.get(timeout=self._timeout)
                        except Empty:
                            msg = f"Connection pool limit of {self._max_pool_size} reached, timeout {self._timeout}"
                            raise RuntimeError(msg) from None

            # Create new connection if needed
            if connection is None:
                connection = self._create_connection()

            with self._lock:
                self._checked_out += 1

            yield connection

        finally:
            if connection is not None:
                # Return connection to pool
                with self._lock:
                    self._checked_out -= 1

                # Validate connection before returning to pool
                if self._is_connection_alive(connection):
                    try:
                        self._pool.put_nowait(connection)
                    except Full:
                        with suppress(Exception):
                            connection.close()
                else:
                    # Connection is dead, close it
                    with suppress(Exception):
                        connection.close()

    def close(self) -> None:
        """Close all connections in the pool."""
        try:
            while True:
                connection = self._pool.get_nowait()
                with suppress(Exception):
                    connection.close()
        except Empty:
            pass  # Pool is empty

        # Clear connection time tracking
        with self._lock:
            self._connection_times.clear()

    def size(self) -> int:
        """Get current pool size."""
        return self._pool.qsize()

    def checked_out(self) -> int:
        """Get number of checked out connections."""
        return self._checked_out

    def acquire(self) -> SqliteConnection:
        """Acquire a connection from the pool without a context manager.

        This method gets a connection from the pool that must be manually
        returned using the release() method.

        Returns:
            SqliteConnection: A connection from the pool

        Raises:
            RuntimeError: If pool limit is reached and timeout expires
        """
        connection = None
        try:
            # Try to get existing connection from pool
            try:
                connection = self._pool.get(timeout=self._timeout)

                # Check if connection should be recycled based on age
                if self._should_recycle(connection) or not self._is_connection_alive(connection):
                    conn_id = id(connection)
                    with suppress(Exception):
                        connection.close()
                    with self._lock:
                        self._connection_times.pop(conn_id, None)
                    connection = None

            except Empty:
                # Pool is empty, check if we can create new connection
                with self._lock:
                    total_connections = self._checked_out + self._pool.qsize()
                    if total_connections < self._max_pool_size:
                        connection = None  # Will create new one below
                    else:
                        # Wait for a connection to become available
                        try:
                            connection = self._pool.get(timeout=self._timeout)
                        except Empty:
                            msg = f"Pool limit of {self._max_pool_size} connections reached"
                            raise RuntimeError(msg) from None

            # Create new connection if needed
            if connection is None:
                connection = self._create_connection()

            with self._lock:
                self._checked_out += 1

        except Exception:
            # If we got a connection but failed somewhere, return it to pool
            if connection is not None:
                with suppress(Full):
                    self._pool.put_nowait(connection)
            raise
        return connection

    def release(self, connection: SqliteConnection) -> None:
        """Return a connection to the pool.

        Args:
            connection: The connection to return to the pool
        """
        if connection is None:
            return

        with self._lock:
            self._checked_out = max(0, self._checked_out - 1)

        # Validate connection before returning to pool
        if self._is_connection_alive(connection):
            try:
                self._pool.put_nowait(connection)
            except Full:
                # Pool is full, close the connection
                with suppress(Exception):
                    connection.close()
                conn_id = id(connection)
                with self._lock:
                    self._connection_times.pop(conn_id, None)
        else:
            # Connection is dead, close it
            with suppress(Exception):
                connection.close()
            conn_id = id(connection)
            with self._lock:
                self._connection_times.pop(conn_id, None)


class SqliteConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with connection pooling for high performance.

    This configuration implements connection pooling to achieve the targeted 4.5x
    performance improvement (2k → 9k TPS) for SQLite workloads.
    """

    driver_type: "ClassVar[type[SqliteDriver]]" = SqliteDriver
    connection_type: "ClassVar[type[SqliteConnection]]" = SqliteConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[SqlitePoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[SqliteConnectionPool]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize SQLite pooled configuration.

        Args:
            pool_config: Pool configuration parameters including connection settings
            pool_instance: Pre-created pool instance
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration
        """
        # Store and parse the unified pool configuration
        if pool_config is None:
            pool_config = {}
        if "database" not in pool_config or pool_config["database"] == ":memory:":
            pool_config["database"] = "file::memory:?cache=shared"
            pool_config["uri"] = True

        super().__init__(
            pool_instance=pool_instance,
            pool_config=cast("dict[str, Any]", pool_config),
            migration_config=migration_config,
            statement_config=statement_config or sqlite_statement_config,
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

    def _create_pool(self) -> SqliteConnectionPool:
        """Create optimized connection pool from unified configuration."""
        config_dict = self._get_connection_config_dict()
        pool_config = self._get_pool_config_dict()
        return SqliteConnectionPool(connection_params=config_dict, **pool_config)

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> SqliteConnection:
        """Get a SQLite connection from the pool.

        This method ensures the pool is created and returns a connection
        from the pool. The connection is checked out from the pool and must
        be properly managed by the caller.

        Returns:
            SqliteConnection: A connection from the pool

        Note:
            For automatic connection management, prefer using provide_connection()
            or provide_session() which handle returning connections to the pool.
            The caller is responsible for returning the connection to the pool
            using pool.release(connection) when done.
        """
        # Ensure pool exists
        pool = self.provide_pool()

        # Use the pool's acquire method
        return pool.acquire()

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[SqliteConnection, None, None]":
        """Provide a pooled SQLite connection context manager."""
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(
        self, *args: "Any", statement_config: "Optional[StatementConfig]" = None, **kwargs: "Any"
    ) -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session using pooled connections."""
        with self.provide_connection(*args, **kwargs) as connection:
            # Use shared config or user-provided config
            yield self.driver_type(connection=connection, statement_config=statement_config or self.statement_config)

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for SQLite types.

        This provides all SQLite-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({"SqliteConnection": SqliteConnection, "SqliteCursor": SqliteCursor})
        return namespace
