"""SQLite database configuration with QueuePool-like connection pooling."""

import sqlite3
import threading
import time
from contextlib import contextmanager, suppress
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
POOL_RECYCLE: Final[int] = 86400
WAL_PRAGMA_SQL: Final[str] = "PRAGMA journal_mode = WAL"
FOREIGN_KEYS_SQL: Final[str] = "PRAGMA foreign_keys = ON"
SYNC_NORMAL_SQL: Final[str] = "PRAGMA synchronous = NORMAL"
CACHE_SIZE_SQL: Final[str] = "PRAGMA cache_size = -64000"
TEMP_STORE_SQL: Final[str] = "PRAGMA temp_store = MEMORY"
MMAP_SIZE_SQL: Final[str] = "PRAGMA mmap_size = 268435456"


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

    pool_min_size: NotRequired[int]
    pool_max_size: NotRequired[int]
    pool_timeout: NotRequired[float]
    pool_recycle_seconds: NotRequired[int]


class SqliteDriverFeatures(TypedDict, total=False):
    json_serializer: str


__all__ = ("SqliteConfig", "SqliteConnectionParams", "SqliteConnectionPool", "SqlitePoolParams", "sqlite3")


class SqliteConnectionPool:
    """Thread-local connection manager for SQLite with performance optimizations.

    Uses thread-local storage to ensure each thread gets its own SQLite connection,
    preventing the thread-safety issues that cause segmentation faults when
    multiple cursors share the same connection concurrently.

    This design trades traditional pooling for thread safety, which is essential
    for SQLite since connections and cursors are not thread-safe.
    """

    __slots__ = (
        "_connection_parameters",
        "_connection_times",
        "_created_connections",
        "_lock",
        "_recycle",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        pool_min_size: int = DEFAULT_MIN_POOL,
        pool_max_size: int = DEFAULT_MAX_POOL,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle_seconds: int = POOL_RECYCLE,
    ) -> None:
        """Initialize the thread-local connection manager."""
        self._connection_parameters = connection_parameters
        self._recycle = pool_recycle_seconds
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._created_connections = 0
        self._connection_times: dict[int, float] = {}

    def _create_connection(self) -> SqliteConnection:
        """Create a new SQLite connection with performance optimizations."""
        connection = sqlite3.connect(**self._connection_parameters)
        connection.row_factory = sqlite3.Row

        connection.execute(WAL_PRAGMA_SQL)
        connection.execute(FOREIGN_KEYS_SQL)
        connection.execute(SYNC_NORMAL_SQL)
        connection.execute(CACHE_SIZE_SQL)
        connection.execute(TEMP_STORE_SQL)
        connection.execute(MMAP_SIZE_SQL)

        conn_id = id(connection)
        with self._lock:
            self._created_connections += 1
            self._connection_times[conn_id] = time.time()

        return connection  # type: ignore[no-any-return]

    def _get_thread_connection(self) -> SqliteConnection:
        """Get or create a connection for the current thread.

        Each thread gets its own dedicated SQLite connection to prevent
        thread-safety issues with concurrent cursor operations.
        """
        if not hasattr(self._thread_local, "connection"):
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        # Check if connection needs recycling
        if self._recycle > 0 and time.time() - self._thread_local.created_at > self._recycle:
            with suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        return self._thread_local.connection

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        if hasattr(self._thread_local, "connection"):
            with suppress(Exception):
                self._thread_local.connection.close()
            del self._thread_local.connection
            if hasattr(self._thread_local, "created_at"):
                del self._thread_local.created_at

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
        """Get a thread-local connection.

        Each thread gets its own dedicated SQLite connection to prevent
        thread-safety issues with concurrent cursor operations.

        Yields:
            SqliteConnection: A thread-local connection.
        """
        connection = self._get_thread_connection()
        try:
            yield connection
        except Exception:
            # On error, close and recreate connection for this thread
            self._close_thread_connection()
            raise

    def close(self) -> None:
        """Close the thread-local connection if it exists."""
        self._close_thread_connection()

    def size(self) -> int:
        """Get current pool size (always 1 for thread-local)."""
        return 1 if hasattr(self._thread_local, "connection") else 0

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0 for thread-local)."""
        return 0

    def acquire(self) -> SqliteConnection:
        """Acquire a thread-local connection.

        Each thread gets its own dedicated SQLite connection to prevent
        thread-safety issues with concurrent cursor operations.

        Returns:
            SqliteConnection: A thread-local connection
        """
        return self._get_thread_connection()

    def release(self, connection: SqliteConnection) -> None:
        """Release a thread-local connection (no-op since connection is thread-owned).

        Args:
            connection: The connection to release (ignored - thread owns it)
        """
        # No-op: thread-local connections are managed per-thread


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
        return SqliteConnectionPool(connection_parameters=config_dict, **pool_config)

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
        pool = self.provide_pool()

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
