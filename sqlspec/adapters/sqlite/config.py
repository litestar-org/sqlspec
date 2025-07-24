"""SQLite database configuration with QueuePool-like connection pooling."""

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager, suppress
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict, Union

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite.driver import SqliteConnection, SqliteDriver
from sqlspec.config import NoPoolSyncConfig, SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    from collections.abc import Generator


logger = logging.getLogger(__name__)

# Performance constants for SQLite pooling optimization
DEFAULT_POOL_SIZE: Final[int] = 20
MAX_OVERFLOW: Final[int] = 10
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
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    factory: NotRequired[Optional[type[SqliteConnection]]]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


__all__ = ("SqliteConfig", "SqliteConnectionParams", "SqliteConnectionPool", "SqlitePooledConfig", "sqlite3")


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
        "_pool",
        "_pool_size",
        "_recycle",
        "_timeout",
    )

    def __init__(
        self,
        connection_params: dict[str, Any],
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = MAX_OVERFLOW,
        timeout: float = POOL_TIMEOUT,
        recycle: int = POOL_RECYCLE,
    ) -> None:
        self._pool: Queue[SqliteConnection] = Queue(maxsize=pool_size + max_overflow)
        self._lock = threading.RLock()
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._timeout = timeout
        self._recycle = recycle
        self._connection_params = connection_params
        self._created_connections = 0
        self._checked_out = 0
        self._connection_times: dict[int, float] = {}  # Track connection creation times

        # Pre-populate core pool
        for _ in range(min(pool_size, 5)):  # Start with 5 connections
            try:
                conn = self._create_connection()
                self._pool.put_nowait(conn)
            except Full:
                break

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

    @contextmanager
    def get_connection(self) -> "Generator[SqliteConnection, None, None]":
        """Get a connection from the pool with automatic return."""
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
                # Pool is empty, check if we can create overflow connection
                with self._lock:
                    if self._checked_out < (self._pool_size + self._max_overflow):
                        connection = None  # Will create new one below
                    else:
                        # Wait for a connection to become available
                        try:
                            connection = self._pool.get(timeout=self._timeout)
                        except Empty:
                            msg = f"QueuePool limit of size {self._pool_size + self._max_overflow} reached, timeout {self._timeout}"
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

                try:
                    # Test connection is still alive
                    connection.execute("SELECT 1")

                    # Return to pool if space available
                    try:
                        self._pool.put_nowait(connection)
                    except Full:
                        # Pool is full, close overflow connection
                        with suppress(Exception):
                            connection.close()

                except Exception:
                    # Connection is broken, close it
                    with suppress(Exception):
                        connection.close()

    def close(self) -> None:
        """Close all connections in the pool."""
        while True:
            try:
                connection = self._pool.get_nowait()
                with suppress(Exception):
                    connection.close()
            except Empty:
                break

        # Clear connection time tracking
        with self._lock:
            self._connection_times.clear()

    def size(self) -> int:
        """Get current pool size."""
        return self._pool.qsize()

    def checked_out(self) -> int:
        """Get number of checked out connections."""
        return self._checked_out


class SqliteConfig(NoPoolSyncConfig[SqliteConnection, SqliteDriver]):
    """Configuration for SQLite database connections with direct field-based configuration."""

    driver_type: ClassVar[type[SqliteDriver]] = SqliteDriver
    connection_type: ClassVar[type[SqliteConnection]] = SqliteConnection
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    default_parameter_style: ClassVar[str] = "qmark"

    def __init__(
        self,
        *,
        connection_config: "Optional[Union[SqliteConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            connection_config: Connection configuration parameters as TypedDict
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
        """
        # Store the connection config and extract/merge extras
        self.connection_config: dict[str, Any] = (
            dict(connection_config) if connection_config else {"database": ":memory:"}
        )
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        super().__init__(
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    def _get_connection_config_dict(self) -> dict[str, Any]:
        """Get connection configuration as plain dict for external library.

        Returns:
            Dictionary with connection parameters, filtering out None values.
        """
        config: dict[str, Any] = dict(self.connection_config)
        # Remove extra key if it exists (it should already be merged)
        config.pop("extra", None)
        # Filter out None values since sqlite3.connect doesn't accept them
        return {k: v for k, v in config.items() if v is not None}

    def create_connection(self) -> SqliteConnection:
        """Create and return a SQLite connection."""
        config = self._get_connection_config_dict()
        connection = sqlite3.connect(**config)
        connection.row_factory = sqlite3.Row
        return connection  # type: ignore[no-any-return]

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[SqliteConnection, None, None]":
        """Provide a SQLite connection context manager.

        Args:
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments

        Yields:
            SqliteConnection: A SQLite connection

        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session context manager.

        Args:
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments

        Yields:
            SqliteDriver: A SQLite driver
        """
        with self.provide_connection(*args, **kwargs) as connection:
            statement_config = self.statement_config
            # Inject parameter style info if not already set
            if statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            yield self.driver_type(connection=connection, config=statement_config)


class SqlitePooledConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with QueuePool-like connection pooling for high performance.

    This configuration implements connection pooling similar to SQLAlchemy's QueuePool
    to achieve the targeted 4.5x performance improvement (2k → 9k TPS) for SQLite workloads.
    """

    driver_type: ClassVar[type[SqliteDriver]] = SqliteDriver
    connection_type: ClassVar[type[SqliteConnection]] = SqliteConnection
    supports_connection_pooling: ClassVar[bool] = True
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "named_colon")
    default_parameter_style: ClassVar[str] = "qmark"

    def __init__(
        self,
        *,
        connection_config: "Optional[Union[SqliteConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = MAX_OVERFLOW,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle: int = POOL_RECYCLE,
        pool_instance: "Optional[SqliteConnectionPool]" = None,
    ) -> None:
        """Initialize SQLite pooled configuration.

        Args:
            connection_config: Connection configuration parameters as TypedDict
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
            pool_size: Core pool size (default: 20)
            max_overflow: Max overflow connections (default: 10)
            pool_timeout: Pool checkout timeout in seconds (default: 30.0)
            pool_recycle: Connection recycle time in seconds (default: 3600)
            pool_instance: Pre-created pool instance
        """
        # Store configuration
        self.connection_config: dict[str, Any] = (
            dict(connection_config) if connection_config else {"database": ":memory:"}
        )
        extras = self.connection_config.pop("extra", {})
        self.connection_config.update(extras)

        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle

        super().__init__(
            pool_instance=pool_instance,
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    def _get_connection_config_dict(self) -> dict[str, Any]:
        """Get connection configuration as plain dict for pool creation."""
        config: dict[str, Any] = dict(self.connection_config)
        config.pop("extra", None)
        return {k: v for k, v in config.items() if v is not None}

    def _create_pool(self) -> SqliteConnectionPool:
        """Create the SQLite connection pool."""
        connection_params = self._get_connection_config_dict()
        return SqliteConnectionPool(
            connection_params=connection_params,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            timeout=self.pool_timeout,
            recycle=self.pool_recycle,
        )

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> SqliteConnection:
        """Create a single connection (bypasses pool)."""
        config = self._get_connection_config_dict()
        connection = sqlite3.connect(**config)
        connection.row_factory = sqlite3.Row
        return connection  # type: ignore[no-any-return]

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[SqliteConnection, None, None]":
        """Provide a pooled SQLite connection context manager."""
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session using pooled connections."""
        with self.provide_connection(*args, **kwargs) as connection:
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            yield self.driver_type(connection=connection, config=statement_config)
