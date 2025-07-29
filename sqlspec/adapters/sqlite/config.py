"""SQLite database configuration with QueuePool-like connection pooling."""

import sqlite3
import threading
import time
from contextlib import contextmanager, suppress
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Any, ClassVar, Final, Optional, TypedDict, Union

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite.driver import SqliteConnection, SqliteCursor, SqliteDriver
from sqlspec.config import SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    from collections.abc import Generator


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


__all__ = ("SqliteConfig", "SqliteConnectionParams", "SqliteConnectionPool", "sqlite3")


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
        min_pool_size: int = DEFAULT_MIN_POOL,
        max_pool_size: int = DEFAULT_MAX_POOL,
        timeout: float = POOL_TIMEOUT,
        recycle: int = POOL_RECYCLE,
    ) -> None:
        """Initialize the connection pool."""
        self._pool: "Queue[SqliteConnection]" = Queue(maxsize=max_pool_size)  # noqa: UP037
        self._lock = threading.RLock()
        self._min_pool_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._timeout = timeout
        self._recycle = recycle
        self._connection_params = connection_params
        self._created_connections = 0
        self._checked_out = 0
        self._connection_times: "dict[int, float]" = {}  # noqa: UP037

        # Pre-populate core pool
        try:
            for _ in range(min_pool_size):
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


class SqliteConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with connection pooling for high performance.

    This configuration implements connection pooling to achieve the targeted 4.5x
    performance improvement (2k → 9k TPS) for SQLite workloads.
    """

    driver_type: "ClassVar[type[SqliteDriver]]" = SqliteDriver
    connection_type: "ClassVar[type[SqliteConnection]]" = SqliteConnection
    supports_connection_pooling: "ClassVar[bool]" = True
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("qmark", "named_colon")
    default_parameter_style: "ClassVar[str]" = "qmark"

    def __init__(
        self,
        *,
        connection_config: "Optional[Union[SqliteConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
        min_pool_size: int = DEFAULT_MIN_POOL,
        max_pool_size: int = DEFAULT_MAX_POOL,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle: int = POOL_RECYCLE,
        pool_instance: "Optional[SqliteConnectionPool]" = None,
    ) -> None:
        """Initialize SQLite pooled configuration.

        Args:
            connection_config: Connection configuration parameters as TypedDict
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
            min_pool_size: Minimum number of connections to maintain (default: 5)
            max_pool_size: Maximum number of connections allowed (default: 20)
            pool_timeout: Pool checkout timeout in seconds (default: 30.0)
            pool_recycle: Connection recycle time in seconds (default: 3600)
            pool_instance: Pre-created pool instance
        """
        # Store configuration
        self.connection_config: "dict[str, Any]" = (  # noqa: UP037
            dict(connection_config) if connection_config else {"database": ":memory:"}
        )
        extras = self.connection_config.pop("extra", {})
        self.connection_config.update(extras)

        # Handle default database setting - ensure empty string or None becomes :memory:
        database = self.connection_config.get("database")
        if not database:  # None, empty string, or other falsy values
            self.connection_config["database"] = ":memory:"
            database = ":memory:"

        # Check if this is an in-memory database and auto-convert to shared memory
        if self._is_memory_database(database):
            self._convert_to_shared_memory()
        # Also handle cases where database is already a shared memory URI
        elif "file::memory:" in database:
            # Ensure uri=True is set for all file::memory: databases
            self.connection_config["uri"] = True

        self.statement_config = statement_config or SQLConfig()
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle

        super().__init__(
            pool_instance=pool_instance,
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    def _convert_to_shared_memory(self) -> None:
        """Convert in-memory database to shared memory for connection pooling.

        Automatically converts :memory: and file::memory: databases to
        file::memory:?cache=shared format to enable safe connection pooling.
        """
        database = self.connection_config.get("database", ":memory:")

        if database == ":memory:":
            # Convert :memory: to shared memory
            self.connection_config["database"] = "file::memory:?cache=shared"
            self.connection_config["uri"] = True
        elif "file::memory:" in database:
            # For file::memory: URIs, ensure they have cache=shared and uri=True
            if "cache=shared" not in database:
                # Add cache=shared to existing file::memory: URI
                separator = "&" if "?" in database else "?"
                self.connection_config["database"] = f"{database}{separator}cache=shared"
            # Always ensure uri=True for file::memory: databases
            self.connection_config["uri"] = True

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""
        config: "dict[str, Any]" = dict(self.connection_config)  # noqa: UP037
        config.pop("extra", None)
        return {k: v for k, v in config.items() if v is not None}

    def _create_pool(self) -> SqliteConnectionPool:
        """Create the SQLite connection pool."""
        connection_params = self._get_connection_config_dict()
        return SqliteConnectionPool(
            connection_params=connection_params,
            min_pool_size=self.min_pool_size,
            max_pool_size=self.max_pool_size,
            timeout=self.pool_timeout,
            recycle=self.pool_recycle,
        )

    def _is_memory_database(self, database: "Optional[str]") -> bool:
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
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[SqliteConnection, None, None]":
        """Provide a pooled SQLite connection context manager."""
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(self, *args: "Any", **kwargs: "Any") -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session using pooled connections."""
        with self.provide_connection(*args, **kwargs) as connection:
            statement_config = self.statement_config
            if statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            yield self.driver_type(connection=connection, config=statement_config)

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
