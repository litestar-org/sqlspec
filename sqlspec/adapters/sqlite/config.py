"""SQLite database configuration with thread-local connections."""

import sqlite3
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._types import SqliteConnection
from sqlspec.adapters.sqlite.driver import SqliteCursor, SqliteDriver, sqlite_statement_config
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


__all__ = ("SqliteConfig", "SqliteConnectionParams", "SqliteConnectionPool")


class SqliteConnectionPool:
    """Thread-local connection manager for SQLite.

    SQLite connections aren't thread-safe, so we use thread-local storage
    to ensure each thread has its own connection. This is simpler and more
    efficient than a traditional pool for SQLite's constraints.
    """

    __slots__ = ("_connection_parameters", "_enable_optimizations", "_thread_local")

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        enable_optimizations: bool = True,
        **kwargs: Any,  # Accept and ignore pool parameters for compatibility
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: SQLite connection parameters
            enable_optimizations: Whether to apply performance PRAGMAs
            **kwargs: Ignored pool parameters for compatibility
        """
        self._connection_parameters = connection_parameters
        self._thread_local = threading.local()
        self._enable_optimizations = enable_optimizations

    def _create_connection(self) -> SqliteConnection:
        """Create a new SQLite connection with optimizations."""
        connection = sqlite3.connect(**self._connection_parameters)

        # Only apply optimizations if requested and not in-memory
        if self._enable_optimizations:
            database = self._connection_parameters.get("database", ":memory:")
            is_memory = database == ":memory:" or database.startswith("file::memory:")

            if not is_memory:
                # WAL mode doesn't work with in-memory databases
                connection.execute("PRAGMA journal_mode = WAL")
                # Set busy timeout for better concurrent access
                connection.execute("PRAGMA busy_timeout = 5000")
                connection.execute("PRAGMA optimize")
            # These work for all database types
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = NORMAL")

        return connection  # type: ignore[no-any-return]

    def _get_thread_connection(self) -> SqliteConnection:
        """Get or create a connection for the current thread."""
        try:
            return cast("SqliteConnection", self._thread_local.connection)
        except AttributeError:
            # Connection doesn't exist for this thread yet
            connection = self._create_connection()
            self._thread_local.connection = connection
            return connection

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        try:
            connection = self._thread_local.connection
            connection.close()
            del self._thread_local.connection
        except AttributeError:
            # No connection for this thread
            pass

    @contextmanager
    def get_connection(self) -> "Generator[SqliteConnection, None, None]":
        """Get a thread-local connection.

        Yields:
            SqliteConnection: A thread-local connection.
        """
        yield self._get_thread_connection()

    def close(self) -> None:
        """Close the thread-local connection if it exists."""
        self._close_thread_connection()

    def acquire(self) -> SqliteConnection:
        """Acquire a thread-local connection.

        Returns:
            SqliteConnection: A thread-local connection
        """
        return self._get_thread_connection()

    def release(self, connection: SqliteConnection) -> None:
        """Release a connection (no-op for thread-local connections).

        Args:
            connection: The connection to release (ignored)
        """
        # No-op: thread-local connections are managed per-thread

    # Compatibility methods that return dummy values
    def size(self) -> int:
        """Get pool size (always 1 for thread-local)."""
        try:
            _ = self._thread_local.connection
        except AttributeError:
            return 0
        return 1

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0)."""
        return 0


class SqliteConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with thread-local connections."""

    driver_type: "ClassVar[type[SqliteDriver]]" = SqliteDriver
    connection_type: "ClassVar[type[SqliteConnection]]" = SqliteConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[SqliteConnectionParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[SqliteConnectionPool]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            pool_config: Configuration parameters including connection settings
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
        # Filter out pool-specific parameters that SQLite doesn't use
        excluded_keys = {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        return {k: v for k, v in self.pool_config.items() if v is not None and k not in excluded_keys}

    def _create_pool(self) -> SqliteConnectionPool:
        """Create connection pool from configuration."""
        config_dict = self._get_connection_config_dict()
        # Pass all pool_config as kwargs to be ignored by the pool
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
