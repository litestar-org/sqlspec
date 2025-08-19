"""SQLite database configuration with thread-local connections."""

import sqlite3
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._types import SqliteConnection

if TYPE_CHECKING:
    from collections.abc import Generator


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


__all__ = ("SqliteConnectionPool",)


class SqliteConnectionPool:
    """Thread-local connection manager for SQLite.

    SQLite connections aren't thread-safe, so we use thread-local storage
    to ensure each thread has its own connection. This is simpler and more
    efficient than a traditional pool for SQLite's constraints.
    """

    __slots__ = ("_connection_parameters", "_enable_optimizations", "_thread_local")

    def __init__(
        self, connection_parameters: "dict[str, Any]", enable_optimizations: bool = True, **kwargs: Any
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

        if self._enable_optimizations:
            database = self._connection_parameters.get("database", ":memory:")
            is_memory = database == ":memory:" or database.startswith("file::memory:")

            if not is_memory:
                connection.execute("PRAGMA journal_mode = WAL")

                connection.execute("PRAGMA busy_timeout = 5000")
                connection.execute("PRAGMA optimize")

            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = NORMAL")

        return connection  # type: ignore[no-any-return]

    def _get_thread_connection(self) -> SqliteConnection:
        """Get or create a connection for the current thread."""
        try:
            return cast("SqliteConnection", self._thread_local.connection)
        except AttributeError:
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
