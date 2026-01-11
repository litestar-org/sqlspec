"""SQLite database configuration with thread-local connections."""

import contextlib
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._typing import SqliteConnection
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Generator


class SqliteConnectionParams(TypedDict):
    """SQLite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: "NotRequired[str | None]"
    check_same_thread: NotRequired[bool]
    factory: "NotRequired[type[SqliteConnection] | None]"
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


__all__ = ("SqliteConnectionPool",)

logger = get_logger(__name__)


class SqliteConnectionPool:
    """Thread-local connection manager for SQLite.

    SQLite connections aren't thread-safe, so we use thread-local storage
    to ensure each thread has its own connection. This is simpler and more
    efficient than a traditional pool for SQLite's constraints.
    """

    __slots__ = (
        "_connection_parameters",
        "_enable_optimizations",
        "_health_check_interval",
        "_recycle_seconds",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        enable_optimizations: bool = True,
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: SQLite connection parameters
            enable_optimizations: Whether to apply performance PRAGMAs
            recycle_seconds: Connection recycle time in seconds (default 24h)
            health_check_interval: Seconds of idle time before running health check
        """
        if "check_same_thread" not in connection_parameters:
            connection_parameters = {**connection_parameters, "check_same_thread": False}
        self._connection_parameters = connection_parameters
        self._thread_local = threading.local()
        self._enable_optimizations = enable_optimizations
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval

    def _create_connection(self) -> SqliteConnection:
        """Create a new SQLite connection with optimizations."""
        connection = sqlite3.connect(**self._connection_parameters)

        if self._enable_optimizations:
            database = self._connection_parameters.get("database", ":memory:")
            is_memory = database == ":memory:" or "mode=memory" in str(database)

            if is_memory:
                connection.execute("PRAGMA journal_mode = MEMORY")
                connection.execute("PRAGMA synchronous = OFF")
                connection.execute("PRAGMA temp_store = MEMORY")
            else:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = NORMAL")
                connection.execute("PRAGMA busy_timeout = 5000")

            connection.execute("PRAGMA foreign_keys = ON")

        return connection  # type: ignore[no-any-return]

    def _is_connection_alive(self, connection: SqliteConnection) -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            connection.execute("SELECT 1")
        except Exception:
            return False
        return True

    def _get_thread_connection(self) -> SqliteConnection:
        """Get or create a connection for the current thread."""
        thread_state = self._thread_local.__dict__
        if "connection" not in thread_state:
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("SqliteConnection", self._thread_local.connection)

        if self._recycle_seconds > 0 and time.time() - self._thread_local.created_at > self._recycle_seconds:
            logger.debug("SQLite connection exceeded recycle time, recreating")
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("SqliteConnection", self._thread_local.connection)

        idle_time = time.time() - thread_state.get("last_used", 0)
        if idle_time > self._health_check_interval and not self._is_connection_alive(self._thread_local.connection):
            logger.debug("SQLite connection failed health check after %.1fs idle, recreating", idle_time)
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        self._thread_local.last_used = time.time()
        return cast("SqliteConnection", self._thread_local.connection)

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        thread_state = self._thread_local.__dict__
        if "connection" in thread_state:
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            del self._thread_local.connection
            if "created_at" in thread_state:
                del self._thread_local.created_at
            if "last_used" in thread_state:
                del self._thread_local.last_used

    @contextmanager
    def get_connection(self) -> "Generator[SqliteConnection, None, None]":
        """Get a thread-local connection.

        Yields:
            SqliteConnection: A thread-local connection.
        """
        connection = self._get_thread_connection()
        try:
            yield connection
        finally:
            with contextlib.suppress(Exception):
                if connection.in_transaction:
                    connection.commit()

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
        else:
            return 1

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0)."""
        return 0
