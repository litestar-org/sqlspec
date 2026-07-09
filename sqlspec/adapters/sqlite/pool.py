"""SQLite database configuration with thread-local connections."""

import contextlib
import logging
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.sqlite._typing import SqliteConnection
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("SqliteConnectionPool",)

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "sqlite"


def _dict_row_factory(cursor: Any, row: "tuple[Any, ...]") -> "dict[str, Any]":
    return {column[0]: row[index] for index, column in enumerate(cursor.description)}


def _resolve_row_factory(row_factory: Any) -> Any:
    if row_factory == "row":
        return sqlite3.Row
    if row_factory == "dict":
        return _dict_row_factory
    if row_factory == "tuple":
        return None
    return row_factory


def _load_extensions(connection: SqliteConnection, extensions: "list[str]") -> None:
    connection.enable_load_extension(True)
    try:
        for extension_path in extensions:
            connection.load_extension(extension_path)
    finally:
        connection.enable_load_extension(False)


def _apply_runtime_setup(connection: SqliteConnection, runtime_setup: "dict[str, Any]") -> None:
    pragmas = runtime_setup.get("pragmas", ())
    if pragmas:
        pragma_script = "\n".join(f"PRAGMA {pragma_name} = {pragma_value};" for pragma_name, pragma_value in pragmas)
        connection.executescript(pragma_script)

    extensions = runtime_setup.get("extensions")
    if extensions:
        _load_extensions(connection, list(extensions))

    for function_config in runtime_setup.get("custom_functions", ()):
        connection.create_function(
            function_config["name"],
            function_config["narg"],
            function_config["func"],
            deterministic=function_config.get("deterministic", False),
        )

    for aggregate_config in runtime_setup.get("custom_aggregates", ()):
        connection.create_aggregate(
            aggregate_config["name"], aggregate_config["narg"], aggregate_config["aggregate_class"]
        )

    for collation_config in runtime_setup.get("custom_collations", ()):
        connection.create_collation(collation_config["name"], collation_config["func"])

    authorizer_callback = runtime_setup.get("authorizer_callback")
    if authorizer_callback is not None:
        connection.set_authorizer(authorizer_callback)

    trace_callback = runtime_setup.get("trace_callback")
    if trace_callback is not None:
        connection.set_trace_callback(trace_callback)

    progress_handler = runtime_setup.get("progress_handler")
    if progress_handler is not None:
        connection.set_progress_handler(progress_handler, runtime_setup.get("progress_handler_interval", 1000))

    if "row_factory" in runtime_setup:
        connection.row_factory = _resolve_row_factory(runtime_setup["row_factory"])

    if "text_factory" in runtime_setup:
        connection.text_factory = runtime_setup["text_factory"]


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
        "_on_connection_create",
        "_pool_id",
        "_recycle_seconds",
        "_runtime_setup",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        enable_optimizations: bool = True,
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[SqliteConnection], None] | None" = None,
        runtime_setup: "dict[str, Any] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: SQLite connection parameters
            enable_optimizations: Whether to apply performance PRAGMAs
            recycle_seconds: Connection recycle time in seconds (default 24h)
            health_check_interval: Seconds of idle time before running health check
            on_connection_create: Callback executed when connection is created
            runtime_setup: Runtime feature configuration applied after internal PRAGMAs
        """
        if "check_same_thread" not in connection_parameters:
            connection_parameters = {**connection_parameters, "check_same_thread": False}
        self._connection_parameters = connection_parameters
        self._thread_local = threading.local()
        self._enable_optimizations = enable_optimizations
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create
        self._runtime_setup = runtime_setup
        self._pool_id = str(uuid.uuid4())[:8]

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        db = self._connection_parameters.get("database", ":memory:")
        if db == ":memory:" or "mode=memory" in str(db):
            return ":memory:"
        return str(db)

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

        if self._runtime_setup is not None:
            _apply_runtime_setup(connection, self._runtime_setup)

        # Call user-provided callback after internal setup
        if self._on_connection_create is not None:
            self._on_connection_create(connection)

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
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.recycle",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
                recycle_seconds=self._recycle_seconds,
                reason="exceeded_recycle_time",
            )
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("SqliteConnection", self._thread_local.connection)

        idle_time = time.time() - thread_state.get("last_used", 0)
        if idle_time > self._health_check_interval and not self._is_connection_alive(self._thread_local.connection):
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.recycle",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
                idle_seconds=round(idle_time, 1),
                reason="failed_health_check",
            )
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
        except Exception:
            with contextlib.suppress(Exception):
                if connection.in_transaction:
                    connection.rollback()
            raise
        else:
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
