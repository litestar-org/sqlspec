"""SQLite database configuration with thread-local connections."""

import contextlib
import logging
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.adapters.sqlite._typing import SqliteConnection
from sqlspec.adapters.sqlite._typing import sqlite_module as sqlite3
from sqlspec.adapters.sqlite.core import end_transaction
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("SqliteConnectionPool",)

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "sqlite"
SQLITE_BUSY_TIMEOUT: Final = 5000
SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS: Final = False
SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS: Final = True
SQLITE_MEMORY_CACHE_SIZE: Final = -16000
SQLITE_WAL_SWITCH_ATTEMPTS: Final = 50
SQLITE_WAL_SWITCH_DELAY: Final = 0.01


def _attempt_wal_switch(connection: "SqliteConnection", attempt: int) -> bool:
    """Attempt a single WAL mode switch, returning True on success."""
    try:
        connection.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc) or attempt == SQLITE_WAL_SWITCH_ATTEMPTS - 1:
            raise
        time.sleep(SQLITE_WAL_SWITCH_DELAY)
        return False
    return True


def _enable_wal(connection: "SqliteConnection") -> None:
    """Retry database and table locks briefly while switching to WAL mode."""
    for attempt in range(SQLITE_WAL_SWITCH_ATTEMPTS):
        if _attempt_wal_switch(connection, attempt):
            return


class SqliteConnectionPool:
    """Thread-local connection manager for SQLite.

    SQLite connections aren't thread-safe, so we use thread-local storage
    to ensure each thread has its own connection. This is simpler and more
    efficient than a traditional pool for SQLite's constraints.
    """

    __slots__ = (
        "_connection_parameters",
        "_connection_registry",
        "_enable_foreign_keys",
        "_enable_optimizations",
        "_generation",
        "_health_check_interval",
        "_is_memory_db",
        "_on_connection_create",
        "_pool_id",
        "_recycle_seconds",
        "_registry_lock",
        "_runtime_setup",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        enable_optimizations: bool = SQLITE_DEFAULT_ENABLE_OPTIMIZATIONS,
        enable_foreign_keys: bool = SQLITE_DEFAULT_ENABLE_FOREIGN_KEYS,
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[SqliteConnection], None] | None" = None,
        runtime_setup: "dict[str, Any] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: SQLite connection parameters
            enable_optimizations: Whether to apply performance PRAGMAs
            enable_foreign_keys: Whether to enable foreign-key enforcement
            recycle_seconds: Connection recycle time in seconds (default 24h)
            health_check_interval: Seconds of idle time before running health check
            on_connection_create: Callback executed when connection is created
            runtime_setup: Runtime feature configuration applied after internal PRAGMAs
        """
        if "check_same_thread" not in connection_parameters:
            connection_parameters = {**connection_parameters, "check_same_thread": False}
        self._connection_parameters = connection_parameters
        database = self._connection_parameters.get("database", ":memory:")
        self._is_memory_db = database == ":memory:" or "mode=memory" in str(database)
        self._thread_local = threading.local()
        self._connection_registry: set[SqliteConnection] = set()
        self._generation = 0
        self._registry_lock = threading.Lock()
        self._enable_optimizations = enable_optimizations
        self._enable_foreign_keys = enable_foreign_keys
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create
        self._runtime_setup = runtime_setup
        self._pool_id = str(uuid4())[:8]

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        db = self._connection_parameters.get("database", ":memory:")
        if db == ":memory:" or "mode=memory" in str(db):
            return ":memory:"
        return str(db)

    def _create_connection(self) -> SqliteConnection:
        """Create a pool-owned connection and record it for shutdown."""
        connection = self.new_connection()
        with self._registry_lock:
            self._connection_registry.add(connection)
        return connection

    def new_connection(self) -> SqliteConnection:
        """Create a standalone connection configured like a pooled one.

        The result is owned by the caller: it is not thread-local and is not
        tracked for pool shutdown.

        Returns:
            SqliteConnection: A newly opened, fully configured connection.
        """
        connection = sqlite3.connect(**self._connection_parameters)

        try:
            if self._enable_optimizations:
                if self._is_memory_db:
                    connection.execute("PRAGMA journal_mode = MEMORY")
                    connection.execute("PRAGMA synchronous = OFF")
                    connection.execute("PRAGMA temp_store = MEMORY")
                    connection.execute(f"PRAGMA cache_size = {SQLITE_MEMORY_CACHE_SIZE}")
                else:
                    _enable_wal(connection)
                    connection.execute("PRAGMA synchronous = NORMAL")

                connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT}")

            if self._enable_foreign_keys:
                connection.execute("PRAGMA foreign_keys = ON")

            if self._runtime_setup is not None:
                _apply_runtime_setup(connection, self._runtime_setup)

            if self._on_connection_create is not None:
                self._on_connection_create(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                connection.close()
            raise

        return cast("SqliteConnection", connection)

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
        current_generation = getattr(self._thread_local, "generation", None)
        if current_generation != self._generation:
            stale = getattr(self._thread_local, "connection", None)
            if stale is not None:
                self._retire_connection(cast("SqliteConnection", stale))
                self._thread_local.connection = None
            self._thread_local.created_at = 0.0
            self._thread_local.last_used = 0.0
            self._thread_local.generation = self._generation

        conn = getattr(self._thread_local, "connection", None)
        now = time.time()
        if conn is None:
            conn = self._create_connection()
            self._thread_local.connection = conn
            self._thread_local.created_at = now
            self._thread_local.last_used = now
            return conn

        created_at = getattr(self._thread_local, "created_at", 0.0)
        if self._recycle_seconds > 0 and (now - created_at) > self._recycle_seconds:
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
            self._retire_connection(cast("SqliteConnection", conn))
            conn = self._create_connection()
            self._thread_local.connection = conn
            self._thread_local.created_at = now
            self._thread_local.last_used = now
            return conn

        last_used = getattr(self._thread_local, "last_used", 0.0)
        idle_time = now - last_used
        if (
            not self._is_memory_db
            and idle_time > self._health_check_interval
            and not self._is_connection_alive(cast("SqliteConnection", conn))
        ):
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
            self._retire_connection(cast("SqliteConnection", conn))
            conn = self._create_connection()
            self._thread_local.connection = conn
            self._thread_local.created_at = now
            self._thread_local.last_used = now
            return conn

        self._thread_local.last_used = now
        return cast("SqliteConnection", conn)

    def _retire_connection(self, connection: SqliteConnection) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._registry_lock:
            self._connection_registry.discard(connection)
        with contextlib.suppress(Exception):
            connection.close()

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        conn = getattr(self._thread_local, "connection", None)
        if conn is not None:
            self._retire_connection(cast("SqliteConnection", conn))
            self._thread_local.connection = None
            self._thread_local.created_at = 0.0
            self._thread_local.last_used = 0.0

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
                end_transaction(connection, commit=False)
            raise
        else:
            with contextlib.suppress(Exception):
                end_transaction(connection, commit=True)

    def close(self) -> None:
        """Close every connection this pool opened, on any thread."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            self._connection_registry.clear()
            self._generation += 1
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()

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
        if getattr(self._thread_local, "connection", None) is not None:
            return 1
        return 0

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0)."""
        return 0


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

    create_window_fn = getattr(connection, "create_window_function", None)
    if create_window_fn is not None:
        for window_config in runtime_setup.get("custom_window_functions", ()):
            create_window_fn(window_config["name"], window_config["narg"], window_config["window_class"])

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
