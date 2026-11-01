"""pymssql database configuration with thread-local connections."""

import contextlib
import logging
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.pymssql._typing import PYMSSQL_MODULE, PymssqlConnection
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("PymssqlConnectionPool",)


logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "pymssql"
pymssql = PYMSSQL_MODULE


class PymssqlConnectionPool:
    """Thread-local connection manager for pymssql."""

    __slots__ = (
        "_checked_out",
        "_connection_parameters",
        "_connection_registry",
        "_connection_threads",
        "_generation",
        "_health_check_interval",
        "_on_connection_create",
        "_pool_id",
        "_recycle_seconds",
        "_registry_lock",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[PymssqlConnection], None] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: pymssql connection parameters
            recycle_seconds: Connection recycle time in seconds (default 24h)
            health_check_interval: Seconds of idle time before running health check
            on_connection_create: Callback executed when connection is created
        """
        self._connection_parameters = connection_parameters
        self._thread_local = threading.local()
        self._connection_registry: set[PymssqlConnection] = set()
        self._connection_threads: dict[PymssqlConnection, threading.Thread] = {}
        self._checked_out = 0
        self._generation = 0
        self._registry_lock = threading.Lock()
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create
        self._pool_id = str(uuid4())[:8]

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        return str(self._connection_parameters.get("database", "unknown"))

    def new_connection(self) -> PymssqlConnection:
        """Open a standalone connection configured like a pooled one.

        The result is owned by the caller: it is not thread-local and is not
        tracked for pool shutdown.

        Returns:
            PymssqlConnection: A newly opened, fully configured connection.
        """
        connection = pymssql.connect(**self._connection_parameters)

        if self._on_connection_create is not None:
            self._on_connection_create(connection)

        return cast("PymssqlConnection", connection)

    @contextmanager
    def get_connection(self) -> "Generator[PymssqlConnection, None, None]":
        """Get a thread-local connection.

        Yields:
            A thread-local database connection.
        """
        depth = getattr(self._thread_local, "depth", 0)
        self._thread_local.depth = depth + 1
        connection = self.acquire()
        try:
            yield connection
        except Exception:
            if getattr(self._thread_local, "depth", 1) <= 1:
                with contextlib.suppress(Exception):
                    self._close_thread_connection()
            raise
        finally:
            self._thread_local.depth = max(0, getattr(self._thread_local, "depth", 1) - 1)
            self.release(connection)

    def close(self) -> None:
        """Close every connection this pool opened, on any thread."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            self._connection_registry.clear()
            self._connection_threads.clear()
            self._checked_out = 0
            self._generation += 1
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()

    def acquire(self) -> PymssqlConnection:
        connection = self._get_thread_connection()
        with self._registry_lock:
            self._checked_out += 1
        return connection

    def release(self, connection: PymssqlConnection) -> None:
        _ = connection
        with self._registry_lock:
            if self._checked_out > 0:
                self._checked_out -= 1

    def size(self) -> int:
        self._prune_dead_thread_connections()
        with self._registry_lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        with self._registry_lock:
            return self._checked_out

    def _prune_dead_thread_connections(self) -> None:
        dead_connections: list[PymssqlConnection] = []
        with self._registry_lock:
            for conn, owner_thread in list(self._connection_threads.items()):
                if not owner_thread.is_alive():
                    self._connection_registry.discard(conn)
                    self._connection_threads.pop(conn, None)
                    dead_connections.append(conn)
        for conn in dead_connections:
            with contextlib.suppress(Exception):
                conn.close()

    def _create_connection(self) -> PymssqlConnection:
        self._prune_dead_thread_connections()
        connection = self.new_connection()

        with self._registry_lock:
            self._connection_registry.add(connection)
            self._connection_threads[connection] = threading.current_thread()

        return connection

    def _is_connection_alive(self, connection: PymssqlConnection) -> bool:
        try:
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()
        except Exception:
            return False
        return True

    def _get_thread_connection(self) -> PymssqlConnection:
        thread_state = self._thread_local.__dict__
        if thread_state.get("generation") != self._generation:
            stale = thread_state.pop("connection", None)
            if stale is not None:
                self._retire_connection(cast("PymssqlConnection", stale))
            thread_state.pop("created_at", None)
            thread_state.pop("last_used", None)
            self._thread_local.generation = self._generation
        if "connection" not in thread_state:
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("PymssqlConnection", self._thread_local.connection)

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
            self._retire_connection(cast("PymssqlConnection", self._thread_local.connection))
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("PymssqlConnection", self._thread_local.connection)

        idle_time = time.time() - thread_state.get("last_used", 0)
        if (
            self._health_check_interval > 0
            and idle_time > self._health_check_interval
            and not self._is_connection_alive(self._thread_local.connection)
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
            self._retire_connection(cast("PymssqlConnection", self._thread_local.connection))
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        self._thread_local.last_used = time.time()
        return cast("PymssqlConnection", self._thread_local.connection)

    def _retire_connection(self, connection: PymssqlConnection) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._registry_lock:
            self._connection_registry.discard(connection)
            self._connection_threads.pop(connection, None)
        with contextlib.suppress(Exception):
            connection.close()

    def _close_thread_connection(self) -> None:
        thread_state = self._thread_local.__dict__
        if "connection" in thread_state:
            self._retire_connection(cast("PymssqlConnection", self._thread_local.connection))
            del self._thread_local.connection
            if "created_at" in thread_state:
                del self._thread_local.created_at
            if "last_used" in thread_state:
                del self._thread_local.last_used
