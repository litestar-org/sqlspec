"""SAP SQL Anywhere thread-safe connection pool."""

import contextlib
import logging
import threading
import time
from contextlib import contextmanager
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.sqlanywhere._typing import SqlanywhereConnection
from sqlspec.exceptions import MissingDependencyError, OperationalError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("SqlanywhereConnectionPool",)

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "sqlanywhere"


class SqlanywhereConnectionPool:
    """Thread-local connection manager for SAP SQL Anywhere over sqlanydb."""

    __slots__ = (
        "_checked_out",
        "_connection_parameters",
        "_connection_registry",
        "_connection_threads",
        "_generation",
        "_health_check_interval",
        "_max_size",
        "_min_size",
        "_on_connection_create",
        "_pool_id",
        "_recycle_seconds",
        "_registry_lock",
        "_thread_local",
        "_timeout",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        min_size: int = 1,
        max_size: int = 10,
        timeout: float = 30.0,
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[SqlanywhereConnection], None] | None" = None,
    ) -> None:
        self._connection_parameters = dict(connection_parameters)
        self._min_size = min_size
        self._max_size = max_size
        self._timeout = timeout
        self._thread_local = threading.local()
        self._connection_registry: set[SqlanywhereConnection] = set()
        self._connection_threads: dict[SqlanywhereConnection, threading.Thread] = {}
        self._checked_out = 0
        self._generation = 0
        self._registry_lock = threading.Lock()
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create
        self._pool_id = str(uuid4())[:8]

    @property
    def _database_name(self) -> str:
        return str(self._connection_parameters.get("dbn", self._connection_parameters.get("eng", "unknown")))

    def new_connection(self) -> SqlanywhereConnection:
        """Open a standalone SAP SQL Anywhere connection via ``sqlanydb.connect``."""
        sqlanydb = _load_sqlanydb_module()
        connection = sqlanydb.connect(**self._connection_parameters)
        if self._on_connection_create is not None:
            self._on_connection_create(connection)
        return cast("SqlanywhereConnection", connection)

    @contextmanager
    def get_connection(self) -> "Generator[SqlanywhereConnection, None, None]":
        """Yield a thread-local SQL Anywhere connection."""
        connection = self.acquire()
        try:
            yield connection
        except Exception:
            with contextlib.suppress(Exception):
                self._close_thread_connection()
            raise
        finally:
            self.release(connection)

    def close(self) -> None:
        """Close every connection opened by this pool."""
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

    def acquire(self) -> SqlanywhereConnection:
        """Acquire a healthy thread-local connection."""
        connection = self._get_thread_connection()
        with self._registry_lock:
            self._checked_out += 1
        return connection

    def release(self, connection: SqlanywhereConnection) -> None:
        """Release a checked-out connection back to the thread-local slot."""
        _ = connection
        with self._registry_lock:
            if self._checked_out > 0:
                self._checked_out -= 1

    def size(self) -> int:
        """Return the number of active connections registered with this pool."""
        self._prune_dead_thread_connections()
        with self._registry_lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        """Return the number of currently checked-out connections."""
        with self._registry_lock:
            return self._checked_out

    def _prune_dead_thread_connections(self) -> None:
        with self._registry_lock:
            dead = [conn for conn, thread in self._connection_threads.items() if not thread.is_alive()]
            for conn in dead:
                self._connection_registry.discard(conn)
                self._connection_threads.pop(conn, None)
        for conn in dead:
            with contextlib.suppress(Exception):
                conn.close()

    def _create_connection(self) -> SqlanywhereConnection:
        self._prune_dead_thread_connections()
        with self._registry_lock:
            if self._max_size > 0 and len(self._connection_registry) >= self._max_size:
                msg = f"Connection pool exhausted (max_size={self._max_size})"
                raise OperationalError(msg)
        connection = self.new_connection()
        with self._registry_lock:
            if self._max_size > 0 and len(self._connection_registry) >= self._max_size:
                with contextlib.suppress(Exception):
                    connection.close()
                msg = f"Connection pool exhausted (max_size={self._max_size})"
                raise OperationalError(msg)
            self._connection_registry.add(connection)
            self._connection_threads[connection] = threading.current_thread()
        return connection

    def _is_connection_alive(self, connection: SqlanywhereConnection) -> bool:
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        except Exception:
            return False
        else:
            return True
        finally:
            if cursor is not None and hasattr(cursor, "close"):
                with contextlib.suppress(Exception):
                    cursor.close()

    def _get_thread_connection(self) -> SqlanywhereConnection:
        now = time.time()
        current_gen = self._generation
        conn = getattr(self._thread_local, "connection", None)
        created_at = getattr(self._thread_local, "created_at", 0.0)
        last_used = getattr(self._thread_local, "last_used", 0.0)
        conn_gen = getattr(self._thread_local, "generation", -1)

        if conn is not None and conn_gen != current_gen:
            self._close_thread_connection()
            conn = None

        if conn is None:
            conn = self._create_connection()
            self._thread_local.connection = conn
            self._thread_local.created_at = now
            self._thread_local.last_used = now
            self._thread_local.generation = current_gen
            return conn

        if self._recycle_seconds > 0 and now - created_at >= self._recycle_seconds:
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.recycle",
                adapter_name=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
                reason="exceeded_recycle_time",
            )
            self._close_thread_connection()
            conn = self._create_connection()
            self._thread_local.connection = conn
            self._thread_local.created_at = now
            self._thread_local.last_used = now
            self._thread_local.generation = current_gen
            return conn

        if (
            self._health_check_interval > 0
            and now - last_used >= self._health_check_interval
            and not self._is_connection_alive(conn)
        ):
            self._close_thread_connection()
            conn = self._create_connection()
            self._thread_local.connection = conn
            self._thread_local.created_at = now
            self._thread_local.generation = current_gen

        self._thread_local.last_used = now
        return cast("SqlanywhereConnection", conn)

    def _close_thread_connection(self) -> None:
        conn = getattr(self._thread_local, "connection", None)
        if conn is not None:
            with self._registry_lock:
                self._connection_registry.discard(conn)
                self._connection_threads.pop(conn, None)
            with contextlib.suppress(Exception):
                conn.close()
            self._thread_local.connection = None
            self._thread_local.created_at = 0.0
            self._thread_local.last_used = 0.0
            self._thread_local.generation = -1


def _load_sqlanydb_module() -> Any:
    """Import ``sqlanydb`` lazily or raise ``MissingDependencyError``."""
    try:
        return import_module("sqlanydb")
    except ImportError as exc:
        raise MissingDependencyError(package="sqlanydb", install_package="sqlanywhere") from exc
