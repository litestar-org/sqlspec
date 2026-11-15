"""SAP SQL Anywhere thread-safe connection pool."""

import asyncio
import contextlib
import logging
import threading
import time
import weakref
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlanywhere._typing import SQLANYDB_MODULE, SqlanywhereConnection
from sqlspec.exceptions import OperationalError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("SqlanywhereConnectionPool",)

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "sqlanywhere"
_MAX_IDLE_TASK_CONNECTIONS: Final[int] = 8


@mypyc_attr(allow_interpreted_subclasses=False)
class SqlanywhereConnectionPool:
    """Thread-local connection manager for SAP SQL Anywhere over sqlanydb."""

    __slots__ = (
        "_checked_out",
        "_connection_parameters",
        "_connection_registry",
        "_connection_threads",
        "_generation",
        "_health_check_interval",
        "_idle_task_connections",
        "_max_size",
        "_min_size",
        "_on_connection_create",
        "_pool_id",
        "_recycle_seconds",
        "_registry_lock",
        "_task_holders",
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
        self._task_holders: weakref.WeakKeyDictionary[asyncio.Task[Any], _TaskConnectionHolder] = (
            weakref.WeakKeyDictionary()
        )
        self._idle_task_connections: list[tuple[SqlanywhereConnection, float, float]] = []
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
        connection = SQLANYDB_MODULE.connect(**self._connection_parameters)
        if self._on_connection_create is not None:
            try:
                self._on_connection_create(connection)
            except BaseException:
                with contextlib.suppress(Exception):
                    connection.close()
                raise
        return connection

    @contextmanager
    def get_connection(self) -> "Generator[SqlanywhereConnection, None, None]":
        """Yield a thread-local SQL Anywhere connection."""
        connection = self.acquire()
        body_failed = False
        try:
            yield connection
        except BaseException:
            body_failed = True
            if self._current_holder().depth <= 1:
                with contextlib.suppress(Exception):
                    self._close_thread_connection()
            raise
        finally:
            if body_failed:
                with contextlib.suppress(Exception):
                    self.release(connection)
            else:
                self.release(connection)

    def close(self) -> None:
        """Close every connection opened by this pool."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            self._connection_registry.clear()
            self._connection_threads.clear()
            self._idle_task_connections.clear()
            self._task_holders.clear()
            self._checked_out = 0
            self._generation += 1
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()

    def acquire(self) -> SqlanywhereConnection:
        """Acquire a healthy thread-local connection."""
        connection = self._get_thread_connection()
        self._current_holder().depth += 1
        with self._registry_lock:
            self._checked_out += 1
        return connection

    def release(self, connection: SqlanywhereConnection) -> None:
        """Release a checked-out connection back to the thread-local slot."""
        if self._current_holder().connection is connection:
            holder = self._current_holder()
            holder.depth = max(0, holder.depth - 1)
        try:
            self._rollback_on_checkin(connection)
        finally:
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

    def _current_holder(self) -> "_TaskConnectionHolder":
        task = _current_async_task()
        if task is None:
            holder = getattr(self._thread_local, "holder", None)
            if holder is None:
                holder = _TaskConnectionHolder(self._generation)
                self._thread_local.holder = holder
            return cast("_TaskConnectionHolder", holder)
        with self._registry_lock:
            holder = self._task_holders.get(task)
            created = holder is None
            if holder is None:
                holder = _TaskConnectionHolder(self._generation)
                self._task_holders[task] = holder
        if created:
            task.add_done_callback(self._retire_task_holder)
        return holder

    def _retire_task_holder(self, task: "asyncio.Task[Any]") -> None:
        with self._registry_lock:
            holder = self._task_holders.pop(task, None)
        if holder is None or holder.connection is None:
            return
        connection = holder.connection
        holder.connection = None
        if holder.depth == 0 and holder.generation == self._generation and not _in_transaction(connection):
            with self._registry_lock:
                if (
                    holder.generation == self._generation
                    and len(self._idle_task_connections) < _MAX_IDLE_TASK_CONNECTIONS
                ):
                    self._idle_task_connections.append((connection, holder.created_at, holder.last_used))
                    return
        self._retire_connection(connection)

    def _claim_idle_connection(self, holder: "_TaskConnectionHolder") -> bool:
        with self._registry_lock:
            if not self._idle_task_connections:
                return False
            connection, created_at, last_used = self._idle_task_connections.pop()
            self._connection_threads[connection] = threading.current_thread()
        holder.connection = connection
        holder.created_at = created_at
        holder.last_used = last_used
        return True

    def _get_thread_connection(self) -> SqlanywhereConnection:
        now = time.time()
        holder = self._current_holder()
        if holder.connection is not None and holder.generation != self._generation:
            self._close_thread_connection()
        holder.generation = self._generation

        if holder.connection is None and not self._claim_idle_connection(holder):
            holder.connection = self._create_connection()
            holder.created_at = now
            holder.last_used = now
            return holder.connection

        conn = cast("SqlanywhereConnection", holder.connection)
        if self._recycle_seconds > 0 and now - holder.created_at >= self._recycle_seconds:
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
            holder.connection = self._create_connection()
            holder.created_at = now
            holder.last_used = now
            return holder.connection

        if (
            self._health_check_interval > 0
            and now - holder.last_used >= self._health_check_interval
            and not self._is_connection_alive(conn)
        ):
            self._close_thread_connection()
            holder.connection = self._create_connection()
            holder.created_at = now

        holder.last_used = now
        return cast("SqlanywhereConnection", holder.connection)

    def _rollback_on_checkin(self, connection: SqlanywhereConnection) -> None:
        """Roll back uncommitted work on the outermost checkin and retire the connection when rollback fails."""
        if self._current_holder().depth > 0:
            return
        if self._current_holder().connection is not connection:
            return
        assert connection is not None
        try:
            connection.rollback()
        except BaseException:
            self._close_thread_connection()
            raise

    def _retire_connection(self, connection: SqlanywhereConnection) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._registry_lock:
            self._connection_registry.discard(connection)
            self._connection_threads.pop(connection, None)
        with contextlib.suppress(Exception):
            connection.close()

    def _close_thread_connection(self) -> None:
        holder = self._current_holder()
        conn = holder.connection
        if conn is not None:
            self._retire_connection(conn)
            holder.connection = None
            holder.created_at = 0.0
            holder.last_used = 0.0
        holder.depth = 0


@mypyc_attr(allow_interpreted_subclasses=False)
class _TaskConnectionHolder:
    """Connection state owned by one asyncio task or, outside a task, one thread."""

    __slots__ = ("connection", "created_at", "depth", "generation", "last_used")

    def __init__(self, generation: int) -> None:
        self.connection: SqlanywhereConnection | None = None
        self.created_at = 0.0
        self.depth = 0
        self.generation = generation
        self.last_used = 0.0


def _current_async_task() -> "asyncio.Task[Any] | None":
    """Return the active asyncio Task when running inside an event loop, otherwise None."""
    loop = asyncio._get_running_loop()
    return asyncio.current_task(loop) if loop is not None else None


def _in_transaction(connection: SqlanywhereConnection) -> bool:
    return bool(getattr(connection, "in_transaction", False))
