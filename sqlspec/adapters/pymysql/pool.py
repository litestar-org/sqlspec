"""PyMySQL database configuration with thread-local connections."""

import asyncio
import contextlib
import logging
import threading
import time
import weakref
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.pymysql._typing import PyMysqlConnect, PyMysqlConnection
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("PyMysqlConnectionPool",)


logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "pymysql"
_pymysql_connect: "PyMysqlConnect" = cast("PyMysqlConnect", PyMysqlConnect)
_MAX_IDLE_TASK_CONNECTIONS: Final[int] = 8
_SERVER_STATUS_IN_TRANS: Final[int] = 1
_COM_RESET_CONNECTION: Final[int] = 0x1F


@mypyc_attr(allow_interpreted_subclasses=False)
class PyMysqlConnectionPool:
    """Thread-local connection manager for PyMySQL.

    Inside a running event loop each asyncio task owns its own connection, and
    connections from finished tasks are parked for reuse.
    """

    __slots__ = (
        "_checked_out",
        "_connection_factory",
        "_connection_parameters",
        "_connection_registry",
        "_connection_threads",
        "_detached_holders",
        "_generation",
        "_health_check_interval",
        "_idle_task_connections",
        "_on_connection_create",
        "_pool_id",
        "_read_only",
        "_recycle_seconds",
        "_registry_lock",
        "_reset_session_on_release",
        "_task_holders",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[PyMysqlConnection], None] | None" = None,
        connection_factory: "Callable[[], PyMysqlConnection] | None" = None,
        read_only: bool = False,
        reset_session_on_release: bool = False,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: PyMySQL connection parameters
            recycle_seconds: Connection recycle time in seconds (default 24h)
            health_check_interval: Seconds of idle time before running health check
            on_connection_create: Callback executed when connection is created
            connection_factory: Optional factory for custom connection creation
            read_only: Initialize each connection in session read-only mode
            reset_session_on_release: Send ``COM_RESET_CONNECTION`` and re-initialize the connection on release
        """
        self._connection_parameters = connection_parameters
        self._connection_factory = connection_factory
        self._read_only = read_only
        self._reset_session_on_release = reset_session_on_release
        self._thread_local = threading.local()
        self._connection_registry: set[PyMysqlConnection] = set()
        self._connection_threads: dict[PyMysqlConnection, threading.Thread] = {}
        self._task_holders: weakref.WeakKeyDictionary[asyncio.Task[Any], _TaskConnectionHolder] = (
            weakref.WeakKeyDictionary()
        )
        self._detached_holders: dict[PyMysqlConnection, _TaskConnectionHolder] = {}
        self._idle_task_connections: list[tuple[PyMysqlConnection, float, float]] = []
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

    def new_connection(self) -> PyMysqlConnection:
        """Open a standalone connection configured like a pooled one.

        The result is owned by the caller: it is not thread-local and is not
        tracked for pool shutdown.

        Returns:
            PyMysqlConnection: A newly opened, fully configured connection.
        """
        if self._connection_factory is not None:
            connection = self._connection_factory()
        else:
            connection = _pymysql_connect(**self._connection_parameters)

        try:
            self._initialize_connection(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                connection.close()
            raise

        return connection

    def _initialize_connection(self, connection: PyMysqlConnection) -> None:
        if self._read_only:
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION TRANSACTION READ ONLY")
        if self._on_connection_create is not None:
            self._on_connection_create(connection)

    def _reset_connection(self, connection: PyMysqlConnection) -> bool:
        """Reset server session state; return ``False`` when the connection must be replaced."""
        execute_command = getattr(connection, "_execute_command", None)
        read_ok_packet = getattr(connection, "_read_ok_packet", None)
        if not callable(execute_command) or not callable(read_ok_packet):
            return False
        execute_command(_COM_RESET_CONNECTION, b"")
        read_ok_packet()
        self._initialize_connection(connection)
        return True

    @contextmanager
    def get_connection(self) -> "Generator[PyMysqlConnection, None, None]":
        """Get a thread-local connection.

        Yields:
            A thread-local database connection.
        """
        connection = self.acquire()
        body_failed = False
        try:
            yield connection
        except BaseException:
            body_failed = True
            raise
        finally:
            if body_failed:
                with contextlib.suppress(Exception):
                    self.release(connection)
            else:
                self.release(connection)

    def close(self) -> None:
        """Close every connection this pool opened, on any thread or task."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            self._connection_registry.clear()
            self._connection_threads.clear()
            self._idle_task_connections.clear()
            self._task_holders.clear()
            self._detached_holders.clear()
            self._checked_out = 0
            self._generation += 1
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()

    def acquire(self) -> PyMysqlConnection:
        connection = self._get_thread_connection()
        self._current_holder().depth += 1
        with self._registry_lock:
            self._checked_out += 1
        return connection

    def release(self, connection: PyMysqlConnection) -> None:
        """Release connection back to the pool, sanitizing transactions.

        The release may run in a different task than the acquire; the checkout is matched to the holder that
        acquired it, and a finished task's connection is parked or closed once its last checkout is released.
        """
        holder = self._holder_for_connection(connection)
        if holder is not None:
            remaining = max(0, holder.depth - 1)
            holder.depth = remaining
            outermost = remaining == 0
        else:
            outermost = True
        try:
            if (
                getattr(getattr(connection, "_result", None), "unbuffered_active", False) is True
                or getattr(connection, "unbuffered_active", False) is True
            ):
                self._discard_checkout(connection, holder)
                return

            if outermost and not self._is_autocommit_enabled(connection):
                try:
                    connection.rollback()
                except BaseException:
                    self._discard_checkout(connection, holder)
                    raise

            if outermost and self._reset_session_on_release:
                try:
                    reset_done = self._reset_connection(connection)
                except Exception as exc:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "pool.connection.reset_failed",
                        adapter=_ADAPTER_NAME,
                        pool_id=self._pool_id,
                        database=self._database_name,
                        error_type=type(exc).__name__,
                    )
                    reset_done = False
                if not reset_done:
                    self._discard_checkout(connection, holder)
        finally:
            with self._registry_lock:
                self._checked_out = max(0, self._checked_out - 1)
            if holder is not None and outermost:
                self._finish_detached_holder(holder)

    def _discard_checkout(self, connection: PyMysqlConnection, holder: "_TaskConnectionHolder | None") -> None:
        """Close a released connection that cannot be reused, detaching it from its holder."""
        if holder is not None:
            self._close_holder_connection(holder)
        else:
            self._retire_connection(connection)

    def size(self) -> int:
        """Report total active connections managed by this pool."""
        self._prune_dead_thread_connections()
        with self._registry_lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        with self._registry_lock:
            return self._checked_out

    @staticmethod
    def _is_autocommit_enabled(connection: PyMysqlConnection) -> bool:
        """Return whether autocommit is active on a PyMySQL connection."""
        get_autocommit = getattr(connection, "get_autocommit", None)
        if callable(get_autocommit):
            return bool(get_autocommit())
        return bool(getattr(connection, "autocommit_mode", False))

    def _prune_dead_thread_connections(self) -> None:
        dead_connections: list[PyMysqlConnection] = []
        with self._registry_lock:
            for conn, owner_thread in list(self._connection_threads.items()):
                if not owner_thread.is_alive():
                    self._connection_registry.discard(conn)
                    self._connection_threads.pop(conn, None)
                    dead_connections.append(conn)
        for conn in dead_connections:
            with contextlib.suppress(Exception):
                conn.close()

    def _create_connection(self) -> PyMysqlConnection:
        self._prune_dead_thread_connections()
        connection = self.new_connection()

        with self._registry_lock:
            self._connection_registry.add(connection)
            self._connection_threads[connection] = threading.current_thread()

        return connection

    def _is_connection_alive(self, connection: PyMysqlConnection) -> bool:
        try:
            connection.ping(reconnect=False)
        except Exception:
            return False
        return True

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
        """Park or close a finished task's connection, deferring a still checked-out one until its release."""
        with self._registry_lock:
            holder = self._task_holders.pop(task, None)
        if holder is None or holder.connection is None:
            return
        connection = holder.connection
        if holder.depth > 0 and holder.generation == self._generation:
            with self._registry_lock:
                self._detached_holders[connection] = holder
            return
        holder.connection = None
        self._park_or_retire(connection, holder)

    def _park_or_retire(self, connection: PyMysqlConnection, holder: "_TaskConnectionHolder") -> None:
        """Park a finished task's released connection for reuse by a later task, or close it.

        A connection is parked only when its holder has no open checkout, so the outermost release has
        already rolled back, and the server status reports no open transaction. Parked connections leave
        the thread map, so pruning a finished loop thread never closes a handle a later task may claim.
        """
        if holder.depth == 0 and holder.generation == self._generation and not _is_in_transaction(connection):
            with self._registry_lock:
                if (
                    holder.generation == self._generation
                    and len(self._idle_task_connections) < _MAX_IDLE_TASK_CONNECTIONS
                ):
                    self._idle_task_connections.append((connection, holder.created_at, holder.last_used))
                    self._connection_threads.pop(connection, None)
                    return
        self._retire_connection(connection)

    def _holder_for_connection(self, connection: PyMysqlConnection) -> "_TaskConnectionHolder | None":
        """Return the holder that checked out ``connection``, even when another task releases it."""
        holder = self._current_holder()
        if holder.connection is connection:
            return holder
        with self._registry_lock:
            detached = self._detached_holders.get(connection)
            if detached is not None:
                return detached
            for task_holder in self._task_holders.values():
                if task_holder.connection is connection:
                    return task_holder
        return None

    def _finish_detached_holder(self, holder: "_TaskConnectionHolder") -> None:
        """Park or close a finished task's connection once its last checkout is released."""
        connection = holder.connection
        if connection is None or not self._detached_holders:
            return
        with self._registry_lock:
            if self._detached_holders.get(connection) is not holder:
                return
            del self._detached_holders[connection]
        holder.connection = None
        self._park_or_retire(connection, holder)

    def _claim_idle_connection(self, holder: "_TaskConnectionHolder") -> bool:
        with self._registry_lock:
            if not self._idle_task_connections:
                return False
            connection, created_at, last_used = self._idle_task_connections.pop()
        with self._registry_lock:
            self._connection_threads[connection] = threading.current_thread()
        holder.connection = connection
        holder.created_at = created_at
        holder.last_used = last_used
        return True

    def _get_thread_connection(self) -> PyMysqlConnection:
        holder = self._current_holder()
        if holder.generation != self._generation:
            stale = holder.connection
            holder.connection = None
            if stale is not None:
                self._retire_connection(stale)
            holder.depth = 0
            holder.generation = self._generation
        if holder.connection is None and not self._claim_idle_connection(holder):
            holder.connection = self._create_connection()
            holder.created_at = time.time()
            holder.last_used = time.time()
            return holder.connection

        connection = cast("PyMysqlConnection", holder.connection)
        if self._recycle_seconds > 0 and time.time() - holder.created_at > self._recycle_seconds:
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
            self._retire_connection(connection)
            holder.connection = self._create_connection()
            holder.created_at = time.time()
            holder.last_used = time.time()
            return holder.connection

        idle_time = time.time() - holder.last_used
        if idle_time > self._health_check_interval and not self._is_connection_alive(connection):
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
            self._retire_connection(connection)
            holder.connection = self._create_connection()
            holder.created_at = time.time()

        holder.last_used = time.time()
        return cast("PyMysqlConnection", holder.connection)

    def _retire_connection(self, connection: PyMysqlConnection) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._registry_lock:
            self._connection_registry.discard(connection)
            self._detached_holders.pop(connection, None)
            self._connection_threads.pop(connection, None)
        with contextlib.suppress(Exception):
            connection.close()

    def _close_thread_connection(self) -> None:
        """Close and detach the calling task's or thread's dedicated connection."""
        self._close_holder_connection(self._current_holder())

    def _close_holder_connection(self, holder: "_TaskConnectionHolder") -> None:
        """Close and detach the connection owned by ``holder``."""
        holder.depth = 0
        connection = holder.connection
        if connection is not None:
            holder.connection = None
            self._retire_connection(connection)
            holder.created_at = 0.0
            holder.last_used = 0.0


@mypyc_attr(allow_interpreted_subclasses=False)
class _TaskConnectionHolder:
    """Connection state owned by one asyncio task or, outside a task, one thread."""

    __slots__ = ("connection", "created_at", "depth", "generation", "last_used")

    def __init__(self, generation: int) -> None:
        self.connection: PyMysqlConnection | None = None
        self.created_at = 0.0
        self.depth = 0
        self.generation = generation
        self.last_used = 0.0


def _current_async_task() -> "asyncio.Task[Any] | None":
    """Return the active asyncio Task when running inside an event loop, otherwise None."""
    loop = asyncio._get_running_loop()
    return asyncio.current_task(loop) if loop is not None else None


def _is_in_transaction(connection: PyMysqlConnection) -> bool:
    return bool(int(getattr(connection, "server_status", 0) or 0) & _SERVER_STATUS_IN_TRANS)
