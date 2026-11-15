"""SQLite database configuration with thread-local connections."""

import asyncio
import contextlib
import logging
import threading
import time
import weakref
from collections.abc import Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import parse_qs, unquote, urlsplit

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite._typing import SqliteConnection
from sqlspec.adapters.sqlite._typing import sqlite_module as sqlite3
from sqlspec.adapters.sqlite.core import create_mapped_exception, end_transaction, is_read_only_sqlite_config
from sqlspec.exceptions import ImproperConfigurationError
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
_MAX_IDLE_TASK_CONNECTIONS: Final[int] = 8
_SESSION_BASELINE_PRAGMAS: Final[tuple[tuple[str, str], ...]] = (
    ("foreign_keys", "OFF"),
    ("query_only", "OFF"),
    ("trusted_schema", "ON"),
    ("cell_size_check", "OFF"),
    ("recursive_triggers", "OFF"),
)
_TEMP_OBJECT_DROP_ORDER: Final[tuple[str, ...]] = ("trigger", "view", "index", "table")


@mypyc_attr(allow_interpreted_subclasses=False)
class SqliteConnectionPool:
    """Thread-local connection manager for SQLite.

    SQLite connections aren't thread-safe, so we use thread-local storage
    to ensure each thread has its own connection. This is simpler and more
    efficient than a traditional pool for SQLite's constraints.
    """

    __slots__ = (
        "_anchor_connection",
        "_check_same_thread",
        "_checked_out_count",
        "_connection_depths",
        "_connection_identities",
        "_connection_parameters",
        "_connection_registry",
        "_enable_foreign_keys",
        "_enable_optimizations",
        "_generation",
        "_health_check_interval",
        "_idle_task_connections",
        "_idle_task_generation",
        "_is_memory_db",
        "_is_read_only",
        "_is_shared_memory",
        "_on_connection_create",
        "_pool_id",
        "_recycle_seconds",
        "_registry_lock",
        "_reset_session_on_release",
        "_runtime_setup",
        "_task_connections",
        "_task_holders",
        "_thread_connections",
        "_thread_local",
        "_write_lock",
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
        is_read_only: bool = False,
        serialize_writes: bool = False,
        reset_session_on_release: bool = False,
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
            is_read_only: Enforce read-only queries and omit write PRAGMAs.
            serialize_writes: Hold a reentrant writer lock throughout each lease.
            reset_session_on_release: Drop temp objects, detach databases, and restore baseline PRAGMAs when a
                connection is released.
        """
        if "check_same_thread" not in connection_parameters:
            connection_parameters = {**connection_parameters, "check_same_thread": False}
        self._is_read_only = is_read_only or is_read_only_sqlite_config(connection_parameters)
        self._write_lock = threading.RLock() if serialize_writes else None
        self._connection_parameters = connection_parameters
        self._check_same_thread = bool(connection_parameters.get("check_same_thread", False))
        self._is_memory_db, self._is_shared_memory = _classify_memory_mode(self._connection_parameters)
        self._anchor_connection: SqliteConnection | None = None
        self._thread_local = threading.local()
        self._connection_registry: set[SqliteConnection] = set()
        self._connection_identities: dict[SqliteConnection, object] = {}
        self._thread_connections: dict[threading.Thread, SqliteConnection] = {}
        self._task_holders: weakref.WeakKeyDictionary[asyncio.Task[Any], _TaskConnectionHolder] = (
            weakref.WeakKeyDictionary()
        )
        self._task_connections: dict[
            weakref.ref[asyncio.Task[Any]], tuple[SqliteConnection, _TaskConnectionHolder]
        ] = {}
        self._idle_task_connections: list[tuple[SqliteConnection, float, float]] = []
        self._idle_task_generation = 0
        self._checked_out_count = 0
        self._connection_depths: dict[SqliteConnection, int] = {}
        self._generation = 0
        self._registry_lock = threading.Lock()
        self._enable_optimizations = enable_optimizations
        self._enable_foreign_keys = enable_foreign_keys
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create
        self._runtime_setup = runtime_setup
        self._reset_session_on_release = reset_session_on_release
        self._pool_id = str(uuid4())[:8]

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        if self._is_memory_db:
            return ":memory:"
        return str(self._connection_parameters.get("database", ":memory:"))

    def new_connection(self) -> SqliteConnection:
        """Create a standalone connection configured like a pooled one.

        The result is owned by the caller: it is not thread-local and is not
        tracked for pool shutdown.

        Returns:
            SqliteConnection: A newly opened, fully configured connection.
        """
        try:
            connection = sqlite3.connect(**self._connection_parameters)
        except sqlite3.Error as exc:
            if getattr(exc, "sqlite_errorcode", 0) & 0xFF in (sqlite3.SQLITE_CANTOPEN, 26):
                raise create_mapped_exception(exc) from exc
            raise

        try:
            if self._enable_optimizations:
                if self._is_memory_db:
                    _execute_and_close(connection, "PRAGMA journal_mode = MEMORY")
                    _execute_and_close(connection, "PRAGMA synchronous = OFF")
                    _execute_and_close(connection, "PRAGMA temp_store = MEMORY")
                    _execute_and_close(connection, f"PRAGMA cache_size = {SQLITE_MEMORY_CACHE_SIZE}")
                elif not self._is_read_only:
                    _enable_wal(connection)
                    _execute_and_close(connection, "PRAGMA synchronous = NORMAL")

                _execute_and_close(connection, f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT}")

            if self._is_read_only:
                _execute_and_close(connection, "PRAGMA query_only = ON")

            if self._enable_foreign_keys:
                _execute_and_close(connection, "PRAGMA foreign_keys = ON")

            if self._runtime_setup is not None:
                _apply_runtime_setup(connection, self._runtime_setup)

            if self._on_connection_create is not None:
                self._on_connection_create(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                connection.close()
            raise

        return cast("SqliteConnection", connection)

    @contextmanager
    def get_connection(self) -> "Generator[SqliteConnection, None, None]":
        """Lease a connection, serializing writer leases when configured.

        Yields:
            A connection with its lease held until context exit.
        """
        with (
            self._write_lock if self._write_lock is not None else contextlib.nullcontext(),
            self._get_connection_unlocked() as connection,
        ):
            yield connection

    def close(self) -> None:
        """Close every connection this pool opened, on any thread."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            orphaned.extend(conn for conn, _, _ in self._idle_task_connections)
            self._connection_registry.clear()
            self._idle_task_connections.clear()
            self._connection_identities.clear()
            self._thread_connections.clear()
            self._task_connections.clear()
            self._task_holders.clear()
            self._connection_depths.clear()
            self._checked_out_count = 0
            anchor = self._anchor_connection
            self._anchor_connection = None
            self._generation += 1
            self._idle_task_generation = self._generation
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()
        if anchor is not None:
            with contextlib.suppress(Exception):
                anchor.close()

    def acquire(self) -> SqliteConnection:
        """Acquire a thread-local connection.

        Returns:
            SqliteConnection: A thread-local connection
        """
        if self._write_lock is not None:
            self._write_lock.acquire()
        try:
            return self._get_thread_connection()
        except BaseException:
            if self._write_lock is not None:
                self._write_lock.release()
            raise

    def release(self, connection: SqliteConnection) -> None:
        """Release a connection and roll back any uncommitted transaction.

        Args:
            connection: The connection to release
        """
        try:
            try:
                end_transaction(connection, commit=False)
            except Exception:
                self._retire_failed_connection(connection)
            else:
                self._reset_connection_or_retire(connection)
        finally:
            if self._write_lock is not None:
                self._write_lock.release()

    def size(self) -> int:
        """Get total number of active thread-local connections in pool."""
        self._prune_dead_threads()
        with self._registry_lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        """Get number of checked out connections."""
        with self._registry_lock:
            return self._checked_out_count

    def connection_token(self, connection: Any) -> object | None:
        """Return the weakref-capable live token for a pool-owned connection."""
        with self._registry_lock:
            return self._connection_identities.get(connection)

    @staticmethod
    def resolve_connection_token(connection: Any, *configs: Any) -> object | None:
        """Resolve the active pool connection token across primary and replica configs."""
        for config in configs:
            if config is None:
                continue
            pool = getattr(config, "connection_instance", None)
            if isinstance(pool, SqliteConnectionPool):
                token = pool.connection_token(connection)
                if token is not None:
                    return token
            replica_groups = getattr(config, "replica_groups", None)
            if isinstance(replica_groups, Mapping):
                for group in replica_groups.values():
                    for replica_config in getattr(group, "configs", ()):
                        replica_pool = getattr(replica_config, "connection_instance", None)
                        if isinstance(replica_pool, SqliteConnectionPool):
                            token = replica_pool.connection_token(connection)
                            if token is not None:
                                return token
        return None

    @contextmanager
    def _get_connection_unlocked(self) -> "Generator[SqliteConnection, None, None]":
        """Get a thread-local connection.

        Yields:
            SqliteConnection: A thread-local connection.
        """
        connection = self._get_thread_connection()
        with self._registry_lock:
            depth = self._connection_depths.get(connection, 0)
            self._connection_depths[connection] = depth + 1
            if depth == 0:
                self._checked_out_count += 1
        outermost = depth == 0
        try:
            yield connection
        except BaseException:
            if outermost:
                try:
                    end_transaction(connection, commit=False)
                except Exception:
                    self._retire_failed_connection(connection)
            raise
        else:
            if outermost:
                try:
                    end_transaction(connection, commit=True)
                except BaseException:
                    try:
                        end_transaction(connection, commit=False)
                    except Exception:
                        self._retire_failed_connection(connection)
                    raise
                self._reset_connection_or_retire(connection)
        finally:
            with self._registry_lock:
                remaining = self._connection_depths.get(connection, 1) - 1
                if remaining > 0:
                    self._connection_depths[connection] = remaining
                else:
                    self._connection_depths.pop(connection, None)
                if outermost and self._checked_out_count > 0:
                    self._checked_out_count -= 1

    def _reset_connection(self, connection: SqliteConnection) -> None:
        if not self._reset_session_on_release:
            return
        runtime_pragmas = self._runtime_setup.get("pragmas", ()) if self._runtime_setup is not None else ()
        _reset_session_state(
            connection,
            query_only=self._is_read_only,
            foreign_keys=self._enable_foreign_keys,
            busy_timeout=self._enable_optimizations,
            runtime_pragmas=runtime_pragmas,
        )

    def _reset_connection_or_retire(self, connection: SqliteConnection) -> None:
        try:
            self._reset_connection(connection)
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
            self._retire_failed_connection(connection)

    def _prune_dead_threads(self) -> None:
        """Prune connections owned by dead threads or completed asyncio tasks."""
        stale_conns: list[SqliteConnection] = []
        with self._registry_lock:
            stale_conns.extend(self._prune_dead_owners_locked())
        for conn in stale_conns:
            with contextlib.suppress(Exception):
                conn.close()

    def _prune_dead_owners_locked(self) -> "list[SqliteConnection]":
        """Pop dead threads and finished asyncio tasks while holding the registry lock."""
        stale_conns: list[SqliteConnection] = []
        dead_threads = [t for t in self._thread_connections if not t.is_alive()]
        for t in dead_threads:
            conn = self._thread_connections.pop(t, None)
            if conn is not None:
                self._connection_registry.discard(conn)
                self._connection_identities.pop(conn, None)
                stale_conns.append(conn)

        current_thread = threading.current_thread()
        if self._idle_task_generation != self._generation:
            for idle_conn, _, _ in self._idle_task_connections:
                self._connection_identities.pop(idle_conn, None)
                stale_conns.append(idle_conn)
            self._idle_task_connections.clear()
            self._idle_task_generation = self._generation

        dead_tasks = [
            tr
            for tr, (conn, _task_holder) in self._task_connections.items()
            if ((task := tr()) is None or task.done()) and self._connection_depths.get(conn, 0) == 0
        ]
        for tr in dead_tasks:
            entry = self._task_connections.pop(tr, None)
            task = tr()
            if task is not None:
                self._task_holders.pop(task, None)
            if entry is None:
                continue
            conn, task_holder = entry
            created_at = task_holder.created_at
            last_used = task_holder.last_used
            owner_thread = task_holder.owner_thread
            task_holder.connection = None
            task_holder.created_at = 0.0
            task_holder.last_used = 0.0
            if conn not in self._connection_registry:
                continue
            self._connection_registry.discard(conn)
            reusable = False
            if len(self._idle_task_connections) < _MAX_IDLE_TASK_CONNECTIONS and (
                owner_thread is current_thread or not self._check_same_thread
            ):
                try:
                    reusable = not getattr(conn, "in_transaction", False)
                except sqlite3.ProgrammingError:
                    reusable = False
            if reusable:
                self._idle_task_connections.append((conn, created_at, last_used))
            else:
                self._connection_identities.pop(conn, None)
                stale_conns.append(conn)
        return stale_conns

    def _create_connection(self) -> SqliteConnection:
        """Create a pool-owned connection and record it for shutdown."""
        connection = self.new_connection()
        with self._registry_lock:
            if (
                self._is_shared_memory
                and self._anchor_connection is None
                and isinstance(connection, sqlite3.Connection)
            ):
                with contextlib.suppress(Exception):
                    self._anchor_connection = sqlite3.connect(**self._connection_parameters)
            self._connection_registry.add(connection)
            self._connection_identities[connection] = _ConnectionIdentity()
        return connection

    def _is_connection_alive(self, connection: SqliteConnection) -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            _execute_and_close(connection, "SELECT 1")
        except Exception:
            return False
        return True

    def _get_connection_holder(self, task: "asyncio.Task[Any]") -> "_TaskConnectionHolder | None":
        with self._registry_lock:
            return self._task_holders.get(task)

    def _get_or_create_connection_holder(self, task: "asyncio.Task[Any]") -> "_TaskConnectionHolder":
        stale_conns: list[SqliteConnection] = []
        created = False
        with self._registry_lock:
            task_holder = self._task_holders.get(task)
            if task_holder is None:
                if not self._idle_task_connections and (self._thread_connections or self._task_connections):
                    stale_conns.extend(self._prune_dead_owners_locked())
                task_holder = _TaskConnectionHolder(self._generation)
                self._task_holders[task] = task_holder
                created = True
        if created:
            task.add_done_callback(self._retire_finished_task)
        for conn in stale_conns:
            with contextlib.suppress(Exception):
                conn.close()
        return task_holder

    def _retire_finished_task(self, task: "asyncio.Task[Any]") -> None:
        stale_conn: SqliteConnection | None = None
        current_thread = threading.current_thread()
        with self._registry_lock:
            self._task_holders.pop(task, None)
            entry = self._task_connections.pop(weakref.ref(task), None)
            if entry is None:
                return
            conn, task_holder = entry
            if self._connection_depths.get(conn, 0) > 0:
                self._task_connections[weakref.ref(task)] = entry
                self._task_holders[task] = task_holder
                return
            created_at = task_holder.created_at
            last_used = task_holder.last_used
            owner_thread = task_holder.owner_thread
            task_holder.connection = None
            task_holder.created_at = 0.0
            task_holder.last_used = 0.0
            if conn not in self._connection_registry:
                return
            self._connection_registry.discard(conn)
            reusable = False
            if (
                self._idle_task_generation == self._generation
                and len(self._idle_task_connections) < _MAX_IDLE_TASK_CONNECTIONS
                and (owner_thread is current_thread or not self._check_same_thread)
            ):
                try:
                    reusable = not getattr(conn, "in_transaction", False)
                except sqlite3.ProgrammingError:
                    reusable = False
            if reusable:
                self._idle_task_connections.append((conn, created_at, last_used))
            else:
                self._connection_identities.pop(conn, None)
                stale_conn = conn
        if stale_conn is not None:
            with contextlib.suppress(Exception):
                stale_conn.close()

    def _register_owner(
        self,
        conn: SqliteConnection,
        task: "asyncio.Task[Any] | None",
        task_holder: "_TaskConnectionHolder | None" = None,
    ) -> None:
        with self._registry_lock:
            if task is None:
                self._thread_connections[threading.current_thread()] = conn
            elif task_holder is not None:
                self._task_connections[weakref.ref(task)] = (conn, task_holder)

    def _discard_idle_connection(self, conn: SqliteConnection) -> None:
        with self._registry_lock:
            self._connection_identities.pop(conn, None)
        with contextlib.suppress(Exception):
            conn.close()

    def _reuse_idle_task_connection(
        self, now: float, expected_generation: int, task: "asyncio.Task[Any]", task_holder: "_TaskConnectionHolder"
    ) -> "tuple[SqliteConnection, float] | None":
        while True:
            stale: list[tuple[SqliteConnection, float, float]] = []
            with self._registry_lock:
                if self._idle_task_generation != self._generation:
                    stale = list(self._idle_task_connections)
                    self._idle_task_connections.clear()
                    self._idle_task_generation = self._generation
                    for stale_conn, _, _ in stale:
                        self._connection_identities.pop(stale_conn, None)
                if expected_generation != self._generation or not self._idle_task_connections:
                    idle_entry = None
                else:
                    idle_entry = self._idle_task_connections.pop()
                    if (
                        not (self._recycle_seconds > 0 and (now - idle_entry[1]) > self._recycle_seconds)
                        and not self._check_same_thread
                        and (now - idle_entry[2]) <= self._health_check_interval
                    ):
                        conn, created_at, _ = idle_entry
                        self._connection_registry.add(conn)
                        if conn not in self._connection_identities:
                            self._connection_identities[conn] = _ConnectionIdentity()
                        self._task_connections[weakref.ref(task)] = (conn, task_holder)
                        return conn, created_at
            for stale_conn, _, _ in stale:
                with contextlib.suppress(Exception):
                    stale_conn.close()
            if idle_entry is None:
                return None
            conn, created_at, last_used = idle_entry
            if self._recycle_seconds > 0 and (now - created_at) > self._recycle_seconds:
                self._discard_idle_connection(conn)
                continue
            if (
                (now - last_used) > self._health_check_interval or self._check_same_thread
            ) and not self._is_connection_alive(conn):
                self._discard_idle_connection(conn)
                continue
            with self._registry_lock:
                if expected_generation != self._generation:
                    self._connection_identities.pop(conn, None)
                    with contextlib.suppress(Exception):
                        conn.close()
                    return None
                self._connection_registry.add(conn)
                if conn not in self._connection_identities:
                    self._connection_identities[conn] = _ConnectionIdentity()
                self._task_connections[weakref.ref(task)] = (conn, task_holder)
                return conn, created_at

    def _bind_holder_connection(self, holder: Any, task: "asyncio.Task[Any] | None", now: float) -> SqliteConnection:
        if task is not None:
            task_holder = cast("_TaskConnectionHolder", holder)
            task_holder.owner_thread = threading.current_thread()
            reused = self._reuse_idle_task_connection(now, task_holder.generation, task, task_holder)
            if reused is not None:
                conn, created_at = reused
                task_holder.connection = conn
                task_holder.created_at = created_at
                task_holder.last_used = now
                return conn
        conn = self._create_connection()
        holder.connection = conn
        holder.created_at = now
        holder.last_used = now
        self._register_owner(conn, task, cast("_TaskConnectionHolder", holder) if task is not None else None)
        return conn

    def _get_thread_connection(self) -> SqliteConnection:
        """Get or create a connection for the current thread or active asyncio task."""
        task = _current_async_task()
        holder: Any = self._get_or_create_connection_holder(task) if task is not None else self._thread_local
        current_generation = getattr(holder, "generation", None)
        if current_generation != self._generation:
            stale = getattr(holder, "connection", None)
            if stale is not None:
                self._retire_connection(cast("SqliteConnection", stale))
                holder.connection = None
            holder.created_at = 0.0
            holder.last_used = 0.0
            holder.generation = self._generation

        conn = getattr(holder, "connection", None)
        now = time.time()
        if conn is None:
            return self._bind_holder_connection(holder, task, now)

        created_at = getattr(holder, "created_at", 0.0)
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
            return self._bind_holder_connection(holder, task, now)

        last_used = getattr(holder, "last_used", 0.0)
        idle_time = now - last_used
        if idle_time > self._health_check_interval and not self._is_connection_alive(cast("SqliteConnection", conn)):
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
            return self._bind_holder_connection(holder, task, now)

        holder.last_used = now
        return cast("SqliteConnection", conn)

    def _retire_connection(self, connection: SqliteConnection) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        to_clear: list[_TaskConnectionHolder] = []
        with self._registry_lock:
            self._connection_registry.discard(connection)
            self._connection_identities.pop(connection, None)
            dead_threads = [t for t, c in self._thread_connections.items() if c is connection]
            for t in dead_threads:
                self._thread_connections.pop(t, None)
            dead_tasks = [tr for tr, (c, _) in self._task_connections.items() if c is connection]
            for tr in dead_tasks:
                entry = self._task_connections.pop(tr, None)
                if entry is not None:
                    to_clear.append(entry[1])
        for task_holder in to_clear:
            task_holder.connection = None
            task_holder.created_at = 0.0
            task_holder.last_used = 0.0
        with contextlib.suppress(Exception):
            connection.close()

    def _retire_failed_connection(self, connection: SqliteConnection) -> None:
        """Close a failed connection and clear the current thread-local or task task_holder."""
        task = _current_async_task()
        holder: Any = self._get_connection_holder(task) if task is not None else self._thread_local
        if holder is not None and getattr(holder, "connection", None) is connection:
            self._close_thread_connection()
        else:
            self._retire_connection(connection)

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread or active asyncio task."""
        task = _current_async_task()
        holder: Any = self._get_connection_holder(task) if task is not None else self._thread_local
        conn = getattr(holder, "connection", None) if holder is not None else None
        if conn is not None and holder is not None:
            self._retire_connection(cast("SqliteConnection", conn))
            holder.connection = None
            holder.created_at = 0.0
            holder.last_used = 0.0


@mypyc_attr(allow_interpreted_subclasses=False)
class _TaskConnectionHolder:
    """Per-task connection task_holder isolating concurrent asyncio tasks on one thread."""

    __slots__ = ("connection", "created_at", "generation", "last_used", "owner_thread")

    def __init__(self, generation: int) -> None:
        self.connection: SqliteConnection | None = None
        self.created_at: float = 0.0
        self.last_used: float = 0.0
        self.generation: int = generation
        self.owner_thread: threading.Thread = threading.current_thread()


@mypyc_attr(native_class=False)
class _ConnectionIdentity:
    """Weakref-capable identity token bound to the lifetime of one pooled connection."""

    __slots__ = ("__weakref__",)


def _current_async_task() -> "asyncio.Task[Any] | None":
    """Return the active asyncio Task when running inside an event loop, otherwise None."""
    loop = asyncio._get_running_loop()
    return asyncio.current_task(loop) if loop is not None else None


def _classify_memory_mode(connection_parameters: "Mapping[str, Any]") -> "tuple[bool, bool]":
    """Classify whether SQLite connection parameters target a memory and shared-cache database."""
    database = str(connection_parameters.get("database", ":memory:"))
    if database == ":memory:":
        return True, False
    if not connection_parameters.get("uri") or not database.startswith("file:"):
        return False, False
    parts = urlsplit(database)
    query = parse_qs(parts.query)
    is_memory_db = unquote(parts.path) == ":memory:" or "memory" in query.get("mode", [])
    is_shared_memory = is_memory_db and "shared" in query.get("cache", [])
    return is_memory_db, is_shared_memory


def _reset_session_state(
    connection: "SqliteConnection", *, query_only: bool, foreign_keys: bool, busy_timeout: bool, runtime_pragmas: "Any"
) -> None:
    """Remove session state a previous lease left on a pooled connection.

    Args:
        connection: Connection whose lease just ended.
        query_only: Whether the pool enforces read-only queries.
        foreign_keys: Whether the pool enables foreign-key enforcement.
        busy_timeout: Whether the pool applies its busy-timeout optimization.
        runtime_pragmas: Configured ``(name, value)`` PRAGMA pairs applied after the baseline.
    """
    original_factory = connection.row_factory
    connection.row_factory = None
    try:
        for _, name, _file in connection.execute("PRAGMA database_list").fetchall():
            if name not in {"main", "temp"}:
                _execute_and_close(connection, f"DETACH DATABASE {_quote_identifier(name)}")
        temp_objects = connection.execute(
            "SELECT type, name FROM temp.sqlite_master WHERE name NOT LIKE 'sqlite!_%' ESCAPE '!'"
        ).fetchall()
        for object_type in _TEMP_OBJECT_DROP_ORDER:
            for found_type, name in temp_objects:
                if found_type == object_type:
                    _execute_and_close(
                        connection, f"DROP {object_type.upper()} IF EXISTS temp.{_quote_identifier(name)}"
                    )
        for pragma_name, pragma_value in _SESSION_BASELINE_PRAGMAS:
            _execute_and_close(connection, f"PRAGMA {pragma_name} = {pragma_value}")
        if busy_timeout:
            _execute_and_close(connection, f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT}")
        if query_only:
            _execute_and_close(connection, "PRAGMA query_only = ON")
        if foreign_keys:
            _execute_and_close(connection, "PRAGMA foreign_keys = ON")
        for pragma_name, pragma_value in runtime_pragmas:
            _execute_and_close(connection, f"PRAGMA {pragma_name} = {pragma_value}")
    finally:
        connection.row_factory = original_factory


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _execute_and_close(connection: "SqliteConnection", statement: str) -> None:
    cursor = connection.execute(statement)
    if cursor is not None:
        with contextlib.suppress(Exception):
            cursor.close()


def _attempt_wal_switch(connection: "SqliteConnection", attempt: int) -> bool:
    """Attempt a single WAL mode switch, returning True on success."""
    try:
        _execute_and_close(connection, "PRAGMA journal_mode = WAL")
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

    window_functions = runtime_setup.get("custom_window_functions", ())
    if window_functions:
        create_window_fn = getattr(connection, "create_window_function", None)
        if create_window_fn is None:
            msg = "Custom SQLite window functions require Python 3.11 or later"
            raise ImproperConfigurationError(msg)
        for window_config in window_functions:
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
