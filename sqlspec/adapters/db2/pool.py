"""IBM Db2 connection pools: thread-local sync connections and a bounded asyncio pool."""

import asyncio
import contextlib
import inspect
import logging
import threading
import time
import weakref
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.db2 import _typing as typing_module
from sqlspec.adapters.db2._typing import Db2Error, ibm_db_dbi
from sqlspec.adapters.db2.core import build_dsn_string, create_mapped_exception
from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator
    from types import TracebackType

__all__ = ("Db2AsyncConnectionPool", "Db2AsyncPoolConnectionContext", "Db2SyncConnectionPool")

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "db2"
_MAX_IDLE_TASK_CONNECTIONS: Final[int] = 8
_IBM_DB_DBI = ibm_db_dbi


def _build_connection_options(base: dict[int, Any], autocommit_mode: int, read_only: bool) -> dict[int, Any]:
    """Prepare fresh native CLI attributes for one physical connection."""
    options = dict(base)
    options[typing_module.ibm_db_dbi.SQL_ATTR_AUTOCOMMIT] = autocommit_mode
    if read_only:
        options.setdefault(
            getattr(typing_module.ibm_db, "SQL_ATTR_ACCESS_MODE", 101),
            getattr(typing_module.ibm_db, "SQL_MODE_READ_ONLY", 1),
        )
    return options


def _set_current_schema(connection: Any, schema: str) -> None:
    """Set the SQL register and SDK catalog schema without CLI keyword quoting."""
    cursor = connection.cursor()
    pending_exception: BaseException | None = None
    try:
        cursor.execute("SET CURRENT SCHEMA = ?", (schema,))
    except BaseException as exc:
        pending_exception = exc
    try:
        cursor.close()
    except BaseException as exc:
        if pending_exception is None:
            pending_exception = exc
    if pending_exception is not None:
        raise pending_exception
    connection.set_current_schema(schema)


async def _set_current_schema_async(connection: Any, schema: str) -> None:
    """Set both schema states, closing the cursor outside the query's exception handler."""
    cursor = await connection.cursor()
    pending_exception: BaseException | None = None
    try:
        await cursor.execute("SET CURRENT SCHEMA = ?", (schema,))
    except BaseException as exc:
        pending_exception = exc
    try:
        await cursor.close()
    except BaseException as exc:
        if pending_exception is None:
            pending_exception = exc
    if pending_exception is not None:
        raise pending_exception
    await connection.set_current_schema(schema)


@mypyc_attr(allow_interpreted_subclasses=False)
class Db2SyncConnectionPool:
    """Thread-local connection manager for IBM Db2."""

    __slots__ = (
        "_autocommit",
        "_conn_options",
        "_connection_parameters",
        "_connection_registry",
        "_detached_holders",
        "_dsn",
        "_generation",
        "_health_check_interval",
        "_idle_task_connections",
        "_on_connection_create",
        "_pool_id",
        "_read_only",
        "_recycle_seconds",
        "_registry_lock",
        "_task_holders",
        "_thread_local",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[Any], None] | None" = None,
        *,
        conn_options: "dict[int, Any] | None" = None,
        read_only: bool | None = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: Normalized Db2 connection parameters. The CLI connection
                string is rendered from them once, here; ``autocommit`` (default True) sets the
                autocommit mode every new connection opens in.
            recycle_seconds: Connection recycle time in seconds (default 24h).
            health_check_interval: Seconds of idle time before running health check.
            on_connection_create: Callback executed when connection is created.
            conn_options: Native CLI attributes copied for each physical connection.
            read_only: Override configured native read-only access mode.
        """
        self._connection_parameters = connection_parameters
        native_parameters = dict(connection_parameters)
        native_parameters.pop("current_schema", None)
        self._dsn = build_dsn_string(native_parameters)
        self._autocommit = bool(connection_parameters.get("autocommit", True))
        self._conn_options = dict(conn_options or {})
        self._read_only = bool(connection_parameters.get("read_only", False)) if read_only is None else read_only
        self._thread_local = threading.local()
        self._connection_registry: set[Any] = set()
        self._task_holders: weakref.WeakKeyDictionary[asyncio.Task[Any], _TaskConnectionHolder] = (
            weakref.WeakKeyDictionary()
        )
        self._detached_holders: dict[Any, _TaskConnectionHolder] = {}
        self._idle_task_connections: list[tuple[Any, float, float]] = []
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

    def new_connection(self) -> Any:
        """Open a standalone connection configured like a pooled one.

        The connection opens in the pool's autocommit mode.

        The schema is applied with a bound statement because CLI keyword parsing cannot preserve
        semicolon-bearing schema values. The SDK catalog schema is synchronized before the hook.

        The result is owned by the caller: it is not thread-local and is not
        tracked for pool shutdown.

        Returns:
            Any: A newly opened, fully configured Db2 connection.

        Raises:
            MissingDependencyError: When ibm_db is not installed.
        """
        autocommit_mode = (
            typing_module.ibm_db_dbi.SQL_AUTOCOMMIT_ON
            if self._autocommit
            else typing_module.ibm_db_dbi.SQL_AUTOCOMMIT_OFF
        )
        options = _build_connection_options(self._conn_options, autocommit_mode, self._read_only)
        try:
            connection = typing_module.ibm_db_dbi.connect(self._dsn, "", "", "", "", options)
        except Db2Error as exc:
            raise create_mapped_exception(exc) from exc

        try:
            schema = self._connection_parameters.get("current_schema")
            if schema is not None:
                _set_current_schema(connection, schema)
            if self._on_connection_create is not None:
                self._on_connection_create(connection)
        except BaseException as exc:
            with contextlib.suppress(BaseException):
                connection.close()
            if isinstance(exc, Db2Error):
                raise create_mapped_exception(exc) from exc
            raise

        return connection

    @contextmanager
    def get_connection(self) -> "Generator[Any, None, None]":
        """Context manager to yield a thread-local connection.

        Yields:
            A thread-local Db2 database connection.
        """
        connection = self.acquire()
        body_failed = False
        try:
            yield connection
        except BaseException:
            body_failed = True
            holder = self._holder_for_connection(connection)
            if holder is not None and holder.depth <= 1:
                with contextlib.suppress(Exception):
                    self._close_holder_connection(holder)
            raise
        finally:
            if body_failed:
                with contextlib.suppress(Exception):
                    self.release(connection)
            else:
                self.release(connection)

    def close(self) -> None:
        """Close every connection this pool opened across all threads."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            self._connection_registry.clear()
            self._idle_task_connections.clear()
            self._task_holders.clear()
            self._detached_holders.clear()
            self._generation += 1
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()

    def acquire(self) -> Any:
        """Acquire a thread-local connection."""
        try:
            connection = self._get_thread_connection()
        except Db2Error as exc:
            raise create_mapped_exception(exc) from exc
        self._current_holder().depth += 1
        return connection

    def release(self, connection: Any) -> None:
        """Release a checked-out connection, rolling back open work on the outermost checkin.

        The release may run in a different task than the acquire; the checkout is matched to the holder that
        acquired it, and a finished task's connection is parked or closed once its last checkout is released.
        """
        holder = self._holder_for_connection(connection)
        if holder is not None:
            holder.depth = max(0, holder.depth - 1)
        try:
            self._rollback_on_checkin(connection, holder)
        finally:
            if holder is not None and holder.depth == 0:
                self._finish_detached_holder(holder)

    def size(self) -> int:
        """Return the total number of active connections registered across all threads."""
        with self._registry_lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        """Return the number of checked out connections from the perspective of this thread."""
        return 1 if self._current_holder().connection is not None else 0

    def _create_connection(self) -> Any:
        """Open a new connection and register it in the shutdown registry."""
        connection = self.new_connection()

        with self._registry_lock:
            self._connection_registry.add(connection)

        return connection

    def _is_connection_alive(self, connection: Any) -> bool:
        """Perform a lightweight ping to verify that the physical connection is alive.

        Uses SYSIBM.SYSDUMMY1 as standard dummy table across IBM Db2 platforms.
        """
        try:
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
                cursor.fetchone()
            finally:
                cursor.close()
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

    def _park_or_retire(self, connection: Any, holder: "_TaskConnectionHolder") -> None:
        """Park a finished task's released connection for reuse by a later task, or close it.

        A connection is parked only when its holder has no open checkout. The DB-API connection exposes no
        transaction probe, so parking relies on the outermost release having rolled back open work.
        """
        if holder.depth == 0 and holder.generation == self._generation:
            with self._registry_lock:
                if (
                    holder.generation == self._generation
                    and len(self._idle_task_connections) < _MAX_IDLE_TASK_CONNECTIONS
                ):
                    self._idle_task_connections.append((connection, holder.created_at, holder.last_used))
                    return
        self._retire_connection(connection)

    def _holder_for_connection(self, connection: Any) -> "_TaskConnectionHolder | None":
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
        holder.connection = connection
        holder.created_at = created_at
        holder.last_used = last_used
        return True

    def _get_thread_connection(self) -> Any:
        """Retrieve, recycle, or validate the connection assigned to the calling thread."""
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
            self._retire_connection(holder.connection)
            holder.connection = self._create_connection()
            holder.created_at = time.time()
            holder.last_used = time.time()
            return holder.connection

        idle_time = time.time() - holder.last_used
        if idle_time > self._health_check_interval and not self._is_connection_alive(holder.connection):
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
            self._retire_connection(holder.connection)
            holder.connection = self._create_connection()
            holder.created_at = time.time()

        holder.last_used = time.time()
        return holder.connection

    def _retire_connection(self, connection: Any) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._registry_lock:
            self._connection_registry.discard(connection)
            self._detached_holders.pop(connection, None)
        with contextlib.suppress(Exception):
            connection.close()

    def _rollback_on_checkin(self, connection: Any, holder: "_TaskConnectionHolder | None") -> None:
        """Roll back uncommitted work on outermost checkin and retire closed or broken connections."""
        if holder is None or holder.depth > 0 or holder.connection is not connection:
            return
        if getattr(connection, "conn_handler", object()) is None:
            self._close_holder_connection(holder)
            return
        if self._autocommit:
            return
        try:
            connection.rollback()
        except BaseException:
            self._close_holder_connection(holder)
            raise

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
        self.connection: Any = None
        self.created_at = 0.0
        self.depth = 0
        self.generation = generation
        self.last_used = 0.0


def _current_async_task() -> "asyncio.Task[Any] | None":
    """Return the active asyncio Task when running inside an event loop, otherwise None."""
    loop = asyncio._get_running_loop()
    return asyncio.current_task(loop) if loop is not None else None


class _Db2PooledConnection:
    """Idle-stack record for one pooled async connection."""

    __slots__ = ("connection", "created_at", "last_used")

    def __init__(self, connection: Any, created_at: float, last_used: float) -> None:
        self.connection = connection
        self.created_at = created_at
        self.last_used = last_used


@mypyc_attr(allow_interpreted_subclasses=False)
class Db2AsyncPoolConnectionContext:
    """Async context manager that checks a connection out of a ``Db2AsyncConnectionPool``."""

    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "Db2AsyncConnectionPool") -> None:
        """Initialize the context manager.

        Args:
            pool: Pool to acquire from and release to.
        """
        self._pool = pool
        self._connection: Any = None

    async def __aenter__(self) -> Any:
        """Acquire a pooled connection.

        Returns:
            Any: The checked-out ``ibm_db_dbi.AsyncConnection``.
        """
        self._connection = await self._pool.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        """Release the connection back to the pool."""
        connection = self._connection
        self._connection = None
        if exc_type is not None:
            with contextlib.suppress(Exception):
                await self._pool.release(connection)
        else:
            await self._pool.release(connection)


@mypyc_attr(allow_interpreted_subclasses=False)
class Db2AsyncConnectionPool:
    """Bounded asyncio pool of ``ibm_db_dbi.AsyncConnection`` objects.

    At most ``max_size`` connections are checked out or being opened at once; callers waiting
    longer than ``acquire_timeout`` get ``ConnectionTimeoutError``. Idle connections are reused
    most-recently-released first, replaced once older than ``recycle_seconds``, and pinged when
    idle for longer than ``health_check_interval``. Closing the pool closes idle connections
    immediately and checked-out connections when they are released.
    """

    __slots__ = (
        "_acquire_timeout",
        "_autocommit",
        "_checked_out",
        "_closed",
        "_conn_options",
        "_connection_parameters",
        "_dsn",
        "_health_check_interval",
        "_idle",
        "_max_size",
        "_on_connection_create",
        "_pool_id",
        "_read_only",
        "_recycle_seconds",
        "_semaphore_instance",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        *,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        recycle_seconds: int = 86400,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[Any], Awaitable[None] | None] | None" = None,
        conn_options: "dict[int, Any] | None" = None,
        read_only: bool | None = None,
    ) -> None:
        """Initialize the pool.

        Args:
            connection_parameters: Normalized Db2 connection parameters. The CLI connection
                string is rendered from them once, here; ``autocommit`` (default True) sets the
                autocommit mode every new connection opens in.
            max_size: Maximum number of connections checked out or being opened at once.
            acquire_timeout: Seconds to wait for a free slot before raising.
            recycle_seconds: Connection age in seconds after which it is replaced (0 disables).
            health_check_interval: Seconds of idle time before a connection is pinged on reuse.
            on_connection_create: Callback run on every new connection; awaited when it returns
                an awaitable.
            conn_options: Native CLI attributes copied for each physical connection.
            read_only: Override configured native read-only access mode.
        """
        self._connection_parameters = connection_parameters
        native_parameters = dict(connection_parameters)
        native_parameters.pop("current_schema", None)
        self._dsn = build_dsn_string(native_parameters)
        self._autocommit = bool(connection_parameters.get("autocommit", True))
        self._conn_options = dict(conn_options or {})
        self._read_only = bool(connection_parameters.get("read_only", False)) if read_only is None else read_only
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create
        self._idle: list[_Db2PooledConnection] = []
        self._checked_out: dict[int, _Db2PooledConnection] = {}
        self._closed = False
        self._semaphore_instance: asyncio.Semaphore | None = None
        self._pool_id = str(uuid4())[:8]

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        """Return the capacity semaphore, creating it on first use inside the running loop."""
        if self._semaphore_instance is None:
            self._semaphore_instance = asyncio.Semaphore(self._max_size)
        return self._semaphore_instance

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        return str(self._connection_parameters.get("database", "unknown"))

    async def new_connection(self) -> Any:
        """Open a standalone connection configured like a pooled one.

        The connection opens in the pool's autocommit mode and the creation hook runs on it. The
        result is owned by the caller and is not tracked by the pool.

        The schema is applied with a bound statement and synchronized with the SDK catalog schema
        before the hook, avoiding the CLI keyword parser's treatment of semicolons.

        Returns:
            Any: A newly opened ``ibm_db_dbi.AsyncConnection``.

        Raises:
            MissingDependencyError: When ibm_db is not installed.
        """
        autocommit_mode = (
            typing_module.ibm_db_dbi.SQL_AUTOCOMMIT_ON
            if self._autocommit
            else typing_module.ibm_db_dbi.SQL_AUTOCOMMIT_OFF
        )
        options = _build_connection_options(self._conn_options, autocommit_mode, self._read_only)
        try:
            connection = await typing_module.ibm_db_dbi.AsyncConnection.connect(self._dsn, "", "", "", "", options)
        except Db2Error as exc:
            raise create_mapped_exception(exc) from exc
        setup_error: BaseException | None = None
        try:
            schema = self._connection_parameters.get("current_schema")
            if schema is not None:
                await _set_current_schema_async(connection, schema)
            if self._on_connection_create is not None:
                result = self._on_connection_create(connection)
                if inspect.isawaitable(result):
                    await result
        except BaseException as exc:
            setup_error = exc
        if setup_error is not None:
            with contextlib.suppress(BaseException):
                await connection.close()
            if isinstance(setup_error, Db2Error):
                raise create_mapped_exception(setup_error) from setup_error
            raise setup_error
        return connection

    async def acquire(self) -> Any:
        """Check a connection out of the pool.

        Returns:
            Any: A pooled ``ibm_db_dbi.AsyncConnection``.

        Raises:
            DatabaseConnectionError: When the pool is closed.
            ConnectionTimeoutError: When no slot frees up within ``acquire_timeout`` seconds.
        """
        if self._closed:
            msg = "Db2 async connection pool is closed"
            raise DatabaseConnectionError(msg)
        semaphore = self._semaphore
        try:
            await asyncio.wait_for(semaphore.acquire(), self._acquire_timeout)
        except TimeoutError as exc:
            msg = f"Timed out after {self._acquire_timeout}s waiting for a Db2 connection"
            raise ConnectionTimeoutError(msg) from exc
        if self._closed:
            semaphore.release()
            msg = "Db2 async connection pool is closed"
            raise DatabaseConnectionError(msg)
        try:
            record = await self._checkout()
        except BaseException as exc:
            semaphore.release()
            if isinstance(exc, Db2Error):
                raise create_mapped_exception(exc) from exc
            raise
        if self._closed:
            try:
                await self._close_connection(record.connection)
            finally:
                semaphore.release()
            msg = "Db2 async connection pool is closed"
            raise DatabaseConnectionError(msg)
        self._checked_out[id(record.connection)] = record
        return record.connection

    async def release(self, connection: Any) -> None:
        """Return a checked-out connection to the pool.

        Connections the pool did not hand out are ignored. After ``close()`` the connection is
        closed instead of being kept.

        Args:
            connection: Connection previously returned by ``acquire()``.
        """
        conn_id = id(connection)
        if conn_id not in self._checked_out:
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.release.unknown",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
            )
            return
        if not self._closed:
            if getattr(connection, "conn_handler", object()) is None:
                await self.discard(connection)
                return
            if not self._autocommit:
                rollback_error: BaseException | None = None
                try:
                    await connection.rollback()
                except BaseException as exc:
                    rollback_error = exc
                if rollback_error is not None:
                    await self.discard(connection)
                    raise rollback_error
        record = self._checked_out.pop(conn_id, None)
        if record is None:
            return
        try:
            if self._closed:
                await self._close_connection(connection)
            else:
                record.last_used = time.monotonic()
                self._idle.append(record)
        finally:
            if self._semaphore_instance is not None:
                self._semaphore_instance.release()
                if self._closed and not self._checked_out:
                    self._semaphore_instance = None

    async def discard(self, connection: Any) -> None:
        """Close a checked-out connection instead of returning it to the pool.

        The pool slot is freed even when closing fails, so the connection is never reused.

        Args:
            connection: Connection previously returned by ``acquire()``.
        """
        record = self._checked_out.pop(id(connection), None)
        try:
            await connection.close()
        finally:
            if record is not None and self._semaphore_instance is not None:
                self._semaphore_instance.release()
                if self._closed and not self._checked_out:
                    self._semaphore_instance = None

    def get_connection(self) -> "Db2AsyncPoolConnectionContext":
        """Return an async context manager that acquires and releases a pooled connection.

        Returns:
            Db2AsyncPoolConnectionContext: The connection context manager.
        """
        return Db2AsyncPoolConnectionContext(self)

    async def close(self) -> None:
        """Close the pool and every idle connection.

        Checked-out connections are closed when they are released.
        """
        self._closed = True
        idle = self._idle
        self._idle = []
        for record in idle:
            await self._close_connection(record.connection)
        if not self._checked_out:
            self._semaphore_instance = None

    def size(self) -> int:
        """Return the number of open connections owned by the pool."""
        return len(self._idle) + len(self._checked_out)

    def checked_out(self) -> int:
        """Return the number of connections currently checked out."""
        return len(self._checked_out)

    async def _checkout(self) -> _Db2PooledConnection:
        """Pop a reusable idle connection, or open a new one when none is left."""
        while self._idle:
            record = self._idle.pop()
            reuse_error: BaseException | None = None
            try:
                if await self._is_reusable(record):
                    return record
            except BaseException as exc:
                reuse_error = exc
            if reuse_error is not None:
                await self._close_connection(record.connection)
                raise reuse_error
            await self._close_connection(record.connection)
        connection = await self.new_connection()
        now = time.monotonic()
        return _Db2PooledConnection(connection, now, now)

    async def _is_reusable(self, record: _Db2PooledConnection) -> bool:
        """Apply the recycle and idle health-check rules to an idle connection."""
        now = time.monotonic()
        if self._recycle_seconds > 0 and now - record.created_at > self._recycle_seconds:
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
            return False
        idle_time = now - record.last_used
        if idle_time > self._health_check_interval and not await self._is_connection_alive(record.connection):
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
            return False
        return True

    async def _is_connection_alive(self, connection: Any) -> bool:
        """Ping the connection with ``SELECT 1 FROM SYSIBM.SYSDUMMY1``.

        Close the cursor outside exception handling so compiled coroutine cleanup
        does not retain the failed query as its caller's ambient exception.
        """
        cursor: Any = None
        pending_error: BaseException | None = None
        try:
            cursor = await connection.cursor()
            await cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            await cursor.fetchone()
        except BaseException as exc:
            pending_error = exc
        if cursor is not None:
            try:
                await cursor.close()
            except BaseException as exc:
                if pending_error is None or isinstance(pending_error, Exception):
                    pending_error = exc
        if pending_error is not None and not isinstance(pending_error, Exception):
            raise pending_error
        return pending_error is None

    async def _close_connection(self, connection: Any) -> None:
        """Close a connection, suppressing driver errors."""
        with contextlib.suppress(Exception):
            await connection.close()
