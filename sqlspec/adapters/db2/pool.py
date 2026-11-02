"""IBM Db2 connection pools: thread-local sync connections and a bounded asyncio pool."""

import asyncio
import contextlib
import inspect
import logging
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.db2 import _typing as typing_module
from sqlspec.adapters.db2._typing import ibm_db_dbi
from sqlspec.adapters.db2.core import build_dsn_string
from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator
    from types import TracebackType

__all__ = ("Db2AsyncConnectionPool", "Db2AsyncPoolConnectionContext", "Db2SyncConnectionPool")

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "db2"
_IBM_DB_DBI = ibm_db_dbi


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


class Db2SyncConnectionPool:
    """Thread-local connection manager for IBM Db2."""

    __slots__ = (
        "_autocommit",
        "_connection_parameters",
        "_connection_registry",
        "_dsn",
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
        on_connection_create: "Callable[[Any], None] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: Normalized Db2 connection parameters. The CLI connection
                string is rendered from them once, here; ``autocommit`` (default True) sets the
                autocommit mode every new connection opens in.
            recycle_seconds: Connection recycle time in seconds (default 24h).
            health_check_interval: Seconds of idle time before running health check.
            on_connection_create: Callback executed when connection is created.
        """
        self._connection_parameters = connection_parameters
        native_parameters = dict(connection_parameters)
        native_parameters.pop("current_schema", None)
        self._dsn = build_dsn_string(native_parameters)
        self._autocommit = bool(connection_parameters.get("autocommit", True))
        self._thread_local = threading.local()
        self._connection_registry: set[Any] = set()
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
        connection = typing_module.ibm_db_dbi.connect(
            self._dsn, "", "", "", "", {typing_module.ibm_db_dbi.SQL_ATTR_AUTOCOMMIT: autocommit_mode}
        )

        try:
            schema = self._connection_parameters.get("current_schema")
            if schema is not None:
                _set_current_schema(connection, schema)
            if self._on_connection_create is not None:
                self._on_connection_create(connection)
        except BaseException:
            with contextlib.suppress(BaseException):
                connection.close()
            raise

        return connection

    @contextmanager
    def get_connection(self) -> "Generator[Any, None, None]":
        """Context manager to yield a thread-local connection.

        Yields:
            A thread-local Db2 database connection.
        """
        connection = self._get_thread_connection()
        try:
            yield connection
        except Exception:
            with contextlib.suppress(Exception):
                self._close_thread_connection()
            raise

    def close(self) -> None:
        """Close every connection this pool opened across all threads."""
        self._close_thread_connection()
        with self._registry_lock:
            orphaned = list(self._connection_registry)
            self._connection_registry.clear()
            self._generation += 1
        for connection in orphaned:
            with contextlib.suppress(Exception):
                connection.close()

    def acquire(self) -> Any:
        """Acquire a thread-local connection."""
        return self._get_thread_connection()

    def release(self, connection: Any) -> None:
        """Release connection back to the thread-local pool."""
        _ = connection

    def size(self) -> int:
        """Return the total number of active connections registered across all threads."""
        with self._registry_lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        """Return the number of checked out connections from the perspective of this thread."""
        return 1 if getattr(self._thread_local, "connection", None) is not None else 0

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

    def _get_thread_connection(self) -> Any:
        """Retrieve, recycle, or validate the connection assigned to the calling thread."""
        thread_state = self._thread_local.__dict__
        if thread_state.get("generation") != self._generation:
            stale = thread_state.pop("connection", None)
            if stale is not None:
                self._retire_connection(stale)
            thread_state.pop("created_at", None)
            thread_state.pop("last_used", None)
            self._thread_local.generation = self._generation
        if "connection" not in thread_state:
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return self._thread_local.connection

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
            self._retire_connection(self._thread_local.connection)
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return self._thread_local.connection

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
            self._retire_connection(self._thread_local.connection)
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        self._thread_local.last_used = time.time()
        return self._thread_local.connection

    def _retire_connection(self, connection: Any) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._registry_lock:
            self._connection_registry.discard(connection)
        with contextlib.suppress(Exception):
            connection.close()

    def _close_thread_connection(self) -> None:
        """Close and detach the calling thread's dedicated connection."""
        thread_state = self._thread_local.__dict__
        if "connection" in thread_state:
            self._retire_connection(self._thread_local.connection)
            del self._thread_local.connection
            if "created_at" in thread_state:
                del self._thread_local.created_at
            if "last_used" in thread_state:
                del self._thread_local.last_used


class _Db2PooledConnection:
    """Idle-stack record for one pooled async connection."""

    __slots__ = ("connection", "created_at", "last_used")

    def __init__(self, connection: Any, created_at: float, last_used: float) -> None:
        self.connection = connection
        self.created_at = created_at
        self.last_used = last_used


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
        await self._pool.release(connection)


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
        "_connection_parameters",
        "_dsn",
        "_health_check_interval",
        "_idle",
        "_max_size",
        "_on_connection_create",
        "_pool_id",
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
        """
        self._connection_parameters = connection_parameters
        native_parameters = dict(connection_parameters)
        native_parameters.pop("current_schema", None)
        self._dsn = build_dsn_string(native_parameters)
        self._autocommit = bool(connection_parameters.get("autocommit", True))
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
        connection = await typing_module.ibm_db_dbi.AsyncConnection.connect(
            self._dsn, "", "", "", "", {typing_module.ibm_db_dbi.SQL_ATTR_AUTOCOMMIT: autocommit_mode}
        )
        try:
            schema = self._connection_parameters.get("current_schema")
            if schema is not None:
                await _set_current_schema_async(connection, schema)
            if self._on_connection_create is not None:
                result = self._on_connection_create(connection)
                if inspect.isawaitable(result):
                    await result
        except BaseException:
            with contextlib.suppress(BaseException):
                await connection.close()
            raise
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
        except BaseException:
            semaphore.release()
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
        record = self._checked_out.pop(id(connection), None)
        if record is None:
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.release.unknown",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
            )
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
            try:
                if await self._is_reusable(record):
                    return record
            except BaseException:
                await self._close_connection(record.connection)
                raise
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
