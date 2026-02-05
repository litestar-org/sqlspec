"""Multi-connection pool for aiosqlite."""

import asyncio
import logging
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import aiosqlite

from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection

__all__ = (
    "AiosqliteConnectTimeoutError",
    "AiosqliteConnectionPool",
    "AiosqlitePoolClosedError",
    "AiosqlitePoolConnection",
    "AiosqlitePoolConnectionContext",
)

logger = get_logger(POOL_LOGGER_NAME)

_ADAPTER_NAME = "aiosqlite"


class AiosqlitePoolClosedError(SQLSpecError):
    """Pool has been closed and cannot accept new operations."""


class AiosqliteConnectTimeoutError(SQLSpecError):
    """Connection could not be established within the specified timeout period."""


class AiosqlitePoolConnection:
    """Wrapper for database connections in the pool."""

    __slots__ = ("_closed", "_healthy", "connection", "id", "idle_since")

    def __init__(self, connection: "AiosqliteConnection") -> None:
        """Initialize pool connection wrapper.

        Args:
            connection: The raw aiosqlite connection
        """
        self.id = uuid4().hex
        self.connection = connection
        self.idle_since: float | None = None
        self._closed = False
        self._healthy = True

    @property
    def idle_time(self) -> float:
        """Get idle time in seconds.

        Returns:
            Idle time in seconds, 0.0 if connection is in use
        """
        if self.idle_since is None:
            return 0.0
        return time.time() - self.idle_since

    @property
    def is_closed(self) -> bool:
        """Check if connection is closed.

        Returns:
            True if connection is closed
        """
        return self._closed

    @property
    def is_healthy(self) -> bool:
        """Check if connection was healthy on last check.

        Returns:
            True if connection is presumed healthy
        """
        return self._healthy and not self._closed

    def mark_as_in_use(self) -> None:
        """Mark connection as in use."""
        self.idle_since = None

    def mark_as_idle(self) -> None:
        """Mark connection as idle."""
        self.idle_since = time.time()

    def mark_unhealthy(self) -> None:
        """Mark connection as unhealthy."""
        self._healthy = False

    async def is_alive(self) -> bool:
        """Check if connection is alive and functional.

        Returns:
            True if connection is healthy
        """
        if self._closed:
            self._healthy = False
            return False
        try:
            await self.connection.execute("SELECT 1")
        except Exception:
            self._healthy = False
            return False
        else:
            self._healthy = True
            return True

    async def reset(self) -> None:
        """Reset connection to clean state."""
        if self._closed:
            return
        with suppress(Exception):
            await self.connection.rollback()

    async def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return
        try:
            with suppress(Exception):
                await self.connection.rollback()
            await self.connection.close()
        except Exception:
            # Note: No pool context available at connection level
            log_with_context(
                logger, logging.DEBUG, "pool.connection.close.error", adapter=_ADAPTER_NAME, connection_id=self.id
            )
        finally:
            self._closed = True


class AiosqlitePoolConnectionContext:
    """Async context manager for pooled aiosqlite connections."""

    __slots__ = ("_connection", "_pool")

    def __init__(self, pool: "AiosqliteConnectionPool") -> None:
        """Initialize the context manager.

        Args:
            pool: Connection pool instance.
        """
        self._pool = pool
        self._connection: AiosqlitePoolConnection | None = None

    async def __aenter__(self) -> "AiosqliteConnection":
        self._connection = await self._pool.acquire()
        return self._connection.connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is None:
            return False
        await self._pool.release(self._connection)
        self._connection = None
        return False


class AiosqliteConnectionPool:
    """Multi-connection pool for aiosqlite."""

    __slots__ = (
        "_closed_event_instance",
        "_connect_timeout",
        "_connection_parameters",
        "_connection_registry",
        "_health_check_interval",
        "_idle_timeout",
        "_lock_instance",
        "_min_size",
        "_on_connection_create",
        "_operation_timeout",
        "_pool_id",
        "_pool_size",
        "_queue_instance",
        "_wal_initialized",
        "_warmed",
    )

    def __init__(
        self,
        connection_parameters: "dict[str, Any]",
        pool_size: int = 5,
        min_size: int = 0,
        connect_timeout: float = 30.0,
        idle_timeout: float = 24 * 60 * 60,
        operation_timeout: float = 10.0,
        health_check_interval: float = 30.0,
        on_connection_create: "Callable[[AiosqliteConnection], Awaitable[None]] | None" = None,
    ) -> None:
        """Initialize connection pool.

        Args:
            connection_parameters: SQLite connection parameters
            pool_size: Maximum number of connections in the pool
            min_size: Minimum connections to pre-create (pool warming)
            connect_timeout: Maximum time to wait for connection acquisition
            idle_timeout: Maximum time a connection can remain idle
            operation_timeout: Maximum time for connection operations
            health_check_interval: Seconds of idle time before running health check
            on_connection_create: Async callback executed when connection is created
        """
        self._connection_parameters = connection_parameters
        self._pool_size = pool_size
        self._min_size = min(min_size, pool_size)
        self._connect_timeout = connect_timeout
        self._idle_timeout = idle_timeout
        self._operation_timeout = operation_timeout
        self._health_check_interval = health_check_interval
        self._on_connection_create = on_connection_create

        self._connection_registry: dict[str, AiosqlitePoolConnection] = {}
        self._wal_initialized = False
        self._warmed = False
        self._pool_id = uuid4().hex[:8]  # Short ID for logging

        self._queue_instance: asyncio.Queue[AiosqlitePoolConnection] | None = None
        self._lock_instance: asyncio.Lock | None = None
        self._closed_event_instance: asyncio.Event | None = None

    @property
    def _queue(self) -> "asyncio.Queue[AiosqlitePoolConnection]":
        """Lazy initialization of asyncio.Queue for Python 3.9 compatibility."""
        if self._queue_instance is None:
            self._queue_instance = asyncio.Queue(maxsize=self._pool_size)
        return self._queue_instance

    @property
    def _lock(self) -> asyncio.Lock:
        """Lazy initialization of asyncio.Lock for Python 3.9 compatibility."""
        if self._lock_instance is None:
            self._lock_instance = asyncio.Lock()
        return self._lock_instance

    @property
    def _closed_event(self) -> asyncio.Event:
        """Lazy initialization of asyncio.Event for Python 3.9 compatibility."""
        if self._closed_event_instance is None:
            self._closed_event_instance = asyncio.Event()
        return self._closed_event_instance

    @property
    def is_closed(self) -> bool:
        """Check if pool is closed.

        Returns:
            True if pool is closed
        """
        return self._closed_event_instance is not None and self._closed_event.is_set()

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        db = self._connection_parameters.get("database", "unknown")
        return str(db).split("/")[-1] if db else "unknown"

    def size(self) -> int:
        """Get total number of connections in pool.

        Returns:
            Total connection count
        """
        return len(self._connection_registry)

    def checked_out(self) -> int:
        """Get number of checked out connections.

        Returns:
            Number of connections currently in use
        """
        if self._queue_instance is None:
            return len(self._connection_registry)
        return len(self._connection_registry) - self._queue.qsize()

    async def _create_connection(self) -> AiosqlitePoolConnection:
        """Create a new connection.

        Returns:
            New pool connection instance
        """
        connection = await aiosqlite.connect(**self._connection_parameters)

        database_path = str(self._connection_parameters.get("database", ""))
        is_shared_cache = "cache=shared" in database_path
        is_memory_db = ":memory:" in database_path or "mode=memory" in database_path

        try:
            if is_memory_db:
                await connection.execute("PRAGMA journal_mode = MEMORY")
                await connection.execute("PRAGMA synchronous = OFF")
                await connection.execute("PRAGMA temp_store = MEMORY")
                await connection.execute("PRAGMA cache_size = -16000")
            else:
                await connection.execute("PRAGMA journal_mode = WAL")
                await connection.execute("PRAGMA synchronous = NORMAL")

            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("PRAGMA busy_timeout = 30000")

            if is_shared_cache and is_memory_db:
                await connection.execute("PRAGMA read_uncommitted = ON")

            await connection.commit()

            if is_shared_cache:
                self._wal_initialized = True

        except Exception:
            log_with_context(
                logger,
                logging.WARNING,
                "pool.connection.configure.error",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
            )
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("PRAGMA busy_timeout = 30000")
            await connection.commit()

        # Call user-provided callback after internal setup
        if self._on_connection_create is not None:
            await self._on_connection_create(connection)

        pool_connection = AiosqlitePoolConnection(connection)
        pool_connection.mark_as_idle()

        async with self._lock:
            self._connection_registry[pool_connection.id] = pool_connection

        return pool_connection

    async def _claim_if_healthy(self, connection: AiosqlitePoolConnection) -> bool:
        """Check if connection is healthy and claim it.

        Uses passive health checks: connections idle less than health_check_interval
        are assumed healthy based on their last known state. Active health checks
        (SELECT 1) are only performed on long-idle connections.

        Args:
            connection: Connection to check and claim

        Returns:
            True if connection was claimed
        """
        if connection.idle_time > self._idle_timeout:
            await self._retire_connection(connection, reason="idle_timeout")
            return False

        if not connection.is_healthy:
            await self._retire_connection(connection, reason="unhealthy")
            return False

        if connection.idle_time > self._health_check_interval:
            try:
                is_alive = await asyncio.wait_for(connection.is_alive(), timeout=self._operation_timeout)
                if not is_alive:
                    await self._retire_connection(connection, reason="health_check_failed")
                    return False
            except asyncio.TimeoutError:
                await self._retire_connection(connection, reason="health_check_timeout")
                return False

        connection.mark_as_in_use()
        return True

    async def _retire_connection(self, connection: AiosqlitePoolConnection, *, reason: str | None = None) -> None:
        """Retire a connection from the pool.

        Args:
            connection: Connection to retire
            reason: Optional reason for retirement
        """
        if reason:
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.retire",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                connection_id=connection.id,
                reason=reason,
            )
        async with self._lock:
            self._connection_registry.pop(connection.id, None)

        try:
            await asyncio.wait_for(connection.close(), timeout=self._operation_timeout)
        except asyncio.TimeoutError:
            log_with_context(
                logger,
                logging.WARNING,
                "pool.connection.close.timeout",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                connection_id=connection.id,
                timeout_seconds=self._operation_timeout,
            )

    async def _try_provision_new_connection(self) -> "AiosqlitePoolConnection | None":
        """Try to create a new connection if under capacity.

        Returns:
            New connection if successful, None if at capacity
        """
        async with self._lock:
            if len(self._connection_registry) >= self._pool_size:
                return None

        try:
            connection = await self._create_connection()
        except Exception:
            log_with_context(
                logger,
                logging.WARNING,
                "pool.connection.create.error",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
                pool_size=len(self._connection_registry),
                max_size=self._pool_size,
            )
            return None
        else:
            connection.mark_as_in_use()
            return connection

    async def _wait_for_healthy_connection(self) -> AiosqlitePoolConnection:
        """Wait for a healthy connection to become available.

        Returns:
            Available healthy connection

        Raises:
            AiosqlitePoolClosedError: If pool is closed while waiting
        """
        while True:
            get_connection_task = asyncio.create_task(self._queue.get())
            pool_closed_task = asyncio.create_task(self._closed_event.wait())

            done, pending = await asyncio.wait(
                {get_connection_task, pool_closed_task}, return_when=asyncio.FIRST_COMPLETED
            )

            try:
                if pool_closed_task in done:
                    msg = "Pool closed during connection acquisition"
                    raise AiosqlitePoolClosedError(msg)

                connection = get_connection_task.result()
                if await self._claim_if_healthy(connection):
                    return connection

            finally:
                for task in pending:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

    async def _warm_pool(self) -> None:
        """Pre-create minimum connections for pool warming.

        Creates connections up to min_size to avoid cold-start latency
        on first requests.
        """
        if self._warmed or self._min_size <= 0:
            return

        self._warmed = True
        connections_needed = self._min_size - len(self._connection_registry)

        if connections_needed <= 0:
            return

        log_with_context(
            logger,
            logging.DEBUG,
            "pool.warmup.start",
            adapter=_ADAPTER_NAME,
            pool_id=self._pool_id,
            database=self._database_name,
            connections_needed=connections_needed,
            min_size=self._min_size,
        )
        tasks = [self._create_connection() for _ in range(connections_needed)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, AiosqlitePoolConnection):
                self._queue.put_nowait(result)
            elif isinstance(result, Exception):
                log_with_context(
                    logger,
                    logging.WARNING,
                    "pool.warmup.connection.error",
                    adapter=_ADAPTER_NAME,
                    pool_id=self._pool_id,
                    error=str(result),
                )

    async def _get_connection(self) -> AiosqlitePoolConnection:
        """Run the three-phase connection acquisition cycle.

        Returns:
            Available connection

        Raises:
            AiosqlitePoolClosedError: If pool is closed
        """
        # Fast path: check closed state directly to avoid property overhead
        if self._closed_event_instance is not None and self._closed_event_instance.is_set():
            msg = "Cannot acquire connection from closed pool"
            raise AiosqlitePoolClosedError(msg)

        if not self._warmed and self._min_size > 0:
            await self._warm_pool()

        # Fast path: try to get from queue without health check overhead for fresh connections
        while not self._queue.empty():
            connection = self._queue.get_nowait()
            # Fast claim for recently-used connections (idle < health_check_interval)
            if connection.idle_since is not None:
                idle_time = time.time() - connection.idle_since
                if idle_time <= self._health_check_interval and connection.is_healthy:
                    connection.idle_since = None  # mark_as_in_use inline
                    return connection
            # Fall back to full health check for older connections
            if await self._claim_if_healthy(connection):
                return connection

        # Try to create new connection if under capacity
        # Fast path: check capacity without lock first
        if len(self._connection_registry) < self._pool_size:
            new_connection = await self._try_provision_new_connection()
            if new_connection is not None:
                return new_connection

        return await self._wait_for_healthy_connection()

    async def acquire(self) -> AiosqlitePoolConnection:
        """Acquire a connection from the pool.

        Returns:
            Available connection

        Raises:
            AiosqliteConnectTimeoutError: If acquisition times out
        """
        # Fast path: try to get connection without timeout wrapper
        # Only use timeout when we need to wait for a connection
        try:
            connection = await self._get_connection()
        except AiosqlitePoolClosedError:
            raise
        except Exception:
            # If fast path fails, fall back to timeout-wrapped acquisition
            try:
                connection = await asyncio.wait_for(self._get_connection(), timeout=self._connect_timeout)
            except asyncio.TimeoutError as e:
                msg = f"Connection acquisition timed out after {self._connect_timeout}s"
                raise AiosqliteConnectTimeoutError(msg) from e

        if not self._wal_initialized and "cache=shared" in str(self._connection_parameters.get("database", "")):
            await asyncio.sleep(0.01)
        return connection

    async def release(self, connection: AiosqlitePoolConnection) -> None:
        """Release a connection back to the pool.

        Args:
            connection: Connection to release
        """
        # Fast path: check closed state directly
        if self._closed_event_instance is not None and self._closed_event_instance.is_set():
            await self._retire_connection(connection)
            return

        if connection.id not in self._connection_registry:
            log_with_context(
                logger,
                logging.WARNING,
                "pool.connection.release.unknown",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                connection_id=connection.id,
            )
            return

        try:
            # Fast path: skip timeout wrapper for reset, just do the rollback directly
            # The rollback itself is fast for SQLite; timeout is overkill for hot path
            with suppress(Exception):
                await connection.connection.rollback()
            connection.idle_since = time.time()  # mark_as_idle inline
            self._queue.put_nowait(connection)
        except Exception as e:
            log_with_context(
                logger,
                logging.WARNING,
                "pool.connection.reset.error",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                connection_id=connection.id,
                error=str(e),
            )
            connection.mark_unhealthy()
            await self._retire_connection(connection)

    def get_connection(self) -> "AiosqlitePoolConnectionContext":
        """Get a connection with automatic release."""
        return AiosqlitePoolConnectionContext(self)

    async def close(self) -> None:
        """Close the connection pool."""
        if self.is_closed:
            return
        self._closed_event.set()

        while not self._queue.empty():
            self._queue.get_nowait()

        async with self._lock:
            connections = list(self._connection_registry.values())
            self._connection_registry.clear()

        if connections:
            close_tasks = [asyncio.wait_for(conn.close(), timeout=self._operation_timeout) for conn in connections]
            results = await asyncio.gather(*close_tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "pool.close.connection.error",
                        adapter=_ADAPTER_NAME,
                        pool_id=self._pool_id,
                        connection_id=connections[i].id,
                        error=str(result),
                    )
