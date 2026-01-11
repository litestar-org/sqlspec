"""Multi-connection pool for aiosqlite."""

import asyncio
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import aiosqlite

from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection

__all__ = (
    "AiosqliteConnectTimeoutError",
    "AiosqliteConnectionPool",
    "AiosqlitePoolClosedError",
    "AiosqlitePoolConnection",
    "AiosqlitePoolConnectionContext",
)

logger = get_logger(__name__)


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
            logger.debug("Error closing connection %s", self.id)
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
        "_operation_timeout",
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
        """
        self._connection_parameters = connection_parameters
        self._pool_size = pool_size
        self._min_size = min(min_size, pool_size)
        self._connect_timeout = connect_timeout
        self._idle_timeout = idle_timeout
        self._operation_timeout = operation_timeout
        self._health_check_interval = health_check_interval

        self._connection_registry: dict[str, AiosqlitePoolConnection] = {}
        self._wal_initialized = False
        self._warmed = False

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
            logger.exception("Failed to configure connection")
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("PRAGMA busy_timeout = 30000")
            await connection.commit()

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
            logger.debug("Retiring connection %s", connection.id, extra={"reason": reason})
        async with self._lock:
            self._connection_registry.pop(connection.id, None)

        try:
            await asyncio.wait_for(connection.close(), timeout=self._operation_timeout)
        except asyncio.TimeoutError:
            logger.warning("Connection %s close timed out during retirement", connection.id)

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
            logger.exception("Failed to create new connection")
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

        logger.debug("Warming pool with %d connections", connections_needed)
        tasks = [self._create_connection() for _ in range(connections_needed)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, AiosqlitePoolConnection):
                self._queue.put_nowait(result)
            elif isinstance(result, Exception):
                logger.warning("Failed to create warm connection: %s", result)

    async def _get_connection(self) -> AiosqlitePoolConnection:
        """Run the three-phase connection acquisition cycle.

        Returns:
            Available connection

        Raises:
            AiosqlitePoolClosedError: If pool is closed
        """
        if self.is_closed:
            msg = "Cannot acquire connection from closed pool"
            raise AiosqlitePoolClosedError(msg)

        if not self._warmed and self._min_size > 0:
            await self._warm_pool()

        while not self._queue.empty():
            connection = self._queue.get_nowait()
            if await self._claim_if_healthy(connection):
                return connection

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
        try:
            connection = await asyncio.wait_for(self._get_connection(), timeout=self._connect_timeout)
            if not self._wal_initialized and "cache=shared" in str(self._connection_parameters.get("database", "")):
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError as e:
            msg = f"Connection acquisition timed out after {self._connect_timeout}s"
            raise AiosqliteConnectTimeoutError(msg) from e
        else:
            return connection

    async def release(self, connection: AiosqlitePoolConnection) -> None:
        """Release a connection back to the pool.

        Args:
            connection: Connection to release
        """
        if self.is_closed:
            await self._retire_connection(connection)
            return

        if connection.id not in self._connection_registry:
            logger.warning("Attempted to release unknown connection: %s", connection.id)
            return

        try:
            await asyncio.wait_for(connection.reset(), timeout=self._operation_timeout)
            connection.mark_as_idle()
            self._queue.put_nowait(connection)
        except Exception as e:
            logger.warning("Failed to reset connection %s during release: %s", connection.id, e)
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
                    logger.warning("Error closing connection %s: %s", connections[i].id, result)
