"""IBM Db2 database configuration with thread-local connections."""

import contextlib
import logging
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.db2.core import build_connection_config, build_dsn_string
from sqlspec.exceptions import MissingDependencyError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.module_loader import import_optional
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("Db2SyncConnectionPool",)

logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "db2"
_IBM_DB_DBI: "Any | None" = None


def _require_ibm_db_dbi() -> Any:
    """Return the ``ibm_db_dbi`` module, importing it on first use.

    Returns:
        Any: The ``ibm_db_dbi`` module.

    Raises:
        MissingDependencyError: When ibm_db is not installed.
    """
    global _IBM_DB_DBI
    if _IBM_DB_DBI is None:
        _IBM_DB_DBI = import_optional("ibm_db_dbi")
        if _IBM_DB_DBI is None:
            raise MissingDependencyError(package="ibm_db", install_package="db2")
    return _IBM_DB_DBI


class Db2SyncConnectionPool:
    """Thread-local connection manager for IBM Db2."""

    __slots__ = (
        "_connection_factory",
        "_connection_parameters",
        "_connection_registry",
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
        connection_factory: "Callable[[], Any] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_parameters: Db2 connection parameters dictionary.
            recycle_seconds: Connection recycle time in seconds (default 24h).
            health_check_interval: Seconds of idle time before running health check.
            on_connection_create: Callback executed when connection is created.
            connection_factory: Optional factory callable for custom connection instantiation.
        """
        self._connection_parameters = connection_parameters
        self._connection_factory = connection_factory
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
        return str(self._connection_parameters.get("database", self._connection_parameters.get("db", "unknown")))

    def _create_connection(self) -> Any:
        """Open a new connection and register it in the shutdown registry."""
        connection = self.new_connection()

        with self._registry_lock:
            self._connection_registry.add(connection)

        return connection

    def new_connection(self) -> Any:
        """Open a standalone connection configured like a pooled one.

        The result is owned by the caller: it is not thread-local and is not
        tracked for pool shutdown.

        Returns:
            Any: A newly opened, fully configured Db2 connection.

        Raises:
            MissingDependencyError: When ibm_db_dbi is not installed and no factory is supplied.
        """
        if self._connection_factory is not None:
            connection = self._connection_factory()
        else:
            ibm_db_dbi = _require_ibm_db_dbi()
            if "dsn" in self._connection_parameters:
                connection = ibm_db_dbi.connect(self._connection_parameters["dsn"], "", "")
            else:
                config = build_connection_config(self._connection_parameters)
                dsn_str = build_dsn_string(config)
                connection = ibm_db_dbi.connect(dsn_str, "", "")

        if self._on_connection_create is not None:
            self._on_connection_create(connection)

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
        """Return the count of active connections allocated to the current thread."""
        try:
            _ = self._thread_local.connection
        except AttributeError:
            return 0
        else:
            return 1

    def checked_out(self) -> int:
        """Return the number of checked out connections from the perspective of this thread."""
        return 0
