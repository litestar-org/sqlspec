"""DuckDB connection pool with thread-local connections."""

import logging
import threading
import time
import uuid
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, Final, cast

import duckdb

from sqlspec.adapters.duckdb._typing import DuckDBConnection
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "duckdb"

DEFAULT_MIN_POOL: Final[int] = 1
DEFAULT_MAX_POOL: Final[int] = 4
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 86400
HEALTH_CHECK_INTERVAL: Final[float] = 30.0

__all__ = ("DuckDBConnectionPool",)


class DuckDBConnectionPool:
    """Thread-local connection manager for DuckDB.

    Uses thread-local storage to ensure each thread gets its own DuckDB connection,
    preventing the thread-safety issues that cause segmentation faults when
    multiple cursors share the same connection concurrently.

    This design trades traditional pooling for thread safety, which is essential
    for DuckDB since connections and cursors are not thread-safe.
    """

    __slots__ = (
        "_connection_config",
        "_connection_times",
        "_created_connections",
        "_extension_flags",
        "_extensions",
        "_health_check_interval",
        "_is_memory_db",
        "_lock",
        "_on_connection_create",
        "_pool_id",
        "_recycle",
        "_secrets",
        "_thread_local",
    )

    def __init__(
        self,
        connection_config: "dict[str, Any]",
        pool_recycle_seconds: int = POOL_RECYCLE,
        health_check_interval: float = HEALTH_CHECK_INTERVAL,
        extensions: "list[dict[str, Any]] | None" = None,
        extension_flags: "dict[str, Any] | None" = None,
        secrets: "list[dict[str, Any]] | None" = None,
        on_connection_create: "Callable[[DuckDBConnection], DuckDBConnection | None] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_config: DuckDB connection configuration
            pool_recycle_seconds: Connection recycle time in seconds
            health_check_interval: Seconds of idle time before running health check
            extensions: List of extensions to install/load
            extension_flags: Connection-level SET statements applied after creation
            secrets: List of secrets to create
            on_connection_create: Callback executed when connection is created
        """
        self._connection_config = connection_config
        self._recycle = pool_recycle_seconds
        self._health_check_interval = health_check_interval
        self._extensions = extensions or []
        self._extension_flags = extension_flags or {}
        self._secrets = secrets or []
        self._on_connection_create = on_connection_create
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._created_connections = 0
        self._connection_times: dict[int, float] = {}
        self._pool_id = str(uuid.uuid4())[:8]
        # Track if this pool uses an in-memory database
        # In-memory databases require connections to stay alive to preserve data
        database = connection_config.get("database", "")
        self._is_memory_db = database.startswith(":memory:") or database == ""

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        db = self._connection_config.get("database", "")
        if db.startswith(":memory:") or db == "":
            return ":memory:"
        return str(db)

    def _create_connection(self) -> DuckDBConnection:
        """Create a new DuckDB connection with extensions and secrets."""
        connect_parameters = {}
        config_dict = {}

        for key, value in self._connection_config.items():
            if key in {"database", "read_only"}:
                connect_parameters[key] = value
            else:
                config_dict[key] = value

        if config_dict:
            connect_parameters["config"] = config_dict

        connection = duckdb.connect(**connect_parameters)

        self._apply_extension_flags(connection)

        for ext_config in self._extensions:
            ext_name = ext_config.get("name")
            if not ext_name:
                continue

            install_kwargs = {}
            if "version" in ext_config:
                install_kwargs["version"] = ext_config["version"]
            if "repository" in ext_config:
                install_kwargs["repository"] = ext_config["repository"]
            if ext_config.get("force_install", False):
                install_kwargs["force_install"] = True

            try:
                if install_kwargs:
                    connection.install_extension(ext_name, **install_kwargs)
                connection.load_extension(ext_name)
            except Exception as e:
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "pool.extension.load.failed",
                    adapter=_ADAPTER_NAME,
                    pool_id=self._pool_id,
                    database=self._database_name,
                    extension=ext_name,
                    error=str(e),
                )

        for secret_config in self._secrets:
            secret_type = secret_config.get("secret_type")
            secret_name = secret_config.get("name")
            secret_value = secret_config.get("value")

            if not (secret_type and secret_name and secret_value):
                continue

            value_pairs = []
            for key, value in secret_value.items():
                escaped_value = str(value).replace("'", "''")
                value_pairs.append(f"'{key}' = '{escaped_value}'")
            value_string = ", ".join(value_pairs)
            scope_clause = ""
            if "scope" in secret_config:
                scope_clause = f" SCOPE '{secret_config['scope']}'"

            sql = f"""
                CREATE SECRET {secret_name} (
                    TYPE {secret_type},
                    {value_string}
                ){scope_clause}
            """
            with suppress(Exception):
                connection.execute(sql)

        if self._on_connection_create:
            with suppress(Exception):
                self._on_connection_create(connection)

        conn_id = id(connection)
        with self._lock:
            self._created_connections += 1
            self._connection_times[conn_id] = time.time()

        return connection

    def _apply_extension_flags(self, connection: DuckDBConnection) -> None:
        """Apply connection-level extension flags via SET statements."""

        if not self._extension_flags:
            return

        for key, value in self._extension_flags.items():
            if not key or not key.replace("_", "").isalnum():
                continue

            normalized = self._normalize_flag_value(value)
            try:
                connection.execute(f"SET {key} = {normalized}")
            except Exception as exc:  # pragma: no cover - best-effort guard
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "pool.flag.set.failed",
                    adapter=_ADAPTER_NAME,
                    pool_id=self._pool_id,
                    database=self._database_name,
                    flag=key,
                    error=str(exc),
                )

    @staticmethod
    def _normalize_flag_value(value: Any) -> str:
        """Convert Python value to DuckDB SET literal."""

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _get_thread_connection(self) -> DuckDBConnection:
        """Get or create a connection for the current thread.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.
        """
        thread_state = self._thread_local.__dict__
        if "connection" not in thread_state:
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("DuckDBConnection", self._thread_local.connection)

        if self._recycle > 0 and time.time() - self._thread_local.created_at > self._recycle:
            with suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("DuckDBConnection", self._thread_local.connection)

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
            with suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        self._thread_local.last_used = time.time()
        return cast("DuckDBConnection", self._thread_local.connection)

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        thread_state = self._thread_local.__dict__
        if "connection" in thread_state:
            with suppress(Exception):
                self._thread_local.connection.close()
            del self._thread_local.connection
            if "created_at" in thread_state:
                del self._thread_local.created_at
            if "last_used" in thread_state:
                del self._thread_local.last_used

    def _is_connection_alive(self, connection: DuckDBConnection) -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            cursor = connection.cursor()
            cursor.close()
        except Exception:
            return False
        return True

    @contextmanager
    def get_connection(self) -> "Generator[DuckDBConnection, None, None]":
        """Get a thread-local connection.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.

        For file-based databases, the connection is closed when the context
        manager exits to release DuckDB's file lock, allowing subsequent
        connections with different configurations.

        For in-memory databases, connections are kept alive to preserve data,
        as in-memory data is lost when the last connection closes.

        Yields:
            DuckDBConnection: A thread-local connection.
        """
        connection = self._get_thread_connection()
        try:
            yield connection
        except Exception:
            self._close_thread_connection()
            raise
        else:
            # Only close connection for file-based databases to release file locks
            # In-memory databases need connections to stay alive to preserve data
            if not self._is_memory_db:
                self._close_thread_connection()

    def close(self) -> None:
        """Close the thread-local connection if it exists."""
        self._close_thread_connection()

    def size(self) -> int:
        """Get current pool size (always 1 for thread-local)."""
        return 1 if "connection" in self._thread_local.__dict__ else 0

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0 for thread-local)."""
        return 0

    def acquire(self) -> DuckDBConnection:
        """Acquire a thread-local connection.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.

        Returns:
            DuckDBConnection: A thread-local connection
        """
        return self._get_thread_connection()

    def release(self, connection: DuckDBConnection) -> None:
        """Release a connection (no-op for thread-local connections).

        Args:
            connection: The connection to release (ignored)
        """
