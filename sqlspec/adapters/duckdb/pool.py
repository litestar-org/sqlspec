"""DuckDB connection pool with thread-local connections."""

import logging
import threading
import time
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, Final, Optional, cast

import duckdb

from sqlspec.adapters.duckdb._types import DuckDBConnection

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


logger = logging.getLogger(__name__)

DEFAULT_MIN_POOL: Final[int] = 1
DEFAULT_MAX_POOL: Final[int] = 4
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 86400

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
        "_extensions",
        "_lock",
        "_on_connection_create",
        "_recycle",
        "_secrets",
        "_thread_local",
    )

    def __init__(
        self,
        connection_config: "dict[str, Any]",
        pool_recycle_seconds: int = POOL_RECYCLE,
        extensions: "Optional[list[dict[str, Any]]]" = None,
        secrets: "Optional[list[dict[str, Any]]]" = None,
        on_connection_create: "Optional[Callable[[DuckDBConnection], None]]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_config: DuckDB connection configuration
            pool_recycle_seconds: Connection recycle time in seconds
            extensions: List of extensions to install/load
            secrets: List of secrets to create
            on_connection_create: Callback executed when connection is created
            **kwargs: Additional parameters ignored for compatibility
        """
        self._connection_config = connection_config
        self._recycle = pool_recycle_seconds
        self._extensions = extensions or []
        self._secrets = secrets or []
        self._on_connection_create = on_connection_create
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._created_connections = 0
        self._connection_times: dict[int, float] = {}

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
                logger.debug("Failed to load DuckDB extension %s: %s", ext_name, e)

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

    def _get_thread_connection(self) -> DuckDBConnection:
        """Get or create a connection for the current thread.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.
        """
        if not hasattr(self._thread_local, "connection"):
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        if self._recycle > 0 and time.time() - self._thread_local.created_at > self._recycle:
            with suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        return cast("DuckDBConnection", self._thread_local.connection)

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        if hasattr(self._thread_local, "connection"):
            with suppress(Exception):
                self._thread_local.connection.close()
            del self._thread_local.connection
            if hasattr(self._thread_local, "created_at"):
                del self._thread_local.created_at

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

        Yields:
            DuckDBConnection: A thread-local connection.
        """
        connection = self._get_thread_connection()
        try:
            yield connection
        except Exception:
            self._close_thread_connection()
            raise

    def close(self) -> None:
        """Close the thread-local connection if it exists."""
        self._close_thread_connection()

    def size(self) -> int:
        """Get current pool size (always 1 for thread-local)."""
        return 1 if hasattr(self._thread_local, "connection") else 0

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
