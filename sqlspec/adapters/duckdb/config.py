"""DuckDB database configuration with connection pooling."""
# ruff: noqa: D107 W293 RUF100 S110 PLR0913 FA100 BLE001 UP037 COM812 ARG002

import logging
import threading
import time
from collections.abc import Sequence
from contextlib import contextmanager, suppress
from queue import Empty as QueueEmpty
from queue import Full, Queue
from typing import TYPE_CHECKING, Any, Final, Optional, TypedDict, cast

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, duckdb_statement_config
from sqlspec.config import SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Callable, ClassVar, Union

    from sqlspec.statement.sql import StatementConfig


logger = logging.getLogger(__name__)

DEFAULT_MIN_POOL: Final[int] = 1  # DuckDB works best with fewer connections
DEFAULT_MAX_POOL: Final[int] = 4  # DuckDB is optimized for single connection use
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 86400  # 24 hours - DuckDB connections should be long-lived

__all__ = (
    "DuckDBConfig",
    "DuckDBConnectionParams",
    "DuckDBConnectionPool",
    "DuckDBDriverFeatures",
    "DuckDBExtensionConfig",
    "DuckDBPoolParams",
    "DuckDBSecretConfig",
)


class DuckDBConnectionPool:
    """Connection pool for DuckDB with performance optimizations.

    While DuckDB has internal connection management, this pool provides
    external connection pooling for managing multiple concurrent connections.
    """

    __slots__ = (
        "_checked_out",
        "_connection_config",
        "_connection_times",
        "_created_connections",
        "_extensions",
        "_lock",
        "_max_pool",
        "_min_pool",
        "_on_connection_create",
        "_pool",
        "_recycle",
        "_secrets",
        "_timeout",
    )

    def __init__(  # noqa: PLR0913
        self,
        connection_config: "dict[str, Any]",  # noqa: UP037
        pool_min_size: int = DEFAULT_MIN_POOL,
        pool_max_size: int = DEFAULT_MAX_POOL,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle_seconds: int = POOL_RECYCLE,
        extensions: "Optional[list[dict[str, Any]]]" = None,  # noqa: FA100, UP037
        secrets: "Optional[list[dict[str, Any]]]" = None,  # noqa: FA100, UP037
        on_connection_create: "Optional[Callable[[DuckDBConnection], None]]" = None,  # noqa: FA100
    ) -> None:
        self._pool: "Queue[DuckDBConnection]" = Queue(maxsize=pool_max_size)  # noqa: UP037
        self._lock = threading.RLock()
        self._min_pool = pool_min_size
        self._max_pool = pool_max_size
        self._timeout = pool_timeout
        self._recycle = pool_recycle_seconds
        self._connection_config = connection_config
        self._extensions = extensions or []
        self._secrets = secrets or []
        self._on_connection_create = on_connection_create
        self._created_connections = 0
        self._checked_out = 0
        self._connection_times: "dict[int, float]" = {}  # noqa: UP037

        # Pre-populate pool
        for _ in range(pool_min_size):
            if self._pool.full():
                break
            conn = self._create_connection()
            try:
                self._pool.put_nowait(conn)
            except Full:
                break

    def _create_connection(self) -> DuckDBConnection:
        """Create a new DuckDB connection with extensions and secrets."""
        # Separate DuckDB connect parameters from config parameters
        connect_params = {}
        config_dict = {}

        for key, value in self._connection_config.items():
            if key in {"database", "read_only"}:
                connect_params[key] = value
            else:
                config_dict[key] = value

        if config_dict:
            connect_params["config"] = config_dict

        connection = duckdb.connect(**connect_params)

        # Install and load extensions
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
            except Exception:  # noqa: BLE001, S110
                pass

        # Configure secrets
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

            sql = f"""  # noqa: S608
                CREATE SECRET {secret_name} (
                    TYPE {secret_type},
                    {value_string}
                ){scope_clause}
            """
            with suppress(Exception):
                connection.execute(sql)

        # Run custom initialization
        if self._on_connection_create:
            with suppress(Exception):
                self._on_connection_create(connection)

        # Track creation time
        conn_id = id(connection)
        with self._lock:
            self._created_connections += 1
            self._connection_times[conn_id] = time.time()

        return connection

    def _should_recycle(self, connection: DuckDBConnection) -> bool:
        """Check if connection should be recycled based on age.

        For DuckDB, we want to keep connections alive as long as possible
        to maintain cache and performance benefits.
        """
        if self._recycle <= 0:
            # Recycling disabled - keep connections indefinitely
            return False

        conn_id = id(connection)
        created_at = self._connection_times.get(conn_id)
        if created_at is None:
            # Missing timestamp, recycle to be safe
            return True
        return (time.time() - created_at) > self._recycle

    def _is_connection_alive(self, connection: DuckDBConnection) -> bool:
        """Check if a connection is still alive and usable.

        For DuckDB, we minimize health checks since connections are
        typically very stable and checks add overhead.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            # Quick check using cursor creation instead of query execution
            cursor = connection.cursor()
            cursor.close()
        except Exception:
            return False
        return True

    @contextmanager
    def get_connection(self) -> "Generator[DuckDBConnection, None, None]":
        """Get a connection from the pool with automatic return.

        Yields:
            DuckDBConnection: A connection instance from the pool.
        """
        connection = None
        try:
            # Try to get existing connection
            try:
                connection = self._pool.get(timeout=self._timeout)

                # Check if connection should be recycled
                if self._should_recycle(connection) or not self._is_connection_alive(connection):
                    conn_id = id(connection)
                    with suppress(Exception):  # noqa: BLE001
                        connection.close()
                    with self._lock:
                        self._connection_times.pop(conn_id, None)
                    connection = None

            except QueueEmpty:
                # Pool is empty, check if we can create new connection
                with self._lock:
                    if self._checked_out < self._max_pool:
                        connection = None  # Will create new one below
                    else:
                        # Wait for a connection to become available
                        try:
                            connection = self._pool.get(timeout=self._timeout)
                        except QueueEmpty:
                            msg = f"Connection pool limit of {self._max_pool} reached, timeout {self._timeout}"
                            raise RuntimeError(msg) from None

            # Create new connection if needed
            if connection is None:
                connection = self._create_connection()

            with self._lock:
                self._checked_out += 1

            yield connection

        finally:
            if connection is not None:
                with self._lock:
                    self._checked_out -= 1

                # Validate connection before returning to pool
                if self._is_connection_alive(connection):
                    # Return to pool if space available
                    try:
                        self._pool.put_nowait(connection)
                    except Full:
                        # Pool is full, close overflow connection
                        with suppress(Exception):  # noqa: BLE001
                            connection.close()
                else:
                    # Connection is dead, close it
                    with suppress(Exception):  # noqa: BLE001
                        connection.close()

    def close(self) -> None:
        """Close all connections in the pool."""
        while True:
            try:
                connection = self._pool.get_nowait()
            except QueueEmpty:
                break
            with suppress(Exception):  # noqa: BLE001
                connection.close()

        # Clear connection time tracking
        with self._lock:
            self._connection_times.clear()

    def size(self) -> int:
        """Get current pool size."""
        return self._pool.qsize()

    def checked_out(self) -> int:
        """Get number of checked out connections."""
        return self._checked_out

    def acquire(self) -> DuckDBConnection:
        """Acquire a connection from the pool without a context manager.

        This method gets a connection from the pool that must be manually
        returned using the release() method.

        Returns:
            DuckDBConnection: A connection from the pool

        Raises:
            RuntimeError: If pool limit is reached and timeout expires
        """
        connection = None
        try:
            # Try to get existing connection
            try:
                connection = self._pool.get(timeout=self._timeout)

                # Check if connection should be recycled
                if self._should_recycle(connection) or not self._is_connection_alive(connection):
                    conn_id = id(connection)
                    with suppress(Exception):
                        connection.close()
                    with self._lock:
                        self._connection_times.pop(conn_id, None)
                    connection = None

            except QueueEmpty:
                # Pool is empty, check if we can create new connection
                with self._lock:
                    if self._checked_out < self._max_pool:
                        connection = None  # Will create new one below
                    else:
                        # Wait for a connection to become available
                        try:
                            connection = self._pool.get(timeout=self._timeout)
                        except QueueEmpty:
                            msg = f"Connection pool limit of {self._max_pool} reached, timeout {self._timeout}"
                            raise RuntimeError(msg) from None

            # Create new connection if needed
            if connection is None:
                connection = self._create_connection()

            with self._lock:
                self._checked_out += 1

            return connection

        except Exception:
            # If we got a connection but failed somewhere, return it to pool
            if connection is not None:
                with suppress(Full):
                    self._pool.put_nowait(connection)
            raise

    def release(self, connection: DuckDBConnection) -> None:
        """Return a connection to the pool.

        Args:
            connection: The connection to return to the pool
        """
        if connection is None:
            return

        with self._lock:
            self._checked_out = max(0, self._checked_out - 1)

        # Validate connection before returning to pool
        if self._is_connection_alive(connection):
            try:
                self._pool.put_nowait(connection)
            except Full:
                # Pool is full, close the connection
                with suppress(Exception):
                    connection.close()
                conn_id = id(connection)
                with self._lock:
                    self._connection_times.pop(conn_id, None)
        else:
            # Connection is dead, close it
            with suppress(Exception):
                connection.close()
            conn_id = id(connection)
            with self._lock:
                self._connection_times.pop(conn_id, None)


class DuckDBConnectionParams(TypedDict, total=False):
    """DuckDB connection parameters."""

    database: NotRequired[str]
    read_only: NotRequired[bool]
    config: NotRequired[dict[str, Any]]
    memory_limit: NotRequired[str]
    threads: NotRequired[int]
    temp_directory: NotRequired[str]
    max_temp_directory_size: NotRequired[str]
    autoload_known_extensions: NotRequired[bool]
    autoinstall_known_extensions: NotRequired[bool]
    allow_community_extensions: NotRequired[bool]
    allow_unsigned_extensions: NotRequired[bool]
    extension_directory: NotRequired[str]
    custom_extension_repository: NotRequired[str]
    autoinstall_extension_repository: NotRequired[str]
    allow_persistent_secrets: NotRequired[bool]
    enable_external_access: NotRequired[bool]
    secret_directory: NotRequired[str]
    enable_object_cache: NotRequired[bool]
    parquet_metadata_cache: NotRequired[str]
    enable_external_file_cache: NotRequired[bool]
    checkpoint_threshold: NotRequired[str]
    enable_progress_bar: NotRequired[bool]
    progress_bar_time: NotRequired[float]
    enable_logging: NotRequired[bool]
    log_query_path: NotRequired[str]
    logging_level: NotRequired[str]
    preserve_insertion_order: NotRequired[bool]
    default_null_order: NotRequired[str]
    default_order: NotRequired[str]
    ieee_floating_point_ops: NotRequired[bool]
    binary_as_string: NotRequired[bool]
    arrow_large_buffer_size: NotRequired[bool]
    errors_as_json: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class DuckDBPoolParams(DuckDBConnectionParams, total=False):
    """Complete pool configuration for DuckDB adapter.

    Combines standardized pool parameters with DuckDB-specific connection parameters.
    """

    # Standardized pool parameters (consistent across ALL adapters)
    pool_min_size: NotRequired[int]
    pool_max_size: NotRequired[int]
    pool_timeout: NotRequired[float]
    pool_recycle_seconds: NotRequired[int]


class DuckDBExtensionConfig(TypedDict, total=False):
    """DuckDB extension configuration for auto-management."""

    name: str
    """Name of the extension to install/load."""

    version: NotRequired[str]
    """Specific version of the extension."""

    repository: NotRequired[str]
    """Repository for the extension (core, community, or custom URL)."""

    force_install: NotRequired[bool]
    """Force reinstallation of the extension."""


class DuckDBSecretConfig(TypedDict, total=False):
    """DuckDB secret configuration for AI/API integrations."""

    secret_type: str
    """Type of secret (e.g., 'openai', 'aws', 'azure', 'gcp')."""

    name: str
    """Name of the secret."""

    value: dict[str, Any]
    """Secret configuration values."""

    scope: NotRequired[str]
    """Scope of the secret (LOCAL or PERSISTENT)."""


class DuckDBDriverFeatures(TypedDict, total=False):
    """TypedDict for DuckDB driver features configuration."""

    extensions: NotRequired[Sequence[DuckDBExtensionConfig]]
    """List of extensions to install/load on connection creation."""
    secrets: NotRequired[Sequence[DuckDBSecretConfig]]
    """List of secrets to create for AI/API integrations."""
    on_connection_create: NotRequired["Callable[[DuckDBConnection], Optional[DuckDBConnection]]"]
    """Callback executed when connection is created."""


class DuckDBConfig(SyncDatabaseConfig[DuckDBConnection, DuckDBConnectionPool, DuckDBDriver]):
    """Enhanced DuckDB configuration with connection pooling and intelligent features.

    This configuration supports all of DuckDB's unique features including:

    - Connection pooling optimized for DuckDB's architecture
    - Extension auto-management and installation
    - Secret management for API integrations
    - Intelligent auto configuration settings
    - High-performance Arrow integration
    - Direct file querying capabilities
    - Performance optimizations for analytics workloads

    DuckDB Connection Pool Best Practices:
    - DuckDB performs best with long-lived connections that maintain cache
    - Default pool size is 1-4 connections (DuckDB is optimized for single connection)
    - Connection recycling is set to 24 hours by default (set to 0 to disable)
    - Shared memory databases use `:memory:shared_db` for proper concurrency
    - Health checks are minimized to reduce overhead
    """

    driver_type: "ClassVar[type[DuckDBDriver]]" = DuckDBDriver
    connection_type: "ClassVar[type[DuckDBConnection]]" = DuckDBConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[DuckDBPoolParams, dict[str, Any]]]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        pool_instance: "Optional[DuckDBConnectionPool]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[Union[DuckDBDriverFeatures, dict[str, Any]]]" = None,
    ) -> None:
        """Initialize DuckDB configuration with intelligent features."""
        if pool_config is None:
            pool_config = {}
        if "database" not in pool_config:
            pool_config["database"] = ":memory:shared_db"

        if pool_config.get("database") in {":memory:", ""}:
            pool_config["database"] = ":memory:shared_db"

        super().__init__(
            pool_config=dict(pool_config),
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or duckdb_statement_config,
            driver_features=cast("dict[str, Any]", driver_features),
        )

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""
        return {
            k: v
            for k, v in self.pool_config.items()
            if v is not None
            and k not in {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        }

    def _get_pool_config_dict(self) -> "dict[str, Any]":
        """Get pool configuration as plain dict for pool creation."""
        return {
            k: v
            for k, v in self.pool_config.items()
            if v is not None and k in {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds"}
        }

    def _create_pool(self) -> DuckDBConnectionPool:
        """Create the DuckDB connection pool."""

        extensions = self.driver_features.get("extensions", None)
        secrets = self.driver_features.get("secrets", None)
        on_connection_create = self.driver_features.get("on_connection_create", None)

        # Convert extension and secret configs to plain dicts for pool compatibility
        extensions_dicts = [dict(ext) for ext in extensions] if extensions else None
        secrets_dicts = [dict(secret) for secret in secrets] if secrets else None

        # Wrap callback to match expected signature (ignore return value)
        pool_callback = None
        if on_connection_create:

            def wrapped_callback(conn: DuckDBConnection) -> None:
                on_connection_create(conn)

            pool_callback = wrapped_callback
        conf = {"extensions": extensions_dicts, "secrets": secrets_dicts, "on_connection_create": pool_callback}

        return DuckDBConnectionPool(
            connection_config=self._get_connection_config_dict(),
            **conf,  # type: ignore[arg-type]
            **self._get_pool_config_dict(),
        )

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> DuckDBConnection:
        """Get a DuckDB connection from the pool.

        This method ensures the pool is created and returns a connection
        from the pool. The connection is checked out from the pool and must
        be properly managed by the caller.

        Returns:
            DuckDBConnection: A connection from the pool

        Note:
            For automatic connection management, prefer using provide_connection()
            or provide_session() which handle returning connections to the pool.
            The caller is responsible for returning the connection to the pool
            using pool.release(connection) when done.
        """
        logger.info("Getting DuckDB connection from pool", extra={"adapter": "duckdb"})

        # Ensure pool exists
        pool = self.provide_pool()

        # Use the pool's acquire method
        connection = pool.acquire()

        logger.info("DuckDB connection acquired from pool", extra={"adapter": "duckdb"})
        return connection

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBConnection, None, None]":
        """Provide a pooled DuckDB connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A DuckDB connection instance.
        """
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "Generator[DuckDBDriver, None, None]":
        """Provide a DuckDB driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A context manager that yields a DuckDBDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            # Use shared config or user-provided config or instance default
            final_statement_config = statement_config or self.statement_config
            driver = self.driver_type(connection=connection, statement_config=final_statement_config)
            yield driver

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for DuckDB types.

        This provides all DuckDB-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({"DuckDBConnection": DuckDBConnection, "DuckDBCursor": DuckDBCursor})
        return namespace
