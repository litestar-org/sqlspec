"""DuckDB database configuration with connection pooling."""
# ruff: noqa: D107 W293 RUF100 S110 PLR0913 FA100 BLE001 UP037 COM812 ARG002

import logging
import threading
import time
from collections.abc import Sequence
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, Final, Optional, TypedDict, cast

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, duckdb_statement_config
from sqlspec.config import SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Callable, ClassVar, Union

    from sqlspec.core.statement import StatementConfig


logger = logging.getLogger(__name__)

DEFAULT_MIN_POOL: Final[int] = 1
DEFAULT_MAX_POOL: Final[int] = 4
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 86400

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
    """Thread-local connection manager for DuckDB with performance optimizations.

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
        """Initialize the thread-local connection manager."""
        self._connection_config = connection_config
        self._recycle = pool_recycle_seconds
        self._extensions = extensions or []
        self._secrets = secrets or []
        self._on_connection_create = on_connection_create
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._created_connections = 0
        self._connection_times: "dict[int, float]" = {}

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
            except Exception:  # noqa: BLE001, S110
                pass

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

        # Check if connection needs recycling
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
            # On error, close and recreate connection for this thread
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

        extensions_dicts = [dict(ext) for ext in extensions] if extensions else None
        secrets_dicts = [dict(secret) for secret in secrets] if secrets else None

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
        pool = self.provide_pool()

        return pool.acquire()

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
            driver = self.driver_type(connection=connection, statement_config=statement_config or self.statement_config)
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
