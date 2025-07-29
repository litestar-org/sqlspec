"""DuckDB database configuration with connection pooling."""
# ruff: noqa: D107 W293 RUF100 S110 PLR0913 FA100 BLE001 UP037 COM812 ARG002

import logging
import threading
import time
from contextlib import contextmanager, suppress
from queue import Empty as QueueEmpty
from queue import Full, Queue
from typing import TYPE_CHECKING, Any, Final, Optional, TypedDict

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver
from sqlspec.config import SyncDatabaseConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence
    from typing import Callable, ClassVar, Union


logger = logging.getLogger(__name__)

# Performance constants for DuckDB pooling
DEFAULT_MIN_POOL: Final[int] = 2
DEFAULT_MAX_POOL: Final[int] = 10
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 3600  # 1 hour

__all__ = (
    "DuckDBConfig",
    "DuckDBConnectionParams",
    "DuckDBConnectionPool",
    "DuckDBExtensionConfig",
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
        min_pool: int = DEFAULT_MIN_POOL,
        max_pool: int = DEFAULT_MAX_POOL,
        timeout: float = POOL_TIMEOUT,
        recycle: int = POOL_RECYCLE,
        extensions: "Optional[list[dict[str, Any]]]" = None,  # noqa: FA100, UP037
        secrets: "Optional[list[dict[str, Any]]]" = None,  # noqa: FA100, UP037
        on_connection_create: "Optional[Callable[[DuckDBConnection], None]]" = None,  # noqa: FA100
    ) -> None:
        self._pool: "Queue[DuckDBConnection]" = Queue(maxsize=max_pool)  # noqa: UP037
        self._lock = threading.RLock()
        self._min_pool = min_pool
        self._max_pool = max_pool
        self._timeout = timeout
        self._recycle = recycle
        self._connection_config = connection_config
        self._extensions = extensions or []
        self._secrets = secrets or []
        self._on_connection_create = on_connection_create
        self._created_connections = 0
        self._checked_out = 0
        self._connection_times: "dict[int, float]" = {}  # noqa: UP037

        # Pre-populate pool
        for _ in range(min_pool):
            if self._pool.full():
                break
            conn = self._create_connection()
            try:
                self._pool.put_nowait(conn)
            except Full:
                break

    def _create_connection(self) -> DuckDBConnection:
        """Create a new DuckDB connection with extensions and secrets."""
        connection = duckdb.connect(**self._connection_config)

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
        """Check if connection should be recycled based on age."""
        conn_id = id(connection)
        created_at = self._connection_times.get(conn_id)
        if created_at is None:
            return True
        return (time.time() - created_at) > self._recycle

    def _is_connection_alive(self, connection: DuckDBConnection) -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            connection.execute("SELECT 1").fetchall()
        except Exception:
            return False
        else:
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


class DuckDBConfig(SyncDatabaseConfig[DuckDBConnection, DuckDBConnectionPool, DuckDBDriver]):
    """Enhanced DuckDB configuration with connection pooling and intelligent features.

    This configuration supports all of DuckDB's unique features including:

    - Connection pooling for concurrent operations
    - Extension auto-management and installation
    - Secret management for API integrations
    - Intelligent auto configuration settings
    - High-performance Arrow integration
    - Direct file querying capabilities
    - Performance optimizations for analytics workloads
    """

    driver_type: "ClassVar[type[DuckDBDriver]]" = DuckDBDriver
    connection_type: "ClassVar[type[DuckDBConnection]]" = DuckDBConnection
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("qmark", "numeric")
    default_parameter_style: "ClassVar[str]" = "qmark"
    supports_connection_pooling: "ClassVar[bool]" = True

    def __init__(
        self,
        connection_config: "Optional[Union[DuckDBConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
        extensions: "Optional[Sequence[DuckDBExtensionConfig]]" = None,
        secrets: "Optional[Sequence[DuckDBSecretConfig]]" = None,
        on_connection_create: "Optional[Callable[[DuckDBConnection], Optional[DuckDBConnection]]]" = None,
        min_pool: int = DEFAULT_MIN_POOL,
        max_pool: int = DEFAULT_MAX_POOL,
        pool_timeout: float = POOL_TIMEOUT,
        pool_recycle: int = POOL_RECYCLE,
        pool_instance: "Optional[DuckDBConnectionPool]" = None,
    ) -> None:
        """Initialize DuckDB configuration with intelligent features.

        Args:
            connection_config: Connection configuration parameters
            statement_config: Default SQL statement configuration
            extensions: List of extension dicts to auto-install/load with keys: name, version, repository, force_install
            secrets: List of secret dicts for AI/API integrations with keys: secret_type, name, value, scope
            on_connection_create: Callback executed when connection is created
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
            min_pool: Minimum number of connections to maintain (default: 2)
            max_pool: Maximum number of connections allowed (default: 10)
            pool_timeout: Pool checkout timeout in seconds (default: 30.0)
            pool_recycle: Connection recycle time in seconds (default: 3600)
            pool_instance: Pre-created pool instance

        Example:
            >>> config = DuckDBConfig(
            ...     connection_config={
            ...         "database": ":memory:",
            ...         "memory_limit": "1GB",
            ...         "threads": 4,
            ...         "autoload_known_extensions": True,
            ...     },
            ...     extensions=[
            ...         {"name": "spatial", "repository": "core"},
            ...         {"name": "aws", "repository": "core"},
            ...     ],
            ...     secrets=[
            ...         {
            ...             "secret_type": "openai",
            ...             "name": "my_openai_secret",
            ...             "value": {"api_key": "sk-..."},
            ...         }
            ...     ],
            ... )
        """
        # Store connection parameters and extract/merge extras
        self.connection_config: dict[str, Any] = dict(connection_config) if connection_config else {}
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        # Set default database if not provided or empty
        if "database" not in self.connection_config or not self.connection_config["database"]:
            self.connection_config["database"] = ":memory:"

        # Convert basic :memory: to unique named memory database for pooling
        # Named memory databases already work with sharing
        database = self.connection_config.get("database", ":memory:")
        if database == ":memory:":
            # Basic :memory: doesn't share between connections, convert to unique named
            import uuid
            unique_id = str(uuid.uuid4())[:8]  # Short unique identifier
            self.connection_config["database"] = f":memory:pool_{unique_id}"

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.extensions = list(extensions) if extensions else []
        self.secrets = list(secrets) if secrets else []
        self.on_connection_create = on_connection_create
        self.min_pool = min_pool
        self.max_pool = max_pool
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle

        super().__init__(
            pool_instance=pool_instance,
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

    def _get_connection_config_dict(self) -> dict[str, Any]:
        """Get connection configuration as plain dict for external library.

        Returns:
            Dictionary with connection parameters properly separated for DuckDB.
        """
        connect_params: dict[str, Any] = {}

        # Handle database parameter
        database = self.connection_config.get("database", ":memory:")
        connect_params["database"] = database

        # Handle read_only parameter
        read_only = self.connection_config.get("read_only")
        if read_only is not None:
            connect_params["read_only"] = read_only

        # All other parameters go into the config dict
        config_dict: dict[str, Any] = {
            field: value
            for field, value in self.connection_config.items()
            if field not in {"database", "read_only", "config"} and value is not None and value is not Empty
        }

        # Add user-provided config dict
        if "config" in self.connection_config:
            config_dict.update(self.connection_config["config"])

        config_dict.update(config_dict.pop("extra", {}))

        # If we have config parameters, add them
        if config_dict:
            connect_params["config"] = config_dict

        return connect_params

    def _create_pool(self) -> DuckDBConnectionPool:
        """Create the DuckDB connection pool."""
        connection_config = self._get_connection_config_dict()

        # Convert extension and secret configs to plain dicts for pool compatibility
        extensions_dicts = [dict(ext) for ext in self.extensions] if self.extensions else None
        secrets_dicts = [dict(secret) for secret in self.secrets] if self.secrets else None

        # Wrap callback to match expected signature (ignore return value)
        pool_callback = None
        if self.on_connection_create:
            original_callback = self.on_connection_create

            def wrapped_callback(conn: DuckDBConnection) -> None:
                original_callback(conn)

            pool_callback = wrapped_callback

        return DuckDBConnectionPool(
            connection_config=connection_config,
            min_pool=self.min_pool,
            max_pool=self.max_pool,
            timeout=self.pool_timeout,
            recycle=self.pool_recycle,
            extensions=extensions_dicts,
            secrets=secrets_dicts,
            on_connection_create=pool_callback,
        )

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> DuckDBConnection:
        """Create and return a DuckDB connection with intelligent configuration applied (bypasses pool)."""

        logger.info("Creating DuckDB connection", extra={"adapter": "duckdb"})

        try:
            # Get properly typed configuration dictionary
            connect_params = self._get_connection_config_dict()
            connection = duckdb.connect(**connect_params)
            logger.info("DuckDB connection created successfully", extra={"adapter": "duckdb"})

            # Install and load extensions
            for ext_config in self.extensions:
                ext_name = ext_config.get("name")
                if not ext_name:
                    continue

                install_kwargs: "dict[str, Any]" = {}  # noqa: UP037
                if "version" in ext_config:
                    install_kwargs["version"] = ext_config["version"]
                if "repository" in ext_config:
                    install_kwargs["repository"] = ext_config["repository"]
                if ext_config.get("force_install", False):
                    install_kwargs["force_install"] = True

                try:
                    if install_kwargs or self.connection_config.get("autoinstall_known_extensions"):
                        connection.install_extension(ext_name, **install_kwargs)
                    connection.load_extension(ext_name)
                    logger.debug("Loaded DuckDB extension: %s", ext_name, extra={"adapter": "duckdb"})
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "Failed to load DuckDB extension: %s", ext_name, extra={"adapter": "duckdb", "error": str(e)}
                    )

            for secret_config in self.secrets:
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

                try:
                    connection.execute(sql)
                    logger.debug("Created DuckDB secret: %s", secret_name, extra={"adapter": "duckdb"})
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "Failed to create DuckDB secret: %s", secret_name, extra={"adapter": "duckdb", "error": str(e)}
                    )
            if self.on_connection_create:
                try:
                    self.on_connection_create(connection)
                    logger.debug("Executed connection creation hook", extra={"adapter": "duckdb"})
                except Exception as e:  # noqa: BLE001
                    logger.warning("Connection creation hook failed", extra={"adapter": "duckdb", "error": str(e)})

        except Exception as e:
            logger.exception("Failed to create DuckDB connection", extra={"adapter": "duckdb", "error": str(e)})
            raise
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
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBDriver, None, None]":
        """Provide a DuckDB driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A context manager that yields a DuckDBDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            statement_config = self.statement_config
            # Inject parameter style info if not already set
            if statement_config.allowed_parameter_styles is None:
                statement_config = statement_config.replace(
                    allowed_parameter_styles=self.supported_parameter_styles,
                    default_parameter_style=self.default_parameter_style,
                )
            driver = self.driver_type(connection=connection, config=statement_config)
            yield driver

    def _is_memory_database(self, database: str) -> bool:
        """Check if the database is an in-memory database.

        Args:
            database: Database path or connection string

        Returns:
            True if this is an in-memory database
        """
        if not database:
            return True

        # Standard :memory: database
        if database == ":memory:":
            return True

        # Check for :memory: with custom name but NOT shared
        return ":memory:" in database and "shared" not in database

    def _convert_to_shared_memory(self) -> None:
        """Convert in-memory database to shared memory for connection pooling.

        Uses DuckDB's 'md:' prefix for named in-memory databases to maintain
        uniqueness while enabling safe connection pooling.
        """
        database = self.connection_config.get("database", ":memory:")

        if database in {":memory:", ""}:
            # Convert default memory database to a default named instance
            self.connection_config["database"] = "md:_default_shared"
        elif database.startswith(":memory:"):
            # Extract custom name and preserve it with md: prefix
            # Format: ":memory:custom_name" -> "md:custom_name"
            db_name = database.split(":", 2)[-1]  # Get part after second colon
            if db_name:
                self.connection_config["database"] = f"md:{db_name}"
            else:
                # Fallback for malformed ":memory:" variants
                self.connection_config["database"] = "md:_default_shared"

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
