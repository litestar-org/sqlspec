"""DuckDB database configuration using TypedDict for better maintainability."""

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb.driver import DuckDBConnection, DuckDBDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager


logger = logging.getLogger(__name__)

__all__ = (
    "DuckDBConfig",
    "DuckDBConnectionConfig",
    "DuckDBExtensionConfig",
    "DuckDBSecretConfig",
)


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


class DuckDBConnectionConfig(TypedDict, total=False):
    """DuckDB connection configuration as TypedDict.

    All parameters for duckdb.connect() and configuration settings.
    """

    # Core connection parameters
    database: NotRequired[str]
    """Path to the DuckDB database file. Use ':memory:' for in-memory database."""

    read_only: NotRequired[bool]
    """Whether to open the database in read-only mode."""

    config: NotRequired[dict[str, Any]]
    """DuckDB configuration options passed directly to the connection."""

    # Resource management
    memory_limit: NotRequired[str]
    """Maximum memory usage (e.g., '1GB', '80% of RAM')."""

    threads: NotRequired[int]
    """Number of threads to use for parallel query execution."""

    temp_directory: NotRequired[str]
    """Directory for temporary files during spilling."""

    max_temp_directory_size: NotRequired[str]
    """Maximum size of temp directory (e.g., '1GB')."""

    # Extension configuration
    autoload_known_extensions: NotRequired[bool]
    """Automatically load known extensions when needed."""

    autoinstall_known_extensions: NotRequired[bool]
    """Automatically install known extensions when needed."""

    allow_community_extensions: NotRequired[bool]
    """Allow community-built extensions."""

    allow_unsigned_extensions: NotRequired[bool]
    """Allow unsigned extensions (development only)."""

    extension_directory: NotRequired[str]
    """Directory to store extensions."""

    custom_extension_repository: NotRequired[str]
    """Custom endpoint for extension installation."""

    autoinstall_extension_repository: NotRequired[str]
    """Override endpoint for autoloading extensions."""

    # Security and access
    allow_persistent_secrets: NotRequired[bool]
    """Enable persistent secret storage."""

    enable_external_access: NotRequired[bool]
    """Allow external file system access."""

    secret_directory: NotRequired[str]
    """Directory for persistent secrets."""

    # Performance optimizations
    enable_object_cache: NotRequired[bool]
    """Enable caching of objects (e.g., Parquet metadata)."""

    parquet_metadata_cache: NotRequired[bool]
    """Cache Parquet metadata for repeated access."""

    enable_external_file_cache: NotRequired[bool]
    """Cache external files in memory."""

    checkpoint_threshold: NotRequired[str]
    """WAL size threshold for automatic checkpoints."""

    # User experience
    enable_progress_bar: NotRequired[bool]
    """Show progress bar for long queries."""

    progress_bar_time: NotRequired[int]
    """Time in milliseconds before showing progress bar."""

    # Logging and debugging
    enable_logging: NotRequired[bool]
    """Enable DuckDB logging."""

    log_query_path: NotRequired[str]
    """Path to log queries for debugging."""

    logging_level: NotRequired[str]
    """Log level (DEBUG, INFO, WARNING, ERROR)."""

    # Data processing settings
    preserve_insertion_order: NotRequired[bool]
    """Whether to preserve insertion order in results."""

    default_null_order: NotRequired[str]
    """Default NULL ordering (NULLS_FIRST, NULLS_LAST)."""

    default_order: NotRequired[str]
    """Default sort order (ASC, DESC)."""

    ieee_floating_point_ops: NotRequired[bool]
    """Use IEEE 754 compliant floating point operations."""

    # File format settings
    binary_as_string: NotRequired[bool]
    """Interpret binary data as string in Parquet files."""

    arrow_large_buffer_size: NotRequired[bool]
    """Use large Arrow buffers for strings, blobs, etc."""

    # Error handling
    errors_as_json: NotRequired[bool]
    """Return errors in JSON format."""


class DuckDBConfig(NoPoolSyncConfig[DuckDBConnection, DuckDBDriver]):
    """Enhanced DuckDB configuration with intelligent features and modern architecture.

    DuckDB is an embedded analytical database that doesn't require connection pooling.
    This configuration supports all of DuckDB's unique features including:

    - Extension auto-management and installation
    - Secret management for API integrations
    - Intelligent auto configuration settings
    - High-performance Arrow integration
    - Direct file querying capabilities
    - Performance optimizations for analytics workloads
    """

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[DuckDBDriver]] = DuckDBDriver

    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "numeric")
    """DuckDB supports ? (qmark) and $1, $2 (numeric) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """DuckDB's native parameter style is ? (qmark)."""

    def __init__(
        self,
        connection_config: "Optional[DuckDBConnectionConfig]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        instrumentation: "Optional[InstrumentationConfig]" = None,
        default_row_type: type[DictRow] = DictRow,
        extensions: "Optional[list[DuckDBExtensionConfig]]" = None,
        secrets: "Optional[list[DuckDBSecretConfig]]" = None,
        on_connection_create: "Optional[Callable[[DuckDBConnection], Optional[DuckDBConnection]]]" = None,
    ) -> None:
        """Initialize DuckDB configuration with intelligent features.

        Args:
            connection_config: DuckDB connection and configuration parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            extensions: List of extensions to auto-install/load
            secrets: List of secrets for AI/API integrations
            on_connection_create: Callback executed when connection is created

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
        self.connection_config = connection_config or {"database": ":memory:"}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        # DuckDB intelligent features
        self.extensions = extensions or []
        self.secrets = secrets or []
        self.on_connection_create = on_connection_create

        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),
        )

    @property
    def connection_type(self) -> type[DuckDBConnection]:  # type: ignore[override]
        """Return the connection type."""
        return DuckDBConnection

    @property
    def driver_type(self) -> type[DuckDBDriver]:  # type: ignore[override]
        """Return the driver type."""
        return DuckDBDriver

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        return {k: v for k, v in (self.connection_config or {}).items() if v is not Empty}

    def create_connection(self) -> DuckDBConnection:
        """Create and return a DuckDB connection with intelligent configuration applied."""

        if self.instrumentation.log_pool_operations:
            logger.info("Creating DuckDB connection", extra={"adapter": "duckdb"})

        try:
            config_dict = self.connection_config_dict
            connection = duckdb.connect(**config_dict)

            if self.instrumentation.log_pool_operations:
                logger.info("DuckDB connection created successfully", extra={"adapter": "duckdb"})

            # Install and load extensions
            for ext_config in self.extensions:
                ext_name = None
                try:
                    ext_name = ext_config.get("name")
                    if not ext_name:
                        continue

                    # Install extension if needed
                    install_kwargs: dict[str, Any] = {}
                    if "version" in ext_config:
                        install_kwargs["version"] = ext_config["version"]
                    if "repository" in ext_config:
                        install_kwargs["repository"] = ext_config["repository"]
                    if ext_config.get("force_install", False):
                        install_kwargs["force_install"] = True

                    if install_kwargs or self.connection_config.get("autoinstall_known_extensions", False):
                        connection.install_extension(ext_name, **install_kwargs)

                    # Load the extension
                    connection.load_extension(ext_name)

                    if self.instrumentation.log_pool_operations:
                        logger.debug("Loaded DuckDB extension: %s", ext_name, extra={"adapter": "duckdb"})

                except Exception as e:
                    if self.instrumentation.log_pool_operations and ext_name:
                        logger.warning(
                            "Failed to load DuckDB extension: %s",
                            ext_name,
                            extra={"adapter": "duckdb", "error": str(e)},
                        )

            for secret_config in self.secrets:
                secret_name = None
                try:
                    secret_type = secret_config.get("secret_type")
                    secret_name = secret_config.get("name")
                    secret_value = secret_config.get("value")

                    if secret_type and secret_name and secret_value:
                        # Build the secret creation SQL
                        value_pairs = []
                        for key, value in secret_value.items():
                            # Escape single quotes in values
                            escaped_value = str(value).replace("'", "''")
                            value_pairs.append(f"'{key}' = '{escaped_value}'")
                        value_string = ", ".join(value_pairs)

                        # Add scope if specified
                        scope_clause = ""
                        if "scope" in secret_config:
                            scope_clause = f" SCOPE '{secret_config['scope']}'"

                        sql = f"""
                            CREATE SECRET {secret_name} (
                                TYPE {secret_type},
                                {value_string}
                            ){scope_clause}
                        """
                        connection.execute(sql)

                        if self.instrumentation.log_pool_operations:
                            logger.debug("Created DuckDB secret: %s", secret_name, extra={"adapter": "duckdb"})

                except Exception as e:
                    if self.instrumentation.log_pool_operations and secret_name:
                        logger.warning(
                            "Failed to create DuckDB secret: %s",
                            secret_name,
                            extra={"adapter": "duckdb", "error": str(e)},
                        )

            # Run connection creation hook
            if self.on_connection_create:
                try:
                    self.on_connection_create(connection)
                    if self.instrumentation.log_pool_operations:
                        logger.debug("Executed connection creation hook", extra={"adapter": "duckdb"})
                except Exception as e:
                    if self.instrumentation.log_pool_operations:
                        logger.warning("Connection creation hook failed", extra={"adapter": "duckdb", "error": str(e)})

        except Exception as e:
            logger.exception("Failed to create DuckDB connection", extra={"adapter": "duckdb", "error": str(e)})
            raise
        return connection

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBConnection, None, None]":
        """Provide a DuckDB connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A DuckDB connection instance.
        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[DuckDBDriver]":
        """Provide a DuckDB driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A context manager that yields a DuckDBDriver instance.
        """

        @contextmanager
        def session_manager() -> "Generator[DuckDBDriver, None, None]":
            with self.provide_connection(*args, **kwargs) as connection:
                # Create statement config with parameter style info if not already set
                statement_config = self.statement_config
                if statement_config.allowed_parameter_styles is None:
                    from dataclasses import replace

                    statement_config = replace(
                        statement_config,
                        allowed_parameter_styles=self.supported_parameter_styles,
                        target_parameter_style=self.preferred_parameter_style,
                    )

                driver = self.driver_type(
                    connection=connection,
                    config=statement_config,
                )
                yield driver

        return session_manager()
