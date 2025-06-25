"""DuckDB database configuration with direct field-based configuration."""

import logging
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb.driver import DuckDBConnection, DuckDBDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType


logger = logging.getLogger(__name__)

__all__ = ("CONNECTION_FIELDS", "DuckDBConfig", "DuckDBExtensionConfig", "DuckDBSecretConfig")


CONNECTION_FIELDS = frozenset(
    {
        "database",
        "read_only",
        "config",
        "memory_limit",
        "threads",
        "temp_directory",
        "max_temp_directory_size",
        "autoload_known_extensions",
        "autoinstall_known_extensions",
        "allow_community_extensions",
        "allow_unsigned_extensions",
        "extension_directory",
        "custom_extension_repository",
        "autoinstall_extension_repository",
        "allow_persistent_secrets",
        "enable_external_access",
        "secret_directory",
        "enable_object_cache",
        "parquet_metadata_cache",
        "enable_external_file_cache",
        "checkpoint_threshold",
        "enable_progress_bar",
        "progress_bar_time",
        "enable_logging",
        "log_query_path",
        "logging_level",
        "preserve_insertion_order",
        "default_null_order",
        "default_order",
        "ieee_floating_point_ops",
        "binary_as_string",
        "arrow_large_buffer_size",
        "errors_as_json",
    }
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

    __slots__ = (
        "_dialect",
        "allow_community_extensions",
        "allow_persistent_secrets",
        "allow_unsigned_extensions",
        "arrow_large_buffer_size",
        "autoinstall_extension_repository",
        "autoinstall_known_extensions",
        "autoload_known_extensions",
        "binary_as_string",
        "checkpoint_threshold",
        "config",
        "custom_extension_repository",
        "database",
        "default_null_order",
        "default_order",
        "default_row_type",
        "enable_external_access",
        "enable_external_file_cache",
        "enable_logging",
        "enable_object_cache",
        "enable_progress_bar",
        "errors_as_json",
        "extension_directory",
        "extensions",
        "extras",
        "ieee_floating_point_ops",
        "log_query_path",
        "logging_level",
        "max_temp_directory_size",
        "memory_limit",
        "on_connection_create",
        "parquet_metadata_cache",
        "pool_instance",
        "preserve_insertion_order",
        "progress_bar_time",
        "read_only",
        "secret_directory",
        "secrets",
        "statement_config",
        "temp_directory",
        "threads",
    )

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = False

    driver_type: type[DuckDBDriver] = DuckDBDriver
    connection_type: type[DuckDBConnection] = DuckDBConnection

    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark", "numeric")
    """DuckDB supports ? (qmark) and $1, $2 (numeric) parameter styles."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """DuckDB's native parameter style is ? (qmark)."""

    def __init__(
        self,
        statement_config: "Optional[SQLConfig]" = None,
        default_row_type: type[DictRow] = DictRow,
        # Core connection parameters
        database: Optional[str] = None,
        read_only: Optional[bool] = None,
        config: Optional[dict[str, Any]] = None,
        # Resource management
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        temp_directory: Optional[str] = None,
        max_temp_directory_size: Optional[str] = None,
        # Extension configuration
        autoload_known_extensions: Optional[bool] = None,
        autoinstall_known_extensions: Optional[bool] = None,
        allow_community_extensions: Optional[bool] = None,
        allow_unsigned_extensions: Optional[bool] = None,
        extension_directory: Optional[str] = None,
        custom_extension_repository: Optional[str] = None,
        autoinstall_extension_repository: Optional[str] = None,
        # Security and access
        allow_persistent_secrets: Optional[bool] = None,
        enable_external_access: Optional[bool] = None,
        secret_directory: Optional[str] = None,
        # Performance optimizations
        enable_object_cache: Optional[bool] = None,
        parquet_metadata_cache: Optional[bool] = None,
        enable_external_file_cache: Optional[bool] = None,
        checkpoint_threshold: Optional[str] = None,
        # User experience
        enable_progress_bar: Optional[bool] = None,
        progress_bar_time: Optional[int] = None,
        # Logging and debugging
        enable_logging: Optional[bool] = None,
        log_query_path: Optional[str] = None,
        logging_level: Optional[str] = None,
        # Data processing settings
        preserve_insertion_order: Optional[bool] = None,
        default_null_order: Optional[str] = None,
        default_order: Optional[str] = None,
        ieee_floating_point_ops: Optional[bool] = None,
        # File format settings
        binary_as_string: Optional[bool] = None,
        arrow_large_buffer_size: Optional[bool] = None,
        # Error handling
        errors_as_json: Optional[bool] = None,
        # DuckDB intelligent features
        extensions: "Optional[Sequence[DuckDBExtensionConfig]]" = None,
        secrets: "Optional[Sequence[DuckDBSecretConfig]]" = None,
        on_connection_create: "Optional[Callable[[DuckDBConnection], Optional[DuckDBConnection]]]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize DuckDB configuration with intelligent features.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            database: Path to the DuckDB database file. Use ':memory:' for in-memory database
            read_only: Whether to open the database in read-only mode
            config: DuckDB configuration options passed directly to the connection
            memory_limit: Maximum memory usage (e.g., '1GB', '80% of RAM')
            threads: Number of threads to use for parallel query execution
            temp_directory: Directory for temporary files during spilling
            max_temp_directory_size: Maximum size of temp directory (e.g., '1GB')
            autoload_known_extensions: Automatically load known extensions when needed
            autoinstall_known_extensions: Automatically install known extensions when needed
            allow_community_extensions: Allow community-built extensions
            allow_unsigned_extensions: Allow unsigned extensions (development only)
            extension_directory: Directory to store extensions
            custom_extension_repository: Custom endpoint for extension installation
            autoinstall_extension_repository: Override endpoint for autoloading extensions
            allow_persistent_secrets: Enable persistent secret storage
            enable_external_access: Allow external file system access
            secret_directory: Directory for persistent secrets
            enable_object_cache: Enable caching of objects (e.g., Parquet metadata)
            parquet_metadata_cache: Cache Parquet metadata for repeated access
            enable_external_file_cache: Cache external files in memory
            checkpoint_threshold: WAL size threshold for automatic checkpoints
            enable_progress_bar: Show progress bar for long queries
            progress_bar_time: Time in milliseconds before showing progress bar
            enable_logging: Enable DuckDB logging
            log_query_path: Path to log queries for debugging
            logging_level: Log level (DEBUG, INFO, WARNING, ERROR)
            preserve_insertion_order: Whether to preserve insertion order in results
            default_null_order: Default NULL ordering (NULLS_FIRST, NULLS_LAST)
            default_order: Default sort order (ASC, DESC)
            ieee_floating_point_ops: Use IEEE 754 compliant floating point operations
            binary_as_string: Interpret binary data as string in Parquet files
            arrow_large_buffer_size: Use large Arrow buffers for strings, blobs, etc.
            errors_as_json: Return errors in JSON format
            extensions: List of extension dicts to auto-install/load with keys: name, version, repository, force_install
            secrets: List of secret dicts for AI/API integrations with keys: secret_type, name, value, scope
            on_connection_create: Callback executed when connection is created
            **kwargs: Additional parameters (stored in extras)

        Example:
            >>> config = DuckDBConfig(
            ...     database=":memory:",
            ...     memory_limit="1GB",
            ...     threads=4,
            ...     autoload_known_extensions=True,
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
        # Store connection parameters as instance attributes
        self.database = database or ":memory:"
        self.read_only = read_only
        self.config = config
        self.memory_limit = memory_limit
        self.threads = threads
        self.temp_directory = temp_directory
        self.max_temp_directory_size = max_temp_directory_size
        self.autoload_known_extensions = autoload_known_extensions
        self.autoinstall_known_extensions = autoinstall_known_extensions
        self.allow_community_extensions = allow_community_extensions
        self.allow_unsigned_extensions = allow_unsigned_extensions
        self.extension_directory = extension_directory
        self.custom_extension_repository = custom_extension_repository
        self.autoinstall_extension_repository = autoinstall_extension_repository
        self.allow_persistent_secrets = allow_persistent_secrets
        self.enable_external_access = enable_external_access
        self.secret_directory = secret_directory
        self.enable_object_cache = enable_object_cache
        self.parquet_metadata_cache = parquet_metadata_cache
        self.enable_external_file_cache = enable_external_file_cache
        self.checkpoint_threshold = checkpoint_threshold
        self.enable_progress_bar = enable_progress_bar
        self.progress_bar_time = progress_bar_time
        self.enable_logging = enable_logging
        self.log_query_path = log_query_path
        self.logging_level = logging_level
        self.preserve_insertion_order = preserve_insertion_order
        self.default_null_order = default_null_order
        self.default_order = default_order
        self.ieee_floating_point_ops = ieee_floating_point_ops
        self.binary_as_string = binary_as_string
        self.arrow_large_buffer_size = arrow_large_buffer_size
        self.errors_as_json = errors_as_json

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        # DuckDB intelligent features
        self.extensions = extensions or []
        self.secrets = secrets or []
        self.on_connection_create = on_connection_create
        self._dialect: DialectType = None

        super().__init__()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for duckdb.connect()."""
        # DuckDB connect() only accepts database, read_only, and config parameters
        connect_params: dict[str, Any] = {}

        # Set database if provided
        if hasattr(self, "database") and self.database is not None:
            connect_params["database"] = self.database

        # Set read_only if provided
        if hasattr(self, "read_only") and self.read_only is not None:
            connect_params["read_only"] = self.read_only

        # All other parameters go into the config dict
        config_dict = {}
        for field in CONNECTION_FIELDS:
            if field not in {"database", "read_only", "config"}:
                value = getattr(self, field, None)
                if value is not None and value is not Empty:
                    config_dict[field] = value

        # Add extras to config dict
        config_dict.update(self.extras)

        # If we have config parameters, add them
        if config_dict:
            connect_params["config"] = config_dict

        return connect_params

    def create_connection(self) -> DuckDBConnection:
        """Create and return a DuckDB connection with intelligent configuration applied."""

        logger.info("Creating DuckDB connection", extra={"adapter": "duckdb"})

        try:
            config_dict = self.connection_config_dict
            connection = duckdb.connect(**config_dict)
            logger.info("DuckDB connection created successfully", extra={"adapter": "duckdb"})

            # Install and load extensions
            for ext_config in self.extensions:
                ext_name = None
                try:
                    ext_name = ext_config.get("name")
                    if not ext_name:
                        continue
                    install_kwargs: dict[str, Any] = {}
                    if "version" in ext_config:
                        install_kwargs["version"] = ext_config["version"]
                    if "repository" in ext_config:
                        install_kwargs["repository"] = ext_config["repository"]
                    if ext_config.get("force_install", False):
                        install_kwargs["force_install"] = True

                    if install_kwargs or self.autoinstall_known_extensions:
                        connection.install_extension(ext_name, **install_kwargs)
                    connection.load_extension(ext_name)
                    logger.debug("Loaded DuckDB extension: %s", ext_name, extra={"adapter": "duckdb"})

                except Exception as e:
                    if ext_name:
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
                        connection.execute(sql)
                        logger.debug("Created DuckDB secret: %s", secret_name, extra={"adapter": "duckdb"})

                except Exception as e:
                    if secret_name:
                        logger.warning(
                            "Failed to create DuckDB secret: %s",
                            secret_name,
                            extra={"adapter": "duckdb", "error": str(e)},
                        )
            if self.on_connection_create:
                try:
                    self.on_connection_create(connection)
                    logger.debug("Executed connection creation hook", extra={"adapter": "duckdb"})
                except Exception as e:
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
                    statement_config = replace(
                        statement_config,
                        allowed_parameter_styles=self.supported_parameter_styles,
                        target_parameter_style=self.preferred_parameter_style,
                    )

                driver = self.driver_type(connection=connection, config=statement_config)
                yield driver

        return session_manager()
