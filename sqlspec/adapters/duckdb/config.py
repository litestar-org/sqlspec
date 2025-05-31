from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict

from typing_extensions import NotRequired

from sqlspec.adapters.duckdb.driver import DuckDBConnection, DuckDBDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Generator


__all__ = ("DuckDBConfig", "DuckDBConnectionConfig")


class DuckDBExtensionConfig(TypedDict, total=False):
    """DuckDB extension configuration."""

    name: str
    """Name of the extension to install/load."""

    version: NotRequired[str]
    """Specific version of the extension."""

    repository: NotRequired[str]
    """Custom repository for the extension."""


class DuckDBSecretConfig(TypedDict, total=False):
    """DuckDB secret configuration for AI/API integrations."""

    secret_type: str
    """Type of secret (e.g., 'open_prompt', 'aws', 'azure')."""

    name: str
    """Name of the secret."""

    value: dict[str, Any]
    """Secret configuration values."""


class DuckDBConnectionConfig(TypedDict, total=False):
    """DuckDB connection configuration as TypedDict.

    All parameters for duckdb.connect().
    """

    database: NotRequired[str]
    """Path to the DuckDB database file. Use ':memory:' for in-memory database."""

    read_only: NotRequired[bool]
    """Whether to open the database in read-only mode."""

    config: NotRequired[dict[str, Any]]
    """DuckDB configuration options."""


class DuckDBConfig(NoPoolSyncConfig[DuckDBConnection, DuckDBDriver]):
    """Enhanced DuckDB configuration with extension autoconfiguration and intelligent features.

    Supports the missing DuckDB production features including:
    - Extension auto-management and installation
    - Secret management for API integrations
    - Connection lifecycle hooks for custom setup
    - DuckDB autoconfiguration settings
    - Performance optimizations for production
    """

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    def __init__(
        self,
        connection_config: Optional[DuckDBConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
        # Intelligent Features from Main Branch
        extensions: Optional[list[DuckDBExtensionConfig]] = None,
        secrets: Optional[list[DuckDBSecretConfig]] = None,
        on_connection_create: Optional[Callable[[DuckDBConnection], None]] = None,
        # DuckDB Autoconfiguration (Missing Production Features)
        autoload_known_extensions: bool = True,
        autoinstall_known_extensions: bool = False,
        extension_directory: Optional[str] = None,
        custom_extension_repository: Optional[str] = None,
        autoinstall_extension_repository: Optional[str] = None,
        # Security Settings
        allow_community_extensions: bool = True,
        allow_unsigned_extensions: bool = False,
        allow_persistent_secrets: bool = True,
        enable_external_access: bool = True,
        # Performance Settings
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        temp_directory: Optional[str] = None,
        # Intelligent Caching
        parquet_metadata_cache: bool = True,
        enable_external_file_cache: bool = True,
        # User Experience
        enable_progress_bar: bool = True,
        enable_logging: bool = False,
        log_query_path: Optional[str] = None,
    ) -> None:
        """Initialize enhanced DuckDB configuration with intelligent features.

        Args:
            connection_config: DuckDB connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results

            # Intelligent Features from Main Branch
            extensions: List of extensions to auto-install/load (e.g., [{"name": "open_prompt"}])
            secrets: List of secrets for AI/API integrations (e.g., OpenAI, Gemini)
            on_connection_create: Callback to run when connection is created (for macros, etc.)

            # DuckDB Autoconfiguration (Production Features)
            autoload_known_extensions: Automatically load known extensions when needed
            autoinstall_known_extensions: Automatically install known extensions when needed
            extension_directory: Directory to store extensions (important for production)
            custom_extension_repository: Custom endpoint for extension installation
            autoinstall_extension_repository: Override endpoint for autoloading

            # Security Controls
            allow_community_extensions: Allow community-built extensions
            allow_unsigned_extensions: Allow unsigned extensions (dev only)
            allow_persistent_secrets: Enable persistent secret storage
            enable_external_access: Allow external file system access

            # Performance & Memory
            memory_limit: Maximum memory usage (e.g., '1GB', '80% of RAM')
            threads: Number of threads to use
            temp_directory: Directory for temporary files

            # Intelligent Caching (Performance Boost)
            parquet_metadata_cache: Cache Parquet metadata for repeated access
            enable_external_file_cache: Cache external files in memory

            # User Experience
            enable_progress_bar: Show progress bar for long queries
            enable_logging: Enable DuckDB logging
            log_query_path: Path to log queries for debugging
        """
        self.connection_config = connection_config or {"database": ":memory:"}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        # Intelligent features from main branch
        self.extensions = extensions or []
        self.secrets = secrets or []
        self.on_connection_create = on_connection_create

        # Store the intelligent DuckDB configuration
        self.duckdb_settings: dict[str, Any] = {
            # Extension autoconfiguration - these were missing!
            "autoload_known_extensions": autoload_known_extensions,
            "autoinstall_known_extensions": autoinstall_known_extensions,
            "allow_community_extensions": allow_community_extensions,
            "allow_unsigned_extensions": allow_unsigned_extensions,
            # Security and access
            "allow_persistent_secrets": allow_persistent_secrets,
            "enable_external_access": enable_external_access,
            # Performance optimizations
            "parquet_metadata_cache": parquet_metadata_cache,
            "enable_external_file_cache": enable_external_file_cache,
            # User experience
            "enable_progress_bar": enable_progress_bar,
            "enable_logging": enable_logging,
        }

        # Add optional settings
        if extension_directory:
            self.duckdb_settings["extension_directory"] = extension_directory
        if custom_extension_repository:
            self.duckdb_settings["custom_extension_repository"] = custom_extension_repository
        if autoinstall_extension_repository:
            self.duckdb_settings["autoinstall_extension_repository"] = autoinstall_extension_repository
        if memory_limit:
            self.duckdb_settings["memory_limit"] = memory_limit
        if threads:
            self.duckdb_settings["threads"] = threads
        if temp_directory:
            self.duckdb_settings["temp_directory"] = temp_directory
        if log_query_path:
            self.duckdb_settings["log_query_path"] = log_query_path

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
        """Return the connection configuration as a dict."""
        # Filter out empty values and return clean dict
        return {k: v for k, v in self.connection_config.items() if v is not Empty}

    def create_connection(self) -> DuckDBConnection:
        """Create and return a DuckDB connection with intelligent configuration applied."""
        import duckdb

        config = self.connection_config_dict
        connection = duckdb.connect(**config)

        # Apply the intelligent DuckDB settings
        for setting, value in self.duckdb_settings.items():
            try:
                if isinstance(value, bool):
                    connection.execute(f"SET {setting} = {str(value).lower()}")
                elif isinstance(value, (int, float)):
                    connection.execute(f"SET {setting} = {value}")
                elif isinstance(value, str):
                    connection.execute(f"SET {setting} = '{value}'")
            except Exception:
                # Some settings might not be available in all DuckDB versions
                # Gracefully continue rather than failing
                pass

        # Install and load extensions (intelligent feature from main branch)
        for ext_config in self.extensions:
            try:
                ext_name = ext_config.get("name")
                if ext_name:
                    # Try to install if autoinstall is enabled
                    if self.duckdb_settings.get("autoinstall_known_extensions", False):
                        connection.execute(f"INSTALL {ext_name}")
                    # Load the extension
                    connection.execute(f"LOAD {ext_name}")
            except Exception:
                # Extension might already be installed or not available
                pass

        # Create secrets for AI/API integrations (intelligent feature from main branch)
        for secret_config in self.secrets:
            try:
                secret_type = secret_config.get("secret_type")
                secret_name = secret_config.get("name")
                secret_value = secret_config.get("value")

                if secret_type and secret_name and secret_value:
                    # Build the secret creation SQL
                    value_pairs = []
                    for key, value in secret_value.items():
                        value_pairs.append(f"'{key}' = '{value}'")
                    value_string = ", ".join(value_pairs)

                    connection.execute(f"""
                        CREATE SECRET {secret_name} (
                            TYPE {secret_type},
                            {value_string}
                        )
                    """)
            except Exception:
                # Secret might already exist or have issues
                pass

        # Run connection creation hook (intelligent feature from main branch)
        if self.on_connection_create:
            try:
                self.on_connection_create(connection)
            except Exception:
                # Don't fail connection creation if hook fails
                pass

        return connection

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBConnection, None, None]":
        """Provide a DuckDB connection context manager."""
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBDriver, None, None]":
        """Provide a DuckDB driver session context manager."""
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(
                connection=connection,
                config=self.statement_config,
            )
