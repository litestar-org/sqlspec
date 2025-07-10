"""DuckDB database configuration with direct field-based configuration."""

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict, Union

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb.driver import DuckDBConnection, DuckDBDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence


logger = logging.getLogger(__name__)

__all__ = ("DuckDBConfig", "DuckDBConnectionParams", "DuckDBExtensionConfig", "DuckDBSecretConfig")


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

    driver_type: "ClassVar[type[DuckDBDriver]]" = DuckDBDriver
    connection_type: "ClassVar[type[DuckDBConnection]]" = DuckDBConnection
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ("qmark", "numeric")
    default_parameter_style: "ClassVar[str]" = "qmark"

    def __init__(
        self,
        connection_config: "Optional[Union[DuckDBConnectionParams, dict[str, Any]]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        default_row_type: type[DictRow] = DictRow,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
        extensions: "Optional[Sequence[DuckDBExtensionConfig]]" = None,
        secrets: "Optional[Sequence[DuckDBSecretConfig]]" = None,
        on_connection_create: "Optional[Callable[[DuckDBConnection], Optional[DuckDBConnection]]]" = None,
    ) -> None:
        """Initialize DuckDB configuration with intelligent features.

        Args:
            connection_config: Connection configuration parameters
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            extensions: List of extension dicts to auto-install/load with keys: name, version, repository, force_install
            secrets: List of secret dicts for AI/API integrations with keys: secret_type, name, value, scope
            on_connection_create: Callback executed when connection is created
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements

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

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.extensions = extensions or []
        self.secrets = secrets or []
        self.on_connection_create = on_connection_create
        super().__init__(
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

    def create_connection(self) -> DuckDBConnection:
        """Create and return a DuckDB connection with intelligent configuration applied."""

        logger.info("Creating DuckDB connection", extra={"adapter": "duckdb"})

        try:
            # Get properly typed configuration dictionary
            connect_params = self._get_connection_config_dict()
            connection = duckdb.connect(**connect_params)
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

                    if install_kwargs or self.connection_config.get("autoinstall_known_extensions"):
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
