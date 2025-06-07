"""ADBC database configuration using TypedDict for better maintainability."""

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict

from typing_extensions import NotRequired

from sqlspec.adapters.adbc.driver import AdbcConnection, AdbcDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger("sqlspec.adapters.adbc")

__all__ = (
    "AdbcConfig",
    "AdbcConnectionConfig",
)


class AdbcConnectionConfig(TypedDict, total=False):
    """ADBC connection configuration as TypedDict.

    Universal configuration for ADBC connections supporting multiple database drivers
    including PostgreSQL, SQLite, DuckDB, BigQuery, Snowflake, and Flight SQL.
    """

    # Core connection parameters
    uri: NotRequired[str]
    """Database URI (e.g., 'postgresql://...', 'sqlite://...', 'bigquery://...')."""

    driver_name: NotRequired[str]
    """Full dotted path to ADBC driver connect function or driver alias."""

    # Database-specific parameters
    db_kwargs: NotRequired[dict[str, Any]]
    """Additional database-specific connection parameters."""

    conn_kwargs: NotRequired[dict[str, Any]]
    """Additional connection-specific parameters."""

    # Driver-specific configurations
    adbc_driver_manager_entrypoint: NotRequired[str]
    """Override for driver manager entrypoint."""

    # Connection options
    autocommit: NotRequired[bool]
    """Enable autocommit mode."""

    isolation_level: NotRequired[str]
    """Transaction isolation level."""

    # Performance options
    batch_size: NotRequired[int]
    """Batch size for bulk operations."""

    query_timeout: NotRequired[int]
    """Query timeout in seconds."""

    connection_timeout: NotRequired[int]
    """Connection timeout in seconds."""

    # Security options
    ssl_mode: NotRequired[str]
    """SSL mode for secure connections."""

    ssl_cert: NotRequired[str]
    """SSL certificate path."""

    ssl_key: NotRequired[str]
    """SSL private key path."""

    ssl_ca: NotRequired[str]
    """SSL certificate authority path."""

    # Authentication
    username: NotRequired[str]
    """Database username."""

    password: NotRequired[str]
    """Database password."""

    token: NotRequired[str]
    """Authentication token (for cloud services)."""

    # Cloud-specific options (BigQuery, Snowflake, etc.)
    project_id: NotRequired[str]
    """Project ID (BigQuery)."""

    dataset_id: NotRequired[str]
    """Dataset ID (BigQuery)."""

    account: NotRequired[str]
    """Account identifier (Snowflake)."""

    warehouse: NotRequired[str]
    """Warehouse name (Snowflake)."""

    database: NotRequired[str]
    """Database name."""

    schema: NotRequired[str]
    """Schema name."""

    role: NotRequired[str]
    """Role name (Snowflake)."""

    # Flight SQL specific
    authorization_header: NotRequired[str]
    """Authorization header for Flight SQL."""

    grpc_options: NotRequired[dict[str, Any]]
    """gRPC specific options for Flight SQL."""


class AdbcConfig(NoPoolSyncConfig[AdbcConnection, AdbcDriver]):
    """Enhanced ADBC configuration with universal database connectivity.

    ADBC (Arrow Database Connectivity) provides a unified interface for connecting
    to multiple database systems with high-performance Arrow-native data transfer.

    This configuration supports:
    - Universal driver detection and loading
    - High-performance Arrow data streaming
    - Bulk ingestion operations
    - Multiple database backends (PostgreSQL, SQLite, DuckDB, BigQuery, Snowflake, etc.)
    - Intelligent driver path resolution
    - Cloud database integrations
    """

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    # Driver class reference for dialect resolution
    driver_class: ClassVar[type[AdbcDriver]] = AdbcDriver

    # Parameter style support information - dynamic based on driver
    # These are used as defaults when driver cannot be determined
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark",)
    """ADBC parameter styles depend on the underlying driver."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """ADBC default parameter style is ? (qmark)."""

    def __init__(
        self,
        connection_config: Optional[AdbcConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        on_connection_create: Optional[Callable[[AdbcConnection], None]] = None,
    ) -> None:
        """Initialize ADBC configuration with universal connectivity features.

        Args:
            connection_config: ADBC connection and driver parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            on_connection_create: Callback executed when connection is created

        Example:
            >>> # PostgreSQL via ADBC
            >>> config = AdbcConfig(
            ...     connection_config={
            ...         "uri": "postgresql://user:pass@localhost/db",
            ...         "driver_name": "adbc_driver_postgresql",
            ...     }
            ... )

            >>> # DuckDB via ADBC
            >>> config = AdbcConfig(
            ...     connection_config={
            ...         "uri": "duckdb://mydata.db",
            ...         "driver_name": "duckdb",
            ...         "db_kwargs": {"read_only": False},
            ...     }
            ... )

            >>> # BigQuery via ADBC
            >>> config = AdbcConfig(
            ...     connection_config={
            ...         "driver_name": "bigquery",
            ...         "project_id": "my-project",
            ...         "dataset_id": "my_dataset",
            ...     }
            ... )
        """
        self.connection_config = connection_config or {}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.on_connection_create = on_connection_create
        self._dialect: DialectType = None
        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),  # pyright: ignore
        )

    @property
    def connection_type(self) -> type[AdbcConnection]:  # type: ignore[override]
        """Return the connection type."""
        return AdbcConnection

    @property
    def driver_type(self) -> type[AdbcDriver]:  # type: ignore[override]
        """Return the driver type."""
        return AdbcDriver

    def _resolve_driver_name(self) -> str:
        """Resolve and normalize the ADBC driver name.

        Supports both full driver paths and convenient aliases.

        Returns:
            The normalized driver connect function path.

        Raises:
            ImproperConfigurationError: If driver cannot be determined.
        """
        driver_name = self.connection_config.get("driver_name")
        uri = self.connection_config.get("uri")

        # If explicit driver path is provided, normalize it
        if isinstance(driver_name, str):
            # Handle convenience aliases
            driver_aliases = {
                "sqlite": "adbc_driver_sqlite.dbapi.connect",
                "sqlite3": "adbc_driver_sqlite.dbapi.connect",
                "adbc_driver_sqlite": "adbc_driver_sqlite.dbapi.connect",
                "duckdb": "adbc_driver_duckdb.dbapi.connect",
                "adbc_driver_duckdb": "adbc_driver_duckdb.dbapi.connect",
                "postgres": "adbc_driver_postgresql.dbapi.connect",
                "postgresql": "adbc_driver_postgresql.dbapi.connect",
                "pg": "adbc_driver_postgresql.dbapi.connect",
                "adbc_driver_postgresql": "adbc_driver_postgresql.dbapi.connect",
                "snowflake": "adbc_driver_snowflake.dbapi.connect",
                "sf": "adbc_driver_snowflake.dbapi.connect",
                "adbc_driver_snowflake": "adbc_driver_snowflake.dbapi.connect",
                "bigquery": "adbc_driver_bigquery.dbapi.connect",
                "bq": "adbc_driver_bigquery.dbapi.connect",
                "adbc_driver_bigquery": "adbc_driver_bigquery.dbapi.connect",
                "flightsql": "adbc_driver_flightsql.dbapi.connect",
                "adbc_driver_flightsql": "adbc_driver_flightsql.dbapi.connect",
                "grpc": "adbc_driver_flightsql.dbapi.connect",
            }

            resolved_driver = driver_aliases.get(driver_name, driver_name)

            # Ensure it ends with .dbapi.connect
            if not resolved_driver.endswith(".dbapi.connect"):
                resolved_driver = f"{resolved_driver}.dbapi.connect"

            return resolved_driver

        # Auto-detect from URI if no explicit driver
        if isinstance(uri, str):
            if uri.startswith("postgresql://"):
                return "adbc_driver_postgresql.dbapi.connect"
            if uri.startswith("sqlite://"):
                return "adbc_driver_sqlite.dbapi.connect"
            if uri.startswith("duckdb://"):
                return "adbc_driver_duckdb.dbapi.connect"
            if uri.startswith("grpc://"):
                return "adbc_driver_flightsql.dbapi.connect"
            if uri.startswith("snowflake://"):
                return "adbc_driver_snowflake.dbapi.connect"
            if uri.startswith("bigquery://"):
                return "adbc_driver_bigquery.dbapi.connect"

        # Could not determine driver
        msg = (
            "Could not determine ADBC driver connect path. Please specify 'driver_name' "
            "(e.g., 'adbc_driver_postgresql' or 'postgresql') or provide a supported 'uri'. "
            f"URI: {uri}, Driver Name: {driver_name}"
        )
        raise ImproperConfigurationError(msg)

    def _get_connect_func(self) -> Callable[..., AdbcConnection]:
        """Get the ADBC driver connect function.

        Returns:
            The driver connect function.

        Raises:
            ImproperConfigurationError: If driver cannot be loaded.
        """
        driver_path = self._resolve_driver_name()

        if self.instrumentation.log_pool_operations:
            logger.debug("Loading ADBC driver: %s", driver_path, extra={"adapter": "adbc"})

        try:
            connect_func = import_string(driver_path)
        except ImportError as e:
            # Try adding .dbapi.connect suffix as fallback
            driver_path_with_suffix = f"{driver_path}.dbapi.connect"
            try:
                connect_func = import_string(driver_path_with_suffix)
                if self.instrumentation.log_pool_operations:
                    logger.info(
                        "Loaded ADBC driver with suffix: %s", driver_path_with_suffix, extra={"adapter": "adbc"}
                    )
            except ImportError as e2:
                msg = (
                    f"Failed to import ADBC connect function from '{driver_path}' or "
                    f"'{driver_path_with_suffix}'. Is the driver installed? "
                    f"Original errors: {e} / {e2}"
                )
                raise ImproperConfigurationError(msg) from e2

        if not callable(connect_func):
            msg = f"The path '{driver_path}' did not resolve to a callable function."
            raise ImproperConfigurationError(msg)

        return connect_func  # type: ignore[no-any-return]

    def _get_dialect(self) -> "DialectType":
        """Get the SQL dialect type based on the ADBC driver.

        Returns:
            The SQL dialect type for the ADBC driver.
        """
        try:
            driver_path = self._resolve_driver_name()
        except ImproperConfigurationError:
            return None

        if "postgres" in driver_path:
            return "postgres"
        if "sqlite" in driver_path:
            return "sqlite"
        if "duckdb" in driver_path:
            return "duckdb"
        if "bigquery" in driver_path:
            return "bigquery"
        if "snowflake" in driver_path:
            return "snowflake"
        if "flightsql" in driver_path or "grpc" in driver_path:
            return "sqlite"
        return None

    def _get_parameter_styles(self) -> tuple[tuple[str, ...], str]:
        """Get parameter styles based on the underlying driver.

        Returns:
            Tuple of (supported_parameter_styles, preferred_parameter_style)
        """
        try:
            driver_path = self._resolve_driver_name()

            # Map driver paths to parameter styles
            if "postgresql" in driver_path:
                return (("numeric",), "numeric")  # $1, $2, ...
            if "sqlite" in driver_path:
                return (("qmark", "named_colon"), "qmark")  # ? or :name
            if "duckdb" in driver_path:
                return (("qmark", "numeric"), "qmark")  # ? or $1
            if "bigquery" in driver_path:
                return (("named_at",), "named_at")  # @name
            if "snowflake" in driver_path:
                return (("qmark", "numeric"), "qmark")  # ? or :1

        except Exception:
            # If we can't determine driver, use defaults
            return (self.supported_parameter_styles, self.preferred_parameter_style)
        return (("qmark",), "qmark")

    def create_connection(self) -> AdbcConnection:
        """Create and return a new ADBC connection using the specified driver.

        Returns:
            A new ADBC connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        if self.instrumentation.log_pool_operations:
            logger.info("Creating ADBC connection", extra={"adapter": "adbc"})

        try:
            connect_func = self._get_connect_func()
            config_dict = self.connection_config_dict

            if self.instrumentation.log_pool_operations:
                logger.debug("ADBC connection config: %s", config_dict, extra={"adapter": "adbc"})

            connection = connect_func(**config_dict)

            if self.instrumentation.log_pool_operations:
                logger.info("ADBC connection created successfully", extra={"adapter": "adbc"})

            # Execute connection creation hook
            if self.on_connection_create:
                try:
                    self.on_connection_create(connection)
                    if self.instrumentation.log_pool_operations:
                        logger.debug("Executed connection creation hook", extra={"adapter": "adbc"})
                except Exception as e:
                    if self.instrumentation.log_pool_operations:
                        logger.warning("Connection creation hook failed", extra={"adapter": "adbc", "error": str(e)})

        except Exception as e:
            driver_name = self.connection_config.get("driver_name", "Unknown")
            msg = f"Could not configure ADBC connection using driver '{driver_name}'. Error: {e}"
            logger.exception(msg, extra={"adapter": "adbc", "error": str(e)})
            raise ImproperConfigurationError(msg) from e
        return connection

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[AdbcConnection, None, None]":
        """Provide an ADBC connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An ADBC connection instance.
        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[AdbcDriver]":
        """Provide an ADBC driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A context manager that yields an AdbcDriver instance.
        """

        @contextmanager
        def session_manager() -> "Generator[AdbcDriver, None, None]":
            with self.provide_connection(*args, **kwargs) as connection:
                # Get parameter styles based on the actual driver
                supported_styles, preferred_style = self._get_parameter_styles()

                # Create statement config with parameter style info if not already set
                statement_config = self.statement_config
                if statement_config.allowed_parameter_styles is None:
                    from dataclasses import replace

                    statement_config = replace(
                        statement_config,
                        allowed_parameter_styles=supported_styles,
                        target_parameter_style=preferred_style,
                    )

                driver = self.driver_type(
                    connection=connection,
                    config=statement_config,
                )
                yield driver

        return session_manager()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Get the connection configuration dictionary.

        Returns:
            The connection configuration dictionary.
        """
        config = {k: v for k, v in self.connection_config.items() if v is not Empty}

        # Process URI based on driver type
        if "driver_name" in config:
            driver_name = config["driver_name"]

            if "uri" in config:
                uri = config["uri"]

                # SQLite: strip sqlite:// prefix
                if driver_name in {"sqlite", "sqlite3", "adbc_driver_sqlite"} and uri.startswith("sqlite://"):  # pyright: ignore
                    config["uri"] = uri[9:]  # Remove "sqlite://" # pyright: ignore

                # DuckDB: convert uri to path
                elif driver_name in {"duckdb", "adbc_driver_duckdb"} and uri.startswith("duckdb://"):  # pyright: ignore
                    config["path"] = uri[9:]  # Remove "duckdb://" # pyright: ignore
                    config.pop("uri", None)

            # BigQuery: wrap certain parameters in db_kwargs
            if driver_name in {"bigquery", "bq", "adbc_driver_bigquery"}:
                bigquery_params = ["project_id", "dataset_id", "token"]
                db_kwargs = config.get("db_kwargs", {})

                for param in bigquery_params:
                    if param in config and param != "db_kwargs":
                        db_kwargs[param] = config.pop(param)  # pyright: ignore

                if db_kwargs:
                    config["db_kwargs"] = db_kwargs

            # For other drivers (like PostgreSQL), merge db_kwargs into top level
            elif "db_kwargs" in config and driver_name not in {"bigquery", "bq", "adbc_driver_bigquery"}:
                db_kwargs = config.pop("db_kwargs")
                if isinstance(db_kwargs, dict):
                    config.update(db_kwargs)

            # Remove driver_name from config as it's not a connection parameter
            config.pop("driver_name", None)

        return config
