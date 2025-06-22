"""ADBC database configuration using TypedDict for better maintainability."""

import logging
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional

from sqlspec.adapters.adbc.driver import AdbcConnection, AdbcDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger("sqlspec.adapters.adbc")

CONNECTION_FIELDS = frozenset(
    {
        "uri",
        "driver_name",
        "db_kwargs",
        "conn_kwargs",
        "adbc_driver_manager_entrypoint",
        "autocommit",
        "isolation_level",
        "batch_size",
        "query_timeout",
        "connection_timeout",
        "ssl_mode",
        "ssl_cert",
        "ssl_key",
        "ssl_ca",
        "username",
        "password",
        "token",
        "project_id",
        "dataset_id",
        "account",
        "warehouse",
        "database",
        "schema",
        "role",
        "authorization_header",
        "grpc_options",
    }
)

__all__ = ("CONNECTION_FIELDS", "AdbcConfig")


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

    __slots__ = (
        "_dialect",
        "account",
        "adbc_driver_manager_entrypoint",
        "authorization_header",
        "autocommit",
        "batch_size",
        "conn_kwargs",
        "connection_timeout",
        "database",
        "dataset_id",
        "db_kwargs",
        "default_row_type",
        "driver_name",
        "extras",
        "grpc_options",
        "isolation_level",
        "on_connection_create",
        "password",
        "pool_instance",
        "project_id",
        "query_timeout",
        "role",
        "schema",
        "ssl_ca",
        "ssl_cert",
        "ssl_key",
        "ssl_mode",
        "statement_config",
        "token",
        "uri",
        "username",
        "warehouse",
    )

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = False
    driver_type: type[AdbcDriver] = AdbcDriver
    connection_type: type[AdbcConnection] = AdbcConnection

    # Parameter style support information - dynamic based on driver
    # These are used as defaults when driver cannot be determined
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark",)
    """ADBC parameter styles depend on the underlying driver."""

    preferred_parameter_style: ClassVar[str] = "qmark"
    """ADBC default parameter style is ? (qmark)."""

    def __init__(
        self,
        statement_config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        on_connection_create: Optional[Callable[[AdbcConnection], None]] = None,
        # Core connection parameters
        uri: Optional[str] = None,
        driver_name: Optional[str] = None,
        # Database-specific parameters
        db_kwargs: Optional[dict[str, Any]] = None,
        conn_kwargs: Optional[dict[str, Any]] = None,
        # Driver-specific configurations
        adbc_driver_manager_entrypoint: Optional[str] = None,
        # Connection options
        autocommit: Optional[bool] = None,
        isolation_level: Optional[str] = None,
        # Performance options
        batch_size: Optional[int] = None,
        query_timeout: Optional[int] = None,
        connection_timeout: Optional[int] = None,
        # Security options
        ssl_mode: Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None,
        ssl_ca: Optional[str] = None,
        # Authentication
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        # Cloud-specific options
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        account: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        # Flight SQL specific
        authorization_header: Optional[str] = None,
        grpc_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ADBC configuration with universal connectivity features.

        Args:
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            on_connection_create: Callback executed when connection is created
            uri: Database URI (e.g., 'postgresql://...', 'sqlite://...', 'bigquery://...')
            driver_name: Full dotted path to ADBC driver connect function or driver alias
            driver: Backward compatibility alias for driver_name
            db_kwargs: Additional database-specific connection parameters
            conn_kwargs: Additional connection-specific parameters
            adbc_driver_manager_entrypoint: Override for driver manager entrypoint
            autocommit: Enable autocommit mode
            isolation_level: Transaction isolation level
            batch_size: Batch size for bulk operations
            query_timeout: Query timeout in seconds
            connection_timeout: Connection timeout in seconds
            ssl_mode: SSL mode for secure connections
            ssl_cert: SSL certificate path
            ssl_key: SSL private key path
            ssl_ca: SSL certificate authority path
            username: Database username
            password: Database password
            token: Authentication token (for cloud services)
            project_id: Project ID (BigQuery)
            dataset_id: Dataset ID (BigQuery)
            account: Account identifier (Snowflake)
            warehouse: Warehouse name (Snowflake)
            database: Database name
            schema: Schema name
            role: Role name (Snowflake)
            authorization_header: Authorization header for Flight SQL
            grpc_options: gRPC specific options for Flight SQL
            **kwargs: Additional parameters (stored in extras)

        Example:
            >>> # PostgreSQL via ADBC
            >>> config = AdbcConfig(
            ...     uri="postgresql://user:pass@localhost/db",
            ...     driver_name="adbc_driver_postgresql",
            ... )

            >>> # DuckDB via ADBC
            >>> config = AdbcConfig(
            ...     uri="duckdb://mydata.db",
            ...     driver_name="duckdb",
            ...     db_kwargs={"read_only": False},
            ... )

            >>> # BigQuery via ADBC
            >>> config = AdbcConfig(
            ...     driver_name="bigquery",
            ...     project_id="my-project",
            ...     dataset_id="my_dataset",
            ... )
        """

        # Store connection parameters as instance attributes
        self.uri = uri
        self.driver_name = driver_name
        self.db_kwargs = db_kwargs
        self.conn_kwargs = conn_kwargs
        self.adbc_driver_manager_entrypoint = adbc_driver_manager_entrypoint
        self.autocommit = autocommit
        self.isolation_level = isolation_level
        self.batch_size = batch_size
        self.query_timeout = query_timeout
        self.connection_timeout = connection_timeout
        self.ssl_mode = ssl_mode
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.ssl_ca = ssl_ca
        self.username = username
        self.password = password
        self.token = token
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.authorization_header = authorization_header
        self.grpc_options = grpc_options

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.on_connection_create = on_connection_create
        self._dialect: DialectType = None
        super().__init__()

    def _resolve_driver_name(self) -> str:
        """Resolve and normalize the ADBC driver name.

        Supports both full driver paths and convenient aliases.

        Returns:
            The normalized driver connect function path.

        Raises:
            ImproperConfigurationError: If driver cannot be determined.
        """
        driver_name = self.driver_name
        uri = self.uri

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

        try:
            connect_func = import_string(driver_path)
        except ImportError as e:
            driver_path_with_suffix = f"{driver_path}.dbapi.connect"
            try:
                connect_func = import_string(driver_path_with_suffix)
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

        dialect_map = {
            "postgres": "postgres",
            "sqlite": "sqlite",
            "duckdb": "duckdb",
            "bigquery": "bigquery",
            "snowflake": "snowflake",
            "flightsql": "sqlite",
            "grpc": "sqlite",
        }
        for keyword, dialect in dialect_map.items():
            if keyword in driver_path:
                return dialect
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

        try:
            connect_func = self._get_connect_func()
            connection = connect_func(**self.connection_config_dict)

            if self.on_connection_create:
                self.on_connection_create(connection)
        except Exception as e:
            driver_name = self.driver_name or "Unknown"
            msg = f"Could not configure ADBC connection using driver '{driver_name}'. Error: {e}"
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
                    statement_config = replace(
                        statement_config,
                        allowed_parameter_styles=supported_styles,
                        target_parameter_style=preferred_style,
                    )

                driver = self.driver_type(connection=connection, config=statement_config)
                yield driver

        return session_manager()

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Get the connection configuration dictionary.

        Returns:
            The connection configuration dictionary.
        """
        # Gather non-None connection parameters
        config = {
            field: getattr(self, field)
            for field in CONNECTION_FIELDS
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }

        # Merge extras parameters
        config.update(self.extras)

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
