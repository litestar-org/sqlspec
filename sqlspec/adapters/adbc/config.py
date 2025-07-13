"""ADBC database configuration using TypedDict for better maintainability."""

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict, Union

from typing_extensions import NotRequired

from sqlspec.adapters.adbc.driver import AdbcConnection, AdbcDriver
from sqlspec.adapters.adbc.pipeline_steps import adbc_null_transform_step
from sqlspec.config import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger("sqlspec.adapters.adbc")


class AdbcConnectionParams(TypedDict, total=False):
    """ADBC connection parameters."""

    uri: NotRequired[str]
    driver_name: NotRequired[str]
    db_kwargs: NotRequired[dict[str, Any]]
    conn_kwargs: NotRequired[dict[str, Any]]
    adbc_driver_manager_entrypoint: NotRequired[str]
    autocommit: NotRequired[bool]
    isolation_level: NotRequired[str]
    batch_size: NotRequired[int]
    query_timeout: NotRequired[float]
    connection_timeout: NotRequired[float]
    ssl_mode: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_ca: NotRequired[str]
    username: NotRequired[str]
    password: NotRequired[str]
    token: NotRequired[str]
    project_id: NotRequired[str]
    dataset_id: NotRequired[str]
    account: NotRequired[str]
    warehouse: NotRequired[str]
    database: NotRequired[str]
    schema: NotRequired[str]
    role: NotRequired[str]
    authorization_header: NotRequired[str]
    grpc_options: NotRequired[dict[str, Any]]
    extra: NotRequired[dict[str, Any]]


__all__ = ("AdbcConfig", "AdbcConnectionParams")


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

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = False
    driver_type: ClassVar[type[AdbcDriver]] = AdbcDriver
    connection_type: ClassVar[type[AdbcConnection]] = AdbcConnection
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("qmark",)
    default_parameter_style: ClassVar[str] = "qmark"

    def __init__(
        self,
        *,
        connection_config: Optional[Union[AdbcConnectionParams, dict[str, Any]]] = None,
        statement_config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        on_connection_create: Optional[Callable[[AdbcConnection], None]] = None,
        migration_config: Optional[dict[str, Any]] = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 1000,
    ) -> None:
        """Initialize ADBC configuration with universal connectivity features.

        Args:
            connection_config: Connection configuration parameters
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            on_connection_create: Callback executed when connection is created
            migration_config: Migration configuration
            enable_adapter_cache: Enable SQL compilation caching
            adapter_cache_size: Max cached SQL statements
            extra: Additional parameters (stored in extras)

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
        # Handle both TypedDict and dict inputs
        if connection_config is None:
            connection_config = {}

        # Convert to mutable dict if TypedDict
        self.connection_config: dict[str, Any] = dict(connection_config)

        # Extract and merge extras if present
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra", {})
            self.connection_config.update(extras)

        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.on_connection_create = on_connection_create
        super().__init__(
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
        )

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

            if not resolved_driver.endswith(".dbapi.connect"):
                resolved_driver = f"{resolved_driver}.dbapi.connect"

            return resolved_driver

        # Auto-detect from URI if no explicit driver
        if isinstance(uri, str):
            # URI scheme detection
            if uri.startswith(("postgresql://", "postgres://")):
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

            # Special SQLite patterns
            if uri == ":memory:" or uri.startswith("file:"):
                return "adbc_driver_sqlite.dbapi.connect"

            # File extension-based detection for local paths (not URLs or unknown schemes)
            if not uri.startswith(("http://", "https://", "ftp://", "sftp://")) and "://" not in uri:
                uri_lower = uri.lower()

                # SQLite extensions
                if uri_lower.endswith((".sqlite", ".sqlite3", ".db")):
                    return "adbc_driver_sqlite.dbapi.connect"

                # DuckDB extensions
                if uri_lower.endswith((".duckdb", ".ddb")):
                    return "adbc_driver_duckdb.dbapi.connect"

                # Only default to SQLite for paths that look like file paths with clear directory separators
                # and don't conflict with data file extensions
                if ("/" in uri or "\\" in uri or uri.startswith(("./", "../"))) and not uri_lower.endswith(
                    (".parquet", ".csv", ".json", ".txt", ".log", ".xml")
                ):
                    return "adbc_driver_sqlite.dbapi.connect"

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
            Tuple of (supported_parameter_styles, default_parameter_style)
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
            return (self.supported_parameter_styles, self.default_parameter_style)
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
            connection_config_dict = self._get_connection_config_dict()
            connection = connect_func(**connection_config_dict)

            if self.on_connection_create:
                self.on_connection_create(connection)
        except Exception as e:
            driver_name = self.connection_config.get("driver_name", "Unknown")
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
                supported_styles, preferred_style = self._get_parameter_styles()

                statement_config = self.statement_config
                if statement_config is not None:
                    if statement_config.dialect is None:
                        statement_config = statement_config.replace(dialect=self._get_dialect())

                    if statement_config.allowed_parameter_styles is None:
                        statement_config = statement_config.replace(
                            allowed_parameter_styles=supported_styles, default_parameter_style=preferred_style
                        )
                    if self._get_dialect() == "postgres":
                        custom_pipeline_steps = [adbc_null_transform_step]
                        if statement_config.custom_pipeline_steps:
                            custom_pipeline_steps.extend(statement_config.custom_pipeline_steps)

                        statement_config = statement_config.replace(custom_pipeline_steps=custom_pipeline_steps)

                driver = self.driver_type(connection=connection, config=statement_config)
                yield driver

        return session_manager()

    def _get_connection_config_dict(self) -> dict[str, Any]:
        """Get the connection configuration dictionary.

        Returns:
            The connection configuration dictionary.
        """
        # Return a copy of the connection config
        config = dict(self.connection_config)

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

            config.pop("driver_name", None)

        return config
