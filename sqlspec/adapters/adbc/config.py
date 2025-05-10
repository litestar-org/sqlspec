from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

from sqlspec.adapters.adbc.driver import AdbcConnection, AdbcDriver
from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType
from sqlspec.utils.module_loader import import_string

if TYPE_CHECKING:
    from collections.abc import Generator


__all__ = ("AdbcConfig",)


@dataclass
class AdbcConfig(NoPoolSyncConfig["AdbcConnection", "AdbcDriver"]):
    """Configuration for ADBC connections.

    This class provides configuration options for ADBC database connections using the
    ADBC Driver Manager.([1](https://arrow.apache.org/adbc/current/python/api/adbc_driver_manager.html))
    """

    uri: "Union[str, EmptyType]" = Empty
    """Database URI"""
    driver_name: "Union[str, EmptyType]" = Empty
    """Full dotted path to the ADBC driver's connect function (e.g., 'adbc_driver_sqlite.dbapi.connect')"""
    db_kwargs: "Optional[dict[str, Any]]" = None
    """Additional database-specific connection parameters"""
    conn_kwargs: "Optional[dict[str, Any]]" = None
    """Additional database-specific connection parameters"""
    connection_type: "type[AdbcConnection]" = field(init=False, default_factory=lambda: AdbcConnection)
    """Type of the connection object"""
    driver_type: "type[AdbcDriver]" = field(init=False, default_factory=lambda: AdbcDriver)  # type: ignore[type-abstract,unused-ignore]
    """Type of the driver object"""
    pool_instance: None = field(init=False, default=None, hash=False)
    """No connection pool is used for ADBC connections"""

    def _set_adbc(self) -> str:
        """Identify the driver type based on the URI (if provided) or preset driver name.

        Raises:
            ImproperConfigurationError: If the driver name is not recognized or supported.

        Returns:
            str: The driver name to be used for the connection.
        """

        if isinstance(self.driver_name, str):
            if self.driver_name != "adbc_driver_sqlite.dbapi.connect" and self.driver_name in {
                "sqlite",
                "sqlite3",
                "adbc_driver_sqlite",
            }:
                self.driver_name = "adbc_driver_sqlite.dbapi.connect"
            elif self.driver_name != "adbc_driver_duckdb.dbapi.connect" and self.driver_name in {
                "duckdb",
                "adbc_driver_duckdb",
            }:
                self.driver_name = "adbc_driver_duckdb.dbapi.connect"
            elif self.driver_name != "adbc_driver_postgresql.dbapi.connect" and self.driver_name in {
                "postgres",
                "adbc_driver_postgresql",
                "postgresql",
                "pg",
            }:
                self.driver_name = "adbc_driver_postgresql.dbapi.connect"
            elif self.driver_name != "adbc_driver_snowflake.dbapi.connect" and self.driver_name in {
                "snowflake",
                "adbc_driver_snowflake",
                "sf",
            }:
                self.driver_name = "adbc_driver_snowflake.dbapi.connect"
            elif self.driver_name != "adbc_driver_bigquery.dbapi.connect" and self.driver_name in {
                "bigquery",
                "adbc_driver_bigquery",
                "bq",
            }:
                self.driver_name = "adbc_driver_bigquery.dbapi.connect"
            elif self.driver_name != "adbc_driver_flightsql.dbapi.connect" and self.driver_name in {
                "flightsql",
                "adbc_driver_flightsql",
                "grpc",
            }:
                self.driver_name = "adbc_driver_flightsql.dbapi.connect"
            return self.driver_name

        # If driver_name wasn't explicit, try to determine from URI
        if isinstance(self.uri, str) and self.uri.startswith("postgresql://"):
            self.driver_name = "adbc_driver_postgresql.dbapi.connect"
        elif isinstance(self.uri, str) and self.uri.startswith("sqlite://"):
            self.driver_name = "adbc_driver_sqlite.dbapi.connect"
        elif isinstance(self.uri, str) and self.uri.startswith("grpc://"):
            self.driver_name = "adbc_driver_flightsql.dbapi.connect"
        elif isinstance(self.uri, str) and self.uri.startswith("snowflake://"):
            self.driver_name = "adbc_driver_snowflake.dbapi.connect"
        elif isinstance(self.uri, str) and self.uri.startswith("bigquery://"):
            self.driver_name = "adbc_driver_bigquery.dbapi.connect"
        elif isinstance(self.uri, str) and self.uri.startswith("duckdb://"):
            self.driver_name = "adbc_driver_duckdb.dbapi.connect"

        # Check if we successfully determined a driver name
        if self.driver_name is Empty or not isinstance(self.driver_name, str):
            msg = (
                "Could not determine ADBC driver connect path. Please specify 'driver_name' "
                "(e.g., 'adbc_driver_sqlite.dbapi.connect') or provide a supported 'uri'. "
                f"URI: {self.uri}, Driver Name: {self.driver_name}"
            )
            raise ImproperConfigurationError(msg)
        return self.driver_name

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Omits the 'uri' key for known in-memory database types.

        Returns:
            A string keyed dict of config kwargs for the adbc_driver_manager.dbapi.connect function.
        """
        config = {}
        db_kwargs = self.db_kwargs or {}
        conn_kwargs = self.conn_kwargs or {}
        if isinstance(self.uri, str) and self.uri.startswith("sqlite://"):
            db_kwargs["uri"] = self.uri.replace("sqlite://", "")
        elif isinstance(self.uri, str) and self.uri.startswith("duckdb://"):
            db_kwargs["path"] = self.uri.replace("duckdb://", "")
        elif isinstance(self.uri, str):
            db_kwargs["uri"] = self.uri
        if isinstance(self.driver_name, str) and self.driver_name.startswith("adbc_driver_bigquery"):
            config["db_kwargs"] = db_kwargs
        else:
            config = db_kwargs
        if conn_kwargs:
            config["conn_kwargs"] = conn_kwargs
        return config

    def _get_connect_func(self) -> "Callable[..., AdbcConnection]":
        self._set_adbc()
        driver_path = cast("str", self.driver_name)
        try:
            connect_func = import_string(driver_path)
        except ImportError as e:
            # Check if the error is likely due to missing suffix and try again
            if ".dbapi.connect" not in driver_path:
                try:
                    driver_path += ".dbapi.connect"
                    connect_func = import_string(driver_path)
                except ImportError as e2:
                    msg = f"Failed to import ADBC connect function from '{self.driver_name}' or '{driver_path}'. Is the driver installed and the path correct? Original error: {e} / {e2}"
                    raise ImproperConfigurationError(msg) from e2
            else:
                # Original import failed, and suffix was already present or added
                msg = f"Failed to import ADBC connect function from '{driver_path}'. Is the driver installed and the path correct? Original error: {e}"
                raise ImproperConfigurationError(msg) from e
        if not callable(connect_func):
            msg = f"The path '{driver_path}' did not resolve to a callable function."
            raise ImproperConfigurationError(msg)
        return connect_func  # type: ignore[no-any-return]

    def create_connection(self) -> "AdbcConnection":
        """Create and return a new database connection using the specific driver.

        Returns:
            A new ADBC connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        try:
            connect_func = self._get_connect_func()
            return connect_func(**self.connection_config_dict)
        except Exception as e:
            # Include driver name in error message for better context
            driver_name = self.driver_name if isinstance(self.driver_name, str) else "Unknown/Missing"
            # Use the potentially modified driver_path from _get_connect_func if available,
            # otherwise fallback to self.driver_name for the error message.
            # This requires _get_connect_func to potentially return the used path or store it.
            # For simplicity now, we stick to self.driver_name in the message.
            msg = f"Could not configure the ADBC connection using driver path '{driver_name}'. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[AdbcConnection, None, None]":
        """Create and provide a database connection using the specific driver.

        Yields:
            Connection: A database connection instance.
        """

        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[AdbcDriver, None, None]":
        """Create and provide a database session.

        Yields:
            An ADBC driver instance with an active connection.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)
