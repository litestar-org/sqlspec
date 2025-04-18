from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from adbc_driver_manager.dbapi import Connection

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType

if TYPE_CHECKING:
    from collections.abc import Generator


__all__ = ("Adbc",)


@dataclass
class Adbc(NoPoolSyncConfig["Connection", "AdbcDriver"]):
    """Configuration for ADBC connections.

    This class provides configuration options for ADBC database connections using the
    ADBC Driver Manager.([1](https://arrow.apache.org/adbc/current/python/api/adbc_driver_manager.html))
    """

    uri: "Union[str, EmptyType]" = Empty
    """Database URI"""
    driver_name: "Union[str, EmptyType]" = Empty
    """Name of the ADBC driver to use"""
    db_kwargs: "Optional[dict[str, Any]]" = None
    """Additional database-specific connection parameters"""
    conn_kwargs: "Optional[dict[str, Any]]" = None
    """Additional database-specific connection parameters"""
    connection_type: "type[Connection]" = field(init=False, default_factory=lambda: Connection)
    """Type of the connection object"""
    driver_type: "type[AdbcDriver]" = field(init=False, default_factory=lambda: AdbcDriver)  # type: ignore[type-abstract,unused-ignore]
    """Type of the driver object"""
    pool_instance: None = field(init=False, default=None)
    """No connection pool is used for ADBC connections"""

    def _set_adbc(self) -> str:
        """Identify the driver type based on the URI (if provided) or preset driver name.

        Raises:
            ImproperConfigurationError: If the driver name is not recognized or supported.

        Returns:
            str: The driver name to be used for the connection.
        """

        if isinstance(self.driver_name, str):
            return self.driver_name
        if isinstance(self.uri, str) and self.uri.startswith("postgresql://"):
            self.driver_name = "adbc_driver_postgresql"
        elif isinstance(self.uri, str) and self.uri.startswith("sqlite://"):
            self.driver_name = "adbc_driver_sqlite"
        elif isinstance(self.uri, str) and self.uri.startswith("grpc://"):
            self.driver_name = "adbc_driver_flightsql"
        elif isinstance(self.uri, str) and self.uri.startswith("snowflake://"):
            self.driver_name = "adbc_driver_snowflake"
        elif isinstance(self.uri, str) and self.uri.startswith("bigquery://"):
            self.driver_name = "adbc_driver_bigquery"
        elif isinstance(self.uri, str) and self.uri.startswith("duckdb://"):
            self.driver_name = "adbc_driver_duckdb"

        else:
            msg = f"Unsupported driver name: {self.driver_name}"
            raise ImproperConfigurationError(msg)
        return self.driver_name

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the adbc_driver_manager.dbapi.connect function.
        """
        config: dict[str, Any] = {}
        config["driver"] = self._set_adbc()
        db_kwargs = self.db_kwargs or {}
        conn_kwargs = self.conn_kwargs or {}
        if self.uri is not Empty:
            db_kwargs["uri"] = self.uri
        config["db_kwargs"] = db_kwargs
        config["conn_kwargs"] = conn_kwargs
        return config

    def create_connection(self) -> "Connection":
        """Create and return a new database connection.

        Returns:
            A new ADBC connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        try:
            from adbc_driver_manager.dbapi import connect

            return connect(**self.connection_config_dict)
        except Exception as e:
            msg = f"Could not configure the ADBC connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[Connection, None, None]":
        """Create and provide a database connection.

        Yields:
            Connection: A database connection instance.
        """
        from adbc_driver_manager.dbapi import connect

        with connect(**self.connection_config_dict) as connection:
            yield connection

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[AdbcDriver, None, None]":
        """Create and provide a database session.

        Yields:
            An ADBC driver instance with an active connection.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)
