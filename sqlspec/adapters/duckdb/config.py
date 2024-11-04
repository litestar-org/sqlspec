from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.types.configs import GenericDatabaseConfig
from sqlspec.types.empty import Empty, EmptyType
from sqlspec.utils.dataclass import simple_asdict

if TYPE_CHECKING:
    from collections.abc import Generator

    from duckdb import DuckDBPyConnection

__all__ = ("DuckDBConfig",)


@dataclass
class DuckDBConfig(GenericDatabaseConfig):
    """Configuration for DuckDB database connections.

    This class provides configuration options for DuckDB database connections, wrapping all parameters
    available to duckdb.connect().

    For details see: https://duckdb.org/docs/api/python/overview#connection-options
    """

    database: str | EmptyType = Empty
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk. If not specified, an in-memory database will be created."""

    read_only: bool | EmptyType = Empty
    """If True, the database will be opened in read-only mode. This is required if multiple processes want to access the same database file at the same time."""

    config: dict[str, Any] | EmptyType = Empty
    """A dictionary of configuration options to be passed to DuckDB. These can include settings like 'access_mode', 'max_memory', 'threads', etc.

    For details see: https://duckdb.org/docs/api/python/overview#connection-options
    """

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the duckdb.connect() function.
        """
        config = simple_asdict(self, exclude_empty=True, convert_nested=False)
        if not config.get("database"):
            config["database"] = ":memory:"
        return config

    def create_connection(self) -> DuckDBPyConnection:
        """Create and return a new database connection.

        Returns:
            A new DuckDB connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        import duckdb

        try:
            return duckdb.connect(**self.connection_config_dict)
        except Exception as e:
            msg = f"Could not configure the DuckDB connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @contextmanager
    def lifespan(self, *args: Any, **kwargs: Any) -> Generator[None, None, None]:
        """Manage the lifecycle of a database connection.

        Yields:
            None

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        connection = self.create_connection()
        try:
            yield
        finally:
            connection.close()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[DuckDBPyConnection, None, None]:
        """Create and provide a database connection.

        Yields:
            A DuckDB connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()
