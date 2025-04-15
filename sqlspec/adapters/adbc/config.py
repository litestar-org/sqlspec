from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import TypeAlias

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.base import NoPoolSyncConfig
from sqlspec.typing import Empty, EmptyType

if TYPE_CHECKING:
    from collections.abc import Generator

    from adbc_driver_manager.dbapi import Connection

__all__ = ("Adbc",)
Driver: TypeAlias = AdbcDriver


@dataclass
class Adbc(NoPoolSyncConfig["Connection", "Driver"]):
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

    @property
    def connection_params(self) -> "dict[str, Any]":
        """Return the connection parameters as a dict."""
        return {
            k: v
            for k, v in {"uri": self.uri, "driver": self.driver_name, **(self.db_kwargs or {})}.items()
            if v is not Empty
        }

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[Connection, None, None]":
        """Create and provide a database connection.

        Yields:
            Connection: A database connection instance.
        """
        from adbc_driver_manager.dbapi import connect

        with connect(**self.connection_params) as connection:
            yield connection

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[Driver, None, None]":
        """Create and provide a database connection.

        Yields:
            A Aiosqlite driver instance.


        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection, results_as_dict=True)
