from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlspec.base import NoPoolSyncConfig
from sqlspec.typing import Empty, EmptyType

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any

    from adbc_driver_manager.dbapi import Connection

__all__ = ("AdbcDatabaseConfig",)


@dataclass
class AdbcDatabaseConfig(NoPoolSyncConfig["Connection"]):
    """Configuration for ADBC connections.

    This class provides configuration options for ADBC database connections using the
    ADBC Driver Manager.([1](https://arrow.apache.org/adbc/current/python/api/adbc_driver_manager.html))
    """

    __supports_connection_pooling = False
    __is_async = False

    uri: str | EmptyType = Empty
    """Database URI"""
    driver_name: str | EmptyType = Empty
    """Name of the ADBC driver to use"""
    db_kwargs: dict[str, Any] | None = None
    """Additional database-specific connection parameters"""

    @property
    def connection_params(self) -> dict[str, Any]:
        """Return the connection parameters as a dict."""
        return {
            k: v
            for k, v in {"uri": self.uri, "driver": self.driver_name, **(self.db_kwargs or {})}.items()
            if v is not Empty
        }

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[Connection, None, None]:
        """Create and provide a database connection."""
        from adbc_driver_manager.dbapi import connect

        with connect(**self.connection_params) as connection:
            yield connection
