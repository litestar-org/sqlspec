from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator
    from sqlite3 import Connection

__all__ = ("SqliteConfig",)


@dataclass
class SqliteConfig(NoPoolSyncConfig["Connection"]):
    """Configuration for SQLite database connections.

    This class provides configuration options for SQLite database connections, wrapping all parameters
    available to sqlite3.connect().

    For details see: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    """

    database: str = ":memory:"
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk."""

    timeout: float | EmptyType = Empty
    """How many seconds the connection should wait before raising an OperationalError when a table is locked. If another thread or process has acquired a shared lock, a wait for the specified timeout occurs."""

    detect_types: int | EmptyType = Empty
    """Control whether and how data types are detected. It can be 0 (default) or a combination of PARSE_DECLTYPES and PARSE_COLNAMES."""

    isolation_level: Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None | EmptyType = Empty
    """The isolation_level of the connection. This can be None for autocommit mode or one of "DEFERRED", "IMMEDIATE" or "EXCLUSIVE"."""

    check_same_thread: bool | EmptyType = Empty
    """If True (default), ProgrammingError is raised if the database connection is used by a thread other than the one that created it. If False, the connection may be shared across multiple threads."""

    factory: type[Connection] | EmptyType = Empty
    """A custom Connection class factory. If given, must be a callable that returns a Connection instance."""

    cached_statements: int | EmptyType = Empty
    """The number of statements that SQLite will cache for this connection. The default is 128."""

    uri: bool | EmptyType = Empty
    """If set to True, database is interpreted as a URI with supported options."""

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the sqlite3.connect() function.
        """
        return dataclass_to_dict(self, exclude_empty=True, convert_nested=False)

    def create_connection(self) -> Connection:
        """Create and return a new database connection.

        Returns:
            A new SQLite connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        import sqlite3

        try:
            return sqlite3.connect(**self.connection_config_dict)  # type: ignore[no-any-return,unused-ignore]
        except Exception as e:
            msg = f"Could not configure the SQLite connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[Connection, None, None]:
        """Create and provide a database connection.

        Yields:
            A SQLite connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()
