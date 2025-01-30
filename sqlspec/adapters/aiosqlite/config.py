from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from sqlite3 import Connection as SQLite3Connection
    from typing import Literal

    from aiosqlite import Connection

__all__ = ("AiosqliteConfig",)


@dataclass
class AiosqliteConfig(NoPoolSyncConfig["Connection"]):
    """Configuration for Aiosqlite database connections.

    This class provides configuration options for Aiosqlite database connections, wrapping all parameters
    available to aiosqlite.connect().

    For details see: https://github.com/omnilib/aiosqlite/blob/main/aiosqlite/__init__.pyi
    """

    database: str = field(default=":memory:")
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk."""
    timeout: float | EmptyType = field(default=Empty)
    """How many seconds the connection should wait before raising an OperationalError when a table is locked. If another thread or process has acquired a shared lock, a wait for the specified timeout occurs."""
    detect_types: int | EmptyType = field(default=Empty)
    """Control whether and how data types are detected. It can be 0 (default) or a combination of PARSE_DECLTYPES and PARSE_COLNAMES."""
    isolation_level: Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None | EmptyType = field(default=Empty)
    """The isolation_level of the connection. This can be None for autocommit mode or one of "DEFERRED", "IMMEDIATE" or "EXCLUSIVE"."""
    check_same_thread: bool | EmptyType = field(default=Empty)
    """If True (default), ProgrammingError is raised if the database connection is used by a thread other than the one that created it. If False, the connection may be shared across multiple threads."""
    factory: type[SQLite3Connection] | EmptyType = field(default=Empty)
    """A custom Connection class factory. If given, must be a callable that returns a Connection instance."""
    cached_statements: int | EmptyType = field(default=Empty)
    """The number of statements that SQLite will cache for this connection. The default is 128."""
    uri: bool | EmptyType = field(default=Empty)
    """If set to True, database is interpreted as a URI with supported options."""

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the aiosqlite.connect() function.
        """
        return dataclass_to_dict(self, exclude_empty=True, convert_nested=False)

    async def create_connection(self) -> Connection:
        """Create and return a new database connection.

        Returns:
            A new Aiosqlite connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        import aiosqlite

        try:
            return await aiosqlite.connect(**self.connection_config_dict)
        except Exception as e:
            msg = f"Could not configure the Aiosqlite connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[Connection, None]:
        """Create and provide a database connection.

        Yields:
            An Aiosqlite connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        connection = await self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()
