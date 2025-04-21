from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

import aiosqlite

from sqlspec.adapters.aiosqlite.driver import AiosqliteConnection, AiosqliteDriver
from sqlspec.base import NoPoolAsyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import Literal


__all__ = ("AiosqliteConfig",)


@dataclass
class AiosqliteConfig(NoPoolAsyncConfig["AiosqliteConnection", "AiosqliteDriver"]):
    """Configuration for Aiosqlite database connections.

    This class provides configuration options for Aiosqlite database connections, wrapping all parameters
    available to aiosqlite.connect().

    For details see: https://github.com/omnilib/aiosqlite/blob/main/aiosqlite/__init__.pyi
    """

    database: "Union[str, EmptyType]" = field(default=":memory:")
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk."""
    timeout: "Union[float, EmptyType]" = field(default=Empty)
    """How many seconds the connection should wait before raising an OperationalError when a table is locked. If another thread or process has acquired a shared lock, a wait for the specified timeout occurs."""
    detect_types: "Union[int, EmptyType]" = field(default=Empty)
    """Control whether and how data types are detected. It can be 0 (default) or a combination of PARSE_DECLTYPES and PARSE_COLNAMES."""
    isolation_level: "Optional[Union[Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE'], EmptyType]]" = field(default=Empty)
    """The isolation_level of the connection. This can be None for autocommit mode or one of "DEFERRED", "IMMEDIATE" or "EXCLUSIVE"."""
    check_same_thread: "Union[bool, EmptyType]" = field(default=Empty)
    """If True (default), ProgrammingError is raised if the database connection is used by a thread other than the one that created it. If False, the connection may be shared across multiple threads."""
    cached_statements: "Union[int, EmptyType]" = field(default=Empty)
    """The number of statements that SQLite will cache for this connection. The default is 128."""
    uri: "Union[bool, EmptyType]" = field(default=Empty)
    """If set to True, database is interpreted as a URI with supported options."""
    connection_type: "type[AiosqliteConnection]" = field(init=False, default_factory=lambda: AiosqliteConnection)
    """Type of the connection object"""
    driver_type: "type[AiosqliteDriver]" = field(init=False, default_factory=lambda: AiosqliteDriver)  # type: ignore[type-abstract,unused-ignore]
    """Type of the driver object"""

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the aiosqlite.connect() function.
        """
        return dataclass_to_dict(
            self,
            exclude_empty=True,
            convert_nested=False,
            exclude={"pool_instance", "connection_type", "driver_type"},
        )

    async def create_connection(self) -> "AiosqliteConnection":
        """Create and return a new database connection.

        Returns:
            A new Aiosqlite connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        try:
            return await aiosqlite.connect(**self.connection_config_dict)
        except Exception as e:
            msg = f"Could not configure the Aiosqlite connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @asynccontextmanager
    async def provide_connection(self, *args: "Any", **kwargs: "Any") -> "AsyncGenerator[AiosqliteConnection, None]":
        """Create and provide a database connection.

        Yields:
            An Aiosqlite connection instance.

        """
        connection = await self.create_connection()
        try:
            yield connection
        finally:
            await connection.close()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AiosqliteDriver, None]":
        """Create and provide a database connection.

        Yields:
            A Aiosqlite driver instance.


        """
        async with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)
