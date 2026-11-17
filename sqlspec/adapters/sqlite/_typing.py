"""SQLite adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import sqlite3 as sqlite_module
from typing import TYPE_CHECKING, Any

from sqlspec.driver._session import SyncDriverSessionContext

_SqliteConnection = sqlite_module.Connection

if TYPE_CHECKING:
    from typing import TypeAlias

    from sqlspec.adapters.sqlite.driver import SqliteDriver

    SqliteConnection: TypeAlias = _SqliteConnection
    SqliteConnectionFactory: TypeAlias = type[sqlite_module.Connection]
    SqliteRawCursor: TypeAlias = sqlite_module.Cursor
else:
    SqliteConnection = _SqliteConnection
    SqliteConnectionFactory = type[sqlite_module.Connection]
    SqliteRawCursor = sqlite_module.Cursor

__all__ = (
    "SqliteConnection",
    "SqliteConnectionFactory",
    "SqliteCursor",
    "SqliteRawCursor",
    "SqliteSessionContext",
    "sqlite_module",
)


class SqliteCursor:
    """Context manager for SQLite cursor management.

    Provides automatic cursor creation and cleanup for SQLite database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "SqliteConnection") -> None:
        """Initialize cursor manager.

        Args:
            connection: SQLite database connection
        """
        self.connection = connection
        self.cursor: SqliteRawCursor | None = None

    def __enter__(self) -> "SqliteRawCursor":
        """Create and return a new cursor.

        Returns:
            Active SQLite cursor object
        """
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        """Clean up cursor resources.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class SqliteSessionContext(SyncDriverSessionContext):
    """Sync context manager for SQLite sessions."""

    __slots__ = ()

    if TYPE_CHECKING:

        def __enter__(self) -> "SqliteDriver": ...

    def _resolve_driver_class(self) -> "type[SqliteDriver]":
        from sqlspec.adapters.sqlite.driver import SqliteDriver

        return SqliteDriver
