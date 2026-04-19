"""SQLite adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import sqlite3
from sqlite3 import Error as SqliteError
from sqlite3 import connect as sqlite3_connect
from typing import TYPE_CHECKING, Any

_SqliteConnection = sqlite3.Connection

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.sqlite.driver import SqliteDriver
    from sqlspec.core import StatementConfig

    SqliteConnection: TypeAlias = _SqliteConnection
    SqliteRawCursor: TypeAlias = sqlite3.Cursor

if not TYPE_CHECKING:
    SqliteConnection = _SqliteConnection
    SqliteRawCursor = sqlite3.Cursor


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


class SqliteSessionContext:
    """Sync context manager for SQLite sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[SqliteDriver], SqliteDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: SqliteDriver | None = None

    def __enter__(self) -> "SqliteDriver":
        from sqlspec.adapters.sqlite.driver import SqliteDriver

        self._connection = self._acquire_connection()
        self._driver = SqliteDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "SqliteConnection",
    "SqliteCursor",
    "SqliteError",
    "SqliteRawCursor",
    "SqliteSessionContext",
    "sqlite3_connect",
)
