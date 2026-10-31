"""SQLite adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import sqlite3 as sqlite_module
from typing import TYPE_CHECKING, Any, ClassVar

_SqliteConnection = sqlite_module.Connection

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.sqlite.driver import SqliteDriver
    from sqlspec.core import StatementConfig

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
        "_driver_class",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    _default_driver_class: ClassVar[Any] = None

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[SqliteDriver], SqliteDriver]",
        driver_class: "type[SqliteDriver] | None" = None,
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_class = driver_class or self._default_driver_class
        self._connection: Any = None
        self._driver: SqliteDriver | None = None

    def __enter__(self) -> "SqliteDriver":
        driver_cls = self._driver_class
        if driver_cls is None:
            from sqlspec.adapters.sqlite.driver import SqliteDriver

            driver_cls = SqliteDriver

        self._connection = self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None
