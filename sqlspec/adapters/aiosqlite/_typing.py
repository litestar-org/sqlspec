"""AIOSQLite adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import sqlite3 as aiosqlite_sqlite_module
from typing import TYPE_CHECKING

import aiosqlite as aiosqlite_module

from sqlspec.driver._session import AsyncDriverSessionContext

_AiosqliteConnection = aiosqlite_module.Connection

if TYPE_CHECKING:
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

    AiosqliteConnection: TypeAlias = _AiosqliteConnection
    AiosqliteConnectionFactory: TypeAlias = type[aiosqlite_sqlite_module.Connection]
    AiosqliteRawCursor: TypeAlias = aiosqlite_module.Cursor
else:
    AiosqliteConnection = _AiosqliteConnection
    AiosqliteConnectionFactory = type[aiosqlite_sqlite_module.Connection]
    AiosqliteRawCursor = aiosqlite_module.Cursor

__all__ = (
    "AiosqliteConnection",
    "AiosqliteConnectionFactory",
    "AiosqliteCursor",
    "AiosqliteRawCursor",
    "AiosqliteSessionContext",
    "aiosqlite_module",
    "aiosqlite_sqlite_module",
)


class AiosqliteCursor:
    """Async context manager for AIOSQLite cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection
        self.cursor: AiosqliteRawCursor | None = None

    async def __aenter__(self) -> "AiosqliteRawCursor":
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class AiosqliteSessionContext(AsyncDriverSessionContext):
    """Async context manager for AIOSQLite sessions."""

    __slots__ = ()

    if TYPE_CHECKING:

        async def __aenter__(self) -> "AiosqliteDriver": ...

    def _resolve_driver_class(self) -> "type[AiosqliteDriver]":
        from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

        return AiosqliteDriver
