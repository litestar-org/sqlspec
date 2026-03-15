# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""AIOSQLite adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

import aiosqlite

_AiosqliteConnection = aiosqlite.Connection

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
    from sqlspec.core import StatementConfig

    AiosqliteConnection: TypeAlias = _AiosqliteConnection
    AiosqliteCursorType: TypeAlias = aiosqlite.Cursor

if not TYPE_CHECKING:
    AiosqliteConnection = _AiosqliteConnection
    AiosqliteCursorType = aiosqlite.Cursor


class AiosqliteCursor:
    """Async context manager for AIOSQLite cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection
        self.cursor: Any = None

    async def __aenter__(self) -> Any:
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> None:
        if exc_type is not None:
            return
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class AiosqliteSessionContext:
    """Async context manager for AIOSQLite sessions.

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
        prepare_driver: "Callable[[AiosqliteDriver], AiosqliteDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: AiosqliteDriver | None = None

    async def __aenter__(self) -> "AiosqliteDriver":
        from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

        self._connection = await self._acquire_connection()
        self._driver = AiosqliteDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("AiosqliteConnection", "AiosqliteCursor", "AiosqliteCursorType", "AiosqliteSessionContext")
