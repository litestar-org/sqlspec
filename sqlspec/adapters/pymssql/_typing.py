"""pymssql adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

import pymssql as _pymssql  # pyright: ignore[reportMissingTypeStubs]
from pymssql import Connection as _PymssqlConnection  # pyright: ignore[reportMissingTypeStubs]
from pymssql import Cursor as _PymssqlRawCursor  # pyright: ignore[reportMissingTypeStubs]

PYMSSQL_MODULE = _pymssql

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.pymssql.driver import PymssqlDriver
    from sqlspec.core import StatementConfig

    PymssqlConnection: TypeAlias = _PymssqlConnection
    PymssqlRawCursor: TypeAlias = _PymssqlRawCursor

if not TYPE_CHECKING:
    PymssqlConnection = _PymssqlConnection
    PymssqlRawCursor = _PymssqlRawCursor

__all__ = ("PYMSSQL_MODULE", "PymssqlConnection", "PymssqlCursor", "PymssqlRawCursor", "PymssqlSessionContext")


class PymssqlCursor:
    """Context manager for pymssql cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PymssqlConnection") -> None:
        self.connection = connection
        self.cursor: PymssqlRawCursor | None = None

    def __enter__(self) -> "PymssqlRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class PymssqlSessionContext:
    """Sync context manager for pymssql sessions."""

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
        prepare_driver: "Callable[[PymssqlDriver], PymssqlDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PymssqlDriver | None = None

    def __enter__(self) -> "PymssqlDriver":
        from sqlspec.adapters.pymssql.driver import PymssqlDriver

        self._connection = self._acquire_connection()
        self._driver = PymssqlDriver(
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
