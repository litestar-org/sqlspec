"""pymssql adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
import importlib
from typing import TYPE_CHECKING, Any

import pymssql as PYMSSQL_MODULE  # noqa: N812
from pymssql import Connection, Cursor
from pymssql import Error as PymssqlError

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from pymssql._pymssql import QueryParams as PymssqlQueryParams

    from sqlspec.adapters.pymssql.driver import PymssqlDriver
    from sqlspec.core import StatementConfig

    PymssqlConnection: TypeAlias = Connection
    PymssqlRawCursor: TypeAlias = Cursor
else:
    PymssqlQueryParams = Any
    PymssqlConnection = Connection
    PymssqlRawCursor = Cursor

__all__ = (
    "PYMSSQL_MODULE",
    "PymssqlConnection",
    "PymssqlCursor",
    "PymssqlError",
    "PymssqlQueryParams",
    "PymssqlRawCursor",
    "PymssqlSessionContext",
)


class PymssqlCursor:
    """Context manager for pymssql cursor operations."""

    __slots__ = ("_as_dict", "connection", "cursor")

    def __init__(self, connection: "PymssqlConnection", *, as_dict: bool | None = None) -> None:
        self.connection = connection
        self.cursor: PymssqlRawCursor | None = None
        self._as_dict = as_dict

    def __enter__(self) -> "PymssqlRawCursor":
        if self._as_dict is None:
            self.cursor = self.connection.cursor()
        else:
            try:
                raw_cursor: Any = (
                    self.connection.cursor(as_dict=True) if self._as_dict else self.connection.cursor(as_dict=False)
                )
                self.cursor = raw_cursor
            except TypeError:
                self.cursor = self.connection.cursor()
        assert self.cursor is not None
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


_DRIVER_CLS: "type[PymssqlDriver] | None" = None


class PymssqlSessionContext:
    """Sync context manager for pymssql sessions."""

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_cls",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[PymssqlDriver], PymssqlDriver]",
        driver_cls: "type[PymssqlDriver] | None" = None,
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_cls = driver_cls
        self._connection: Any = None
        self._driver: PymssqlDriver | None = None

    def __enter__(self) -> "PymssqlDriver":
        global _DRIVER_CLS
        driver_cls = self._driver_cls or _DRIVER_CLS
        if driver_cls is None:
            module = importlib.import_module("sqlspec.adapters.pymssql.driver")
            _DRIVER_CLS = module.PymssqlDriver
            driver_cls = _DRIVER_CLS
        assert driver_cls is not None
        self._connection = self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        assert self._driver is not None
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._driver is not None and (exc_type is not None or self._driver._connection_in_transaction()):
            with contextlib.suppress(Exception):
                self._driver.rollback()
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
            self._driver = None
        return None
