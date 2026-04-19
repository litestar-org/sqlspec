"""PyMySQL adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

import pymysql
from pymysql import MySQLError as PymysqlMySQLError
from pymysql import connect as pymysql_connect
from pymysql import cursors as pymysql_cursors
from pymysql.constants import FIELD_TYPE as PymysqlFieldType  # noqa: N811

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.pymysql.driver import PyMysqlDriver
    from sqlspec.core import StatementConfig

    PyMysqlConnection: TypeAlias = pymysql.connections.Connection
    PyMysqlRawCursor: TypeAlias = pymysql.cursors.Cursor

if not TYPE_CHECKING:
    PyMysqlConnection = pymysql.connections.Connection
    PyMysqlRawCursor = pymysql.cursors.Cursor

PymysqlDictCursor = pymysql_cursors.DictCursor


class PyMysqlCursor:
    """Context manager for PyMySQL cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PyMysqlConnection") -> None:
        self.connection = connection
        self.cursor: PyMysqlRawCursor | None = None

    def __enter__(self) -> "PyMysqlRawCursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class PyMysqlSessionContext:
    """Sync context manager for PyMySQL sessions."""

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
        prepare_driver: "Callable[[PyMysqlDriver], PyMysqlDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: PyMysqlDriver | None = None

    def __enter__(self) -> "PyMysqlDriver":
        from sqlspec.adapters.pymysql.driver import PyMysqlDriver

        self._connection = self._acquire_connection()
        self._driver = PyMysqlDriver(
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
    "PyMysqlConnection",
    "PyMysqlCursor",
    "PyMysqlRawCursor",
    "PyMysqlSessionContext",
    "PymysqlDictCursor",
    "PymysqlFieldType",
    "PymysqlMySQLError",
    "pymysql_connect",
    "pymysql_cursors",
)
