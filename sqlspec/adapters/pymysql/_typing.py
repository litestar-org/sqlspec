"""PyMySQL adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from typing import TYPE_CHECKING, Any

import pymysql
from pymysql import MySQLError as PyMysqlMySQLError
from pymysql.connections import Connection as PyMysqlConnection
from pymysql.constants import FIELD_TYPE as _PYMYSQL_FIELD_TYPE
from pymysql.constants import SERVER_STATUS as _PYMYSQL_SERVER_STATUS
from pymysql.cursors import RE_INSERT_VALUES as PYMYSQL_INSERT_VALUES_PATTERN
from pymysql.cursors import Cursor as PyMysqlRawCursor
from pymysql.cursors import DictCursor as PyMysqlDictCursor
from pymysql.cursors import SSCursor as PyMysqlSSCursor

from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import Protocol, TypeAlias

    from google.cloud.sql.connector import Connector as PyMysqlCloudSqlConnector

    from sqlspec.adapters.pymysql.driver import PyMysqlDriver
    from sqlspec.core import StatementConfig

    class PyMysqlFieldTypeProtocol(Protocol):
        JSON: int

    class PyMysqlServerStatusProtocol(Protocol):
        SERVER_STATUS_IN_TRANS: int

    PyMysqlConnect: TypeAlias = type["PyMysqlConnection"]
    PyMysqlFieldType: TypeAlias = PyMysqlFieldTypeProtocol
    PyMysqlServerStatus: TypeAlias = PyMysqlServerStatusProtocol

if not TYPE_CHECKING:
    PyMysqlConnect = pymysql.connect
    PyMysqlFieldType = _PYMYSQL_FIELD_TYPE
    PyMysqlServerStatus = _PYMYSQL_SERVER_STATUS

    def _pymysql_cloud_sql_connector(*args: Any, **kwargs: Any) -> Any:
        connector_cls = import_optional_attr("google.cloud.sql.connector", "Connector")
        if connector_cls is None:
            msg = "Cannot import 'Connector' from 'google.cloud.sql.connector'"
            raise ImportError(msg)
        return connector_cls(*args, **kwargs)

    PyMysqlCloudSqlConnector = _pymysql_cloud_sql_connector


__all__ = (
    "PYMYSQL_INSERT_VALUES_PATTERN",
    "PyMysqlCloudSqlConnector",
    "PyMysqlConnect",
    "PyMysqlConnection",
    "PyMysqlCursor",
    "PyMysqlDictCursor",
    "PyMysqlFieldType",
    "PyMysqlMySQLError",
    "PyMysqlRawCursor",
    "PyMysqlSSCursor",
    "PyMysqlServerStatus",
    "PyMysqlSessionContext",
)


_resolved_driver_type: "type[PyMysqlDriver] | None" = None


class PyMysqlCursor:
    """Context manager for PyMySQL cursor operations."""

    __slots__ = ("connection", "cursor", "cursor_class")

    def __init__(self, connection: "PyMysqlConnection", cursor_class: "type[PyMysqlRawCursor] | None" = None) -> None:
        self.connection = connection
        self.cursor_class = cursor_class
        self.cursor: PyMysqlRawCursor | None = None

    def __enter__(self) -> "PyMysqlRawCursor":
        if self.cursor_class is not None:
            self.cursor = self.connection.cursor(self.cursor_class)
        else:
            self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
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
        acquire_connection: "Callable[[], PyMysqlConnection]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[PyMysqlDriver], PyMysqlDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: PyMysqlConnection | None = None
        self._driver: PyMysqlDriver | None = None

    def __enter__(self) -> "PyMysqlDriver":
        driver_cls = _get_pymysql_driver_type()
        self._connection = self._acquire_connection()
        driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        self._driver = driver
        return self._prepare_driver(driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        return None


def _get_pymysql_driver_type() -> "type[PyMysqlDriver]":
    global _resolved_driver_type
    driver_cls = _resolved_driver_type
    if driver_cls is None:
        driver_cls = import_optional_attr("sqlspec.adapters.pymysql.driver", "PyMysqlDriver")
        _resolved_driver_type = driver_cls
    return driver_cls
