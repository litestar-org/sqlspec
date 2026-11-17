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

from sqlspec.driver._session import SyncDriverSessionContext
from sqlspec.typing import import_optional_attr

if TYPE_CHECKING:
    from typing import Protocol, TypeAlias

    from google.cloud.sql.connector import Connector as PyMysqlCloudSqlConnector

    from sqlspec.adapters.pymysql.driver import PyMysqlDriver

    class PyMysqlFieldTypeProtocol(Protocol):
        JSON: int

    class PyMysqlServerStatusProtocol(Protocol):
        SERVER_STATUS_IN_TRANS: int

    PyMysqlConnect: TypeAlias = type["PyMysqlConnection"]
    PyMysqlFieldType: TypeAlias = PyMysqlFieldTypeProtocol
    PyMysqlServerStatus: TypeAlias = PyMysqlServerStatusProtocol
else:
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


class PyMysqlSessionContext(SyncDriverSessionContext):
    """Sync context manager for PyMySQL sessions."""

    __slots__ = ()

    def _resolve_driver_class(self) -> "type[PyMysqlDriver]":
        from sqlspec.adapters.pymysql.driver import PyMysqlDriver

        return PyMysqlDriver
