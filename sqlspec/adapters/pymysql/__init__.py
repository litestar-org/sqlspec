"""PyMySQL adapter for SQLSpec."""

from sqlspec.adapters.pymysql._typing import PyMysqlConnection, PyMysqlCursor
from sqlspec.adapters.pymysql.config import (
    PyMysqlConfig,
    PyMysqlConnectionParams,
    PyMysqlDriverFeatures,
    PyMysqlPoolParams,
    PyMysqlSslParams,
    build_connection_config,
)
from sqlspec.adapters.pymysql.core import default_statement_config
from sqlspec.adapters.pymysql.driver import PyMysqlDriver, PyMysqlExceptionHandler

__all__ = (
    "PyMysqlConfig",
    "PyMysqlConnection",
    "PyMysqlConnectionParams",
    "PyMysqlCursor",
    "PyMysqlDriver",
    "PyMysqlDriverFeatures",
    "PyMysqlExceptionHandler",
    "PyMysqlPoolParams",
    "PyMysqlSslParams",
    "build_connection_config",
    "default_statement_config",
)
