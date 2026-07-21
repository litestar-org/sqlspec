"""Shared SQL Server integration fixtures."""

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.mssql import MSSQLService

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.pymssql import PymssqlConfig

__all__ = (
    "arrow_odbc_mssql_config",
    "mssql_python_config",
    "mssql_python_connection_config",
    "pymssql_config",
    "pymssql_connection_config",
)


def _mssql_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    return {
        "server": mssql_service.host,
        "port": mssql_service.port,
        "database": mssql_service.database,
        "user": mssql_service.user,
        "password": mssql_service.password,
    }


def _mssql_python_connection_config(mssql_service: "MSSQLService", *, autocommit: bool = True) -> "dict[str, Any]":
    connection_config = _mssql_connection_config(mssql_service)
    connection_config.update({
        "encrypt": False,
        "trust_server_certificate": True,
        "autocommit": autocommit,
        "pool_enabled": False,
    })
    return connection_config


def _arrow_odbc_connection_config(mssql_service: "MSSQLService") -> "dict[str, str]":
    return {"connection_string": mssql_service.connection_string}


@pytest.fixture(scope="session")
def mssql_python_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    """Provide shared mssql-python connection parameters."""
    return _mssql_python_connection_config(mssql_service)


@pytest.fixture(scope="session")
def mssql_python_config(mssql_python_connection_config: "dict[str, Any]") -> "Generator[MssqlPythonConfig, None, None]":
    """Provide a session-scoped mssql-python configuration."""
    config = MssqlPythonConfig(connection_config=dict(mssql_python_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
def pymssql_connection_config(mssql_service: "MSSQLService") -> "dict[str, Any]":
    """Provide shared pymssql connection parameters."""
    return _mssql_connection_config(mssql_service)


@pytest.fixture(scope="session")
def pymssql_config(pymssql_connection_config: "dict[str, Any]") -> "Generator[PymssqlConfig, None, None]":
    """Provide a session-scoped pymssql configuration."""
    config = PymssqlConfig(connection_config=dict(pymssql_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
def arrow_odbc_mssql_config(mssql_service: "MSSQLService") -> "Generator[ArrowOdbcConfig, None, None]":
    """Provide a session-scoped arrow-odbc SQL Server configuration."""
    config = ArrowOdbcConfig(
        connection_config=_arrow_odbc_connection_config(mssql_service),
        driver_features={"dbms_name": "Microsoft SQL Server"},
    )
    try:
        yield config
    finally:
        config.close_pool()
