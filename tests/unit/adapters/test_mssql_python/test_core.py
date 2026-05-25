"""Unit tests for mssql_python adapter core helpers."""

import pytest

from sqlspec.adapters.mssql_python._typing import MSSQL_PYTHON_MODULE
from sqlspec.adapters.mssql_python.core import build_connection_config, create_mapped_exception
from sqlspec.exceptions import DatabaseConnectionError, UniqueViolationError


def test_build_connection_config_uses_supplied_connection_string() -> None:
    """A pre-built ODBC connection string should pass through unchanged."""
    connection_string, kwargs = build_connection_config({
        "connection_string": "Server=localhost;Database=tempdb;",
        "timeout": 15,
        "native_uuid": True,
    })

    assert connection_string == "Server=localhost;Database=tempdb;"
    assert kwargs == {"timeout": 15, "native_uuid": True}


def test_build_connection_config_from_parts_formats_odbc_options() -> None:
    """Connection parts should be formatted into a semicolon-delimited ODBC string."""
    connection_string, kwargs = build_connection_config({
        "server": "localhost",
        "port": 1433,
        "database": "app",
        "uid": "sa",
        "pwd": "secret",
        "encrypt": False,
        "trust_server_certificate": True,
        "autocommit": True,
    })

    assert connection_string == (
        "Server=localhost,1433;Database=app;UID=sa;PWD=secret;Encrypt=no;TrustServerCertificate=yes;"
    )
    assert kwargs == {"autocommit": True}


def test_create_mapped_exception_extracts_sql_server_error_number() -> None:
    """SQL Server native error numbers should map to specific SQLSpec exceptions."""
    exc = MSSQL_PYTHON_MODULE.IntegrityError(
        "23000",
        "[23000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Violation of UNIQUE KEY constraint. (2627)",
    )

    mapped = create_mapped_exception(exc)

    assert isinstance(mapped, UniqueViolationError)
    assert "2627" in str(mapped)


def test_create_mapped_exception_falls_back_for_connection_errors() -> None:
    """Known connection error numbers should map to DatabaseConnectionError."""
    exc = MSSQL_PYTHON_MODULE.OperationalError(
        "08001",
        "[08001] [Microsoft][ODBC Driver 18 for SQL Server]Named Pipes Provider: "
        "Could not open a connection to SQL Server (53)",
    )

    mapped = create_mapped_exception(exc)

    assert isinstance(mapped, DatabaseConnectionError)


def test_build_connection_config_requires_server_when_missing_connection_string() -> None:
    """Part-based configuration should fail fast without a server."""
    with pytest.raises(ValueError, match="server"):
        build_connection_config({"database": "app"})
