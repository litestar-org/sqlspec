"""Unit tests for mssql_python adapter core helpers."""

import pytest

from sqlspec.adapters.mssql_python._typing import MSSQL_PYTHON_MODULE
from sqlspec.adapters.mssql_python.core import build_connection_config, create_mapped_exception
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    ForeignKeyViolationError,
    NotNullViolationError,
    UniqueViolationError,
)


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


def test_build_connection_config_no_duplicate_uid() -> None:
    """Passing both 'uid' and 'user' produces exactly one UID= option."""
    connection_string, _ = build_connection_config({"server": "srv", "uid": "user1", "user": "user2"})

    assert connection_string.count("UID=") == 1
    assert "UID=user1" in connection_string
    assert "user=user2" not in connection_string


def test_build_connection_config_no_duplicate_pwd() -> None:
    """Passing both 'pwd' and 'password' produces exactly one PWD= option."""
    connection_string, _ = build_connection_config({"server": "srv", "pwd": "secret1", "password": "secret2"})

    assert connection_string.count("PWD=") == 1
    assert "PWD=secret1" in connection_string
    assert "password=secret2" not in connection_string


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


@pytest.mark.parametrize(
    ("message", "expected_type", "expected_detail"),
    [
        (
            'The INSERT statement conflicted with the FOREIGN KEY constraint "FK_Orders_Customers". (547)',
            ForeignKeyViolationError,
            "foreign key constraint violation",
        ),
        (
            'The UPDATE statement conflicted with the CHECK constraint "CK_Employee_Salary". (547)',
            CheckViolationError,
            "check constraint violation",
        ),
        (
            'The UPDATE statement conflicted with the check constraint "CK_Employee_Salary". (547)',
            CheckViolationError,
            "check constraint violation",
        ),
        (
            'The DELETE statement conflicted with constraint "Unknown_Constraint". (547)',
            ForeignKeyViolationError,
            "foreign key constraint violation",
        ),
    ],
)
def test_create_mapped_exception_disambiguates_547_check_vs_foreign_key(
    message: str, expected_type: type[Exception], expected_detail: str
) -> None:
    """SQL Server 547 distinguishes CHECK from foreign-key constraint violations."""
    mapped = create_mapped_exception(Exception(message))

    assert isinstance(mapped, expected_type)
    assert expected_detail in str(mapped)


@pytest.mark.parametrize(
    ("message", "expected_type"),
    [
        ("Violation of UNIQUE KEY constraint. Cannot insert duplicate key", UniqueViolationError),
        (
            "Cannot insert the value NULL into column 'required_field'; column does not allow nulls",
            NotNullViolationError,
        ),
        ("The INSERT statement conflicted with the CHECK constraint", CheckViolationError),
        ("The INSERT statement conflicted with the FOREIGN KEY constraint", ForeignKeyViolationError),
    ],
)
def test_create_mapped_exception_classifies_constraint_messages_without_error_numbers(
    message: str, expected_type: type[Exception]
) -> None:
    """Constraint messages remain classifiable when the driver omits SQL Server error numbers."""
    mapped = create_mapped_exception(MSSQL_PYTHON_MODULE.IntegrityError("23000", message))

    assert isinstance(mapped, expected_type)


def test_build_connection_config_requires_server_when_missing_connection_string() -> None:
    """Part-based configuration should fail fast without a server."""
    with pytest.raises(ValueError, match="server"):
        build_connection_config({"database": "app"})
