"""Unit tests for mssql_python adapter core helpers."""

import pytest

from sqlspec.adapters.mssql_python._typing import mssql_python_module
from sqlspec.adapters.mssql_python.core import build_connection_config, create_mapped_exception, extract_error_number
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    ForeignKeyViolationError,
    NotNullViolationError,
    UniqueViolationError,
)
from sqlspec.utils.config_tools import parse_odbc_connection_string


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
    exc = mssql_python_module.IntegrityError(
        "23000",
        "[23000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Violation of UNIQUE KEY constraint. (2627)",
    )

    mapped = create_mapped_exception(exc)

    assert isinstance(mapped, UniqueViolationError)
    assert "2627" in str(mapped)


def test_create_mapped_exception_falls_back_for_connection_errors() -> None:
    """Known connection error numbers should map to DatabaseConnectionError."""
    exc = mssql_python_module.OperationalError(
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
    mapped = create_mapped_exception(mssql_python_module.IntegrityError("23000", message))

    assert isinstance(mapped, expected_type)


def test_build_connection_config_requires_server_when_missing_connection_string() -> None:
    """Part-based configuration should fail fast without a server."""
    with pytest.raises(ValueError, match="server"):
        build_connection_config({"database": "app"})


def test_build_connection_config_merges_discrete_database_into_connection_string() -> None:
    """Discrete database field must be appended when not present in connection_string."""
    connection_string, _ = build_connection_config({
        "connection_string": "Server=host;UID=u;PWD=p;",
        "database": "sales",
    })

    assert "Database=sales" in connection_string
    assert "Server=host" in connection_string
    assert "UID=u" in connection_string
    assert "PWD=p" in connection_string


def test_build_connection_config_discrete_field_overrides_existing_in_connection_string() -> None:
    """Discrete fields must override existing keys in connection_string without duplicates."""
    connection_string, _ = build_connection_config({
        "connection_string": "Server=host;Database=master;UID=u;PWD=p;",
        "database": "sales",
    })

    assert "Database=sales" in connection_string
    assert "Database=master" not in connection_string
    assert connection_string.lower().count("database=") == 1


def test_build_connection_config_merges_port_with_connection_string_server() -> None:
    """Discrete port must be attached to the server defined in connection_string."""
    connection_string, _ = build_connection_config({"connection_string": "Server=host;UID=u;PWD=p;", "port": 1433})

    assert "Server=host,1433" in connection_string


def test_build_connection_config_merges_extra_options_into_connection_string() -> None:
    """Discrete extra dictionary options must be merged into connection_string."""
    connection_string, _ = build_connection_config({
        "connection_string": "Server=host;UID=u;PWD=p;",
        "extra": {"ApplicationIntent": "ReadOnly"},
    })

    assert "ApplicationIntent=ReadOnly" in connection_string


def test_build_connection_config_merges_boolean_options() -> None:
    """Discrete boolean flags must override connection_string options with ODBC yes/no."""
    connection_string, _ = build_connection_config({"connection_string": "Server=host;Encrypt=yes;", "encrypt": False})

    assert "Encrypt=no" in connection_string
    assert "Encrypt=yes" not in connection_string
    assert connection_string.lower().count("encrypt=") == 1


def test_build_connection_config_discrete_server_overrides_connection_string() -> None:
    """Discrete server must override server from connection_string."""
    connection_string, _ = build_connection_config({
        "connection_string": "Server=oldhost;UID=u;PWD=p;",
        "server": "newhost",
        "port": 14333,
    })

    assert "Server=newhost,14333" in connection_string
    assert "oldhost" not in connection_string


def test_build_connection_config_braced_values_and_trailing_options() -> None:
    """Braced values with escaped closing braces and unquoted trailing options must be preserved."""
    connection_string, _ = build_connection_config({
        "connection_string": "Driver={ODBC Driver 18 for SQL Server};PWD={p}}wd};Server=myhost",
        "database": "sales",
    })

    assert "Driver={ODBC Driver 18 for SQL Server}" in connection_string
    assert "PWD={p}}wd}" in connection_string
    assert "Server=myhost" in connection_string
    assert "Database=sales" in connection_string


def test_build_connection_config_extra_dict_and_arbitrary_options() -> None:
    """Extra dict None values must be ignored, and arbitrary unmapped config keys must be merged."""
    connection_string, _ = build_connection_config({
        "connection_string": "Server=myhost;",
        "extra": {"ApplicationIntent": "ReadOnly", "IgnoredOption": None},
        "CustomParam": "custom_val",
        "IgnoredParam": None,
    })

    assert "ApplicationIntent=ReadOnly" in connection_string
    assert "CustomParam=custom_val" in connection_string
    assert "IgnoredOption" not in connection_string
    assert "IgnoredParam" not in connection_string


def test_parse_odbc_connection_string_edge_cases() -> None:
    """Parser handles leading/duplicate semicolons, empty values, trailing tokens, and unclosed braces."""
    parsed = parse_odbc_connection_string("; ;Server=host; ;Key= ;Driver={ODBC Driver} ;EmptyKey=  ")
    parsed_dict = dict(parsed)

    assert parsed_dict["Server"] == "host"
    assert parsed_dict["Key"] == ""
    assert parsed_dict["Driver"] == "{ODBC Driver}"
    assert parsed_dict["EmptyKey"] == ""

    assert parse_odbc_connection_string("Incomplete={no_close") == [("Incomplete", "{no_close")]
    assert parse_odbc_connection_string("Server=host;  ") == [("Server", "host")]
    assert parse_odbc_connection_string("DanglingToken") == []


def test_extract_error_number_from_attribute() -> None:
    """extract_error_number retrieves native integer attribute 'number'."""

    class CustomError(Exception):
        number = 2627

    assert extract_error_number(CustomError("duplicate key")) == 2627


def test_extract_error_number_from_args_tuple() -> None:
    """extract_error_number extracts integer from exception args."""
    assert extract_error_number(Exception(1205, "Deadlock found")) == 1205


def test_extract_error_number_from_string_regex() -> None:
    """extract_error_number parses error numbers formatted as (1205) or error 1205."""
    assert extract_error_number(Exception("Transaction was deadlocked on lock resources (1205)")) == 1205
    assert extract_error_number(Exception("Msg 4712, Level 16, State 1")) == 4712
    assert extract_error_number(Exception("No numbers here")) is None
