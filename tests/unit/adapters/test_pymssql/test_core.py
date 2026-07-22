"""pymssql core helper tests."""

from typing import Any

import pytest

from sqlspec.core import SQL, ParameterStyle
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    ForeignKeyViolationError,
    NotNullViolationError,
    UniqueViolationError,
)


def test_profile_uses_tsql_and_pyformat_execution() -> None:
    """The pymssql profile should compile T-SQL to pyformat placeholders."""
    from sqlspec.adapters.pymssql.core import default_statement_config, driver_profile

    parameter_config = default_statement_config.parameter_config

    assert default_statement_config.dialect == "tsql"
    assert driver_profile.default_execution_style is ParameterStyle.POSITIONAL_PYFORMAT
    supported_execution_styles = driver_profile.supported_execution_styles
    assert supported_execution_styles is not None
    assert ParameterStyle.POSITIONAL_PYFORMAT in supported_execution_styles
    assert ParameterStyle.NAMED_PYFORMAT not in supported_execution_styles
    assert parameter_config.default_execution_parameter_style is ParameterStyle.POSITIONAL_PYFORMAT
    supported_execution_parameter_styles = parameter_config.supported_execution_parameter_styles
    assert supported_execution_parameter_styles is not None
    assert ParameterStyle.POSITIONAL_PYFORMAT in supported_execution_parameter_styles
    assert ParameterStyle.NAMED_PYFORMAT not in supported_execution_parameter_styles


def test_statement_config_compiles_qmark_input_to_percent_s() -> None:
    """Qmark input should execute as positional pyformat for pymssql."""
    from sqlspec.adapters.pymssql.core import default_statement_config

    statement = SQL("SELECT * FROM dbo.users WHERE id = ?", 3, statement_config=default_statement_config)

    compiled_sql, parameters = statement.compile()

    assert "WHERE id = %s" in compiled_sql
    assert parameters in ([3], (3,))


def test_statement_config_compiles_named_pyformat_input_to_positional() -> None:
    """Named pyformat input should compile to pymssql's supported positional style."""
    from sqlspec.adapters.pymssql.core import default_statement_config

    statement = SQL(
        "SELECT * FROM dbo.users WHERE id = %(user_id)s", {"user_id": 3}, statement_config=default_statement_config
    )

    compiled_sql, parameters = statement.compile()

    assert "WHERE id = %s" in compiled_sql
    assert parameters in ([3], (3,))


def test_format_identifier_and_insert_statement_use_tsql_identifiers() -> None:
    """Generated DML helpers should quote T-SQL identifiers and use %s placeholders."""
    from sqlspec.adapters.pymssql.core import build_insert_statement, format_identifier

    assert format_identifier("dbo.users") == "[dbo].[users]"
    assert format_identifier("[sales].[order]]items]") == "[sales].[order]]items]"
    assert build_insert_statement("dbo.users", ["id", "display_name"]) == (
        "INSERT INTO [dbo].[users] ([id], [display_name]) VALUES (%s, %s)"
    )


@pytest.mark.parametrize(
    ("message", "expected_type"),
    [
        ("Violation of UNIQUE KEY constraint (2627)", UniqueViolationError),
        ("Cannot open database requested by the login (4060)", DatabaseConnectionError),
    ],
)
def test_create_mapped_exception_maps_tsql_error_numbers(message: str, expected_type: type[Exception]) -> None:
    """SQL Server error numbers should map to SQLSpec exceptions."""
    from sqlspec.adapters.pymssql.core import create_mapped_exception

    exc = create_mapped_exception(Exception(message))

    assert isinstance(exc, expected_type)
    assert "SQL Server error" in str(exc)


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
    from sqlspec.adapters.pymssql.core import create_mapped_exception

    mapped = create_mapped_exception(Exception(message))

    assert isinstance(mapped, expected_type)
    assert expected_detail in str(mapped)


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (Exception(2627, b"Violation of UNIQUE KEY constraint"), UniqueViolationError),
        (Exception("Cannot insert the value NULL; column does not allow nulls"), NotNullViolationError),
        (Exception("The INSERT statement conflicted with the CHECK constraint"), CheckViolationError),
        (Exception("The INSERT statement conflicted with the FOREIGN KEY constraint"), ForeignKeyViolationError),
    ],
)
def test_create_mapped_exception_classifies_native_constraint_shapes(
    error: Exception, expected_type: type[Exception]
) -> None:
    """Native pymssql argument and message shapes map to specific constraint exceptions."""
    from sqlspec.adapters.pymssql.core import create_mapped_exception

    assert isinstance(create_mapped_exception(error), expected_type)


def test_normalize_execute_many_parameters_passes_through() -> None:
    """normalize_execute_many_parameters returns the batch payload unchanged."""
    from sqlspec.adapters.pymssql.core import normalize_execute_many_parameters

    assert normalize_execute_many_parameters([]) == []

    rows: list[tuple[Any, ...]] = [(1,), (2,)]
    assert normalize_execute_many_parameters(rows) is rows
