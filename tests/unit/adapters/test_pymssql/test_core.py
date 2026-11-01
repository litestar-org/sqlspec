"""pymssql core helper tests."""

from typing import Any

import pytest

from sqlspec.adapters.pymssql.core import (
    build_connection_config,
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    escape_literal_percent,
    extract_error_number,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
)
from sqlspec.core import SQL, ParameterStyle
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    NotNullViolationError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)


def test_profile_uses_tsql_and_pyformat_execution() -> None:
    """The pymssql profile should compile T-SQL to pyformat placeholders."""
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
    statement = SQL("SELECT * FROM dbo.users WHERE id = ?", 3, statement_config=default_statement_config)

    compiled_sql, parameters = statement.compile()

    assert "WHERE id = %s" in compiled_sql
    assert parameters in ([3], (3,))


def test_statement_config_compiles_named_pyformat_input_to_positional() -> None:
    """Named pyformat input should compile to pymssql's supported positional style."""
    statement = SQL(
        "SELECT * FROM dbo.users WHERE id = %(user_id)s", {"user_id": 3}, statement_config=default_statement_config
    )

    compiled_sql, parameters = statement.compile()

    assert "WHERE id = %s" in compiled_sql
    assert parameters in ([3], (3,))


def test_format_identifier_and_insert_statement_use_tsql_identifiers() -> None:
    """Generated DML helpers should quote T-SQL identifiers and use %s placeholders."""
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
    assert isinstance(create_mapped_exception(error), expected_type)


def test_normalize_execute_many_parameters_passes_through() -> None:
    """normalize_execute_many_parameters returns the batch payload unchanged."""
    assert normalize_execute_many_parameters([]) == []

    rows: list[tuple[Any, ...]] = [(1,), (2,)]
    assert normalize_execute_many_parameters(rows) is rows


def test_extract_error_number() -> None:
    """extract_error_number detects error number from attribute, tuple, or regex."""

    class AttributeException(Exception):
        number = 2627

    class BoolAttributeException(Exception):
        number = True

    class ZeroAttributeException(Exception):
        number = 0

    assert extract_error_number(AttributeException("duplicate key")) == 2627
    assert extract_error_number(BoolAttributeException("Msg 2627, Level 14")) == 2627
    assert extract_error_number(ZeroAttributeException("Msg 2627, Level 14")) == 2627
    assert extract_error_number(ZeroAttributeException("Plain error")) is None
    assert extract_error_number(Exception(True, "Msg 1205, Level 13")) == 1205
    assert extract_error_number(Exception(0, "Msg 1205, Level 13")) == 1205
    assert extract_error_number(Exception(1205, "Deadlock found")) == 1205
    assert extract_error_number(Exception("Violation of UNIQUE KEY constraint (2627)")) == 2627
    assert extract_error_number(Exception("Plain error")) is None


def test_collect_rows_preserves_list_identity() -> None:
    """collect_rows avoids copying when the input rows are already a list."""
    input_rows = [(1, "Alice"), (2, "Bob")]
    description = [("id",), ("name",)]
    rows, column_names, row_format = collect_rows(input_rows, description)

    assert rows is input_rows
    assert column_names == ["id", "name"]
    assert row_format == "tuple"


def test_normalize_execute_parameters_preserves_tuples() -> None:
    """normalize_execute_parameters passes tuples through directly."""
    params = (1, "Alice")
    assert normalize_execute_parameters(params) is params


def test_extract_error_number_from_nested_tuple() -> None:
    """extract_error_number extracts integer from nested tuple args."""
    assert extract_error_number(Exception((2627, b"Violation of UNIQUE KEY constraint"))) == 2627


@pytest.mark.parametrize(
    ("code", "expected_type"),
    [
        (102, SQLParsingError),
        (105, SQLParsingError),
        (156, SQLParsingError),
        (207, SQLParsingError),
        (208, SQLParsingError),
        (241, DataError),
        (242, DataError),
        (245, DataError),
        (2628, DataError),
        (8115, DataError),
        (8152, DataError),
        (1222, QueryTimeoutError),
        (3960, SerializationConflictError),
        (229, PermissionDeniedError),
        (230, PermissionDeniedError),
        (262, PermissionDeniedError),
    ],
)
def test_create_mapped_exception_maps_expanded_sql_server_error_codes(
    code: int, expected_type: type[Exception]
) -> None:
    """Expanded SQL Server error codes map to their specific SQLSpec exception types."""
    mapped = create_mapped_exception(Exception(code, f"SQL Server failure ({code})"))
    assert isinstance(mapped, expected_type)


def test_escape_literal_percent_preserves_placeholders_and_escapes_literals() -> None:
    """escape_literal_percent doubles literal % while keeping %s placeholders intact when parameters exist."""
    validator = default_statement_config.parameter_validator
    sql_with_params = "SELECT * FROM dbo.items WHERE discount LIKE '50%' AND category = %s"
    assert (
        escape_literal_percent(sql_with_params, ("books",), validator)
        == "SELECT * FROM dbo.items WHERE discount LIKE '50%%' AND category = %s"
    )
    sql_without_params = "SELECT * FROM dbo.items WHERE discount LIKE '50%'"
    assert escape_literal_percent(sql_without_params, (), validator) == sql_without_params


def test_build_connection_config_merges_extra_mapping() -> None:
    """build_connection_config pops and merges the extra mapping without overriding explicit keys."""
    cfg = build_connection_config({
        "server": "sql.local",
        "timeout": 10,
        "extra": {"timeout": 99, "appname": "analytics", "ignored": None},
    })
    assert cfg["server"] == "sql.local"
    assert cfg["timeout"] == 10
    assert cfg["appname"] == "analytics"
    assert "extra" not in cfg
    assert "ignored" not in cfg


def test_freetds_error_code_mapping() -> None:
    """FreeTDS 20000-series codes and InterfaceError map to appropriate SQLSpec errors."""

    class InterfaceError(Exception):
        pass

    assert isinstance(create_mapped_exception(Exception(20003, "DBPROCESS is dead")), QueryTimeoutError)
    assert isinstance(
        create_mapped_exception(Exception(20002, "Adaptive Server connection failed")), DatabaseConnectionError
    )
    assert isinstance(create_mapped_exception(Exception(20009, "Server is unavailable")), DatabaseConnectionError)
    assert isinstance(create_mapped_exception(Exception(20047, "Connection closed")), DatabaseConnectionError)
    assert isinstance(create_mapped_exception(InterfaceError("Connection broken")), DatabaseConnectionError)


def test_percent_escaping_performance() -> None:
    """escape_literal_percent correctly handles string literals and placeholders."""
    validator = default_statement_config.parameter_validator
    sql = "SELECT [id] FROM [t] WHERE [name] LIKE '50%' AND [cat] = %s"
    escaped = escape_literal_percent(sql, ("books",), validator)
    assert escaped == "SELECT [id] FROM [t] WHERE [name] LIKE '50%%' AND [cat] = %s"
