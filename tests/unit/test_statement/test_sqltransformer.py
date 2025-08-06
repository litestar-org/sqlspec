"""Tests for SQLTransformer - the single-pass SQL transformation system."""

from typing import Any

import pytest

from sqlspec.statement.sql import SQL
from sqlspec.statement.transformer import SQLTransformer


@pytest.mark.parametrize(
    "sql,parameters,expected_operation,expected_parameters_type",
    [
        # Basic SELECT queries
        ("SELECT * FROM users", [None], "SELECT", list),
        ("SELECT * FROM users WHERE id = ?", [123], "SELECT", list),
        ("SELECT * FROM users WHERE id = ? AND name = ?", [123, "test"], "SELECT", list),
        # Named styles are preserved when compatible
        ("SELECT * FROM users WHERE id = :id", {"id": 123}, "SELECT", dict),
        # Parameter style conversion cases - preserve compatible styles
        ("SELECT * FROM users WHERE id = %(id)s", {"id": 123}, "SELECT", dict),
        ("SELECT * FROM users WHERE id = :id AND name = :name", {"id": 123, "name": "test"}, "SELECT", dict),
        # INSERT queries
        ("INSERT INTO users (name) VALUES (?)", ["test"], "INSERT", list),
        ("INSERT INTO users (name) VALUES (%(name)s)", {"name": "test"}, "INSERT", dict),
        # UPDATE queries
        ("UPDATE users SET name = ? WHERE id = ?", ["new_name", 123], "UPDATE", list),
        # DELETE queries
        ("DELETE FROM users WHERE id = ?", [123], "DELETE", list),
    ],
)
def test_basic_sql_operations(
    sql: str, parameters: Any, expected_operation: str, expected_parameters_type: type
) -> None:
    """Test basic SQL operations with various parameter styles."""
    from sqlspec.statement.sql import StatementConfig

    # Use postgres dialect for pyformat styles
    config = None
    if "%(" in sql:
        config = StatementConfig(dialect="postgres")

    if isinstance(parameters, dict):
        statement = SQL(sql, statement_config=config, **parameters)
    elif isinstance(parameters, (list, tuple)):
        statement = SQL(sql, *parameters, statement_config=config)
    else:
        statement = SQL(sql, parameters, statement_config=config)

    # Test operation type detection
    assert statement.operation_type == expected_operation

    # Test compilation
    compiled_sql, compiled_parameters = statement.compile()
    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0

    # Test parameter format preservation
    if parameters is not None:
        assert isinstance(compiled_parameters, expected_parameters_type)
    else:
        assert compiled_parameters is None


@pytest.mark.parametrize(
    "sql,parameters,expected_sql",
    [
        # All styles should be normalized to QMARK (the default)
        ("SELECT * FROM users WHERE id = ?", [123], "SELECT * FROM users WHERE id = ?"),
        ("SELECT * FROM users WHERE id = :id", {"id": 123}, "SELECT * FROM users WHERE id = ?"),
        ("SELECT * FROM users WHERE id = %(id)s", {"id": 123}, "SELECT * FROM users WHERE id = ?"),
        (
            "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s",
            {"id": 123, "name": "test"},
            "SELECT * FROM users WHERE id = ? AND name = ?",
        ),
    ],
)
def test_parameter_style_normalization(sql: str, parameters: Any, expected_sql: str) -> None:
    """Test that parameter styles are normalized to the config's default execution style when needed."""
    from sqlspec.statement.sql import StatementConfig
    from sqlspec.parameters import ParameterStyle, ParameterStyleConfig

    # Create config that explicitly requires QMARK for execution
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_execution_parameter_styles={ParameterStyle.QMARK},
            default_execution_parameter_style=ParameterStyle.QMARK,
        )
    )
    transformer = SQLTransformer(parameters=parameters, dialect="postgres", config=config)

    compiled_sql, compiled_params = transformer.compile(sql)

    # All styles should be normalized to QMARK when execution styles are restricted
    assert compiled_sql == expected_sql
    # Should produce compiled parameters
    assert compiled_params is not None


@pytest.mark.parametrize(
    "parameters,expected_format",
    [
        ([1, 2, 3], "list"),
        ((1, 2, 3), "tuple"),
        ({"a": 1, "b": 2}, "dict"),
        ("single", "other"),
        (123, "other"),
        (None, "other"),
    ],
)
def test_parameter_format_detection(parameters: Any, expected_format: str) -> None:
    """Test parameter format detection for preservation."""
    from sqlspec.statement.sql import StatementConfig

    config = StatementConfig()
    transformer = SQLTransformer(parameters=parameters, dialect="postgres", config=config)
    assert transformer.parameter_style == expected_format


@pytest.mark.parametrize(
    "original_parameters,processed_dict,expected_result",
    [
        # List format preservation
        ([1, 2, 3], {"0": 1, "1": 2, "2": 3}, [1, 2, 3]),
        # Tuple format preservation
        ((1, 2, 3), {"0": 1, "1": 2, "2": 3}, (1, 2, 3)),
        # Dict format (no conversion needed)
        ({"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": 2}),
        # Non-numeric keys (no conversion)
        ([1, 2], {"param_0": 1, "param_1": 2}, [1, 2]),
    ],
)
def test_parameter_format_conversion(
    original_parameters: Any, processed_dict: dict[str, Any], expected_result: Any
) -> None:
    """Test conversion back to original parameter format."""
    from sqlspec.statement.sql import StatementConfig

    config = StatementConfig()
    transformer = SQLTransformer(parameters=original_parameters, dialect="postgres", config=config)
    result = transformer._convert_to_original_format(processed_dict)
    assert result == expected_result
    assert isinstance(result, type(expected_result))


def test_sql_transformer_integration() -> None:
    """Test complete SQLTransformer integration with SQL class."""
    from sqlspec.statement.sql import StatementConfig
    
    # Test case that previously failed with SQLGlot parsing
    sql = "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s"
    parameters = {"id": 123, "name": "test_user"}

    # Pyformat needs a dialect to parse properly
    config = StatementConfig(dialect="postgres")
    statement = SQL(sql, statement_config=config, **parameters)

    # Should not raise any exceptions
    compiled_sql, compiled_parameters = statement.compile()

    # Should produce valid results
    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0
    # Default config preserves compatible styles, so we get a dict for pyformat
    assert compiled_parameters == {"id": 123, "name": "test_user"}
    # The SQL should preserve pyformat style when compatible
    assert "%(id)s" in compiled_sql or "?" in compiled_sql
    assert statement.operation_type == "SELECT"


def test_empty_sql_handling() -> None:
    """Test handling of edge cases."""
    # Empty SQL should be handled gracefully (SQLGlot converts to 'SELECT')
    statement = SQL("", None)
    compiled_sql, _ = statement.compile()
    assert compiled_sql == "SELECT"  # SQLGlot's default for empty SQL

    # Whitespace-only SQL should be handled gracefully
    statement = SQL("   ", None)
    compiled_sql, _ = statement.compile()
    assert compiled_sql == "SELECT"  # SQLGlot's default for whitespace-only SQL


def test_sql_without_parameters() -> None:
    """Test SQL statements without any parameters."""
    sql = "SELECT * FROM users"
    # Use empty list to indicate no parameters
    statement = SQL(sql, [])

    compiled_sql, compiled_parameters = statement.compile()
    assert compiled_sql == sql  # Should be unchanged
    assert compiled_parameters is None
    assert statement.operation_type == "SELECT"


def test_sql_property_access() -> None:
    """Test that SQL properties work correctly."""
    sql = "SELECT * FROM users WHERE id = ?"
    parameters = [123]
    statement = SQL(sql, parameters)

    # Test sql property
    compiled_sql = statement.sql
    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0

    # Test parameters property
    assert statement.parameters == parameters

    # Test expression property
    expression = statement.expression
    assert expression is not None


@pytest.mark.parametrize(
    "sql,expected_operation",
    [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'test'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INT)", "EXECUTE"),  # Non-standard operations default to EXECUTE
    ],
)
def test_operation_type_detection(sql: str, expected_operation: str) -> None:
    """Test operation type detection for various SQL statement types."""
    statement = SQL(sql, None)
    assert statement.operation_type == expected_operation
