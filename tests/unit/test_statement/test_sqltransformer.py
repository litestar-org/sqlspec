"""Tests for SQLTransformer - the single-pass SQL transformation system."""

import pytest

from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.statement.transformer import SQLTransformer


@pytest.fixture
def default_config():
    """Default StatementConfig for tests."""
    return StatementConfig()


@pytest.mark.parametrize(
    "sql,parameters,expected_operation,expected_parameters_type",
    [
        # Basic SELECT queries
        ("SELECT * FROM users", None, "SELECT", type(None)),
        ("SELECT * FROM users WHERE id = ?", [123], "SELECT", list),
        ("SELECT * FROM users WHERE id = ? AND name = ?", (123, "test"), "SELECT", tuple),
        ("SELECT * FROM users WHERE id = :id", {"id": 123}, "SELECT", dict),
        # Parameter style conversion cases
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
def test_basic_sql_operations(sql, parameters, expected_operation, expected_parameters_type, default_config):
    """Test basic SQL operations with various parameter styles."""
    statement = SQL(sql, parameters, statement_config=default_config)

    # Test operation type detection
    assert statement.operation_type == expected_operation

    # Test compilation
    compiled_sql, compiled_parameters = statement.compile()
    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0

    # Test parameter format preservation
    if parameters is not None:
        assert type(compiled_parameters) == expected_parameters_type
    else:
        assert compiled_parameters is None


@pytest.mark.parametrize(
    "sql,parameters,should_convert_parameters",
    [
        # SQLGlot compatible styles (no conversion needed)
        ("SELECT * FROM users WHERE id = ?", [123], False),
        ("SELECT * FROM users WHERE id = :id", {"id": 123}, False),
        # SQLGlot incompatible styles (conversion needed)
        ("SELECT * FROM users WHERE id = %(id)s", {"id": 123}, True),
        ("SELECT * FROM users WHERE id = %(id)s AND name = %(name)s", {"id": 123, "name": "test"}, True),
    ],
)
def test_parameter_style_conversion(sql, parameters, should_convert_parameters, default_config):
    """Test parameter style conversion for SQLGlot compatibility."""
    transformer = SQLTransformer(parameters=parameters, dialect="postgres", config=default_config)

    converted_sql, param_info = transformer._convert_parameter_styles(sql)

    if should_convert_parameters:
        # SQL should be different after conversion
        assert converted_sql != sql
        # Parameter info should be extracted
        assert len(param_info) > 0
    else:
        # SQL should be unchanged
        assert converted_sql == sql


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
def test_parameter_format_detection(parameters, expected_format, default_config):
    """Test parameter format detection for preservation."""
    transformer = SQLTransformer(parameters=parameters, dialect="postgres", config=default_config)
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
        ([1, 2], {"param_0": 1, "param_1": 2}, {"param_0": 1, "param_1": 2}),
    ],
)
def test_parameter_format_conversion(original_parameters, processed_dict, expected_result, default_config):
    """Test conversion back to original parameter format."""
    transformer = SQLTransformer(parameters=original_parameters, dialect="postgres", config=default_config)
    result = transformer._convert_to_original_format(processed_dict)
    assert result == expected_result
    assert type(result) == type(expected_result)


def test_sql_transformer_integration(default_config):
    """Test complete SQLTransformer integration with SQL class."""
    # Test case that previously failed with SQLGlot parsing
    sql = "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s"
    parameters = {"id": 123, "name": "test_user"}

    statement = SQL(sql, parameters, statement_config=default_config)

    # Should not raise any exceptions
    compiled_sql, compiled_parameters = statement.compile()

    # Should produce valid results
    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0
    assert compiled_parameters == parameters
    assert statement.operation_type == "SELECT"


def test_empty_sql_handling(default_config):
    """Test handling of edge cases."""
    # Empty SQL should be handled gracefully
    with pytest.raises(Exception):  # SQLGlot will raise an exception for empty SQL
        SQL("", None, statement_config=default_config).compile()

    # Whitespace-only SQL should be handled gracefully
    with pytest.raises(Exception):  # SQLGlot will raise an exception for whitespace-only SQL
        SQL("   ", None, statement_config=default_config).compile()


def test_sql_without_parameters(default_config):
    """Test SQL statements without any parameters."""
    sql = "SELECT * FROM users"
    statement = SQL(sql, None, statement_config=default_config)

    compiled_sql, compiled_parameters = statement.compile()
    assert compiled_sql == sql  # Should be unchanged
    assert compiled_parameters is None
    assert statement.operation_type == "SELECT"


def test_sql_property_access(default_config):
    """Test that SQL properties work correctly."""
    sql = "SELECT * FROM users WHERE id = ?"
    parameters = [123]
    statement = SQL(sql, parameters, statement_config=default_config)

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
def test_operation_type_detection(sql, expected_operation, default_config):
    """Test operation type detection for various SQL statement types."""
    statement = SQL(sql, None, statement_config=default_config)
    assert statement.operation_type == expected_operation
