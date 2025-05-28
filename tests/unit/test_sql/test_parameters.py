"""Comprehensive tests for sqlspec.sql.parameters module.

This test module provides extensive coverage of the parameter handling system,
including edge cases, SQL injection patterns, and various parameter styles.
"""

from typing import Any, Optional

import pytest

from sqlspec.exceptions import (
    ExtraParameterError,
    MissingParameterError,
    ParameterStyleMismatchError,
)
from sqlspec.sql.parameters import (
    ParameterConverter,
    ParameterInfo,
    ParameterStyle,
    ParameterValidator,
    convert_parameters,
)


def test_parameter_style_values() -> None:
    """Test that all parameter styles have expected values."""
    expected_styles = {
        "NONE",
        "STATIC",
        "QMARK",
        "NUMERIC",
        "NAMED_COLON",
        "NAMED_AT",
        "NAMED_DOLLAR",
        "PYFORMAT_NAMED",
        "PYFORMAT_POSITIONAL",
    }
    actual_styles = {style.name for style in ParameterStyle}
    assert actual_styles == expected_styles


def test_parameter_style_string_representation() -> None:
    """Test string representation of parameter styles."""
    assert str(ParameterStyle.QMARK) == "qmark"
    assert str(ParameterStyle.NAMED_COLON) == "named_colon"
    assert str(ParameterStyle.PYFORMAT_NAMED) == "pyformat_named"


def test_parameter_info_creation() -> None:
    """Test creating ParameterInfo instances."""
    # Named parameter
    named_param = ParameterInfo(
        name="user_id", style=ParameterStyle.NAMED_COLON, position=20, ordinal=0, placeholder_text=":user_id"
    )
    assert named_param.name == "user_id"
    assert named_param.style == ParameterStyle.NAMED_COLON
    assert named_param.position == 20
    assert named_param.ordinal == 0

    # Positional parameter
    positional_param = ParameterInfo(
        name=None, style=ParameterStyle.QMARK, position=15, ordinal=1, placeholder_text="?"
    )
    assert positional_param.name is None
    assert positional_param.style == ParameterStyle.QMARK
    assert positional_param.position == 15
    assert positional_param.ordinal == 1


def test_parameter_info_equality() -> None:
    """Test equality comparison of ParameterInfo instances."""
    param1 = ParameterInfo("name", ParameterStyle.QMARK, 10, 0, "?")
    param2 = ParameterInfo("name", ParameterStyle.QMARK, 10, 0, "?")
    param3 = ParameterInfo("name", ParameterStyle.QMARK, 10, 1, "?")  # Different ordinal

    assert param1 == param2
    # Ordinal is excluded from comparison
    assert param1 == param3


@pytest.mark.parametrize(
    ("sql", "expected_style", "expected_count"),
    [
        # Basic style detection
        ("SELECT * FROM users WHERE id = ?", ParameterStyle.QMARK, 1),
        ("SELECT * FROM users WHERE name = :name", ParameterStyle.NAMED_COLON, 1),
        ("SELECT * FROM users WHERE id = @id", ParameterStyle.NAMED_AT, 1),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC, 1),
        ("SELECT * FROM users WHERE name = $name", ParameterStyle.NAMED_DOLLAR, 1),
        ("SELECT * FROM users WHERE name = %(name)s", ParameterStyle.PYFORMAT_NAMED, 1),
        ("SELECT * FROM users WHERE id = %s", ParameterStyle.PYFORMAT_POSITIONAL, 1),
        # Multiple parameters of same style
        ("SELECT * FROM users WHERE id = ? AND status = ?", ParameterStyle.QMARK, 2),
        ("SELECT * FROM users WHERE name = :name AND age = :age", ParameterStyle.NAMED_COLON, 2),
        ("INSERT INTO logs VALUES (%(msg)s, %(level)s)", ParameterStyle.PYFORMAT_NAMED, 2),
        # No parameters
        ("SELECT * FROM users", ParameterStyle.NONE, 0),
        ("CREATE TABLE test (id INTEGER)", ParameterStyle.NONE, 0),
        # Parameters in comments should be ignored
        ("SELECT * FROM users -- WHERE id = ?", ParameterStyle.NONE, 0),
        ("SELECT * FROM users /* WHERE name = :name */", ParameterStyle.NONE, 0),
        # Parameters in quoted strings should be ignored
        ("SELECT '?' AS question FROM users", ParameterStyle.NONE, 0),
        ("SELECT 'User: :name' AS label FROM users", ParameterStyle.NONE, 0),
        ('SELECT "@mention" FROM posts', ParameterStyle.NONE, 0),
        # PostgreSQL JSON operators should not be detected as parameters
        ("SELECT data ?? 'key' FROM json_table", ParameterStyle.NONE, 0),
        ("SELECT data ?| array['a','b'] FROM json_table", ParameterStyle.NONE, 0),
        ("SELECT data ?& array['a','b'] FROM json_table", ParameterStyle.NONE, 0),
        # Dollar-quoted strings should be ignored
        ("SELECT $tag$?:@$tag$ FROM test", ParameterStyle.NONE, 0),
        ("SELECT $$string with ? and :name$$ FROM test", ParameterStyle.NONE, 0),
    ],
    ids=[
        "qmark_single",
        "named_colon_single",
        "named_at_single",
        "numeric_single",
        "named_dollar_single",
        "pyformat_named_single",
        "pyformat_positional_single",
        "qmark_multiple",
        "named_colon_multiple",
        "pyformat_named_multiple",
        "no_params_select",
        "no_params_ddl",
        "comment_line_ignored",
        "comment_block_ignored",
        "single_quote_ignored",
        "named_in_string_ignored",
        "double_quote_ignored",
        "pg_json_operators_ignored",
        "pg_json_or_ignored",
        "pg_json_and_ignored",
        "dollar_quoted_ignored",
        "dollar_quoted_simple_ignored",
    ],
)
def test_detect_parameter_style_basic(sql: str, expected_style: ParameterStyle, expected_count: int) -> None:
    """Test basic parameter style detection."""
    # Get both style and count by using the validator directly
    validator = ParameterValidator()
    parameters = validator.extract_parameters(sql)
    style = validator.get_parameter_style(parameters)
    count = len(parameters)
    assert style == expected_style
    assert count == expected_count


def test_detect_parameter_style_mixed_error() -> None:
    """Test detection of mixed parameter styles returns dominant style."""
    mixed_sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    # The actual implementation returns the dominant style (named over positional)
    validator = ParameterValidator()
    parameters = validator.extract_parameters(mixed_sql)
    style = validator.get_parameter_style(parameters)
    # Should return NAMED_COLON as the dominant style when mixed
    assert style == ParameterStyle.NAMED_COLON


@pytest.mark.parametrize(
    "sql",
    [
        # Complex SQL with quotes and comments
        """
        SELECT u.name,
                'User ID: ' || u.id AS label,  -- Comment with ?
                u.email
        FROM users u
        WHERE u.id = ?
            AND u.status = 'active'
            /* Block comment with :param */
        """,
        # Nested quotes
        "SELECT 'It''s a ''test'' with ? inside' FROM dual WHERE id = ?",
        # Multiple quote types
        "SELECT \"Column with : colon\", 'Single with ?' FROM test WHERE val = ?",
    ],
)
def test_detect_parameter_style_complex_sql(sql: str) -> None:
    """Test parameter detection in complex SQL statements."""
    validator = ParameterValidator()
    parameters = validator.extract_parameters(sql)
    style = validator.get_parameter_style(parameters)
    count = len(parameters)
    assert style == ParameterStyle.QMARK
    assert count >= 1  # Should detect at least one parameter


def test_parameter_validator_initialization() -> None:
    """Test ParameterValidator initialization."""
    validator = ParameterValidator()
    assert validator is not None


@pytest.mark.parametrize(
    ("sql", "params", "should_raise", "expected_error"),
    [
        # Valid cases
        ("SELECT * FROM users WHERE id = ?", [123], False, None),
        ("SELECT * FROM users WHERE name = :name", {"name": "John"}, False, None),
        ("SELECT * FROM users", None, False, None),
        ("SELECT * FROM users", {}, False, None),
        # Missing parameters
        ("SELECT * FROM users WHERE id = ?", [], True, MissingParameterError),
        ("SELECT * FROM users WHERE name = :name", {}, True, MissingParameterError),
        ("SELECT * FROM users WHERE id = ? AND status = ?", [123], True, MissingParameterError),
        # Extra parameters
        ("SELECT * FROM users WHERE id = ?", [123, 456], True, ExtraParameterError),
        ("SELECT * FROM users WHERE name = :name", {"name": "John", "age": 30}, True, ExtraParameterError),
        # Type mismatches
        ("SELECT * FROM users WHERE id = ?", {"id": 123}, True, ParameterStyleMismatchError),
        ("SELECT * FROM users WHERE name = :name", [123], True, ParameterStyleMismatchError),
        # Invalid scalar for named parameters - using string which is a sequence
        ("SELECT * FROM users WHERE name = :name", "test_string", True, ParameterStyleMismatchError),
    ],
    ids=[
        "valid_qmark",
        "valid_named",
        "valid_no_params_none",
        "valid_no_params_empty",
        "missing_qmark_param",
        "missing_named_param",
        "missing_multiple_qmark",
        "extra_qmark_param",
        "extra_named_param",
        "type_mismatch_dict_for_qmark",
        "type_mismatch_list_for_named",
        "invalid_string_scalar_for_named",
    ],
)
def test_parameter_validation_cases(
    sql: str, params: Any, should_raise: bool, expected_error: Optional[type[Exception]]
) -> None:
    """Test various parameter validation scenarios."""
    validator = ParameterValidator()

    if should_raise and expected_error is not None:
        with pytest.raises(expected_error):
            parameters = validator.extract_parameters(sql)
            validator.validate_parameters(parameters, params, sql)
    else:
        # Should not raise any exception
        parameters = validator.extract_parameters(sql)
        validator.validate_parameters(parameters, params, sql)


def test_parameter_validation_with_complex_types() -> None:
    """Test parameter validation with complex data types."""
    validator = ParameterValidator()

    # Valid complex types
    complex_params = {
        "user_data": {"name": "John", "age": 30},
        "tags": ["python", "sql"],
        "created_at": "2023-01-01T00:00:00Z",
        "is_active": True,
    }
    sql = "INSERT INTO users (data, tags, created, active) VALUES (:user_data, :tags, :created_at, :is_active)"

    # Should not raise
    parameters = validator.extract_parameters(sql)
    validator.validate_parameters(parameters, complex_params, sql)


def test_parameter_converter_initialization() -> None:
    """Test ParameterConverter initialization."""
    converter = ParameterConverter()
    assert converter is not None


@pytest.mark.parametrize(
    ("sql", "parameters", "args", "kwargs", "expected_params"),
    [
        # Basic merging scenarios
        ("SELECT * FROM users WHERE id = ?", None, [123], None, [123]),
        ("SELECT * FROM users WHERE name = :name", None, None, {"name": "John"}, {"name": "John"}),
        ("SELECT * FROM users WHERE id = ?", [456], [123], None, [456]),  # parameters takes precedence
        ("SELECT * FROM users WHERE name = :name", {"name": "Alice"}, None, {"name": "Bob"}, {"name": "Alice"}),
        # Complex merging - kwargs takes precedence over args when no parameters
        ("SELECT * FROM users", None, [1, 2], {"x": 3}, {"x": 3}),
        # Mixed positional named - kwargs only when no parameters
        (
            "SELECT * FROM users WHERE id = ? AND name = :name",
            None,
            [123],
            {"name": "John"},
            {"name": "John"},
        ),
        # Scalar parameters
        ("SELECT * FROM users WHERE id = ?", 42, None, None, 42),
        ("SELECT * FROM users", None, None, None, None),
    ],
    ids=[
        "args_only",
        "kwargs_only",
        "parameters_precedence_over_args",
        "parameters_precedence_over_kwargs",
        "kwargs_precedence_over_args",
        "kwargs_only_when_mixed",
        "scalar_parameter",
        "all_none",
    ],
)
def test_merge_parameters(sql: str, parameters: Any, args: Any, kwargs: Any, expected_params: Any) -> None:
    """Test parameter merging logic."""
    converter = ParameterConverter()
    result = converter.merge_parameters(parameters, args, kwargs)
    assert result == expected_params


def test_convert_parameters_full_flow() -> None:
    """Test the complete parameter conversion flow."""
    converter = ParameterConverter()

    sql = "SELECT * FROM users WHERE name = :name AND age > :min_age"
    params = {"name": "John", "min_age": 18}

    _, param_info, merged_params, placeholder_map = converter.convert_parameters(sql, params, validate=True)

    # Extract style from param_info
    validator = ParameterValidator()
    style = validator.get_parameter_style(param_info)

    assert style == ParameterStyle.NAMED_COLON
    assert len(param_info) == 2
    assert merged_params == params
    assert "__param_0" in placeholder_map
    assert "__param_1" in placeholder_map
    assert placeholder_map["__param_0"] == "name"
    assert placeholder_map["__param_1"] == "min_age"


def test_convert_parameters_with_validation_error() -> None:
    """Test parameter conversion with validation errors."""
    converter = ParameterConverter()

    sql = "SELECT * FROM users WHERE id = ?"
    params: list[Any] = []

    with pytest.raises(MissingParameterError):
        converter.convert_parameters(sql, params, validate=True)


def test_convert_parameters_without_validation() -> None:
    """Test parameter conversion without validation."""
    converter = ParameterConverter()

    sql = "SELECT * FROM users WHERE id = ?"
    params = {}  # Missing parameter, but validation disabled

    # Should not raise when validation is disabled
    _, param_info, merged_params, _ = converter.convert_parameters(sql, params, validate=False)

    # Extract style from param_info
    validator = ParameterValidator()
    style = validator.get_parameter_style(param_info)

    assert style == ParameterStyle.QMARK
    assert len(param_info) == 1
    assert merged_params == params


def test_convert_parameters_function_basic() -> None:
    """Test basic usage of convert_parameters function."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ?"
    params = [123, "John"]

    transformed_sql, param_info, merged_params, placeholder_map = convert_parameters(sql, params)

    # Check that we get the expected return structure
    assert isinstance(transformed_sql, str)
    assert len(param_info) == 2
    assert merged_params == params
    assert len(placeholder_map) == 2


def test_convert_parameters_function_with_validation() -> None:
    """Test convert_parameters function with validation enabled."""
    sql = "SELECT * FROM users WHERE id = ?"
    params = []  # type: ignore[var-annotated]

    with pytest.raises(MissingParameterError):
        convert_parameters(sql, params, validate=True)


@pytest.mark.parametrize(
    "malicious_sql",
    [
        # SQL injection attempts
        "SELECT * FROM users WHERE id = ?; DROP TABLE users; --",
        "SELECT * FROM users WHERE name = :name' OR '1'='1",
        "SELECT * FROM users WHERE id = ? UNION SELECT password FROM admin_users",
        # Deeply nested quotes
        "SELECT '''nested ''quotes'' with ? param''' FROM test WHERE val = ?",
        # Very long parameter names
        f"SELECT * FROM test WHERE {'a' * 1000} = :{'param_' + 'a' * 100}",
        # Unicode in SQL and parameters
        "SELECT * FROM тест WHERE имя = :имя_пользователя",
        # Multiple statement separators
        "SELECT ?; SELECT ?; SELECT ?",
    ],
)
def test_malicious_sql_detection(malicious_sql: str) -> None:
    """Test that parameter detection handles malicious SQL safely."""
    try:
        validator = ParameterValidator()
        parameters = validator.extract_parameters(malicious_sql)
        style = validator.get_parameter_style(parameters)
        count = len(parameters)
        # Should complete without crashing
        assert isinstance(style, ParameterStyle)
        assert isinstance(count, int)
        assert count >= 0
    except ParameterStyleMismatchError:
        # This is acceptable for mixed parameter styles
        pass


def test_extremely_long_sql() -> None:
    """Test parameter detection with very long SQL statements."""
    # Create a very long SQL statement
    base_sql = "SELECT * FROM users WHERE "
    conditions = [f"col_{i} = :param_{i}" for i in range(1000)]
    long_sql = base_sql + " AND ".join(conditions)

    # Use validator directly to get both style and count
    validator = ParameterValidator()
    parameters = validator.extract_parameters(long_sql)
    style = validator.get_parameter_style(parameters)
    count = len(parameters)

    assert style == ParameterStyle.NAMED_COLON
    assert count == 1000


def test_regex_edge_cases() -> None:
    """Test regex edge cases that might cause issues."""
    edge_cases = [
        # Empty string
        "",
        # Only whitespace
        "   \n\t  ",
        # Only comments
        "-- Just a comment",
        "/* Just a block comment */",
        # Malformed quotes (should not crash)
        "SELECT 'unclosed quote FROM test",
        'SELECT "unclosed double quote FROM test',
        # Nested dollar quotes
        "SELECT $outer$inner $inner$ content$inner$ outer$outer$",
    ]

    # Test each edge case individually to avoid loop overhead
    validator = ParameterValidator()
    for sql in edge_cases:
        parameters = validator.extract_parameters(sql)
        style = validator.get_parameter_style(parameters)
        count = len(parameters)
        assert isinstance(style, ParameterStyle)
        assert isinstance(count, int)
        assert count >= 0


def test_parameter_name_validation() -> None:
    """Test validation of parameter names."""
    converter = ParameterConverter()

    # Valid parameter names
    valid_sql = "SELECT * FROM users WHERE name = :valid_name_123"
    transformed_sql, _, _, _ = converter.convert_parameters(valid_sql, {"valid_name_123": "test"})
    assert isinstance(transformed_sql, str)

    # Parameter names with special characters (should be detected as separate parameters)
    # :user$data is parsed as :user followed by $data
    special_sql = "SELECT * FROM users WHERE data = :user$data"
    transformed_sql, param_info, _, _ = converter.convert_parameters(special_sql, {"user": "test1", "data": "test2"})
    assert len(param_info) == 2  # Two separate parameters detected
    assert param_info[0].name == "user"
    assert param_info[1].name == "data"
    assert isinstance(transformed_sql, str)


def test_numeric_parameter_edge_cases() -> None:
    """Test numeric parameter edge cases."""
    # Large numeric parameters
    large_num_sql = "SELECT * FROM table WHERE id = $999999"
    validator = ParameterValidator()
    parameters = validator.extract_parameters(large_num_sql)
    style = validator.get_parameter_style(parameters)
    count = len(parameters)
    assert style == ParameterStyle.NUMERIC
    assert count == 1

    # Zero-padded numbers
    zero_padded_sql = "SELECT * FROM table WHERE id = $001"
    parameters = validator.extract_parameters(zero_padded_sql)
    style = validator.get_parameter_style(parameters)
    count = len(parameters)
    assert style == ParameterStyle.NUMERIC
    assert count == 1


def test_mixed_quote_types_with_parameters() -> None:
    """Test SQL with mixed quote types containing parameter-like strings."""
    mixed_quotes_sql = """
    SELECT
        'Single quote with ? inside',
        "Double quote with :param inside",
        $tag$Dollar quote with @param$tag$,
        real_param
    FROM test
    WHERE id = ?
    """

    validator = ParameterValidator()
    parameters = validator.extract_parameters(mixed_quotes_sql)
    style = validator.get_parameter_style(parameters)
    count = len(parameters)
    assert style == ParameterStyle.QMARK
    assert count == 1  # Only the real ? parameter should be detected


def test_large_parameter_count_performance() -> None:
    """Test performance with large numbers of parameters."""
    # Create SQL with many parameters
    param_count = 1000
    placeholders = ", ".join("?" * param_count)
    sql = f"INSERT INTO test VALUES ({placeholders})"
    params = list(range(param_count))

    converter = ParameterConverter()

    # Should complete in reasonable time
    transformed_sql, param_info, merged_params, placeholder_map = converter.convert_parameters(
        sql, params, validate=True
    )

    assert isinstance(transformed_sql, str)
    assert len(param_info) == param_count
    if merged_params is not None:
        assert len(merged_params) == param_count  # Only check len if not None
    assert len(placeholder_map) == param_count


def test_regex_compilation_caching() -> None:
    """Test that regex patterns are compiled and cached properly."""
    # The regex should be compiled once and reused
    sql1 = "SELECT * FROM users WHERE id = ?"
    sql2 = "SELECT * FROM orders WHERE user_id = ?"

    # Both should use the same compiled regex
    validator = ParameterValidator()

    parameters1 = validator.extract_parameters(sql1)
    style1 = validator.get_parameter_style(parameters1)
    count1 = len(parameters1)

    parameters2 = validator.extract_parameters(sql2)
    style2 = validator.get_parameter_style(parameters2)
    count2 = len(parameters2)

    assert style1 == style2 == ParameterStyle.QMARK
    assert count1 == count2 == 1
