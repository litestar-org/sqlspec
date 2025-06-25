"""Unit tests for Oracle numeric parameter handling (:1, :2 style).

Oracle supports both named parameters (:name) and numeric parameters (:1, :2).
This module tests the specific handling of Oracle's numeric parameter style.
"""

from typing import TYPE_CHECKING, Any, Optional, Union

import pytest

from sqlspec.exceptions import MissingParameterError, ParameterError, SQLValidationError
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator
from sqlspec.statement.sql import SQL, SQLConfig

if TYPE_CHECKING:
    pass


# Test Oracle numeric parameter detection
@pytest.mark.parametrize(
    "sql,expected_style",
    [
        ("INSERT INTO users VALUES (:1, :2)", ParameterStyle.POSITIONAL_COLON),
        ("SELECT * FROM users WHERE id = :1", ParameterStyle.POSITIONAL_COLON),
        ("UPDATE users SET name = :1 WHERE id = :2", ParameterStyle.POSITIONAL_COLON),
        ("DELETE FROM users WHERE id = :1 AND status = :2", ParameterStyle.POSITIONAL_COLON),
        # Multiple digit numbers
        ("SELECT * FROM users WHERE id = :10", ParameterStyle.POSITIONAL_COLON),
        ("INSERT INTO big_table VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)", ParameterStyle.POSITIONAL_COLON),
    ],
    ids=["insert", "select", "update", "delete", "double_digit", "many_params"],
)
def test_positional_colon_parameter_detection(sql: str, expected_style: ParameterStyle) -> None:
    """Test that :1, :2 style parameters are detected as POSITIONAL_COLON."""
    validator = ParameterValidator()
    params = validator.extract_parameters(sql)
    style = validator.get_parameter_style(params)
    assert style == expected_style


@pytest.mark.parametrize(
    "sql,parameters,expected_param_count",
    [
        ("INSERT INTO users VALUES (:1, :2)", [1, "john"], 2),
        ("SELECT * FROM users WHERE id = :1", [42], 1),
        ("UPDATE users SET name = :1, email = :2 WHERE id = :3", ["john", "john@example.com", 1], 3),
        # Out of order parameters
        ("SELECT * FROM users WHERE id = :2 AND name = :1", ["john", 42], 2),
        # Repeated parameters
        ("SELECT * FROM users WHERE id = :1 OR parent_id = :1", [42], 2),  # Each occurrence is counted
    ],
    ids=["insert", "select", "update", "out_of_order", "repeated"],
)
def test_positional_colon_parameter_extraction(sql: str, parameters: list[Any], expected_param_count: int) -> None:
    """Test extraction of Oracle numeric parameters."""
    stmt = SQL(sql, parameters=parameters)
    assert len(stmt.parameter_info) == expected_param_count


# Test mixed parameter styles
@pytest.mark.parametrize(
    "sql,parameters,error_type",
    [
        # Mixed numeric and named parameters
        (
            "SELECT * FROM users WHERE id = :1 AND status = :status",
            {"1": 42, "status": "active"},
            None,  # Should work with dict
        ),
        (
            "SELECT * FROM users WHERE id = :1 AND status = :status",
            [42],  # Missing status parameter
            MissingParameterError,
        ),
        # Mixing different parameter styles
        (
            "SELECT * FROM users WHERE id = :1 AND name = ?",
            [42, "john"],
            None,  # Mixed styles allowed if config permits
        ),
    ],
    ids=["mixed_dict_ok", "mixed_missing_param", "mixed_styles"],
)
def test_mixed_parameter_styles(
    sql: str, parameters: Union[list[Any], dict[str, Any]], error_type: Optional[type[Exception]]
) -> None:
    """Test handling of mixed parameter styles."""
    # Enable parameter validation by setting allowed_parameter_styles
    config = SQLConfig(
        allowed_parameter_styles=("positional_colon", "qmark", "named_colon"),
        allow_mixed_parameter_styles=True,  # Allow mixed styles for these tests
        strict_mode=True,  # Enable strict mode to raise validation errors
    )
    if error_type:
        stmt = SQL(sql, parameters=parameters, _config=config)
        # In strict mode, validation errors are wrapped in SQLValidationError
        with pytest.raises((error_type, SQLValidationError)):
            # Trigger validation by accessing property
            _ = stmt.to_sql()
    else:
        stmt = SQL(sql, parameters=parameters, _config=config)
        # Should not raise
        _ = stmt.to_sql()


# Test parameter conversion
@pytest.mark.parametrize(
    "sql,input_params,target_style,expected_output",
    [
        # List to Oracle numeric dict
        ("INSERT INTO users VALUES (:1, :2)", ["john", 42], ParameterStyle.POSITIONAL_COLON, {"1": "john", "2": 42}),
        # Oracle numeric dict to list (for databases that expect positional)
        ("INSERT INTO users VALUES (:1, :2)", {"1": "john", "2": 42}, ParameterStyle.QMARK, ["john", 42]),
        # Out of order numeric parameters
        ("SELECT * WHERE id = :2 AND name = :1", ["john", 42], ParameterStyle.POSITIONAL_COLON, {"1": "john", "2": 42}),
        # Numeric parameters with gaps
        (
            "SELECT * WHERE id = :1 AND status = :3",
            {"1": 42, "3": "active"},
            ParameterStyle.POSITIONAL_COLON,
            {"1": 42, "3": "active"},
        ),
    ],
    ids=["list_to_oracle", "oracle_to_list", "out_of_order", "with_gaps"],
)
def test_positional_colon_parameter_conversion(
    sql: str,
    input_params: Union[list[Any], dict[str, Any]],
    target_style: ParameterStyle,
    expected_output: Union[list[Any], dict[str, Any]],
) -> None:
    """Test conversion between parameter formats."""
    stmt = SQL(sql, parameters=input_params)
    result = stmt.get_parameters(target_style)
    assert result == expected_output


# Test SQL generation with Oracle numeric parameters
@pytest.mark.parametrize(
    "sql,parameters,placeholder_style,expected_sql_contains",
    [
        # Preserve Oracle numeric style
        ("INSERT INTO users VALUES (:1, :2)", ["john", 42], ParameterStyle.POSITIONAL_COLON, [":1", ":2"]),
        # Convert to question marks
        ("INSERT INTO users VALUES (:1, :2)", ["john", 42], ParameterStyle.QMARK, ["?", "?"]),
        # Convert to named style
        ("INSERT INTO users VALUES (:1, :2)", ["john", 42], ParameterStyle.NAMED_COLON, [":arg_0", ":arg_1"]),
        # Convert to numeric dollar style
        ("INSERT INTO users VALUES (:1, :2)", ["john", 42], ParameterStyle.NUMERIC, ["$1", "$2"]),
    ],
    ids=["preserve_oracle", "to_qmark", "to_named", "to_numeric_dollar"],
)
def test_positional_colon_to_sql_conversion(
    sql: str, parameters: list[Any], placeholder_style: ParameterStyle, expected_sql_contains: list[str]
) -> None:
    """Test SQL generation with different placeholder styles."""
    stmt = SQL(sql, parameters=parameters)
    result = stmt.to_sql(placeholder_style=placeholder_style)
    for expected in expected_sql_contains:
        assert expected in result


# Test edge cases
def test_positional_colon_vs_named_colon() -> None:
    """Test that :1 is treated differently from :name."""
    # Numeric style
    sql1 = "SELECT * FROM users WHERE id = :1"
    stmt1 = SQL(sql1, parameters=[42])
    assert stmt1.parameter_info[0].style == ParameterStyle.POSITIONAL_COLON
    assert stmt1.parameter_info[0].name == "1"

    # Named style
    sql2 = "SELECT * FROM users WHERE id = :id"
    stmt2 = SQL(sql2, parameters={"id": 42})
    assert stmt2.parameter_info[0].style == ParameterStyle.NAMED_COLON
    assert stmt2.parameter_info[0].name == "id"


def test_positional_colon_with_execute_many() -> None:
    """Test Oracle numeric parameters with execute_many."""
    sql = "INSERT INTO users VALUES (:1, :2)"
    params = [[1, "john"], [2, "jane"], [3, "bob"]]
    stmt = SQL(sql).as_many(params)

    assert stmt.is_many
    assert stmt.parameters == params

    # Each parameter set should work
    for param_set in params:
        single_stmt = SQL(sql, parameters=param_set)
        assert len(single_stmt.parameter_info) == 2


@pytest.mark.parametrize(
    "sql,expected_order",
    [
        # Basic ordering
        ("SELECT :1, :2, :3 FROM dual", ["1", "2", "3"]),
        # Out of order
        ("SELECT :3, :1, :2 FROM dual", ["3", "1", "2"]),
        # With gaps
        ("SELECT :1, :5, :3 FROM dual", ["1", "5", "3"]),
        # Mixed with named (all treated as names in mixed mode)
        ("SELECT :1, :name, :2 FROM dual", ["1", "name", "2"]),
        # Double digits
        ("SELECT :10, :2, :11, :1 FROM dual", ["10", "2", "11", "1"]),
    ],
    ids=["basic_order", "out_of_order", "with_gaps", "mixed_named", "double_digits"],
)
def test_positional_colon_parameter_order(sql: str, expected_order: list[str]) -> None:
    """Test that parameter order is preserved correctly."""
    validator = ParameterValidator()
    params = validator.extract_parameters(sql)
    param_names = [p.name for p in params]
    assert param_names == expected_order


def test_positional_colon_regex_precedence() -> None:
    """Test that :1 is matched before :name in regex to avoid :1name being parsed as :1."""
    sql = "SELECT :1, :2something, :name, :3 FROM dual"
    # When we have :2something, it should be treated as :2 followed by "something"
    # not as a parameter named "2something"

    validator = ParameterValidator()
    params = validator.extract_parameters(sql)
    param_names = [p.name for p in params]

    # Should extract :1, :2, :name, :3 (not :2something)
    assert "1" in param_names
    assert "2" in param_names
    assert "name" in param_names
    assert "3" in param_names
    assert "2something" not in param_names


@pytest.mark.parametrize(
    "sql,parameters,should_fail",
    [
        # Valid cases
        ("INSERT INTO users VALUES (:1, :2)", [1, "john"], False),
        ("INSERT INTO users VALUES (:1, :2)", {"1": 1, "2": "john"}, False),
        # Missing parameters
        ("INSERT INTO users VALUES (:1, :2)", [1], True),
        ("INSERT INTO users VALUES (:1, :2)", {"1": 1}, True),  # Missing "2"
        # Extra parameters (should be ok)
        ("INSERT INTO users VALUES (:1, :2)", [1, "john", "extra"], False),
        ("INSERT INTO users VALUES (:1, :2)", {"1": 1, "2": "john", "3": "extra"}, False),
        # Empty parameters
        ("INSERT INTO users VALUES (:1, :2)", [], True),
        ("INSERT INTO users VALUES (:1, :2)", {}, True),
    ],
    ids=[
        "valid_list",
        "valid_dict",
        "missing_list",
        "missing_dict",
        "extra_list",
        "extra_dict",
        "empty_list",
        "empty_dict",
    ],
)
def test_positional_colon_parameter_validation(
    sql: str, parameters: Union[list[Any], dict[str, Any]], should_fail: bool
) -> None:
    """Test parameter validation for Oracle numeric style."""
    # Enable parameter validation by setting allowed_parameter_styles
    config = SQLConfig(
        allowed_parameter_styles=("positional_colon", "positional_colon"),
        strict_mode=True,  # Enable strict mode to raise validation errors
    )
    stmt = SQL(sql, parameters=parameters, _config=config)

    if should_fail:
        with pytest.raises((ParameterError, MissingParameterError, SQLValidationError)):
            # Trigger validation
            _ = stmt.to_sql()
    else:
        # Should not raise
        result = stmt.to_sql()
        assert ":1" in result or "?" in result  # Depending on normalization


# Test special cases
def test_positional_colon_in_strings_and_comments() -> None:
    """Test that :1 in strings and comments is not treated as a parameter."""
    sql = """
    SELECT
        'This is :1 in a string' as col1,
        "Another :2 in quotes" as col2,
        -- This :3 is in a comment
        /* And :4 in a block comment */
        :5 as real_param
    FROM dual
    """
    # Create SQL with strict mode disabled to avoid validation errors
    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(strict_mode=False)
    stmt = SQL(sql, parameters=[42], _config=config)

    # Should only find :5 as a real parameter
    assert len(stmt.parameter_info) == 1
    assert stmt.parameter_info[0].name == "5"


def test_positional_colon_with_zero() -> None:
    """Test that :0 is handled correctly (some databases start at 0)."""
    sql = "SELECT * FROM users WHERE id = :0"
    stmt = SQL(sql, parameters=[42])

    assert len(stmt.parameter_info) == 1
    assert stmt.parameter_info[0].name == "0"

    # Convert to dict format
    params = stmt.get_parameters(ParameterStyle.POSITIONAL_COLON)
    assert params == {"0": 42}


def test_positional_colon_large_numbers() -> None:
    """Test handling of large parameter numbers."""
    # Create SQL with parameters :1 through :100
    placeholders = ", ".join(f":{i}" for i in range(1, 101))
    sql = f"INSERT INTO big_table VALUES ({placeholders})"

    # Create corresponding parameter list
    params = list(range(1, 101))

    stmt = SQL(sql, parameters=params)
    assert len(stmt.parameter_info) == 100

    # Convert to dict and verify
    param_dict = stmt.get_parameters(ParameterStyle.POSITIONAL_COLON)
    assert len(param_dict) == 100
    assert param_dict["50"] == 50
    assert param_dict["100"] == 100
