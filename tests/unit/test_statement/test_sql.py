"""Unit tests for sqlspec.statement.sql module.

Tests the core SQL statement functionality including SQL class, SQLConfig,
immutable statement operations, parameter handling, validation, and filtering.
"""

import re
from dataclasses import replace
from typing import Any
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp

from sqlspec.exceptions import (
    RiskLevel,
    SQLValidationError,
)
from sqlspec.statement.filters import LimitOffsetFilter, SearchFilter
from sqlspec.statement.parameters import ParameterInfo, ParameterStyle
from sqlspec.statement.pipelines import ValidationResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.mark.parametrize(
    ("config_data", "expected_attrs"),
    [
        # Default configuration
        (
            {},
            {
                "enable_parsing": True,
                "enable_validation": True,
                "enable_transformations": True,
                "enable_analysis": False,
                "strict_mode": True,
                "cache_parsed_expression": True,
                "analysis_cache_size": 1000,
            },
        ),
        # Custom configuration
        (
            {
                "enable_parsing": False,
                "enable_validation": False,
                "strict_mode": False,
                "analysis_cache_size": 500,
            },
            {
                "enable_parsing": False,
                "enable_validation": False,
                "enable_transformations": True,  # Unchanged default
                "enable_analysis": False,  # Unchanged default
                "strict_mode": False,
                "cache_parsed_expression": True,  # Unchanged default
                "analysis_cache_size": 500,
            },
        ),
    ],
    ids=["default_config", "custom_config"],
)
def test_sqlconfig_initialization(config_data: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test SQLConfig initialization with default and custom values."""
    config = SQLConfig(**config_data)

    for attr_name, expected_value in expected_attrs.items():
        assert getattr(config, attr_name) == expected_value


def test_sqlconfig_get_pipeline_default() -> None:
    """Test get_pipeline returns TransformerPipeline with default components."""
    config = SQLConfig(enable_analysis=True, enable_validation=True)
    pipeline = config.get_pipeline()

    assert hasattr(pipeline, "execute")
    assert hasattr(pipeline, "components")
    assert len(pipeline.components) > 0  # Should have default components


def test_sqlconfig_get_pipeline_custom_components() -> None:
    """Test get_pipeline with custom processing components."""
    mock_component = Mock()
    config = SQLConfig(processing_pipeline_components=[mock_component])
    pipeline = config.get_pipeline()

    assert mock_component in pipeline.components


def test_sqlconfig_immutable_operations() -> None:
    """Test SQLConfig supports dataclass replace operations."""
    original_config = SQLConfig(strict_mode=True, enable_parsing=True)
    modified_config = replace(original_config, strict_mode=False)

    assert original_config.strict_mode is True
    assert modified_config.strict_mode is False
    assert modified_config.enable_parsing is True  # Unchanged


@pytest.mark.parametrize(
    ("sql_input", "expected_sql"),
    [
        ("SELECT * FROM users", "SELECT * FROM users"),
        ("   SELECT id FROM products   ", "SELECT id FROM products"),  # Whitespace trimmed
        ("", "SELECT"),  # Empty string becomes empty SELECT
    ],
    ids=["simple_select", "whitespace_preserved", "empty_string"],
)
def test_sql_string_initialization(sql_input: str, expected_sql: str) -> None:
    """Test SQL initialization with string inputs."""
    stmt = SQL(sql_input)

    if expected_sql == "SELECT":
        assert "SELECT" in stmt.sql.upper()
    else:
        assert stmt.sql == expected_sql
    assert stmt.expression is not None
    assert stmt.parameters in (None, {}, [])


@pytest.mark.parametrize(
    ("sql", "params_source", "params_value", "expected_params"),
    [
        ("SELECT * FROM users WHERE id = ?", "parameters", [123], [123]),
        ("SELECT * FROM users WHERE name = :name", "kwargs", {"name": "John"}, {"name": "John"}),
        ("SELECT * FROM users WHERE id = :id", "parameters", {"id": 456}, {"id": 456}),
        ("SELECT * FROM users WHERE id = ? AND active = ?", "parameters", [789, True], [789, True]),
        (
            "SELECT * FROM users WHERE name = :name AND dept = :dept",
            "kwargs",
            {"name": "Alice", "dept": "Engineering"},
            {"name": "Alice", "dept": "Engineering"},
        ),
    ],
    ids=["positional_parameters", "named_kwargs", "explicit_parameters", "multiple_positional", "multiple_named"],
)
def test_sql_with_parameters(sql: str, params_source: str, params_value: Any, expected_params: Any) -> None:
    """Test SQL initialization with various parameter types."""
    if params_source == "parameters":
        stmt = SQL(sql, parameters=params_value)
    elif params_source == "kwargs":
        stmt = SQL(sql, **params_value)
    else:
        pytest.fail(f"Invalid params_source: {params_source}")

    assert stmt.sql == sql
    assert stmt.parameters == expected_params


def test_sql_with_sqlglot_expression() -> None:
    """Test SQL initialization with SQLGlot Expression object."""
    expression = exp.Select().select(exp.Star()).from_("users")
    stmt = SQL(expression)

    assert stmt.expression == expression
    assert "SELECT * FROM USERS" in stmt.sql.upper()
    assert stmt.parameters in (None, {}, [])


def test_sql_wrapping_existing_sql() -> None:
    """Test SQL initialization by wrapping another SQL instance."""
    original_stmt = SQL("SELECT * FROM users WHERE id = ?", parameters=[123])

    # Wrap without changes
    wrapped_stmt = SQL(original_stmt)
    assert wrapped_stmt.sql == original_stmt.sql
    assert wrapped_stmt.parameters == original_stmt.parameters

    # Wrap with new parameters
    new_stmt = SQL(original_stmt, parameters=[456])
    assert new_stmt.sql == original_stmt.sql
    assert new_stmt.parameters == [456]
    assert original_stmt.parameters == [123]  # Original unchanged


@pytest.mark.parametrize(
    ("config_data", "should_have_expression"),
    [
        ({"enable_parsing": True}, True),
        ({"enable_parsing": False}, False),
    ],
    ids=["parsing_enabled", "parsing_disabled"],
)
def test_sql_parsing_configuration(config_data: dict[str, Any], should_have_expression: bool) -> None:
    """Test SQL initialization respects parsing configuration."""
    config = SQLConfig(**config_data)
    stmt = SQL("SELECT * FROM users", config=config)

    if should_have_expression:
        assert stmt.expression is not None
    else:
        assert stmt.expression is None


def test_parameter_precedence() -> None:
    """Test parameter precedence: parameters > kwargs."""
    sql = "SELECT * FROM users WHERE id = :id"
    stmt = SQL(sql, parameters={"id": 1}, id=3)

    assert stmt.parameters == {"id": 1}


@pytest.mark.parametrize(
    ("sql", "parameters", "kwargs", "expected_type"),
    [
        ("SELECT * FROM users WHERE id = ? AND name = :name", [123], {"name": "John"}, dict),
        ("SELECT * FROM users", [], {}, None),
    ],
    ids=["mixed_params", "no_params"],
)
def test_mixed_parameter_handling(sql: str, parameters: list[Any], kwargs: dict[str, Any], expected_type: Any) -> None:
    """Test handling of mixed parameter styles."""
    config = SQLConfig(enable_parsing=True)

    stmt = SQL(sql, parameters=parameters, config=config, **kwargs)
    if expected_type is None:
        assert stmt.parameters in (None, (), [])
    elif expected_type is dict:
        assert isinstance(stmt.parameters, dict)


@pytest.mark.parametrize(
    ("sql", "params", "expected_param_info_count"),
    [
        ("SELECT * FROM users", None, 0),
        ("SELECT * FROM users WHERE id = ?", [123], 1),
        ("SELECT * FROM users WHERE id = :id", {"id": 123}, 1),
        ("SELECT * FROM users WHERE id = ? AND name = ?", [123, "John"], 2),
        ("SELECT * FROM users WHERE id = :id AND name = :name", {"id": 123, "name": "John"}, 2),
    ],
    ids=["no_params", "single_positional", "single_named", "multiple_positional", "multiple_named"],
)
def test_parameter_info_extraction(sql: str, params: Any, expected_param_info_count: int) -> None:
    """Test parameter information extraction."""
    stmt = SQL(sql, parameters=params)

    assert len(stmt.parameter_info) == expected_param_info_count
    for param_info in stmt.parameter_info:
        assert isinstance(param_info, ParameterInfo)
        assert hasattr(param_info, "name")
        assert hasattr(param_info, "style")
        assert hasattr(param_info, "position")


def test_scalar_parameter_handling() -> None:
    """Test handling of scalar parameters."""
    sql = "SELECT * FROM users WHERE id = ?"
    stmt = SQL(sql, parameters=123)

    assert stmt.parameters == 123


@pytest.mark.parametrize(
    ("original_sql", "original_params", "target_style", "expected_pattern"),
    [
        ("SELECT * FROM users WHERE id = ?", [123], ParameterStyle.NAMED_COLON, r"id = :param_\d+"),
        ("SELECT * FROM users WHERE name = :name", {"name": "John"}, ParameterStyle.QMARK, "name = ?"),
        ("SELECT * FROM users WHERE id = ?", [123], ParameterStyle.PYFORMAT_NAMED, r"id = %\(param_\d+\)s"),
        (
            "SELECT * FROM users WHERE name = :name",
            {"name": "John"},
            ParameterStyle.PYFORMAT_POSITIONAL,
            "name = %s",
        ),
    ],
    ids=["qmark_to_named", "named_to_qmark", "qmark_to_pyformat_named", "named_to_pyformat_positional"],
)
def test_placeholder_style_conversion(
    original_sql: str, original_params: Any, target_style: ParameterStyle, expected_pattern: str
) -> None:
    """Test conversion between different placeholder styles."""
    stmt = SQL(original_sql, parameters=original_params)
    converted_sql = stmt.to_sql(placeholder_style=target_style)

    import re

    assert re.search(expected_pattern, converted_sql)


def test_static_placeholder_substitution() -> None:
    """Test static placeholder substitution with literal values."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ? AND active = ?"
    stmt = SQL(sql, parameters=[123, "John's Cafe", True])

    static_sql = stmt.to_sql(placeholder_style=ParameterStyle.STATIC)

    assert "123" in static_sql
    assert "'John''s Cafe'" in static_sql  # SQL escaping
    assert "TRUE" in static_sql.upper() or "1" in static_sql  # Boolean representation
    assert "?" not in static_sql


@pytest.mark.parametrize(
    ("include_separator", "expected_suffix"),
    [
        (True, ";"),
        (False, ""),
    ],
    ids=["with_separator", "without_separator"],
)
def test_statement_separator_handling(include_separator: bool, expected_suffix: str) -> None:
    """Test statement separator inclusion."""
    stmt = SQL("SELECT * FROM users")
    result_sql = stmt.to_sql(include_statement_separator=include_separator)

    if expected_suffix:
        assert result_sql.endswith(expected_suffix)
    else:
        assert not result_sql.endswith(";")


@pytest.fixture
def mock_validation_result() -> ValidationResult:
    """Create a mock validation result."""
    return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, issues=[])


@pytest.fixture
def unsafe_validation_result() -> ValidationResult:
    """Create an unsafe validation result."""
    return ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=["SQL injection detected"])


def test_validation_enabled_safe_sql(mock_validation_result: ValidationResult) -> None:
    """Test validation with safe SQL."""
    config = SQLConfig(enable_validation=True, strict_mode=True)

    # Mock the pipeline instead of the validator factory
    with patch.object(config, "get_pipeline") as mock_get_pipeline:
        mock_pipeline = Mock()
        from sqlglot import parse_one

        actual_expr = parse_one("SELECT * FROM users")
        mock_pipeline.execute.return_value = (actual_expr, mock_validation_result)
        mock_get_pipeline.return_value = mock_pipeline

        stmt = SQL("SELECT * FROM users", config=config)

        assert stmt.is_safe is True
        assert stmt.validation_result is not None
        assert stmt.validation_result.is_safe is True


def test_validation_enabled_unsafe_sql_strict_mode(unsafe_validation_result: ValidationResult) -> None:
    """Test validation with unsafe SQL in strict mode raises exception."""
    config = SQLConfig(enable_validation=True, strict_mode=True)

    # Mock the pipeline instead of the validator factory
    with patch.object(config, "get_pipeline") as mock_get_pipeline:
        mock_pipeline = Mock()
        from sqlglot import parse_one

        actual_expr = parse_one("DROP TABLE users")
        mock_pipeline.execute.return_value = (actual_expr, unsafe_validation_result)
        mock_get_pipeline.return_value = mock_pipeline

        with pytest.raises(SQLValidationError):
            SQL("DROP TABLE users", config=config)


def test_validation_enabled_unsafe_sql_non_strict_mode(unsafe_validation_result: ValidationResult) -> None:
    """Test validation with unsafe SQL in non-strict mode allows creation."""
    config = SQLConfig(enable_validation=True, strict_mode=False)

    # Mock the pipeline instead of the validator factory
    with patch.object(config, "get_pipeline") as mock_get_pipeline:
        mock_pipeline = Mock()
        from sqlglot import parse_one

        actual_expr = parse_one("DROP TABLE users")
        mock_pipeline.execute.return_value = (actual_expr, unsafe_validation_result)
        mock_get_pipeline.return_value = mock_pipeline

        stmt = SQL("DROP TABLE users", config=config)

        assert stmt.is_safe is False
        assert stmt.validation_result is not None
        assert stmt.validation_result.is_safe is False


def test_validation_disabled() -> None:
    """Test validation disabled skips validation checks."""
    config = SQLConfig(enable_validation=False)
    stmt = SQL("DROP TABLE users", config=config)

    # Should not validate anything
    assert stmt.validation_result is None


def test_validate_method() -> None:
    """Test explicit validate() method call."""
    stmt = SQL("SELECT * FROM users", config=SQLConfig(enable_validation=False))

    # Validation should work even if disabled in config
    result = stmt.validate()
    assert isinstance(result, ValidationResult)


def test_copy_with_new_sql() -> None:
    """Test copying SQL with new SQL statement."""
    original = SQL("SELECT * FROM users", parameters=[123])
    copied = original.copy(statement="SELECT * FROM products")

    assert original.sql == "SELECT * FROM users"
    assert copied.sql == "SELECT * FROM products"
    assert original is not copied


def test_copy_with_new_parameters() -> None:
    """Test copying SQL with new parameters."""
    original = SQL("SELECT * FROM users WHERE id = ?", parameters=[123])
    copied = original.copy(parameters=[456])

    assert original.parameters == [123]
    assert copied.parameters == [456]
    assert original is not copied


def test_copy_with_new_config() -> None:
    """Test copying SQL with new configuration."""
    original_config = SQLConfig(strict_mode=True)
    new_config = SQLConfig(strict_mode=False)

    original = SQL("SELECT * FROM users", config=original_config)
    copied = original.copy(config=new_config)

    assert original.config.strict_mode is True
    assert copied.config.strict_mode is False
    assert original is not copied


def test_copy_with_filters_via_append_filter() -> None:
    """Test copying SQL with additional filters via append_filter."""
    original = SQL("SELECT * FROM users")
    search_filter = SearchFilter("name", "john")

    # Use append_filter for adding filters, then copy if needed
    filtered = original.append_filter(search_filter)
    copied = filtered.copy()

    assert "LIKE" in copied.sql.upper() or "ILIKE" in copied.sql.upper()
    assert original.sql == "SELECT * FROM users"  # Original unchanged
    assert copied is not original  # Different instances


def test_immutable_modification_methods() -> None:
    """Test that modification methods return new instances."""
    original = SQL("SELECT * FROM users")

    # where() should return new instance
    with_where = original.where("active = true")
    assert original is not with_where
    assert "active" not in original.sql
    assert "active" in with_where.sql

    # limit() should return new instance
    with_limit = original.limit(10)
    assert original is not with_limit
    assert "LIMIT" not in original.sql.upper()
    assert "LIMIT" in with_limit.sql.upper()


def test_add_named_parameter_immutability() -> None:
    """Test add_named_parameter returns new instance."""
    original = SQL("SELECT * FROM users")
    with_param = original.add_named_parameter("user_id", 123)

    assert original is not with_param
    assert original.parameters in (None, {}, [])
    assert isinstance(with_param.parameters, dict)
    assert "user_id" in with_param.parameters


def test_append_filter() -> None:
    """Test append_filter method."""
    stmt = SQL("SELECT * FROM users")
    search_filter = SearchFilter("name", "john")

    filtered_stmt = stmt.append_filter(search_filter)

    assert stmt is not filtered_stmt  # New instance
    assert "name" in filtered_stmt.sql.lower()
    assert "like" in filtered_stmt.sql.lower() or "ilike" in filtered_stmt.sql.lower()


def test_multiple_filters() -> None:
    """Test applying multiple filters."""
    stmt = SQL("SELECT * FROM users")
    search_filter = SearchFilter("name", "john")
    limit_filter = LimitOffsetFilter(10, 0)

    filtered_stmt = stmt.append_filter(search_filter).append_filter(limit_filter)

    assert "name" in filtered_stmt.sql.lower()
    assert "limit" in filtered_stmt.sql.lower()


def test_filter_in_constructor() -> None:
    """Test filters applied during construction."""
    search_filter = SearchFilter("email", "test")
    stmt = SQL("SELECT * FROM users", search_filter)

    # Check that the filter was applied - should add WHERE clause with email condition
    assert "where" in stmt.sql.lower()
    assert "email" in stmt.sql.lower()
    assert "like" in stmt.sql.lower() or "ilike" in stmt.sql.lower()


def test_where_method() -> None:
    """Test where() method with conditions."""
    stmt = SQL("SELECT * FROM users")

    # String condition
    with_where = stmt.where("active = true")
    assert "active" in with_where.sql.lower()
    assert "true" in with_where.sql.lower()

    # Multiple conditions
    with_multiple = stmt.where("active = true", "department = 'Engineering'")
    assert "active" in with_multiple.sql.lower()
    assert "department" in with_multiple.sql.lower()


@pytest.mark.parametrize(
    ("limit_value", "use_parameter", "expected_pattern"),
    [
        (10, False, "LIMIT 10"),
        (50, True, r"LIMIT :limit_\d+"),
    ],
    ids=["literal_limit", "parameterized_limit"],
)
def test_limit_method(limit_value: int, use_parameter: bool, expected_pattern: str) -> None:
    """Test limit() method."""
    stmt = SQL("SELECT * FROM users")
    limited_stmt = stmt.limit(limit_value, use_parameter=use_parameter)

    if use_parameter:
        assert ":" in limited_stmt.sql  # Check that a parameter is used
        # Check that 'limit' is in one of the parameter names and has the correct value
        params = limited_stmt.parameters
        if isinstance(params, dict):
            assert any(k.startswith("limit") and v == limit_value for k, v in params.items()), (
                "Parameter for limit not found or has incorrect value in dict"
            )
        elif isinstance(params, (list, tuple)):
            # Assuming if it's a list/tuple, the order might matter or it's a single param
            # This might need adjustment based on how limit parameters are stored in lists
            assert limit_value in params, "Limit value not found in list/tuple parameters"
        elif params is None:
            pytest.fail("Parameters are None when use_parameter is True for limit")
        else:
            pytest.fail(f"Unexpected parameter type: {type(params)} for limit")
        # Ensure the LIMIT keyword is present
        assert "LIMIT" in limited_stmt.sql.upper()
    else:
        assert re.search(expected_pattern, limited_stmt.sql, re.IGNORECASE)


@pytest.mark.parametrize(
    ("offset_value", "use_parameter", "expected_pattern"),
    [
        (20, False, "OFFSET 20"),
        (100, True, r"OFFSET :offset_\d+"),
    ],
    ids=["literal_offset", "parameterized_offset"],
)
def test_offset_method(offset_value: int, use_parameter: bool, expected_pattern: str) -> None:
    """Test offset() method."""
    stmt = SQL("SELECT * FROM users")
    offset_stmt = stmt.offset(offset_value, use_parameter=use_parameter)

    if use_parameter:
        assert ":" in offset_stmt.sql  # Check that a parameter is used
        # Check that 'offset' is in one of the parameter names and has the correct value
        params = offset_stmt.parameters
        if isinstance(params, dict):
            assert any(k.startswith("offset") and v == offset_value for k, v in params.items()), (
                "Parameter for offset not found or has incorrect value in dict"
            )
        elif isinstance(params, (list, tuple)):
            # Similar assumption as in limit
            assert offset_value in params, "Offset value not found in list/tuple parameters"
        elif params is None:
            pytest.fail("Parameters are None when use_parameter is True for offset")
        else:
            pytest.fail(f"Unexpected parameter type: {type(params)} for offset")
        # Ensure the OFFSET keyword is present
        assert "OFFSET" in offset_stmt.sql.upper()
    else:
        assert re.search(expected_pattern, offset_stmt.sql, re.IGNORECASE)


def test_order_by_method() -> None:
    """Test order_by() method."""
    stmt = SQL("SELECT * FROM users")

    # Single order expression
    ordered_stmt = stmt.order_by("name ASC")
    assert "ORDER BY" in ordered_stmt.sql.upper()
    assert "name" in ordered_stmt.sql.lower()

    # Multiple order expressions
    multi_ordered = stmt.order_by("name ASC", "created_at DESC")
    assert "ORDER BY" in multi_ordered.sql.upper()
    assert "name" in multi_ordered.sql.lower()
    assert "created_at" in multi_ordered.sql.lower()


def test_get_unique_parameter_name() -> None:
    """Test get_unique_parameter_name method."""
    stmt = SQL("SELECT * FROM users WHERE name = :name", name="John")

    # Test unique name generation
    unique_name = stmt.get_unique_parameter_name("name")
    assert unique_name == "name_1"  # "name" is already taken

    unique_name2 = stmt.get_unique_parameter_name("user_id")
    assert unique_name2 == "user_id"  # "user_id" is available


def test_get_parameters_method() -> None:
    """Test get_parameters() method with different styles."""
    stmt = SQL("SELECT * FROM users WHERE id = :id", id=123)

    # Default style
    params = stmt.get_parameters()
    assert params == {"id": 123}

    # Dict style
    dict_params = stmt.get_parameters("dict")
    assert dict_params == {"id": 123}

    # List style (for named params, returns values)
    list_params = stmt.get_parameters("list")
    assert isinstance(list_params, list)


def test_to_expression_static_method() -> None:
    """Test to_expression static method."""
    # String input
    expr_from_str = SQL.to_expression("SELECT * FROM users")
    assert isinstance(expr_from_str, exp.Expression)

    # Expression input (should return same)
    original_expr = exp.Select().select(exp.Star()).from_("users")
    expr_from_expr = SQL.to_expression(original_expr)
    assert expr_from_expr is original_expr

    # SQL instance input
    stmt = SQL("SELECT * FROM products")
    expr_from_sql = SQL.to_expression(stmt)
    assert isinstance(expr_from_sql, exp.Expression)


def test_str_and_repr_methods() -> None:
    """Test __str__ and __repr__ methods."""
    stmt = SQL("SELECT * FROM users", parameters=[123])

    # __str__ should return SQL
    str_repr = str(stmt)
    assert "SELECT" in str_repr.upper()

    # __repr__ should be detailed
    repr_str = repr(stmt)
    assert "SQL" in repr_str
    assert "statement=" in repr_str


def test_equality_and_hashing() -> None:
    """Test __eq__ and __hash__ methods."""
    stmt1 = SQL("SELECT * FROM users", parameters=[123])
    stmt2 = SQL("SELECT * FROM users", parameters=[123])
    stmt3 = SQL("SELECT * FROM products", parameters=[123])

    # Equality
    assert stmt1 == stmt2
    assert stmt1 != stmt3
    assert stmt1 != "SELECT * FROM users"  # Different type

    # Hashing (for use in sets/dicts)
    stmt_set = {stmt1, stmt2, stmt3}
    assert len(stmt_set) == 2  # stmt1 and stmt2 should be considered equal


def test_invalid_sql_parsing() -> None:
    """Test handling of invalid SQL."""
    with pytest.raises(SQLValidationError):
        SQL("INVALID SQL SYNTAX HERE", config=SQLConfig(strict_mode=True))


def test_empty_sql_handling() -> None:
    """Test handling of empty SQL."""
    stmt = SQL("")
    assert stmt.expression is not None  # Should create empty SELECT
    assert "SELECT" in stmt.sql.upper()


def test_none_sql_handling() -> None:
    """Test handling of None SQL input."""
    # None SQL is handled gracefully by converting to string "None"
    stmt = SQL(None)  # type: ignore[arg-type]
    assert stmt.sql == "None"
    assert stmt.is_safe is True  # Should be considered safe

    # Empty string should be handled gracefully by creating an empty SELECT
    stmt2 = SQL("")
    assert "SELECT" in stmt2.sql.upper()


def test_dialect_configuration() -> None:
    """Test dialect configuration."""
    stmt = SQL("SELECT * FROM users", dialect="postgres")

    assert stmt.dialect == "postgres"
    assert stmt.expression is not None

    # Test dialect affects SQL generation
    sql_output = stmt.to_sql()
    assert "SELECT" in sql_output.upper()


def test_transform_method() -> None:
    """Test transform() method."""
    stmt = SQL("SELECT * FROM users")
    transformed = stmt.transform()

    # Should return new instance
    assert stmt is not transformed
    # Content might be same if no transformations configured
    assert isinstance(transformed, SQL)


def test_property_access() -> None:
    """Test all property accessors."""
    config = SQLConfig(enable_validation=True, enable_analysis=True)
    stmt = SQL("SELECT * FROM users WHERE id = :id", config=config, dialect="postgres", id=123)

    # Test all properties
    assert stmt.sql is not None
    assert stmt.dialect == "postgres"
    assert stmt.config == config
    assert stmt.expression is not None
    assert stmt.parameters == {"id": 123}
    assert isinstance(stmt.parameter_info, list)
    assert stmt.validation_result is not None
    assert isinstance(stmt.is_safe, bool)
    assert stmt.expected_result_type is None  # No builder used


def test_validation_result_property() -> None:
    """Test validation_result property."""
    # With validation enabled
    stmt_with_validation = SQL("SELECT * FROM users", config=SQLConfig(enable_validation=True))
    assert stmt_with_validation.validation_result is not None

    # With validation disabled
    stmt_without_validation = SQL("SELECT * FROM users", config=SQLConfig(enable_validation=False))
    assert stmt_without_validation.validation_result is None


def test_mixed_parameter_styles_in_sql_string() -> None:
    """Test handling of mixed parameter styles (? and :name) in the input SQL string."""
    config = SQLConfig(enable_parsing=True)

    # Test mixed parameter style
    stmt = SQL("SELECT * FROM test WHERE id = ? AND name = :name", parameters=[1], config=config, name="Test")

    # Check that both types of placeholders were present in the input string
    assert "?" in "SELECT * FROM test WHERE id = ? AND name = :name"
    assert ":" in "SELECT * FROM test WHERE id = ? AND name = :name"

    # Verify parameter merging - should be a dict with both positional and named params
    merged_params = stmt.parameters
    assert merged_params is not None
    assert isinstance(merged_params, dict)
    assert "name" in merged_params
    assert merged_params["name"] == "Test"
    # Positional parameter should be converted to _arg_0
    assert "_arg_0" in merged_params
    assert merged_params["_arg_0"] == 1
