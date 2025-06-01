"""Unit tests for sqlspec.statement.sql module.

Tests the core SQL statement functionality including SQL class, SQLConfig,
immutable statement operations, parameter handling, validation, and filtering.
"""

from dataclasses import replace
from typing import Any
from unittest.mock import Mock, patch

import pytest
from sqlglot import exp

from sqlspec.exceptions import (
    ParameterError,
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
                "allow_mixed_parameters": False,
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
                "allow_mixed_parameters": False,  # Unchanged default
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
        ("   SELECT id FROM products   ", "   SELECT id FROM products   "),  # Whitespace preserved
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
        ("SELECT * FROM users WHERE id = ?", "args", [123], [123]),
        ("SELECT * FROM users WHERE name = :name", "kwargs", {"name": "John"}, {"name": "John"}),
        ("SELECT * FROM users WHERE id = :id", "parameters", {"id": 456}, {"id": 456}),
        ("SELECT * FROM users WHERE id = ? AND active = ?", "args", [789, True], [789, True]),
        (
            "SELECT * FROM users WHERE name = :name AND dept = :dept",
            "kwargs",
            {"name": "Alice", "dept": "Engineering"},
            {"name": "Alice", "dept": "Engineering"},
        ),
    ],
    ids=["positional_args", "named_kwargs", "explicit_parameters", "multiple_positional", "multiple_named"],
)
def test_sql_with_parameters(sql: str, params_source: str, params_value: Any, expected_params: Any) -> None:
    """Test SQL initialization with various parameter types."""
    if params_source == "args":
        stmt = SQL(sql, args=params_value)
    elif params_source == "kwargs":
        stmt = SQL(sql, kwargs=params_value)
    elif params_source == "parameters":
        stmt = SQL(sql, parameters=params_value)
    else:
        pytest.fail(f"Invalid params_source: {params_source}")

    assert stmt.sql == sql
    assert stmt.parameters == expected_params


def test_sql_with_sqlglot_expression() -> None:
    """Test SQL initialization with SQLGlot Expression object."""
    expression = exp.Select().select(exp.Star()).from_("users")
    stmt = SQL(expression)

    assert stmt.expression == expression
    assert "SELECT * FROM users" in stmt.sql.upper()
    assert stmt.parameters in (None, {}, [])


def test_sql_wrapping_existing_sql() -> None:
    """Test SQL initialization by wrapping another SQL instance."""
    original_stmt = SQL("SELECT * FROM users WHERE id = ?", args=[123])

    # Wrap without changes
    wrapped_stmt = SQL(original_stmt)
    assert wrapped_stmt.sql == original_stmt.sql
    assert wrapped_stmt.parameters == original_stmt.parameters

    # Wrap with new parameters
    new_stmt = SQL(original_stmt, args=[456])
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
    """Test parameter precedence: parameters > kwargs > args."""
    sql = "SELECT * FROM users WHERE id = :id"
    stmt = SQL(sql, parameters={"id": 1}, args=[2], kwargs={"id": 3})

    assert stmt.parameters == {"id": 1}


@pytest.mark.parametrize(
    ("sql", "args", "kwargs", "expected_type", "allow_mixed"),
    [
        ("SELECT * FROM users WHERE id = ? AND name = :name", [123], {"name": "John"}, dict, True),
        ("SELECT * FROM users WHERE id = ? AND name = :name", [123], {"name": "John"}, ParameterError, False),
        ("SELECT * FROM users", [], {}, None, True),
        ("SELECT * FROM users", [], {}, None, False),
    ],
    ids=["mixed_allowed", "mixed_not_allowed", "no_params_allowed", "no_params_not_allowed"],
)
def test_mixed_parameter_handling(
    sql: str, args: list[Any], kwargs: dict[str, Any], expected_type: Any, allow_mixed: bool
) -> None:
    """Test handling of mixed parameter styles."""
    config = SQLConfig(allow_mixed_parameters=allow_mixed, enable_parsing=False)

    if expected_type == ParameterError:
        with pytest.raises(ParameterError, match="Cannot mix args and kwargs"):
            SQL(sql, args=args, kwargs=kwargs, config=config)
    else:
        stmt = SQL(sql, args=args, kwargs=kwargs, config=config)
        if expected_type is None:
            assert stmt.parameters in (None, (), [])
        elif expected_type is dict:
            assert isinstance(stmt.parameters, (dict, tuple))


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
    stmt = SQL(sql, args=[123, "John's Cafe", True])

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

    with patch("sqlspec.statement.sql._default_validator") as mock_validator_factory:
        mock_validator = Mock()
        mock_validator.validate.return_value = mock_validation_result
        mock_validator.min_risk_to_raise = RiskLevel.HIGH
        mock_validator_factory.return_value = mock_validator

        stmt = SQL("SELECT * FROM users", config=config)

        assert stmt.is_safe is True
        assert stmt.validation_result is not None
        assert stmt.validation_result.is_safe is True


def test_validation_enabled_unsafe_sql_strict_mode(unsafe_validation_result: ValidationResult) -> None:
    """Test validation with unsafe SQL in strict mode raises exception."""
    config = SQLConfig(enable_validation=True, strict_mode=True)

    with patch("sqlspec.statement.sql._default_validator") as mock_validator_factory:
        mock_validator = Mock()
        mock_validator.validate.return_value = unsafe_validation_result
        mock_validator.min_risk_to_raise = RiskLevel.HIGH
        mock_validator_factory.return_value = mock_validator

        with pytest.raises(SQLValidationError):
            SQL("DROP TABLE users", config=config)


def test_validation_enabled_unsafe_sql_non_strict_mode(unsafe_validation_result: ValidationResult) -> None:
    """Test validation with unsafe SQL in non-strict mode allows creation."""
    config = SQLConfig(enable_validation=True, strict_mode=False)

    with patch("sqlspec.statement.sql._default_validator") as mock_validator_factory:
        mock_validator = Mock()
        mock_validator.validate.return_value = unsafe_validation_result
        mock_validator.min_risk_to_raise = RiskLevel.HIGH
        mock_validator_factory.return_value = mock_validator

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
    original = SQL("SELECT * FROM users", args=[123])
    copied = original.copy(statement="SELECT * FROM products")

    assert original.sql == "SELECT * FROM users"
    assert copied.sql == "SELECT * FROM products"
    assert original is not copied


def test_copy_with_new_parameters() -> None:
    """Test copying SQL with new parameters."""
    original = SQL("SELECT * FROM users WHERE id = ?", args=[123])
    copied = original.copy(args=[456])

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

    assert "email" in stmt.sql.lower()
    assert "like" in stmt.sql.lower() or "ilike" in stmt.sql.lower()


def test_where_method() -> None:
    """Test where() method with conditions."""
    stmt = SQL("SELECT * FROM users")

    # String condition
    with_where = stmt.where("active = true")
    assert "active = true" in with_where.sql

    # Multiple conditions
    with_multiple = stmt.where("active = true", "department = 'Engineering'")
    assert "active = true" in with_multiple.sql
    assert "department = 'Engineering'" in with_multiple.sql


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

    import re

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

    import re

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
    stmt = SQL("SELECT * FROM users WHERE name = :name", kwargs={"name": "John"})

    unique_name_1 = stmt.get_unique_parameter_name("search")
    unique_name_2 = stmt.get_unique_parameter_name("search")

    assert unique_name_1 != unique_name_2  # Should be unique
    assert "search" in unique_name_1
    assert "search" in unique_name_2


def test_get_parameters_method() -> None:
    """Test get_parameters() method with different styles."""
    stmt = SQL("SELECT * FROM users WHERE id = :id", kwargs={"id": 123})

    # Default style
    params = stmt.get_parameters()
    assert params == {"id": 123}

    # Specific style
    params_named = stmt.get_parameters(style=ParameterStyle.NAMED_COLON)
    assert isinstance(params_named, dict)


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
    stmt = SQL("SELECT * FROM users", args=[123])

    # __str__ should return SQL
    str_repr = str(stmt)
    assert "SELECT" in str_repr.upper()

    # __repr__ should be detailed
    repr_str = repr(stmt)
    assert "SQL" in repr_str
    assert "sql=" in repr_str


def test_equality_and_hashing() -> None:
    """Test __eq__ and __hash__ methods."""
    stmt1 = SQL("SELECT * FROM users", args=[123])
    stmt2 = SQL("SELECT * FROM users", args=[123])
    stmt3 = SQL("SELECT * FROM products", args=[123])

    # Equality
    assert stmt1 == stmt2
    assert stmt1 != stmt3
    assert stmt1 != "SELECT * FROM users"  # Different type

    # Hashing (for use in sets/dicts)
    stmt_set = {stmt1, stmt2, stmt3}
    assert len(stmt_set) == 2  # stmt1 and stmt2 should be same hash


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
    # This should raise an error or be handled gracefully
    with pytest.raises((ValueError, TypeError, AttributeError)):
        SQL(None)  # type: ignore[arg-type]


def test_dialect_configuration() -> None:
    """Test dialect configuration."""
    stmt = SQL("SELECT * FROM users", dialect="postgresql")
    assert stmt.dialect == "postgresql"

    # Test dialect setter
    stmt.dialect = "mysql"
    assert stmt.dialect == "mysql"


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
    stmt = SQL("SELECT * FROM users WHERE id = :id", config=config, dialect="postgresql", id=123)

    # Basic properties
    assert stmt.sql == "SELECT * FROM users WHERE id = :id"
    assert stmt.dialect == "postgresql"
    assert stmt.config == config
    assert stmt.parameters == {"id": 123}
    assert isinstance(stmt.parameter_info, list)

    # State properties
    assert isinstance(stmt.is_safe, bool)
    assert stmt.expected_result_type is None  # No builder result type set

    # Expression property
    assert stmt.expression is not None
    assert isinstance(stmt.expression, exp.Expression)


def test_validation_result_property() -> None:
    """Test validation_result property."""
    # With validation enabled
    stmt_with_validation = SQL("SELECT * FROM users", config=SQLConfig(enable_validation=True))
    assert stmt_with_validation.validation_result is not None

    # With validation disabled
    stmt_without_validation = SQL("SELECT * FROM users", config=SQLConfig(enable_validation=False))
    assert stmt_without_validation.validation_result is None
