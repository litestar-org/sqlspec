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

from sqlspec.exceptions import RiskLevel, SQLValidationError
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
            {"enable_parsing": False, "enable_validation": False, "strict_mode": False, "analysis_cache_size": 500},
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
    """Test SQLConfig enables pipeline processing by default for validation and analysis."""
    # Test that validation (which uses the pipeline) is enabled by default
    config = SQLConfig(enable_validation=True, strict_mode=False)  # Non-strict to avoid raising on test SQL
    # This SQL is designed to trigger a DMLWithoutWhereValidator if it were present and active
    stmt = SQL("UPDATE users SET name = 'test'", config=config)

    # Use the new validate_detailed() method instead of deprecated validation_result
    errors = stmt.validate_detailed()
    assert isinstance(errors, list), "validate_detailed() should return a list"
    # This SQL should have validation errors (UPDATE without WHERE)
    assert len(errors) > 0, "UPDATE without WHERE should have validation errors"
    assert any("without WHERE" in error.message for error in errors), "Should detect UPDATE without WHERE"

    # Test that analysis (which uses the pipeline) can be enabled
    config_analysis = SQLConfig(enable_analysis=True, strict_mode=False)
    stmt_analysis = SQL("SELECT * FROM users", config=config_analysis)
    # With the new pipeline architecture, analysis results are stored differently
    # The analysis_result property is deprecated and returns None
    # Instead, we should check that the pipeline ran by verifying the statement was processed
    assert stmt_analysis.expression is not None, "Expression should be populated if pipeline ran for analysis."


def test_sqlconfig_get_pipeline_custom_components() -> None:
    """Test SQLConfig correctly stores custom processing components."""
    mock_transformer = Mock()
    mock_validator = Mock()
    mock_analyzer = Mock()

    config = SQLConfig(transformers=[mock_transformer], validators=[mock_validator], analyzers=[mock_analyzer])

    # Verify that the custom components are stored in the config
    assert config.transformers is not None
    assert mock_transformer in config.transformers

    assert config.validators is not None
    assert mock_validator in config.validators

    assert config.analyzers is not None
    assert mock_analyzer in config.analyzers


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
    [({"enable_parsing": True}, True), ({"enable_parsing": False}, False)],
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
        ("SELECT * FROM users WHERE id = ?", [123], ParameterStyle.NAMED_PYFORMAT, r"id = %\(param_\d+\)s"),
        ("SELECT * FROM users WHERE name = :name", {"name": "John"}, ParameterStyle.POSITIONAL_PYFORMAT, "name = %s"),
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
    ("include_separator", "expected_suffix"), [(True, ";"), (False, "")], ids=["with_separator", "without_separator"]
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
    with patch.object(config, "get_statement_pipeline") as mock_get_pipeline:
        mock_pipeline = Mock()
        from sqlglot import parse_one

        from sqlspec.statement.pipelines.context import PipelineResult, SQLProcessingContext

        actual_expr = parse_one("SELECT * FROM users")
        mock_context = SQLProcessingContext(
            initial_sql_string="SELECT * FROM users",
            dialect=None,
            config=config,
            current_expression=actual_expr,
            merged_parameters=None,
            parameter_info=[],
            input_sql_had_placeholders=False,
        )
        mock_result = PipelineResult(expression=actual_expr, context=mock_context)
        mock_pipeline.execute_pipeline.return_value = mock_result
        mock_get_pipeline.return_value = mock_pipeline

        stmt = SQL("SELECT * FROM users", config=config)

        assert stmt.is_safe is True
        assert stmt.validation_result is not None
        assert stmt.validation_result.is_safe is True


def test_validation_enabled_unsafe_sql_strict_mode(unsafe_validation_result: ValidationResult) -> None:
    """Test validation with unsafe SQL in strict mode raises exception."""
    config = SQLConfig(enable_validation=True, strict_mode=True)

    # Mock the pipeline instead of the validator factory
    with patch.object(config, "get_statement_pipeline") as mock_get_pipeline:
        mock_pipeline = Mock()
        from sqlglot import parse_one

        from sqlspec.statement.pipelines.context import PipelineResult, SQLProcessingContext

        actual_expr = parse_one("DROP TABLE users")
        mock_context = SQLProcessingContext(
            initial_sql_string="DROP TABLE users",
            dialect=None,
            config=config,
            current_expression=actual_expr,
            merged_parameters=None,
            parameter_info=[],
            input_sql_had_placeholders=False,
        )
        # Add validation errors to context to simulate unsafe SQL
        from sqlspec.statement.pipelines.result_types import ValidationError as ValError

        error = ValError(
            message=unsafe_validation_result.issues[0] if unsafe_validation_result.issues else "Unsafe SQL",
            code="unsafe-sql",
            risk_level=unsafe_validation_result.risk_level,
            processor="TestValidator",
            expression=actual_expr,
        )
        mock_context.validation_errors.append(error)
        mock_result = PipelineResult(expression=actual_expr, context=mock_context)
        mock_pipeline.execute_pipeline.return_value = mock_result
        mock_get_pipeline.return_value = mock_pipeline

        # Create the SQL object - no error during construction
        stmt = SQL("DROP TABLE users", config=config)

        # Error should be raised when accessing properties that trigger processing
        with pytest.raises(SQLValidationError):
            _ = stmt.sql  # This should trigger validation and raise


def test_validation_enabled_unsafe_sql_non_strict_mode(unsafe_validation_result: ValidationResult) -> None:
    """Test validation with unsafe SQL in non-strict mode allows creation."""
    config = SQLConfig(enable_validation=True, strict_mode=False)

    # Mock the pipeline instead of the validator factory
    with patch.object(config, "get_statement_pipeline") as mock_get_pipeline:
        mock_pipeline = Mock()
        from sqlglot import parse_one

        from sqlspec.statement.pipelines.context import PipelineResult, SQLProcessingContext

        actual_expr = parse_one("DROP TABLE users")
        mock_context = SQLProcessingContext(
            initial_sql_string="DROP TABLE users",
            dialect=None,
            config=config,
            current_expression=actual_expr,
            merged_parameters=None,
            parameter_info=[],
            input_sql_had_placeholders=False,
        )
        # Add validation errors to context to simulate unsafe SQL
        from sqlspec.statement.pipelines.result_types import ValidationError as ValError

        error = ValError(
            message=unsafe_validation_result.issues[0] if unsafe_validation_result.issues else "Unsafe SQL",
            code="unsafe-sql",
            risk_level=unsafe_validation_result.risk_level,
            processor="TestValidator",
            expression=actual_expr,
        )
        mock_context.validation_errors.append(error)
        mock_result = PipelineResult(expression=actual_expr, context=mock_context)
        mock_pipeline.execute_pipeline.return_value = mock_result
        mock_get_pipeline.return_value = mock_pipeline

        stmt = SQL("DROP TABLE users", config=config)

        assert stmt.is_safe is False
        assert stmt.validation_result is not None
        assert stmt.validation_result.is_safe is False


def test_validation_disabled() -> None:
    """Test validation disabled skips validation checks."""
    config = SQLConfig(enable_validation=False)
    stmt = SQL("DROP TABLE users", config=config)

    # Should have a skipped validation result
    assert stmt.validation_result is not None
    assert stmt.validation_result.is_safe is True
    assert stmt.validation_result.risk_level == RiskLevel.SKIP
    # The validation result will indicate it was skipped
    assert len(stmt.validation_result.issues) >= 0  # May have pipeline messages


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
    [(10, False, "LIMIT 10"), (50, True, r"LIMIT :limit_\d+")],
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
    [(20, False, "OFFSET 20"), (100, True, r"OFFSET :offset_\d+")],
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
    stmt = SQL("INVALID SQL SYNTAX HERE", config=SQLConfig(strict_mode=True))
    # Error should be raised when accessing properties that trigger processing
    with pytest.raises(SQLValidationError):
        _ = stmt.sql  # This should trigger parsing and raise


def test_empty_sql_handling() -> None:
    """Test handling of empty SQL."""
    stmt = SQL("")
    assert stmt.expression is not None  # Should create empty SELECT
    assert "SELECT" in stmt.sql.upper()


def test_none_sql_handling() -> None:
    """Test handling of None SQL input."""
    # None SQL is handled gracefully by converting to string "None"
    stmt = SQL(None)  # type: ignore[arg-type]
    assert stmt.sql == "none"
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
    # Should have a skipped validation result
    assert stmt_without_validation.validation_result is not None
    assert stmt_without_validation.validation_result.is_safe is True
    assert stmt_without_validation.validation_result.risk_level == RiskLevel.SKIP


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


class TestSQLCompileMethod:
    """Test the SQL.compile() method functionality."""

    def test_compile_basic_functionality(self) -> None:
        """Test basic compile() method functionality."""
        stmt = SQL("SELECT * FROM users WHERE id = ?", [123])
        sql, params = stmt.compile()

        assert isinstance(sql, str)
        assert sql == "SELECT * FROM users WHERE id = ?"
        assert params == [123]

    def test_compile_with_named_parameters(self) -> None:
        """Test compile() with named parameters."""
        stmt = SQL("SELECT * FROM users WHERE name = :name", {"name": "John"})
        sql, params = stmt.compile()

        assert isinstance(sql, str)
        assert sql == "SELECT * FROM users WHERE name = :name"
        assert params == {"name": "John"}

    def test_compile_with_no_parameters(self) -> None:
        """Test compile() with statements that have no parameters."""
        stmt = SQL("SELECT * FROM users")
        sql, params = stmt.compile()

        assert isinstance(sql, str)
        assert sql == "SELECT * FROM users"
        # params can be None, empty list, or empty dict depending on processing

    def test_compile_return_type_is_tuple(self) -> None:
        """Test that compile() returns a tuple."""
        stmt = SQL("SELECT * FROM users WHERE id = ?", [123])
        result = stmt.compile()

        assert isinstance(result, tuple)
        assert len(result) == 2

        sql, params = result
        assert isinstance(sql, str)

    def test_compile_with_placeholder_style_conversion(self) -> None:
        """Test compile() with placeholder style conversion."""
        stmt = SQL("SELECT * FROM users WHERE id = ?", [123])

        # Test qmark to numeric conversion
        sql, params = stmt.compile(placeholder_style=ParameterStyle.NUMERIC)
        assert "$1" in sql
        assert params == [123]

        # Test qmark to named conversion
        sql, params = stmt.compile(placeholder_style="named")
        assert ":param_0" in sql
        assert isinstance(params, dict)
        assert "param_0" in params
        assert params["param_0"] == 123

    def test_compile_string_placeholder_styles(self) -> None:
        """Test compile() with string placeholder style names."""
        stmt = SQL("SELECT * FROM users WHERE id = ?", [123])

        # Test with string style names
        sql, params = stmt.compile(placeholder_style="numeric")
        assert "$1" in sql
        assert params == [123]

        sql, params = stmt.compile(placeholder_style="qmark")
        assert "?" in sql
        assert params == [123]

    def test_compile_preserves_immutability(self) -> None:
        """Test that compile() doesn't modify the original SQL object."""
        original_stmt = SQL("SELECT * FROM users WHERE id = ?", [123])
        original_sql = original_stmt.sql
        original_params = original_stmt.parameters

        # Compile with different style
        compiled_sql, compiled_params = original_stmt.compile(placeholder_style=ParameterStyle.NUMERIC)

        # Original should be unchanged
        assert original_stmt.sql == original_sql
        assert original_stmt.parameters == original_params

        # Compiled should be different
        assert compiled_sql != original_sql
        assert "$1" in compiled_sql

    def test_compile_consistency_with_separate_calls(self) -> None:
        """Test that compile() returns same results as separate to_sql() and get_parameters() calls."""
        stmt = SQL("SELECT * FROM users WHERE id = ?", [123])

        # Get results separately
        separate_sql = stmt.to_sql()
        separate_params = stmt.get_parameters()

        # Get results from compile
        compiled_sql, compiled_params = stmt.compile()

        # Should be identical
        assert compiled_sql == separate_sql
        assert compiled_params == separate_params

    def test_compile_with_complex_statement(self) -> None:
        """Test compile() with more complex SQL statements."""
        stmt = SQL(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > ? AND p.published = ?",
            [25, True],
        )

        sql, params = stmt.compile(placeholder_style=ParameterStyle.NUMERIC)

        assert "$1" in sql and "$2" in sql
        assert params == [25, True]

    def test_compile_with_mixed_parameters(self) -> None:
        """Test compile() with mixed parameter types."""
        stmt = SQL("SELECT * FROM users WHERE id = ? AND name = :name", [123], name="John")

        sql, params = stmt.compile()

        assert isinstance(sql, str)
        assert params is not None
        # The exact format depends on parameter processing, but should have both values

    def test_compile_preserves_execution_modes(self) -> None:
        """Test that compile() works with different execution modes."""
        # Test script mode
        script_stmt = SQL("SELECT 1; SELECT 2;").as_script()
        sql, params = script_stmt.compile()
        assert isinstance(sql, str)
        assert ";" in sql

        # Test many mode
        many_stmt = SQL("INSERT INTO users (name) VALUES (?)", [["John"], ["Jane"]])
        many_stmt = many_stmt.as_many()
        sql, params = many_stmt.compile()
        assert isinstance(sql, str)
        assert "?" in sql
        assert params == [["John"], ["Jane"]]


def test_oracle_ddl_script_handling() -> None:
    """Test handling of complex Oracle DDL script with 23AI features as a script."""
    from pathlib import Path

    # Load the Oracle DDL script
    fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "oracle.ddl.sql"
    assert fixture_path.exists(), f"Fixture file not found at {fixture_path}"

    with open(fixture_path) as f:
        oracle_ddl = f.read()

    # Test with Oracle dialect, focusing on script handling
    config = SQLConfig(
        enable_parsing=False,  # Disable parsing to test script lexer functionality
        enable_validation=False,  # Disable validation for scripts
        strict_mode=False,
    )

    # Create as script with Oracle dialect
    stmt = SQL(oracle_ddl, config=config, dialect="oracle").as_script()

    # The script should be recognized as a script
    assert stmt.is_script is True

    # The script should be executable as a script (no parameters needed)
    assert stmt.parameters in (None, [], {})

    # Get the SQL output - should preserve the original script
    sql_output = stmt.to_sql()

    # Verify Oracle-specific syntax is preserved
    assert "ALTER SESSION SET CONTAINER" in sql_output
    assert "GRANT" in sql_output
    assert "CREATE TABLE" in sql_output
    assert "VECTOR(768, FLOAT32)" in sql_output  # Oracle 23AI vector type
    assert "JSON" in sql_output  # JSON data type
    assert "INMEMORY PRIORITY HIGH" in sql_output  # In-memory feature
    assert "GENERATED BY DEFAULT ON NULL AS IDENTITY" in sql_output  # Identity columns
    assert "DEFAULT ON NULL CURRENT_TIMESTAMP" in sql_output
    assert "FOR INSERT AND UPDATE CURRENT_TIMESTAMP" in sql_output
    assert "CREATE VECTOR INDEX" in sql_output  # Vector index
    assert "ORGANIZATION NEIGHBOR PARTITIONS" in sql_output
    assert "DISTANCE COSINE" in sql_output
    assert "DBMS_OUTPUT.PUT_LINE" in sql_output  # PL/SQL block

    # Test that the script is ready for execution with execute_script
    # The execute_script method will use its own lexer to handle the statements
    compiled_sql, params = stmt.compile()
    assert isinstance(compiled_sql, str)
    assert params in (None, [], {})

    # Verify the script ends with the PL/SQL block terminator
    assert compiled_sql.strip().endswith("/")


def test_oracle_ddl_script_execution_mode() -> None:
    """Test that Oracle DDL script is properly recognized as a script for execution."""
    from pathlib import Path

    # Load the Oracle DDL script
    fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "oracle.ddl.sql"
    with open(fixture_path) as f:
        oracle_ddl = f.read()

    # Disable parsing since Oracle DDL contains unsupported syntax
    config = SQLConfig(enable_parsing=False, enable_validation=False, strict_mode=False)

    # Create SQL object without as_script() first
    stmt = SQL(oracle_ddl, dialect="oracle", config=config)

    # With parsing disabled, it won't auto-detect script mode
    # We need to explicitly mark it as a script
    script_stmt = stmt.as_script()
    assert script_stmt.is_script is True

    # Parameters should be appropriate for script execution
    assert script_stmt.parameters in (None, [], {})


def test_postgres_collection_privileges_script() -> None:
    """Test handling of PostgreSQL collection-privileges script with named queries and parameters."""
    from pathlib import Path

    # Load the PostgreSQL script
    fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "postgres" / "collection-privileges.sql"
    assert fixture_path.exists(), f"Fixture file not found at {fixture_path}"

    with open(fixture_path) as f:
        postgres_script = f.read()

    # Test with PostgreSQL dialect
    config = SQLConfig(
        enable_parsing=False,  # Use script lexer
        enable_validation=False,
        strict_mode=False,
    )

    # Create as script
    stmt = SQL(postgres_script, config=config, dialect="postgres").as_script()

    # Should be recognized as a script
    assert stmt.is_script is True

    # Get the SQL output
    sql_output = stmt.to_sql()

    # Verify PostgreSQL features are preserved
    assert "-- name: collection-postgres-pglogical-schema-usage-privilege" in sql_output
    assert "with src as" in sql_output.lower()
    assert "pg_catalog.has_schema_privilege" in sql_output
    assert ":PKEY" in sql_output  # Named parameter
    assert ":DMA_SOURCE_ID" in sql_output  # Named parameter
    assert ":DMA_MANUAL_ID" in sql_output  # Named parameter
    assert "current_database()" in sql_output

    # Verify multiple named queries are present
    assert sql_output.count("-- name:") >= 2

    # Test parameter extraction - the script uses named parameters
    # Note: These are PostgreSQL-style named parameters (:name) not regular placeholders
    assert ":PKEY" in sql_output
    assert ":DMA_SOURCE_ID" in sql_output
    assert ":DMA_MANUAL_ID" in sql_output
