"""Unit tests for Parameter Style Validator.

This module tests the Parameter Style validator including:
- Parameter style validation (qmark, named_colon, numeric, etc.)
- Mixed parameter style detection
- Allowed/disallowed parameter style enforcement
- Complex query parameter extraction
- Target style suggestions
- Configuration handling
"""

from typing import TYPE_CHECKING

import pytest
from sqlglot import parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.parameters import ParameterValidator
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators._parameter_style import ParameterStyleValidator
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# Test Data
@pytest.fixture
def context() -> SQLProcessingContext:
    """Create a processing context."""
    return SQLProcessingContext(initial_sql_string="SELECT 1", dialect=None, config=SQLConfig())


@pytest.fixture
def param_validator() -> ParameterValidator:
    """Create a parameter validator for extracting parameters."""
    return ParameterValidator()


def create_validator(fail_on_violation: bool = False) -> ParameterStyleValidator:
    """Create a parameter style validator instance."""
    return ParameterStyleValidator(fail_on_violation=fail_on_violation)


def setup_test_context(
    context: SQLProcessingContext, sql: str, param_validator: ParameterValidator, provide_dummy_params: bool = True
) -> None:
    """Setup test context with SQL and optional dummy parameters."""
    # Set up basic context
    context.initial_sql_string = sql
    context.current_expression = parse_one(sql)
    context.parameter_info = param_validator.extract_parameters(sql)

    if provide_dummy_params and context.parameter_info:
        # Provide dummy parameters to avoid missing parameter errors
        # These tests are about style validation, not parameter presence
        if any(p.style.value in {"named_colon", "named_at"} for p in context.parameter_info):
            # Named parameters
            context.merged_parameters = {p.name: f"dummy_{p.name}" for p in context.parameter_info}
        else:
            # Positional parameters
            context.merged_parameters = ["dummy_value"] * len(context.parameter_info)


# Parameter Style Detection Tests
@pytest.mark.parametrize(
    "sql,allowed_styles,should_pass,expected_risk",
    [
        ("SELECT * FROM users WHERE id = ?", ("qmark", "named_colon"), True, None),
        ("SELECT * FROM users WHERE id = :user_id", ("qmark", "named_colon"), True, None),
        ("SELECT * FROM users WHERE id = $1", ("numeric",), True, None),
        ("SELECT * FROM users WHERE id = @user_id", ("named_at",), True, None),
        ("SELECT * FROM users WHERE id = :user_id", ("qmark",), False, RiskLevel.HIGH),
        ("SELECT * FROM users WHERE id = $1", ("qmark",), False, RiskLevel.HIGH),
    ],
    ids=[
        "qmark_allowed",
        "named_colon_allowed",
        "numeric_allowed",
        "named_at_allowed",
        "named_colon_disallowed",
        "numeric_disallowed",
    ],
)
def test_parameter_style_validation(
    sql: str,
    allowed_styles: tuple[str, ...],
    should_pass: bool,
    expected_risk: RiskLevel,
    context: SQLProcessingContext,
    param_validator: ParameterValidator,
) -> None:
    """Test that parameter styles are validated correctly."""
    validator = create_validator()
    context.config.allowed_parameter_styles = allowed_styles

    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    if should_pass:
        assert len(context.validation_errors) == 0
    else:
        assert len(context.validation_errors) >= 1
        assert context.validation_errors[0].risk_level == expected_risk
        assert "not supported" in context.validation_errors[0].message


# Mixed Parameter Style Tests
@pytest.mark.parametrize(
    "sql,allowed_styles,allow_mixed,should_pass,expected_error_pattern",
    [
        ("SELECT * FROM users WHERE id = ? AND name = :name", ("qmark", "named_colon"), True, True, None),
        (
            "SELECT * FROM users WHERE id = ? AND name = :name",
            ("qmark", "named_colon"),
            False,
            False,
            "Mixed parameter styles detected",
        ),
        ("SELECT * FROM users WHERE id = $1 AND name = $2", ("numeric",), False, True, None),  # Same style, no mixing
    ],
    ids=["mixed_allowed", "mixed_disallowed", "same_style_ok"],
)
def test_mixed_parameter_styles(
    sql: str,
    allowed_styles: tuple[str, ...],
    allow_mixed: bool,
    should_pass: bool,
    expected_error_pattern: str,
    context: SQLProcessingContext,
    param_validator: ParameterValidator,
) -> None:
    """Test detection of mixed parameter styles."""
    validator = create_validator()
    context.config.allowed_parameter_styles = allowed_styles
    context.config.allow_mixed_parameter_styles = allow_mixed

    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    if should_pass:
        assert len(context.validation_errors) == 0
    else:
        assert len(context.validation_errors) >= 1
        assert context.validation_errors[0].risk_level == RiskLevel.HIGH
        if expected_error_pattern:
            assert expected_error_pattern in context.validation_errors[0].message


# Specific Parameter Style Tests
@pytest.mark.parametrize(
    "sql,style_name,allowed_styles",
    [
        ("SELECT * FROM users WHERE id = $1 AND name = $2", "numeric", ("numeric",)),
        ("SELECT * FROM users WHERE id = @user_id AND name = @user_name", "named_at", ("named_at",)),
    ],
    ids=["numeric_style", "named_at_style"],
)
def test_specific_parameter_styles(
    sql: str,
    style_name: str,
    allowed_styles: tuple[str, ...],
    context: SQLProcessingContext,
    param_validator: ParameterValidator,
) -> None:
    """Test detection of specific parameter styles."""
    validator = create_validator()
    context.config.allowed_parameter_styles = allowed_styles

    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    assert len(context.validation_errors) == 0


def test_pyformat_positional_style(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test detection of pyformat positional style (%s)."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("pyformat_positional",)

    # %s style requires special handling as SQLGlot may not parse it directly
    sql = "SELECT * FROM users WHERE id = %s AND name = %s"
    context.initial_sql_string = sql
    # Use a compatible expression for SQLGlot parsing
    context.current_expression = parse_one("SELECT * FROM users WHERE id = ? AND name = ?")
    context.parameter_info = param_validator.extract_parameters(sql)
    # Provide dummy parameters
    context.merged_parameters = ["dummy_id", "dummy_name"]

    validator.process(context.current_expression, context)

    assert len(context.validation_errors) == 0


def test_pyformat_named_style(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test detection of pyformat named style (%(name)s)."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("pyformat_named",)

    # %(name)s style requires special handling as SQLGlot may not parse it directly
    sql = "SELECT * FROM users WHERE id = %(user_id)s AND name = %(user_name)s"
    context.initial_sql_string = sql
    # Use a compatible expression for SQLGlot parsing
    context.current_expression = parse_one("SELECT * FROM users WHERE id = ? AND name = ?")
    context.parameter_info = param_validator.extract_parameters(sql)
    # Provide dummy parameters for named style
    context.merged_parameters = {"user_id": "dummy_id", "user_name": "dummy_name"}

    validator.process(context.current_expression, context)

    assert len(context.validation_errors) == 0


# Edge Cases and Configuration Tests
def test_no_parameters_in_sql(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test that SQL without parameters passes validation."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("qmark",)

    sql = "SELECT * FROM users WHERE id = 1"
    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    assert len(context.validation_errors) == 0


@pytest.mark.parametrize(
    "allowed_styles,should_validate,provide_params",
    [
        (None, False, False),  # No validation when not configured - don't provide params
        ((), True, True),  # Empty tuple should validate and reject all - provide params
    ],
    ids=["none_configured", "empty_tuple"],
)
def test_configuration_edge_cases(
    allowed_styles: tuple[str, ...],
    should_validate: bool,
    provide_params: bool,
    context: SQLProcessingContext,
    param_validator: ParameterValidator,
) -> None:
    """Test behavior with edge case configurations."""
    validator = create_validator()
    context.config.allowed_parameter_styles = allowed_styles

    sql = "SELECT * FROM users WHERE id = ?"
    setup_test_context(context, sql, param_validator, provide_dummy_params=provide_params)

    validator.process(context.current_expression, context)

    if should_validate:
        # Empty tuple should reject all parameter styles
        assert len(context.validation_errors) >= 1
        assert context.validation_errors[0].risk_level == RiskLevel.HIGH
        assert "not supported" in context.validation_errors[0].message
    else:
        # None should skip validation
        assert len(context.validation_errors) == 0


def test_multiple_style_violations(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test detection of multiple parameter style violations."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("qmark",)
    context.config.allow_mixed_parameter_styles = False

    # Multiple different disallowed styles - use simplified SQL for parsing
    sql = "SELECT * FROM users WHERE id = :id AND name = %(name)s AND email = @email"
    context.initial_sql_string = sql
    # Use a compatible expression for SQLGlot parsing
    context.current_expression = parse_one("SELECT * FROM users WHERE id = ? AND name = ? AND email = ?")
    context.parameter_info = param_validator.extract_parameters(sql)
    # Provide dummy parameters to avoid missing parameter errors
    context.merged_parameters = {"id": "dummy_id", "name": "dummy_name", "email": "dummy_email"}

    validator.process(context.current_expression, context)

    # Should detect both mixed styles and disallowed styles
    assert len(context.validation_errors) >= 2
    assert all(error.risk_level == RiskLevel.HIGH for error in context.validation_errors)


# Complex Query Tests
def test_complex_query_parameter_detection(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test parameter detection in complex queries."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("qmark", "named_colon")
    context.config.allow_mixed_parameter_styles = False  # Default to False for this test

    # Parameters in subqueries and CTEs
    sql = """
    WITH active_users AS (
        SELECT * FROM users WHERE active = ?
    )
    SELECT u.*, o.total
    FROM active_users u
    JOIN (
        SELECT user_id, SUM(amount) as total
        FROM orders
        WHERE created_at > :start_date
        GROUP BY user_id
    ) o ON u.id = o.user_id
    WHERE u.country = ?
    """
    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    # Should detect mixed styles since allow_mixed_parameter_styles is False
    assert len(context.validation_errors) >= 1
    assert context.validation_errors[0].risk_level == RiskLevel.HIGH
    mixed_style_error = any("Mixed parameter styles" in error.message for error in context.validation_errors)
    assert mixed_style_error


def test_complex_query_mixed_allowed(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test complex query with mixed styles allowed."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("qmark", "named_colon")
    context.config.allow_mixed_parameter_styles = True

    # Same complex query as above
    sql = """
    WITH active_users AS (
        SELECT * FROM users WHERE active = ?
    )
    SELECT u.*, o.total
    FROM active_users u
    JOIN (
        SELECT user_id, SUM(amount) as total
        FROM orders
        WHERE created_at > :start_date
        GROUP BY user_id
    ) o ON u.id = o.user_id
    WHERE u.country = ?
    """
    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    # Should pass since mixed styles are allowed
    assert len(context.validation_errors) == 0


# Target Style Configuration Tests
def test_target_style_suggestion(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test that target style is configured but doesn't cause errors."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("qmark", "numeric")
    context.config.target_parameter_style = "numeric"

    # Using qmark instead of preferred numeric
    sql = "SELECT * FROM users WHERE id = ?"
    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    # Should pass since qmark is allowed (target style is just a preference)
    assert len(context.validation_errors) == 0


# Comprehensive Test Scenarios
@pytest.mark.parametrize(
    "sql,config_setup,expected_errors,description",
    [
        ("SELECT id FROM users", {"allowed_parameter_styles": ("qmark",)}, 0, "no_parameters"),
        ("SELECT * FROM users WHERE id = ?", {"allowed_parameter_styles": ("qmark",)}, 0, "single_allowed_style"),
        ("SELECT * FROM users WHERE id = :id", {"allowed_parameter_styles": ("qmark",)}, 1, "single_disallowed_style"),
        (
            "SELECT * FROM users WHERE id = ? AND name = :name",
            {"allowed_parameter_styles": ("qmark", "named_colon"), "allow_mixed_parameter_styles": True},
            0,
            "mixed_allowed",
        ),
        (
            "SELECT * FROM users WHERE id = ? AND name = :name",
            {"allowed_parameter_styles": ("qmark", "named_colon"), "allow_mixed_parameter_styles": False},
            1,
            "mixed_disallowed",
        ),
    ],
    ids=["no_parameters", "single_allowed", "single_disallowed", "mixed_allowed", "mixed_disallowed"],
)
def test_comprehensive_parameter_style_validation(
    sql: str,
    config_setup: dict,
    expected_errors: int,
    description: str,
    context: SQLProcessingContext,
    param_validator: ParameterValidator,
) -> None:
    """Test comprehensive parameter style validation scenarios."""
    validator = create_validator()

    # Configure the context
    for key, value in config_setup.items():
        setattr(context.config, key, value)

    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    assert len(context.validation_errors) == expected_errors


# Fail on Violation Tests
def test_fail_on_violation_enabled(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test that fail_on_violation=True affects validation behavior."""
    from sqlspec.statement.pipelines.validators._parameter_style import UnsupportedParameterStyleError

    validator = create_validator(fail_on_violation=True)
    context.config.allowed_parameter_styles = ("qmark",)

    # Disallowed style
    sql = "SELECT * FROM users WHERE id = :user_id"
    setup_test_context(context, sql, param_validator)

    # Should raise an exception when fail_on_violation=True
    with pytest.raises(UnsupportedParameterStyleError, match="not supported"):
        validator.process(context.current_expression, context)


def test_validator_handles_parsing_errors(context: SQLProcessingContext, param_validator: ParameterValidator) -> None:
    """Test that validator handles edge cases gracefully."""
    validator = create_validator()
    context.config.allowed_parameter_styles = ("qmark",)

    # Valid SQL that might have parsing edge cases
    sql = "SELECT * FROM users WHERE name LIKE '?%' AND id = ?"
    setup_test_context(context, sql, param_validator)

    validator.process(context.current_expression, context)

    # Should handle the query and detect the parameter correctly
    # (The '?' in the LIKE string should not be counted as a parameter)
    assert len(context.validation_errors) == 0
