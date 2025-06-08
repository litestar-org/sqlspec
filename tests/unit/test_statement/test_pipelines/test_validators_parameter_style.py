"""Unit tests for Parameter Style Validator."""

import pytest
from sqlglot import parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.parameters import ParameterValidator
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.validators._parameter_style import ParameterStyleValidator
from sqlspec.statement.sql import SQLConfig


class TestParameterStyleValidator:
    """Test the Parameter Style validator."""

    @pytest.fixture
    def validator(self) -> ParameterStyleValidator:
        """Create a parameter style validator instance."""
        return ParameterStyleValidator(fail_on_violation=False)

    @pytest.fixture
    def context(self) -> SQLProcessingContext:
        """Create a processing context."""
        return SQLProcessingContext(initial_sql_string="SELECT 1", dialect=None, config=SQLConfig())

    @pytest.fixture
    def param_validator(self) -> ParameterValidator:
        """Create a parameter validator for extracting parameters."""
        return ParameterValidator()

    def test_allowed_parameter_style(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test that allowed parameter styles pass validation."""
        context.config.allowed_parameter_styles = ("qmark", "named_colon")

        # qmark style (allowed)
        context.initial_sql_string = "SELECT * FROM users WHERE id = ?"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_disallowed_parameter_style(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test that disallowed parameter styles are flagged."""
        context.config.allowed_parameter_styles = ("qmark",)

        # named_colon style (not allowed)
        context.initial_sql_string = "SELECT * FROM users WHERE id = :user_id"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.HIGH
        assert any("not supported by database" in issue for issue in result.issues)

    def test_mixed_parameter_styles_disallowed(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test detection of mixed parameter styles when not allowed."""
        context.config.allowed_parameter_styles = ("qmark", "named_colon")
        context.config.allow_mixed_parameter_styles = False

        # Mixed styles
        context.initial_sql_string = "SELECT * FROM users WHERE id = ? AND name = :name"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.HIGH
        assert any("Mixed parameter styles detected" in issue for issue in result.issues)

    def test_mixed_parameter_styles_allowed(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test that mixed parameter styles pass when allowed."""
        context.config.allowed_parameter_styles = ("qmark", "named_colon")
        context.config.allow_mixed_parameter_styles = True

        # Mixed styles
        context.initial_sql_string = "SELECT * FROM users WHERE id = ? AND name = :name"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_numeric_parameter_style(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test detection of numeric parameter style ($1, $2, etc.)."""
        context.config.allowed_parameter_styles = ("numeric",)

        context.initial_sql_string = "SELECT * FROM users WHERE id = $1 AND name = $2"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_pyformat_positional_style(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test detection of pyformat positional style (%s)."""
        context.config.allowed_parameter_styles = ("pyformat_positional",)

        context.initial_sql_string = "SELECT * FROM users WHERE id = %s AND name = %s"
        # SQLGlot doesn't support %s placeholders, so use a simple expression for testing
        context.current_expression = parse_one("SELECT * FROM users WHERE id = ? AND name = ?")
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_pyformat_named_style(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test detection of pyformat named style (%(name)s)."""
        context.config.allowed_parameter_styles = ("pyformat_named",)

        context.initial_sql_string = "SELECT * FROM users WHERE id = %(user_id)s AND name = %(user_name)s"
        # SQLGlot doesn't support %(name)s placeholders, so use a simple expression for testing
        context.current_expression = parse_one("SELECT * FROM users WHERE id = ? AND name = ?")
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_named_at_style(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test detection of named @ style (@name)."""
        context.config.allowed_parameter_styles = ("named_at",)

        context.initial_sql_string = "SELECT * FROM users WHERE id = @user_id AND name = @user_name"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_no_parameters_in_sql(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test that SQL without parameters passes validation."""
        context.config.allowed_parameter_styles = ("qmark",)

        context.initial_sql_string = "SELECT * FROM users WHERE id = 1"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No issues

    def test_no_allowed_styles_configured(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test behavior when no allowed styles are configured."""
        context.config.allowed_parameter_styles = None

        context.initial_sql_string = "SELECT * FROM users WHERE id = ?"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.SKIP  # No validation when not configured

    def test_empty_allowed_styles(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test behavior with empty allowed styles tuple."""
        context.config.allowed_parameter_styles = ()

        context.initial_sql_string = "SELECT * FROM users WHERE id = ?"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.HIGH
        assert any("not supported by database" in issue for issue in result.issues)

    def test_multiple_style_violations(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test detection of multiple parameter style violations."""
        context.config.allowed_parameter_styles = ("qmark",)
        context.config.allow_mixed_parameter_styles = False

        # Multiple different disallowed styles
        context.initial_sql_string = "SELECT * FROM users WHERE id = :id AND name = %(name)s AND email = @email"
        # SQLGlot doesn't support %(name)s placeholders, so use a simple expression for testing
        context.current_expression = parse_one("SELECT * FROM users WHERE id = ? AND name = ? AND email = ?")
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        assert result.risk_level == RiskLevel.HIGH
        assert len(result.issues) >= 2  # At least mixed styles and disallowed styles

    def test_complex_query_parameter_detection(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test parameter detection in complex queries."""
        context.config.allowed_parameter_styles = ("qmark", "named_colon")

        # Parameters in subqueries and CTEs
        context.initial_sql_string = """
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
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        _, result = validator.process(context)

        assert result is not None
        # Should detect mixed styles if allow_mixed_parameter_styles is False
        if not context.config.allow_mixed_parameter_styles:
            assert result.risk_level == RiskLevel.HIGH
        else:
            assert result.risk_level == RiskLevel.SKIP

    def test_target_style_suggestion(
        self, validator: ParameterStyleValidator, context: SQLProcessingContext, param_validator: ParameterValidator
    ) -> None:
        """Test that target style is suggested when available."""
        context.config.allowed_parameter_styles = ("qmark", "numeric")
        context.config.target_parameter_style = "numeric"

        # Using qmark instead of preferred numeric
        context.initial_sql_string = "SELECT * FROM users WHERE id = ?"
        context.current_expression = parse_one(context.initial_sql_string)
        context.parameter_info = param_validator.extract_parameters(context.initial_sql_string)

        # Note: This is a feature suggestion - the actual implementation
        # might not include style suggestions, but it could be useful
        _, result = validator.process(context)

        assert result is not None
        # Currently should pass, but could warn about non-preferred style
        assert result.risk_level == RiskLevel.SKIP
