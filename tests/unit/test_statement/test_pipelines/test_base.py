"""Tests for the base pipeline components."""

import sqlglot
from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidator, StatementPipeline, UsesExpression
from sqlspec.statement.sql import SQLConfig


def test_uses_expression_with_sql_string() -> None:
    """Test UsesExpression.get_expression with SQL string input."""
    sql_string = "SELECT * FROM users WHERE id = 1"
    expression = UsesExpression.get_expression(sql_string, dialect="mysql")

    assert isinstance(expression, exp.Expression)
    assert isinstance(expression, exp.Select)


def test_uses_expression_with_empty_string() -> None:
    """Test UsesExpression.get_expression with empty string input."""
    empty_string = ""
    expression = UsesExpression.get_expression(empty_string, dialect="mysql")

    assert isinstance(expression, exp.Expression)
    # Should return a neutral expression for empty input


def test_uses_expression_with_expression_input() -> None:
    """Test UsesExpression.get_expression with expression input."""
    original_expression = sqlglot.parse_one("SELECT 1", read="mysql")
    result_expression = UsesExpression.get_expression(original_expression, dialect="mysql")

    assert result_expression is original_expression


def test_statement_pipeline_initialization() -> None:
    """Test StatementPipeline initialization."""
    # Create a simple pipeline
    StatementPipeline()


def test_pipeline_configuration() -> None:
    """Test pipeline configuration handling."""
    SQLConfig(enable_validation=True, enable_transformations=True)
    pipeline = StatementPipeline()
    assert pipeline is not None


def test_sql_validator_initialization() -> None:
    """Test SQLValidator initialization."""
    # Test with no validators
    validator1 = SQLValidator()
    assert len(validator1.validators) == 0
    assert validator1.min_risk_to_raise == RiskLevel.HIGH

    # Test with custom parameters
    validator2 = SQLValidator(validators=[], min_risk_to_raise=RiskLevel.MEDIUM)
    assert validator2.min_risk_to_raise == RiskLevel.MEDIUM


def test_sql_validator_add_validator() -> None:
    """Test SQLValidator.add_validator functionality."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.statement.pipelines.result_types import ValidationError

    validator = SQLValidator()

    # Create a mock validator that implements ProcessorProtocol
    class MockProcessor:
        def process(self, expression: exp.Expression, context: SQLProcessingContext) -> exp.Expression:
            # Add a validation error to the context
            context.validation_errors.append(
                ValidationError(
                    message="Test error",
                    code="test-error",
                    risk_level=RiskLevel.LOW,
                    processor="MockProcessor",
                    expression=expression,
                )
            )
            return expression

    mock_processor = MockProcessor()
    validator.add_validator(mock_processor)  # type: ignore[arg-type]

    assert len(validator.validators) == 1
    assert validator.validators[0] is mock_processor  # type: ignore


def test_sql_validator_process_with_disabled_validation() -> None:
    """Test SQLValidator.process when validation is disabled."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext

    validator = SQLValidator()
    config = SQLConfig(enable_validation=False)
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result_expression = validator.process(expression, context)

    assert result_expression is expression
    # When validation is disabled, no errors should be added to context
    assert len(context.validation_errors) == 0


def test_transformer_pipeline_initialization() -> None:
    """Test StatementPipeline initialization."""
    # Test with default parameters
    pipeline1 = StatementPipeline()
    assert len(pipeline1.transformers) == 0
    assert len(pipeline1.validators) == 0
    assert len(pipeline1.analyzers) == 0

    # Test with initial components
    pipeline2 = StatementPipeline(transformers=[], validators=[], analyzers=[])
    assert len(pipeline2.transformers) == 0
    assert len(pipeline2.validators) == 0
    assert len(pipeline2.analyzers) == 0


def test_transformer_pipeline_execute_empty() -> None:
    """Test StatementPipeline.execute_pipeline with no components."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext

    pipeline = StatementPipeline()
    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = pipeline.execute_pipeline(context)

    assert result.expression is expression
    assert result.context is context
    # When validation is disabled (default), no errors should be added
    assert len(result.context.validation_errors) == 0
    assert result.context.risk_level == RiskLevel.SAFE


def test_transformer_pipeline_execute_with_mock_components() -> None:
    """Test StatementPipeline.execute_pipeline with mock components."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext

    # Create mock processor with correct signature
    class MockProcessor:
        def process(self, expression: exp.Expression, context: SQLProcessingContext) -> exp.Expression:
            # Return the expression unchanged
            return expression

    pipeline = StatementPipeline()
    # Add mock transformers
    pipeline.transformers = [MockProcessor()]  # type: ignore

    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = pipeline.execute_pipeline(context)

    assert result.expression is expression
    assert result.context is context
    # With no validators, should have no errors
    assert len(result.context.validation_errors) == 0


def test_sql_validator_validate_method() -> None:
    """Test SQLValidator.validate method returns list of errors."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.statement.pipelines.result_types import ValidationError

    class MockValidator:
        def process(self, expression: exp.Expression, context: SQLProcessingContext) -> exp.Expression:
            context.validation_errors.append(
                ValidationError(
                    message="Test validation error",
                    code="test-error",
                    risk_level=RiskLevel.HIGH,
                    processor="MockValidator",
                    expression=expression,
                )
            )
            return expression

    validator = SQLValidator(validators=[MockValidator()])  # type: ignore
    errors = validator.validate("SELECT * FROM users", dialect="mysql")

    assert len(errors) == 1
    assert errors[0].message == "Test validation error"
    assert errors[0].code == "test-error"
    assert errors[0].risk_level == RiskLevel.HIGH


def test_sql_validator_validate_with_no_errors() -> None:
    """Test SQLValidator.validate returns empty list when no errors."""
    validator = SQLValidator()
    errors = validator.validate("SELECT 1", dialect="mysql")

    assert errors == []
    assert isinstance(errors, list)
