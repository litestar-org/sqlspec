"""Tests for the base pipeline components."""

from typing import Optional

import pytest
import sqlglot
from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidator, StatementPipeline, UsesExpression, ValidationResult
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


def test_validation_result_initialization() -> None:
    """Test ValidationResult initialization with various parameters."""
    # Test with minimal parameters
    result1 = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
    assert result1.is_safe
    assert result1.risk_level == RiskLevel.SAFE
    assert result1.issues == []
    assert result1.warnings == []

    # Test with full parameters
    result2 = ValidationResult(
        is_safe=False,
        risk_level=RiskLevel.HIGH,
        issues=["Issue 1", "Issue 2"],
        warnings=["Warning 1"],
        transformed_sql="SELECT * FROM users",
    )
    assert not result2.is_safe
    assert result2.risk_level == RiskLevel.HIGH
    assert len(result2.issues) == 2
    assert len(result2.warnings) == 1
    assert result2.transformed_sql == "SELECT * FROM users"


def test_validation_result_merge() -> None:
    """Test ValidationResult.merge functionality."""
    result1 = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, issues=[], warnings=["Warning 1"])

    result2 = ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=["Issue 1"], warnings=["Warning 2"])

    result1.merge(result2)

    # After merge, should reflect the worse state
    assert not result1.is_safe  # Should be False from result2
    assert result1.risk_level == RiskLevel.HIGH  # Should take higher risk level
    assert len(result1.issues) == 1  # Should include issues from result2
    assert len(result1.warnings) == 2  # Should include warnings from both


def test_validation_result_boolean_conversion() -> None:
    """Test ValidationResult boolean conversion."""
    safe_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
    unsafe_result = ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH)

    assert bool(safe_result) is True
    assert bool(unsafe_result) is False


# ProcessorResult tests removed - ProcessorResult class no longer exists in the codebase
# The new pipeline architecture uses PipelineResult and SQLProcessingContext instead


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

    validator = SQLValidator()

    # Create a mock validator that implements ProcessorProtocol
    class MockProcessor:
        def process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]:
            return context.current_expression or exp.Placeholder(), ValidationResult(
                is_safe=True, risk_level=RiskLevel.SAFE
            )

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
    assert len(result.validation_errors) == 0
    assert result.risk_level == RiskLevel.SAFE


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
    assert len(result.validation_errors) == 0


def test_risk_level_comparison() -> None:
    """Test risk level comparisons in ValidationResult merging."""
    # Test various risk level combinations
    risk_combinations = [
        (RiskLevel.SAFE, RiskLevel.LOW, RiskLevel.LOW),
        (RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.MEDIUM),
        (RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.HIGH),
        (RiskLevel.HIGH, RiskLevel.CRITICAL, RiskLevel.CRITICAL),
    ]

    for risk1, risk2, expected in risk_combinations:
        result1 = ValidationResult(is_safe=True, risk_level=risk1)
        result2 = ValidationResult(is_safe=True, risk_level=risk2)

        result1.merge(result2)
        assert result1.risk_level == expected


def test_validation_result_aggregation() -> None:
    """Test aggregation of multiple validation results."""
    results = [
        ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, warnings=["Warning 1"]),
        ValidationResult(is_safe=False, risk_level=RiskLevel.MEDIUM, issues=["Issue 1"]),
        ValidationResult(is_safe=True, risk_level=RiskLevel.LOW, warnings=["Warning 2"]),
        ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=["Issue 2"]),
    ]

    # Aggregate all results
    aggregated = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
    for result in results:
        aggregated.merge(result)

    # Should reflect the worst case
    assert not aggregated.is_safe  # Any False makes it False
    assert aggregated.risk_level == RiskLevel.HIGH  # Highest risk level
    assert len(aggregated.issues) == 2  # All issues collected
    assert len(aggregated.warnings) == 2  # All warnings collected


def test_transformer_pipeline_validation_aggregation() -> None:
    """Test validation error aggregation in StatementPipeline."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.statement.pipelines.result_types import ValidationError

    # Create mock validators that add errors to context
    class MockValidator:
        def __init__(self, error_message: str, risk_level: RiskLevel) -> None:
            self.error_message = error_message
            self.risk_level = risk_level

        def process(self, expression: exp.Expression, context: SQLProcessingContext) -> exp.Expression:
            # Add validation error to context
            error = ValidationError(
                message=self.error_message,
                code="mock-error",
                risk_level=self.risk_level,
                processor=self.__class__.__name__,
                expression=expression,
            )
            context.validation_errors.append(error)
            return expression

    pipeline = StatementPipeline()
    pipeline.validators = [  # type: ignore[list-item]
        MockValidator("Warning 1", RiskLevel.LOW),  # type: ignore[list-item]
        MockValidator("Issue 1", RiskLevel.MEDIUM),  # type: ignore[list-item]
        MockValidator("Warning 2", RiskLevel.LOW),  # type: ignore[list-item]
    ]

    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = pipeline.execute_pipeline(context)

    # Check that all errors were collected
    assert len(result.validation_errors) == 3
    assert result.has_errors is True
    # Risk level should be the highest (MEDIUM)
    assert result.risk_level == RiskLevel.MEDIUM


def test_uses_expression_error_handling() -> None:
    """Test UsesExpression error handling with invalid SQL."""
    from sqlspec.exceptions import SQLValidationError

    # Test with invalid SQL that should fail parsing
    invalid_sql = "SELECT * FROM"  # Incomplete SQL

    with pytest.raises(SQLValidationError):
        UsesExpression.get_expression(invalid_sql, dialect="mysql")


def test_sql_validator_validate_convenience_method() -> None:
    """Test SQLValidator.validate convenience method."""
    from sqlspec.statement.pipelines.context import SQLProcessingContext

    # Create a validator with a mock processor
    class MockProcessor:
        def process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]:
            return context.current_expression or exp.Placeholder(), ValidationResult(
                is_safe=False, risk_level=RiskLevel.MEDIUM, issues=["Mock issue"]
            )

    mock_processor = MockProcessor()
    validator = SQLValidator(validators=[mock_processor])  # type: ignore[list-item]

    # Test the convenience method
    sql_string = "SELECT * FROM users"
    result = validator.validate(sql_string, "mysql")

    assert isinstance(result, ValidationResult)
    assert not result.is_safe
    assert len(result.issues) == 1


def test_pipeline_component_slots() -> None:
    """Test that pipeline result classes use __slots__ for memory efficiency."""
    # Test ValidationResult slots
    ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
    assert hasattr(ValidationResult, "__slots__")

    # TransformationResult and AnalysisResult no longer exist in the new architecture
    # The new pipeline uses context objects to collect results instead
