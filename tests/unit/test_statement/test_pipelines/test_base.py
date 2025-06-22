"""Unit tests for SQL processing pipeline base components.

This module tests the core pipeline framework including:
- UsesExpression utility for expression conversion
- StatementPipeline for orchestrating SQL processing
- SQLValidator for validation orchestration
- ProcessorProtocol interface
- Error handling and pipeline execution
"""

from typing import Any, Optional

import pytest
import sqlglot
from sqlglot import exp

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.pipelines.base import ProcessorProtocol, SQLValidator, StatementPipeline, UsesExpression
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.result_types import ValidationError
from sqlspec.statement.sql import SQL, SQLConfig


# Helper Classes
class MockProcessor(ProcessorProtocol):
    """Mock processor for testing."""

    def __init__(self, should_error: bool = False, add_validation_error: bool = False) -> None:
        self.should_error = should_error
        self.add_validation_error = add_validation_error
        self.process_called = False
        self.call_count = 0

    def process(self, expression: Optional[exp.Expression], context: SQLProcessingContext) -> Optional[exp.Expression]:
        """Mock process implementation."""
        self.process_called = True
        self.call_count += 1

        if self.should_error:
            raise ValueError("Mock processor error")

        if self.add_validation_error:
            context.validation_errors.append(
                ValidationError(
                    message="Mock validation error",
                    code="mock-error",
                    risk_level=RiskLevel.MEDIUM,
                    processor=self.__class__.__name__,
                    expression=expression,
                )
            )

        return expression


# UsesExpression Tests
@pytest.mark.parametrize(
    "statement,dialect,expected_type",
    [
        ("SELECT * FROM users WHERE id = 1", "mysql", exp.Select),
        ("INSERT INTO users (name) VALUES ('John')", "postgres", exp.Insert),
        ("UPDATE users SET name = 'Jane'", "sqlite", exp.Update),
        ("DELETE FROM users WHERE id = 1", "mysql", exp.Delete),
        ("CREATE TABLE test (id INT)", "postgres", exp.Create),
    ],
    ids=["select", "insert", "update", "delete", "create"],
)
def test_uses_expression_with_sql_strings(statement: str, dialect: str, expected_type: type[exp.Expression]) -> None:
    """Test UsesExpression.get_expression with various SQL string inputs."""
    expression = UsesExpression.get_expression(statement, dialect=dialect)

    assert isinstance(expression, exp.Expression)
    assert isinstance(expression, expected_type)


@pytest.mark.parametrize(
    "empty_input", ["", "   ", "\n\t", None], ids=["empty_string", "whitespace", "newlines_tabs", "none"]
)
def test_uses_expression_with_empty_inputs(empty_input: Any) -> None:
    """Test UsesExpression.get_expression with empty/null inputs."""
    expression = UsesExpression.get_expression(empty_input or "", dialect="mysql")

    assert isinstance(expression, exp.Expression)
    # Should return a neutral expression for empty input
    assert isinstance(expression, exp.Select)


def test_uses_expression_with_expression_input() -> None:
    """Test UsesExpression.get_expression with expression input."""
    original_expression = sqlglot.parse_one("SELECT 1", read="mysql")
    result_expression = UsesExpression.get_expression(original_expression, dialect="mysql")

    assert result_expression is original_expression


def test_uses_expression_with_sql_object() -> None:
    """Test UsesExpression.get_expression with SQL object input."""
    sql_obj = SQL("SELECT * FROM users")
    expression = UsesExpression.get_expression(sql_obj, dialect="mysql")

    assert isinstance(expression, exp.Expression)
    assert isinstance(expression, exp.Select)


def test_uses_expression_with_invalid_sql() -> None:
    """Test UsesExpression.get_expression with invalid SQL."""
    invalid_sql = "SELECT FROM WHERE"

    with pytest.raises(SQLValidationError, match="SQL parsing failed"):
        UsesExpression.get_expression(invalid_sql, dialect="mysql")


# StatementPipeline Tests
def test_statement_pipeline_initialization() -> None:
    """Test StatementPipeline initialization."""
    # Test with default parameters
    pipeline1 = StatementPipeline()
    assert len(pipeline1.transformers) == 0
    assert len(pipeline1.validators) == 0
    assert len(pipeline1.analyzers) == 0

    # Test with empty lists
    pipeline2 = StatementPipeline(transformers=[], validators=[], analyzers=[])
    assert len(pipeline2.transformers) == 0
    assert len(pipeline2.validators) == 0
    assert len(pipeline2.analyzers) == 0

    # Test with mock processors
    mock_transformer = MockProcessor()
    mock_validator = MockProcessor()
    mock_analyzer = MockProcessor()

    pipeline3 = StatementPipeline(
        transformers=[mock_transformer], validators=[mock_validator], analyzers=[mock_analyzer]
    )
    assert len(pipeline3.transformers) == 1
    assert len(pipeline3.validators) == 1
    assert len(pipeline3.analyzers) == 1


def test_statement_pipeline_execute_empty() -> None:
    """Test StatementPipeline.execute_pipeline with no components."""
    pipeline = StatementPipeline()
    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = pipeline.execute_pipeline(context)

    assert result.expression is expression
    assert result.context is context
    assert len(result.context.validation_errors) == 0
    assert result.context.risk_level == RiskLevel.SAFE


@pytest.mark.parametrize(
    "enable_transformations,enable_validation,enable_analysis,expected_calls",
    [
        (True, True, True, 3),  # All stages enabled
        (True, False, False, 1),  # Only transformations
        (False, True, False, 1),  # Only validation
        (False, False, True, 1),  # Only analysis
        (False, False, False, 0),  # Nothing enabled
    ],
    ids=["all_enabled", "transform_only", "validate_only", "analyze_only", "none_enabled"],
)
def test_statement_pipeline_execute_with_stages(
    enable_transformations: bool, enable_validation: bool, enable_analysis: bool, expected_calls: int
) -> None:
    """Test StatementPipeline execution with different stage configurations."""
    mock_transformer = MockProcessor()
    mock_validator = MockProcessor()
    mock_analyzer = MockProcessor()

    pipeline = StatementPipeline(
        transformers=[mock_transformer], validators=[mock_validator], analyzers=[mock_analyzer]
    )

    config = SQLConfig(
        enable_transformations=enable_transformations,
        enable_validation=enable_validation,
        enable_analysis=enable_analysis,
    )
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = pipeline.execute_pipeline(context)

    # Count total calls across all processors
    total_calls = mock_transformer.call_count + mock_validator.call_count + mock_analyzer.call_count
    assert total_calls == expected_calls
    assert result.expression is not None


def test_statement_pipeline_error_handling() -> None:
    """Test StatementPipeline error handling in processors."""
    error_transformer = MockProcessor(should_error=True)
    normal_validator = MockProcessor()

    pipeline = StatementPipeline(transformers=[error_transformer], validators=[normal_validator])

    config = SQLConfig(enable_transformations=True, enable_validation=True)
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = pipeline.execute_pipeline(context)

    # Should have an error from the transformer
    assert len(result.context.validation_errors) >= 1
    transformer_error = next((err for err in result.context.validation_errors if "failed" in err.message), None)
    assert transformer_error is not None
    assert transformer_error.risk_level == RiskLevel.CRITICAL


def test_statement_pipeline_parse_error() -> None:
    """Test StatementPipeline handling of SQL parse errors."""
    pipeline = StatementPipeline()
    config = SQLConfig(enable_parsing=True)

    context = SQLProcessingContext(
        initial_sql_string="INVALID SQL SYNTAX",
        dialect="mysql",
        config=config,
        current_expression=None,  # No expression provided
    )

    result = pipeline.execute_pipeline(context)

    # Should have a parsing error
    assert len(result.context.validation_errors) >= 1
    parse_error = next((err for err in result.context.validation_errors if "Parsing Error" in err.message), None)
    assert parse_error is not None
    assert parse_error.risk_level == RiskLevel.CRITICAL


# SQLValidator Tests
@pytest.mark.parametrize(
    "min_risk_level,validator_count",
    [(RiskLevel.LOW, 0), (RiskLevel.MEDIUM, 2), (RiskLevel.HIGH, 1), (RiskLevel.CRITICAL, 3)],
    ids=["low_risk", "medium_risk", "high_risk", "critical_risk"],
)
def test_sql_validator_initialization(min_risk_level: RiskLevel, validator_count: int) -> None:
    """Test SQLValidator initialization with various parameters."""
    mock_validators = [MockProcessor() for _ in range(validator_count)]

    validator = SQLValidator(validators=mock_validators, min_risk_to_raise=min_risk_level)

    assert len(validator.validators) == validator_count
    assert validator.min_risk_to_raise == min_risk_level


def test_sql_validator_initialization_defaults() -> None:
    """Test SQLValidator initialization with defaults."""
    validator = SQLValidator()
    assert len(validator.validators) == 0
    assert validator.min_risk_to_raise == RiskLevel.HIGH


def test_sql_validator_add_validator() -> None:
    """Test SQLValidator.add_validator functionality."""
    validator = SQLValidator()
    mock_processor = MockProcessor(add_validation_error=True)

    validator.add_validator(mock_processor)

    assert len(validator.validators) == 1
    assert validator.validators[0] is mock_processor


@pytest.mark.parametrize(
    "enable_validation,expected_errors",
    [
        (True, 1),  # Validation enabled, should get error
        (False, 0),  # Validation disabled, no errors
    ],
    ids=["validation_enabled", "validation_disabled"],
)
def test_sql_validator_process_with_validation_toggle(enable_validation: bool, expected_errors: int) -> None:
    """Test SQLValidator.process with validation enabled/disabled."""
    mock_validator_processor = MockProcessor(add_validation_error=True)
    validator = SQLValidator(validators=[mock_validator_processor])

    config = SQLConfig(enable_validation=enable_validation)
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result_expression = validator.process(expression, context)

    assert result_expression is expression
    assert len(context.validation_errors) == expected_errors

    if enable_validation:
        assert mock_validator_processor.process_called
    else:
        assert not mock_validator_processor.process_called


def test_sql_validator_error_handling() -> None:
    """Test SQLValidator error handling for failing validators."""
    error_validator = MockProcessor(should_error=True)
    normal_validator = MockProcessor(add_validation_error=True)

    validator = SQLValidator(validators=[error_validator, normal_validator])

    config = SQLConfig(enable_validation=True)
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result_expression = validator.process(expression, context)

    assert result_expression is expression
    # Should have both the error from failing validator and the validation error
    assert len(context.validation_errors) >= 1

    # Check that both validators were called
    assert error_validator.process_called
    assert normal_validator.process_called


@pytest.mark.parametrize(
    "sql_statement,dialect,expected_error_count",
    [
        ("SELECT * FROM users", "mysql", 1),  # With validation error
        ("INSERT INTO users VALUES (1)", "postgres", 1),  # With validation error
        ("UPDATE users SET name = 'test'", "sqlite", 1),  # With validation error
    ],
    ids=["select_query", "insert_query", "update_query"],
)
def test_sql_validator_validate_method(sql_statement: str, dialect: str, expected_error_count: int) -> None:
    """Test SQLValidator.validate method returns list of errors."""
    mock_validator_processor = MockProcessor(add_validation_error=True)
    validator = SQLValidator(validators=[mock_validator_processor])

    errors = validator.validate(sql_statement, dialect=dialect)

    assert len(errors) == expected_error_count
    if errors:
        assert errors[0].message == "Mock validation error"
        assert errors[0].code == "mock-error"
        assert errors[0].risk_level == RiskLevel.MEDIUM


def test_sql_validator_validate_with_no_validators() -> None:
    """Test SQLValidator.validate returns empty list when no validators."""
    validator = SQLValidator()
    errors = validator.validate("SELECT 1", dialect="mysql")

    assert errors == []
    assert isinstance(errors, list)


def test_sql_validator_validate_with_sql_object() -> None:
    """Test SQLValidator.validate with SQL object input."""
    sql_obj = SQL("SELECT * FROM users")
    mock_validator_processor = MockProcessor(add_validation_error=True)
    validator = SQLValidator(validators=[mock_validator_processor])

    errors = validator.validate(sql_obj, dialect="mysql")

    assert len(errors) == 1
    assert errors[0].message == "Mock validation error"


def test_sql_validator_validate_with_custom_config() -> None:
    """Test SQLValidator.validate with custom config."""
    custom_config = SQLConfig(enable_validation=False)
    mock_validator_processor = MockProcessor(add_validation_error=True)
    validator = SQLValidator(validators=[mock_validator_processor])

    errors = validator.validate("SELECT 1", dialect="mysql", config=custom_config)

    # Should have no errors because validation is disabled
    assert len(errors) == 0
    assert not mock_validator_processor.process_called


def test_processor_protocol_interface() -> None:
    """Test that ProcessorProtocol defines the correct interface."""
    # Verify that our MockProcessor correctly implements the protocol
    mock = MockProcessor()
    assert hasattr(mock, "process")
    assert callable(mock.process)

    # Test the process method signature
    expression = sqlglot.parse_one("SELECT 1", read="mysql")
    config = SQLConfig()
    context = SQLProcessingContext(
        initial_sql_string="SELECT 1", dialect="mysql", config=config, current_expression=expression
    )

    result = mock.process(expression, context)
    assert result is expression
    assert mock.process_called
