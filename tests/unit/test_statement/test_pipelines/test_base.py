"""Function-based tests for base pipeline functionality."""

from typing import Any, Optional

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import (
    AnalysisResult,
    SQLAnalysis,
    SQLTransformation,
    SQLValidation,
    SQLValidator,
    TransformationResult,
    TransformerPipeline,
    UsesExpression,
    ValidationResult,
)
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


def test_transformation_result_initialization() -> None:
    """Test TransformationResult initialization."""
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    # Test with minimal parameters
    result1 = TransformationResult(expression=expression, modified=True)
    assert result1.expression is expression
    assert result1.modified
    assert result1.notes == []

    # Test with notes
    result2 = TransformationResult(
        expression=expression, modified=False, notes=["No changes needed", "Query already optimized"]
    )
    assert not result2.modified
    assert len(result2.notes) == 2


def test_transformation_result_boolean_conversion() -> None:
    """Test TransformationResult boolean conversion."""
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    modified_result = TransformationResult(expression=expression, modified=True)
    unmodified_result = TransformationResult(expression=expression, modified=False)

    assert bool(modified_result) is True
    assert bool(unmodified_result) is False


def test_analysis_result_initialization() -> None:
    """Test AnalysisResult initialization."""
    # Test with minimal parameters
    result1 = AnalysisResult()
    assert result1.metrics == {}
    assert result1.warnings == []
    assert result1.issues == []
    assert result1.notes == []

    # Test with full parameters
    result2 = AnalysisResult(
        metrics={"complexity": 10, "joins": 2},
        warnings=["High complexity"],
        issues=["Potential performance issue"],
        notes=["Analysis completed"],
    )
    assert result2.metrics["complexity"] == 10
    assert len(result2.warnings) == 1
    assert len(result2.issues) == 1
    assert len(result2.notes) == 1


def test_analysis_result_boolean_conversion() -> None:
    """Test AnalysisResult boolean conversion."""
    clean_result = AnalysisResult(issues=[])
    problematic_result = AnalysisResult(issues=["Issue found"])

    assert bool(clean_result) is True  # No issues means True
    assert bool(problematic_result) is False  # Has issues means False


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
    validator = SQLValidator()

    # Create a mock validator
    class MockValidation(SQLValidation):
        def __init__(self) -> None:
            super().__init__(RiskLevel.MEDIUM)

        def validate(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> ValidationResult:
            return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

    mock_validation = MockValidation()
    validator.add_validator(mock_validation)

    assert len(validator.validators) == 1
    assert validator.validators[0] is mock_validation


def test_sql_validator_process_with_disabled_validation() -> None:
    """Test SQLValidator.process when validation is disabled."""
    validator = SQLValidator()
    config = SQLConfig(enable_validation=False)
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    result_expression, validation_result = validator.process(expression, "mysql", config)

    assert result_expression is expression
    assert validation_result is not None
    assert validation_result.is_safe
    assert validation_result.risk_level == RiskLevel.SKIP


def test_transformer_pipeline_initialization() -> None:
    """Test TransformerPipeline initialization."""
    # Test with default parameters
    pipeline1 = TransformerPipeline()
    assert len(pipeline1.components) == 0

    # Test with initial components
    pipeline2 = TransformerPipeline(components=[])
    assert len(pipeline2.components) == 0


def test_transformer_pipeline_execute_empty() -> None:
    """Test TransformerPipeline.execute with no components."""
    pipeline = TransformerPipeline()
    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    result_expression, validation_result = pipeline.execute(expression, "mysql", config)

    assert result_expression is expression
    assert validation_result.is_safe
    assert validation_result.risk_level == RiskLevel.SAFE
    assert len(validation_result.issues) == 0


def test_transformer_pipeline_execute_with_mock_components() -> None:
    """Test TransformerPipeline.execute with mock components."""

    # Create mock processor
    class MockProcessor:
        def process(
            self, expression: exp.Expression, dialect: Optional[DialectType] = None, config: Optional[SQLConfig] = None
        ) -> tuple[exp.Expression, ValidationResult]:
            # Return modified expression and no validation issues
            return expression, ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

    pipeline = TransformerPipeline()
    # Use a list to avoid assignment issues
    pipeline.components = [MockProcessor()]  # type: ignore

    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    result_expression, validation_result = pipeline.execute(expression, "mysql", config)

    assert result_expression is expression
    assert validation_result.is_safe


def test_sql_validation_abstract_base() -> None:
    """Test SQLValidation abstract base class."""

    # Create a concrete implementation
    class ConcreteValidation(SQLValidation):
        def __init__(self) -> None:
            super().__init__(RiskLevel.MEDIUM, RiskLevel.HIGH)

        def validate(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> ValidationResult:
            return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

    validation = ConcreteValidation()
    assert validation.risk_level == RiskLevel.MEDIUM
    assert validation.min_risk_to_raise == RiskLevel.HIGH

    # Test validation method
    expression = sqlglot.parse_one("SELECT 1", read="mysql")
    config = SQLConfig()
    result = validation.validate(expression, "mysql", config)

    assert isinstance(result, ValidationResult)
    assert result.is_safe


def test_sql_transformation_abstract_base() -> None:
    """Test SQLTransformation abstract base class."""

    # Create a concrete implementation
    class ConcreteTransformation(SQLTransformation):
        def transform(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> TransformationResult:
            return TransformationResult(expression=expression, modified=False)

    transformation = ConcreteTransformation()

    # Test transformation method
    expression = sqlglot.parse_one("SELECT 1", read="mysql")
    config = SQLConfig()
    result = transformation.transform(expression, "mysql", config)

    assert isinstance(result, TransformationResult)
    assert result.expression is expression


def test_sql_analysis_abstract_base() -> None:
    """Test SQLAnalysis abstract base class."""

    # Create a concrete implementation
    class ConcreteAnalysis(SQLAnalysis):
        def analyze(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> AnalysisResult:
            return AnalysisResult(metrics={"test": 1})

    analysis = ConcreteAnalysis()

    # Test analysis method
    expression = sqlglot.parse_one("SELECT 1", read="mysql")
    config = SQLConfig()
    result = analysis.analyze(expression, "mysql", config)

    assert isinstance(result, AnalysisResult)
    assert result.metrics["test"] == 1


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
    """Test validation result aggregation in TransformerPipeline."""

    # Create mock processors that return validation results
    class MockValidatingProcessor:
        def __init__(
            self,
            is_safe: bool,
            risk_level: RiskLevel,
            issues: Optional[list[str]] = None,
            warnings: Optional[list[str]] = None,
        ) -> None:
            self.is_safe = is_safe
            self.risk_level = risk_level
            self.issues = issues or []
            self.warnings = warnings or []

        def process(
            self, expression: exp.Expression, dialect: Optional[DialectType] = None, config: Optional[SQLConfig] = None
        ) -> tuple[exp.Expression, ValidationResult]:
            validation_result = ValidationResult(
                is_safe=self.is_safe, risk_level=self.risk_level, issues=self.issues, warnings=self.warnings
            )
            return expression, validation_result

    pipeline = TransformerPipeline()
    pipeline.components = [  # type: ignore
        MockValidatingProcessor(True, RiskLevel.SAFE, warnings=["Warning 1"]),
        MockValidatingProcessor(False, RiskLevel.MEDIUM, issues=["Issue 1"]),
        MockValidatingProcessor(True, RiskLevel.LOW, warnings=["Warning 2"]),
    ]

    config = SQLConfig()
    expression = sqlglot.parse_one("SELECT 1", read="mysql")

    _, validation_result = pipeline.execute(expression, "mysql", config)

    # Should aggregate all validation results
    assert not validation_result.is_safe  # Any component with issues makes it unsafe
    assert validation_result.risk_level == RiskLevel.MEDIUM  # Highest risk level
    assert len(validation_result.issues) == 1  # Issues from unsafe component
    assert len(validation_result.warnings) == 2  # Warnings from all components


def test_uses_expression_error_handling() -> None:
    """Test UsesExpression error handling with invalid SQL."""
    from sqlspec.exceptions import SQLValidationError

    # Test with invalid SQL that should fail parsing
    invalid_sql = "SELECT * FROM"  # Incomplete SQL

    with pytest.raises(SQLValidationError):
        UsesExpression.get_expression(invalid_sql, dialect="mysql")


def test_sql_validator_validate_convenience_method() -> None:
    """Test SQLValidator.validate convenience method."""

    # Create a validator with a mock validation
    class MockValidation(SQLValidation):
        def __init__(self) -> None:
            super().__init__(RiskLevel.MEDIUM)

        def validate(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> ValidationResult:
            return ValidationResult(is_safe=False, risk_level=RiskLevel.MEDIUM, issues=["Mock issue"])

    validator = SQLValidator()
    validator.add_validator(MockValidation())

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

    # Test TransformationResult slots
    expression = sqlglot.parse_one("SELECT 1", read="mysql")
    TransformationResult(expression=expression, modified=False)
    assert hasattr(TransformationResult, "__slots__")

    # Test AnalysisResult slots
    AnalysisResult()
    assert hasattr(AnalysisResult, "__slots__")


def test_pipeline_base_classes_inheritance() -> None:
    """Test that base classes can be properly inherited and extended."""

    # Test extending SQLValidation
    class CustomValidation(SQLValidation):
        def __init__(self, custom_param: Optional[str] = None) -> None:
            super().__init__(RiskLevel.MEDIUM)
            self.custom_param = custom_param

        def validate(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> ValidationResult:
            # Custom validation logic
            if self.custom_param == "strict":
                return ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=["Strict mode"])
            return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

    # Test extending SQLTransformation
    class CustomTransformation(SQLTransformation):
        def transform(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> TransformationResult:
            # Custom transformation logic
            return TransformationResult(expression=expression, modified=True, notes=["Custom transformation applied"])

    # Test extending SQLAnalysis
    class CustomAnalysis(SQLAnalysis):
        def analyze(
            self, expression: exp.Expression, dialect: DialectType, config: SQLConfig, **kwargs: Any
        ) -> AnalysisResult:
            # Custom analysis logic
            return AnalysisResult(metrics={"custom_metric": 42}, notes=["Custom analysis completed"])

    # Test that extensions work correctly
    validation = CustomValidation(custom_param="strict")
    transformation = CustomTransformation()
    analysis = CustomAnalysis()

    expression = sqlglot.parse_one("SELECT 1", read="mysql")
    config = SQLConfig()

    val_result = validation.validate(expression, "mysql", config)
    trans_result = transformation.transform(expression, "mysql", config)
    anal_result = analysis.analyze(expression, "mysql", config)

    assert not val_result.is_safe  # Strict mode
    assert trans_result.modified  # Transformation applied
    assert anal_result.metrics["custom_metric"] == 42  # Custom analysis
