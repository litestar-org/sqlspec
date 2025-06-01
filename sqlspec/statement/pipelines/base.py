"""SQL Processing Pipeline Base.

This module defines the core framework for constructing and executing a series of
SQL processing steps, such as transformations and validations.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

import sqlglot  # Added
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError  # Added

from sqlspec.exceptions import RiskLevel, SQLValidationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.sql import SQLConfig, Statement

__all__ = (
    "AnalysisResult",
    "ProcessorProtocol",
    "SQLAnalysis",
    "SQLTransformation",
    "SQLTransformer",
    "SQLValidation",
    "SQLValidator",
    "TransformationResult",
    "TransformerPipeline",
    "UsesExpression",
    "ValidationResult",
)


logger = logging.getLogger("sqlspec")

ExpressionT = TypeVar("ExpressionT", bound="exp.Expression")


# Copied UsesExpression class here
class UsesExpression:
    """Utility mixin class to get a sqlglot expression from various inputs."""

    @staticmethod
    def get_expression(statement: "Statement", dialect: "DialectType" = None) -> "exp.Expression":
        """Convert SQL input to expression.

        Args:
            statement: The SQL statement to convert to an expression.
            dialect: The SQL dialect.

        Raises:
            SQLValidationError: If the SQL parsing fails.

        Returns:
            An exp.Expression.
        """
        if isinstance(statement, exp.Expression):
            return statement

        # Local import to avoid circular dependency at module level
        from sqlspec.statement.sql import SQL

        if isinstance(statement, SQL):
            expr = statement.expression
            if expr is not None:
                return expr
            # If SQL object has no expression (e.g. parsing disabled), parse its .sql string
            return sqlglot.parse_one(statement.sql, read=dialect)

        # Assuming statement is str hereafter
        sql_str = str(statement)
        if not sql_str or not sql_str.strip():
            return exp.Select()  # Return a neutral, empty expression for empty strings

        try:
            return sqlglot.parse_one(sql_str, read=dialect)
        except SQLGlotParseError as e:
            msg = f"SQL parsing failed: {e}"
            # Provide context (sql_str, risk_level) for the error
            raise SQLValidationError(msg, sql_str, RiskLevel.HIGH) from e


class ProcessorProtocol(ABC, Generic[ExpressionT]):
    """Protocol for a processing step in the SQL pipeline.

    A processor can either transform an expression, validate it, or both.
    """

    @abstractmethod
    def process(
        self,
        expression: "ExpressionT",
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[ExpressionT, Optional[ValidationResult]]":
        """Process the SQL expression.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect.
            config: The SQLConfig, providing context like strict_mode.

        Returns:
            A tuple containing the (potentially modified) expression
            and an optional ValidationResult if this processor performs validation.
        """
        raise NotImplementedError


@dataclass
class TransformerPipeline:
    """Orchestrates a sequence of SQL processing steps."""

    components: "list[ProcessorProtocol[exp.Expression]]" = field(default_factory=list)

    def execute(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, ValidationResult]":
        """Executes all components in the pipeline.

        Args:
            expression: The initial SQL expression.
            dialect: The SQL dialect.
            config: The SQLConfig to provide context to processors and for final validation aggregation.

        Returns:
            A tuple containing the final transformed expression and an aggregated ValidationResult.
        """
        # Need to import ValidationResult here for instantiation if it's not at top level due to TYPE_CHECKING

        current_expression = expression
        aggregated_issues: list[str] = []
        aggregated_warnings: list[str] = []
        # Ensure RiskLevel.SAFE is used correctly
        overall_risk_level = RiskLevel.SAFE
        final_is_safe = True

        if not self.components and config is not None and config.enable_validation:
            # This case implies default validation might be needed,
            # which SQLConfig/SQLStatement should set up by providing default components.
            pass  # SQLStatement handles adding default validator if components list is empty

        for component in self.components:
            current_expression, validation_result_component = component.process(current_expression, dialect, config)
            if validation_result_component:
                aggregated_issues.extend(validation_result_component.issues)
                aggregated_warnings.extend(validation_result_component.warnings)
                if validation_result_component.risk_level.value > overall_risk_level.value:
                    overall_risk_level = validation_result_component.risk_level
                if not validation_result_component.is_safe:
                    final_is_safe = False

        return current_expression, ValidationResult(
            is_safe=final_is_safe and not aggregated_issues,
            risk_level=overall_risk_level,
            issues=aggregated_issues,
            warnings=aggregated_warnings,
        )


class SQLValidation(ABC):
    """Abstract base class for individual SQL validation checks."""

    def __init__(self, risk_level: "RiskLevel", min_risk_to_raise: "Optional[RiskLevel]" = None) -> None:
        self.risk_level = risk_level
        self.min_risk_to_raise = min_risk_to_raise

    @abstractmethod
    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> "ValidationResult":
        """Validate the SQL expression.

        :param expression: The SQL expression to validate.
        :param dialect: The SQL dialect.
        :param config: The SQL configuration.
        :param kwargs: Additional keyword arguments.
        :return: A ValidationResult.
        """
        raise NotImplementedError


class SQLValidator(ProcessorProtocol[exp.Expression]):
    """Main SQL validator that orchestrates multiple validation checks.
    This class functions as a validation pipeline runner.
    """

    def __init__(
        self,
        validators: "Optional[Sequence[SQLValidation]]" = None,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.HIGH,
    ) -> None:
        self.validators: list[SQLValidation] = list(validators) if validators is not None else []
        self.min_risk_to_raise = min_risk_to_raise
        # Ensure BaseSQLValidator is imported for the type hint at runtime if needed

    def add_validator(self, validator: "SQLValidation") -> None:  # Type hint should now work
        """Add a validator to the pipeline."""
        self.validators.append(validator)

    def process(
        self,
        expression: "exp.Expression",
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Process the SQL expression through all configured validators.

        Args:
            expression: The SQL expression to validate.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            A tuple containing the final transformed expression and an aggregated ValidationResult.
        """
        from sqlspec.statement.sql import SQLConfig

        active_config = config if config is not None else SQLConfig()

        if not active_config.enable_validation:
            return expression, ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)

        aggregated_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

        for validator_instance in self.validators:
            result = validator_instance.validate(expression, dialect, active_config, **kwargs)
            aggregated_result.merge(result)

        return expression, aggregated_result

    def validate(
        self,
        sql: "Statement",
        dialect: "DialectType",
        config: "Optional[SQLConfig]" = None,
    ) -> "ValidationResult":
        """Convenience method to validate a raw SQL string or expression.

        Args:
            sql: The SQL statement to validate.
            dialect: The SQL dialect.
            config: The SQL configuration.

        Returns:
            A ValidationResult.
        """
        from sqlspec.statement.sql import SQLConfig

        current_config = config or SQLConfig()
        expression_to_validate = UsesExpression.get_expression(sql, dialect=dialect)

        _, validation_result = self.process(expression_to_validate, dialect, current_config)
        return validation_result or ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)


class ValidationResult:
    """Result of SQL validation with detailed information."""

    __slots__ = (
        "is_safe",
        "issues",
        "risk_level",
        "transformed_sql",
        "warnings",
    )

    def __init__(
        self,
        is_safe: bool,
        risk_level: "RiskLevel",
        issues: "Optional[list[str]]" = None,
        warnings: "Optional[list[str]]" = None,
        transformed_sql: "Optional[str]" = None,
    ) -> None:
        self.is_safe = is_safe
        self.risk_level = risk_level
        self.issues = issues if issues is not None else []
        self.warnings = warnings if warnings is not None else []
        self.transformed_sql = transformed_sql  # Though likely not used by validators

    def merge(self, other: "ValidationResult") -> None:
        """Merge another ValidationResult into this one."""
        if not other.is_safe:
            self.is_safe = False
        self.issues.extend(other.issues)
        self.warnings.extend(other.warnings)
        # Set risk level to the higher of the two
        if other.risk_level.value > self.risk_level.value:
            self.risk_level = other.risk_level

    def __bool__(self) -> bool:
        return self.is_safe


class TransformationResult:
    """Result of SQL transformation with detailed information."""

    __slots__ = (
        "expression",
        "modified",
        "notes",
    )

    def __init__(
        self,
        expression: exp.Expression,
        modified: bool,
        notes: "Optional[list[str]]" = None,
    ) -> None:
        self.expression = expression
        self.modified = modified
        self.notes = notes if notes is not None else []

    def __bool__(self) -> bool:
        return self.modified


class SQLTransformer(ProcessorProtocol[exp.Expression], ABC):
    """Base class for SQL transformers that implement ProcessorProtocol.

    This provides a bridge between the old SQLTransformation pattern and
    the new ProcessorProtocol pipeline architecture.
    """

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Process the expression using the transform method.

        Args:
            expression: The SQL expression to transform.
            dialect: The SQL dialect.
            config: The SQL configuration.

        Returns:
            A tuple containing the transformed expression and None for ValidationResult.
        """
        from sqlspec.statement.sql import SQLConfig

        active_config = config if config is not None else SQLConfig()

        if not active_config.enable_transformations:
            return expression, None

        try:
            result = self.transform(expression, dialect, active_config)
            return result.expression, None
        except Exception as e:
            # If transformation fails, return original expression
            logger.warning("Transformation failed: %s", e)
            return expression, None

    @abstractmethod
    def transform(
        self,
        expression: exp.Expression,
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: Any,
    ) -> TransformationResult:
        """Transform the SQL expression.

        Args:
            expression: The SQL expression to transform.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            A TransformationResult.
        """
        raise NotImplementedError


class AnalysisResult:
    """Result of SQL analysis with detailed metrics and insights."""

    __slots__ = (
        "issues",
        "metrics",
        "notes",
        "warnings",
    )

    def __init__(
        self,
        metrics: "Optional[dict[str, Any]]" = None,
        warnings: "Optional[list[str]]" = None,
        issues: "Optional[list[str]]" = None,
        notes: "Optional[list[str]]" = None,
    ) -> None:
        self.metrics = metrics if metrics is not None else {}
        self.warnings = warnings if warnings is not None else []
        self.issues = issues if issues is not None else []
        self.notes = notes if notes is not None else []

    def __bool__(self) -> bool:
        return len(self.issues) == 0


class SQLTransformation(ABC):
    """Abstract base class for SQL transformations."""

    @abstractmethod
    def transform(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> TransformationResult:
        """Transform the SQL expression.

        Args:
            expression: The SQL expression to transform.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            A TransformationResult.
        """
        raise NotImplementedError


class SQLAnalysis(ABC):
    """Abstract base class for SQL analysis."""

    @abstractmethod
    def analyze(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> AnalysisResult:
        """Analyze the SQL expression.

        Args:
            expression: The SQL expression to analyze.
            dialect: The SQL dialect.
            config: The SQL configuration.
            kwargs: Additional keyword arguments.

        Returns:
            An AnalysisResult.
        """
        raise NotImplementedError
