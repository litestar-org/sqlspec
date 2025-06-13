"""SQL Processing Pipeline Base.

This module defines the core framework for constructing and executing a series of
SQL processing steps, such as transformations and validations.
"""

import contextlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

import sqlglot  # Added
from sqlglot import exp
from sqlglot.errors import ParseError as SQLGlotParseError  # Added
from typing_extensions import TypeVar

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.pipelines.context import SQLProcessingContext, StatementPipelineResult
from sqlspec.statement.pipelines.results import ValidationResult
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig, Statement


__all__ = (
    "ProcessorProtocol",
    "SQLValidator",
    "StatementPipeline",
    "UsesExpression",
    "ValidationResult",
)


logger = get_logger("pipelines")

ExpressionT = TypeVar("ExpressionT", bound="exp.Expression")
ResultT = TypeVar("ResultT")


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


class ProcessorProtocol(ABC):
    """Defines the interface for a single processing step in the SQL pipeline."""

    @abstractmethod
    def process(self, expression: "exp.Expression", context: "SQLProcessingContext") -> "exp.Expression":
        """Processes an SQL expression.

        Args:
            expression: The SQL expression to process.
            context: The SQLProcessingContext holding the current state and config.

        Returns:
            The (possibly modified) SQL expression.
        """
        raise NotImplementedError


class StatementPipeline:
    """Orchestrates the processing of an SQL expression through transformers, validators, and analyzers."""

    def __init__(
        self,
        transformers: Optional[list[ProcessorProtocol]] = None,
        validators: Optional[list[ProcessorProtocol]] = None,
        analyzers: Optional[list[ProcessorProtocol]] = None,
    ) -> None:
        self.transformers = transformers or []
        self.validators = validators or []
        self.analyzers = analyzers or []

    def execute_pipeline(self, context: "SQLProcessingContext") -> "StatementPipelineResult":
        """Executes the full pipeline (transform, validate, analyze) using the SQLProcessingContext."""
        correlation_id = CorrelationContext.get()

        # Log pipeline start with context
        if context.config.debug_mode:
            logger.debug(
                "Starting SQL pipeline processing",
                extra={
                    "sql_length": len(context.initial_sql_string),
                    "transformer_count": len(self.transformers),
                    "validator_count": len(self.validators),
                    "analyzer_count": len(self.analyzers),
                    "correlation_id": correlation_id,
                },
            )

        # Ensure initial expression is in context
        if context.current_expression is None:
            if context.config.enable_parsing:
                from sqlspec.statement.sql import SQL  # For to_expression

                try:
                    # Parsing is the first implicit step if no expression is in context
                    context.current_expression = SQL.to_expression(context.initial_sql_string, context.dialect)
                except Exception as e:
                    # Populate validation_result with parsing error and return immediately
                    context.validation_result = ValidationResult(
                        is_safe=False, risk_level=RiskLevel.CRITICAL, issues=[f"SQL Parsing Error: {e}"]
                    )
                    # Return a result indicating parsing failure
                    return StatementPipelineResult(
                        final_expression=None,
                        merged_parameters=context.merged_parameters,
                        parameter_info=context.parameter_info,
                        validation_result=context.validation_result,
                        analysis_result=None,
                        input_sql_had_placeholders=context.input_sql_had_placeholders,
                    )
            else:
                # If parsing is disabled and no expression given, it's a config error for the pipeline.
                # However, SQL._initialize_statement should have handled this by not calling the pipeline
                # or by ensuring current_expression is set if enable_parsing is false.
                # For safety, we can raise or create an error result.
                context.validation_result = ValidationResult(
                    is_safe=False,
                    risk_level=RiskLevel.CRITICAL,
                    issues=["Pipeline executed without an initial expression and parsing disabled."],
                )
                return StatementPipelineResult(
                    final_expression=None,
                    merged_parameters=context.merged_parameters,
                    parameter_info=context.parameter_info,
                    validation_result=context.validation_result,
                    analysis_result=None,
                    input_sql_had_placeholders=context.input_sql_had_placeholders,
                )

        # 1. Transformation Stage
        if context.config.enable_transformations:
            for transformer in self.transformers:
                transformer_name = transformer.__class__.__name__
                if context.config.debug_mode:
                    logger.debug(
                        "Running transformer: %s",
                        transformer_name,
                        extra={
                            "transformer": transformer_name,
                            "correlation_id": correlation_id,
                        },
                    )
                try:
                    context.current_expression = transformer.process(context.current_expression, context)
                except Exception as e:
                    # Log transformation failure as a validation error
                    from sqlspec.statement.pipelines.result_types import ValidationError as ValError
                    error = ValError(
                        message=f"Transformer {transformer_name} failed: {e}",
                        code="transformer-failure",
                        risk_level=RiskLevel.CRITICAL,
                        processor=transformer_name,
                        expression=context.current_expression,
                    )
                    context.validation_errors.append(error)
                    logger.error("Transformer %s failed: %s", transformer_name, e)
                    break

        # 2. Validation Stage
        if context.config.enable_validation:
            for validator_component in self.validators:
                validator_name = validator_component.__class__.__name__
                if context.config.debug_mode:
                    logger.debug(
                        "Running validator: %s",
                        validator_name,
                        extra={
                            "validator": validator_name,
                            "correlation_id": correlation_id,
                        },
                    )
                try:
                    # Validators process and add errors to context
                    validator_component.process(context.current_expression, context)
                except Exception as e:
                    # Log validator failure
                    from sqlspec.statement.pipelines.result_types import ValidationError as ValError
                    error = ValError(
                        message=f"Validator {validator_name} failed: {e}",
                        code="validator-failure", 
                        risk_level=RiskLevel.CRITICAL,
                        processor=validator_name,
                        expression=context.current_expression,
                    )
                    context.validation_errors.append(error)
                    logger.error("Validator %s failed: %s", validator_name, e)

        # 3. Analysis Stage
        if context.config.enable_analysis and context.current_expression is not None:
            for analyzer_component in self.analyzers:
                analyzer_name = analyzer_component.__class__.__name__
                if context.config.debug_mode:
                    logger.debug(
                        "Running analyzer: %s",
                        analyzer_name,
                        extra={
                            "analyzer": analyzer_name,
                            "correlation_id": correlation_id,
                        },
                    )
                try:
                    # Analyzers process and add findings to context
                    analyzer_component.process(context.current_expression, context)
                except Exception as e:
                    # Log analyzer failure
                    from sqlspec.statement.pipelines.result_types import ValidationError as ValError
                    error = ValError(
                        message=f"Analyzer {analyzer_name} failed: {e}",
                        code="analyzer-failure",
                        risk_level=RiskLevel.MEDIUM,  # Analyzer failures are less critical
                        processor=analyzer_name,
                        expression=context.current_expression,
                    )
                    context.validation_errors.append(error)
                    logger.error("Analyzer %s failed: %s", analyzer_name, e)

        # Return new PipelineResult
        from sqlspec.statement.pipelines.context import PipelineResult
        
        # Also create legacy result for backward compatibility
        validation_result = None
        if context.validation_errors:
            validation_result = ValidationResult(
                is_safe=not context.has_errors,
                risk_level=context.risk_level,
                issues=[error.message for error in context.validation_errors],
            )
        
        return StatementPipelineResult(
            final_expression=context.current_expression,
            merged_parameters=context.merged_parameters,
            parameter_info=context.parameter_info,
            validation_result=validation_result,
            analysis_result=None,  # Legacy field
            input_sql_had_placeholders=context.input_sql_had_placeholders,
            aggregated_results=None,  # Legacy field
        )


class SQLValidator(UsesExpression):
    """Main SQL validator that orchestrates multiple validation checks.
    This class functions as a validation pipeline runner.
    """

    def __init__(
        self,
        validators: "Optional[Sequence[ProcessorProtocol]]" = None,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.HIGH,
    ) -> None:
        self.validators: list[ProcessorProtocol] = list(validators) if validators is not None else []
        self.min_risk_to_raise = min_risk_to_raise

    def add_validator(self, validator: "ProcessorProtocol") -> None:
        """Add a validator to the pipeline."""
        self.validators.append(validator)

    def process(self, expression: "exp.Expression", context: "SQLProcessingContext") -> "exp.Expression":
        """Process the expression through all configured validators.

        Args:
            expression: The SQL expression to validate.
            context: The SQLProcessingContext holding the current state and config.

        Returns:
            The expression unchanged (validators don't transform).
        """
        if not context.config.enable_validation:
            # Skip validation - add a skip marker to context
            return expression

        for validator_instance in self.validators:
            try:
                validator_instance.process(expression, context)
            except Exception as e:
                # Add error to context
                from sqlspec.statement.pipelines.result_types import ValidationError as ValError
                error = ValError(
                    message=f"Validator {validator_instance.__class__.__name__} error: {e}",
                    code="validator-error",
                    risk_level=RiskLevel.CRITICAL,
                    processor=validator_instance.__class__.__name__,
                    expression=expression,
                )
                context.validation_errors.append(error)
                logger.warning("Individual validator %s failed: %s", validator_instance.__class__.__name__, e)

        return expression

    def validate(
        self,
        sql: "Statement",
        dialect: "DialectType",
        config: "Optional[SQLConfig]" = None,
    ) -> "ValidationResult":
        """Convenience method to validate a raw SQL string or expression."""
        from sqlspec.statement.pipelines.context import SQLProcessingContext  # Local import for context
        from sqlspec.statement.sql import SQLConfig  # Local import for SQL.to_expression

        current_config = config or SQLConfig()
        expression_to_validate = self.get_expression(sql, dialect=dialect)

        # Create a context for this validation run
        validation_context = SQLProcessingContext(
            initial_sql_string=str(sql),
            dialect=dialect,
            config=current_config,
            current_expression=expression_to_validate,
            initial_expression=expression_to_validate,
            # Other context fields like parameters might not be strictly necessary for all validators
            # but good to pass if available or if validators might need them.
            # For a standalone validate() call, parameter context might be minimal.
            input_sql_had_placeholders=False,  # Assume false for raw validation, or detect
        )
        if isinstance(sql, str):
            with contextlib.suppress(Exception):
                param_val = current_config.parameter_validator
                if param_val.extract_parameters(sql):
                    validation_context.input_sql_had_placeholders = True

        self.process(expression_to_validate, validation_context)
        
        # Convert context errors to legacy ValidationResult
        if validation_context.validation_errors:
            return ValidationResult(
                is_safe=False,
                risk_level=validation_context.risk_level,
                issues=[error.message for error in validation_context.validation_errors],
            )
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)
