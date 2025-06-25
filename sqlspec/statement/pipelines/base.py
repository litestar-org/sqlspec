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
from sqlspec.statement.pipelines.context import PipelineResult
from sqlspec.statement.pipelines.result_types import ValidationError
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.statement.sql import SQLConfig, Statement


__all__ = ("ProcessorProtocol", "SQLValidator", "StatementPipeline", "UsesExpression")


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
            return sqlglot.parse_one(statement.sql, read=dialect)

        # Assuming statement is str hereafter
        sql_str = str(statement)
        if not sql_str or not sql_str.strip():
            return exp.Select()

        try:
            return sqlglot.parse_one(sql_str, read=dialect)
        except SQLGlotParseError as e:
            msg = f"SQL parsing failed: {e}"
            raise SQLValidationError(msg, sql_str, RiskLevel.HIGH) from e


class ProcessorProtocol(ABC):
    """Defines the interface for a single processing step in the SQL pipeline."""

    @abstractmethod
    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        """Processes an SQL expression.

        Args:
            expression: The SQL expression to process.
            context: The SQLProcessingContext holding the current state and config.

        Returns:
            The (possibly modified) SQL expression for transformers, or None for validators/analyzers.
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

    def execute_pipeline(self, context: "SQLProcessingContext") -> "PipelineResult":
        """Executes the full pipeline (transform, validate, analyze) using the SQLProcessingContext."""
        CorrelationContext.get()
        if context.current_expression is None:
            if context.config.enable_parsing:
                try:
                    context.current_expression = sqlglot.parse_one(context.initial_sql_string, dialect=context.dialect)
                except Exception as e:
                    error = ValidationError(
                        message=f"SQL Parsing Error: {e}",
                        code="parsing-error",
                        risk_level=RiskLevel.CRITICAL,
                        processor="StatementPipeline",
                        expression=None,
                    )
                    context.validation_errors.append(error)

                    return PipelineResult(expression=exp.Select(), context=context)
            else:
                # If parsing is disabled and no expression given, it's a config error for the pipeline.
                # However, SQL._initialize_statement should have handled this by not calling the pipeline
                # or by ensuring current_expression is set if enable_parsing is false.
                # For safety, we can raise or create an error result.

                error = ValidationError(
                    message="Pipeline executed without an initial expression and parsing disabled.",
                    code="no-expression",
                    risk_level=RiskLevel.CRITICAL,
                    processor="StatementPipeline",
                    expression=None,
                )
                context.validation_errors.append(error)

                return PipelineResult(
                    expression=exp.Select(),  # Default empty expression
                    context=context,
                )

        # 1. Transformation Stage
        if context.config.enable_transformations:
            for transformer in self.transformers:
                transformer_name = transformer.__class__.__name__
                try:
                    if context.current_expression is not None:
                        context.current_expression = transformer.process(context.current_expression, context)
                except Exception as e:
                    # Log transformation failure as a validation error

                    error = ValidationError(
                        message=f"Transformer {transformer_name} failed: {e}",
                        code="transformer-failure",
                        risk_level=RiskLevel.CRITICAL,
                        processor=transformer_name,
                        expression=context.current_expression,
                    )
                    context.validation_errors.append(error)
                    logger.exception("Transformer %s failed", transformer_name)
                    break

        # 2. Validation Stage
        if context.config.enable_validation:
            for validator_component in self.validators:
                validator_name = validator_component.__class__.__name__
                try:
                    # Validators process and add errors to context
                    if context.current_expression is not None:
                        validator_component.process(context.current_expression, context)
                except Exception as e:
                    # Log validator failure

                    error = ValidationError(
                        message=f"Validator {validator_name} failed: {e}",
                        code="validator-failure",
                        risk_level=RiskLevel.CRITICAL,
                        processor=validator_name,
                        expression=context.current_expression,
                    )
                    context.validation_errors.append(error)
                    logger.exception("Validator %s failed", validator_name)

        # 3. Analysis Stage
        if context.config.enable_analysis and context.current_expression is not None:
            for analyzer_component in self.analyzers:
                analyzer_name = analyzer_component.__class__.__name__
                try:
                    analyzer_component.process(context.current_expression, context)
                except Exception as e:
                    error = ValidationError(
                        message=f"Analyzer {analyzer_name} failed: {e}",
                        code="analyzer-failure",
                        risk_level=RiskLevel.MEDIUM,
                        processor=analyzer_name,
                        expression=context.current_expression,
                    )
                    context.validation_errors.append(error)
                    logger.exception("Analyzer %s failed", analyzer_name)

        return PipelineResult(expression=context.current_expression or exp.Select(), context=context)


class SQLValidator(ProcessorProtocol, UsesExpression):
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

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        """Process the expression through all configured validators.

        Args:
            expression: The SQL expression to validate.
            context: The SQLProcessingContext holding the current state and config.

        Returns:
            The expression unchanged (validators don't transform).
        """
        if expression is None:
            return None

        if not context.config.enable_validation:
            # Skip validation - add a skip marker to context
            return expression

        self._run_validators(expression, context)
        return expression

    @staticmethod
    def _validate_safely(
        validator_instance: "ProcessorProtocol", expression: "exp.Expression", context: "SQLProcessingContext"
    ) -> None:
        try:
            validator_instance.process(expression, context)
        except Exception as e:
            # Add error to context

            error = ValidationError(
                message=f"Validator {validator_instance.__class__.__name__} error: {e}",
                code="validator-error",
                risk_level=RiskLevel.CRITICAL,
                processor=validator_instance.__class__.__name__,
                expression=expression,
            )
            context.validation_errors.append(error)
            logger.warning("Individual validator %s failed: %s", validator_instance.__class__.__name__, e)

    def _run_validators(self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext") -> None:
        """Run all validators and handle exceptions."""
        if not expression:
            # If no expression, nothing to validate
            return
        for validator_instance in self.validators:
            self._validate_safely(validator_instance, expression, context)

    def validate(
        self, sql: "Statement", dialect: "DialectType", config: "Optional[SQLConfig]" = None
    ) -> "list[ValidationError]":
        """Convenience method to validate a raw SQL string or expression.

        Returns:
            List of ValidationError objects found during validation.
        """
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

        # Return the list of validation errors
        return list(validation_context.validation_errors)
