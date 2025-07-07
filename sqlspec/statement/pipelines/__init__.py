"""SQL Statement Processing Pipelines.

This module defines the framework for processing SQL statements through a series of
configurable stages: transformation, validation, and analysis.

Key Components:
- `SQLProcessingContext`: Holds shared data and state during pipeline execution.
- `StatementPipelineResult`: Encapsulates the final results of a pipeline run.
- `StatementPipeline`: The main orchestrator for executing the processing stages.
- `ProcessorProtocol`: The base protocol for all pipeline components (transformers,
  validators, analyzers).
- `ValidationError`: Represents a single issue found during validation.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import sqlglot
from sqlglot import exp
from typing_extensions import TypeVar

from sqlspec.exceptions import RiskLevel
from sqlspec.protocols import ProcessorProtocol
from sqlspec.statement.pipelines.context import (
    AnalysisFinding,
    SQLProcessingContext,
    TransformationLog,
    ValidationError,
)
from sqlspec.statement.pipelines.transformers import (
    CommentAndHintRemover,
    ExpressionSimplifier,
    ParameterizeLiterals,
    SimplificationConfig,
)
from sqlspec.statement.pipelines.validators import (
    DMLSafetyConfig,
    DMLSafetyValidator,
    ParameterStyleValidator,
    PerformanceConfig,
    PerformanceValidator,
    SecurityValidator,
    SecurityValidatorConfig,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.statement.parameters import ParameterInfo
    from sqlspec.typing import SQLParameterType


__all__ = (
    "AnalysisFinding",
    "CommentAndHintRemover",
    "DMLSafetyConfig",
    "DMLSafetyValidator",
    "ExpressionSimplifier",
    "ParameterStyleValidator",
    "ParameterizeLiterals",
    "PerformanceConfig",
    "PerformanceValidator",
    "PipelineResult",
    "ProcessorProtocol",
    "SQLProcessingContext",
    "SecurityValidator",
    "SecurityValidatorConfig",
    "SimplificationConfig",
    "StatementPipeline",
    "TransformationLog",
    "ValidationError",
)

logger = get_logger("pipelines")

ExpressionT = TypeVar("ExpressionT", bound="exp.Expression")
ResultT = TypeVar("ResultT")


# Import from context module to avoid duplication


@dataclass
class PipelineResult:
    """Final result of pipeline execution."""

    expression: exp.Expression
    """The SQL expression after all transformations."""

    context: SQLProcessingContext
    """Contains all collected results."""

    @property
    def validation_errors(self) -> list[ValidationError]:
        """Get validation errors from context."""
        return self.context.validation_errors

    @property
    def has_errors(self) -> bool:
        """Check if any validation errors exist."""
        return self.context.has_errors

    @property
    def risk_level(self) -> RiskLevel:
        """Get overall risk level."""
        return self.context.risk_level

    @property
    def merged_parameters(self) -> "SQLParameterType":
        """Get merged parameters from context."""
        return self.context.merged_parameters

    @property
    def parameter_info(self) -> "list[ParameterInfo]":
        """Get parameter info from context."""
        return self.context.parameter_info


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

    def _run_processors(
        self,
        processors: list[ProcessorProtocol],
        context: SQLProcessingContext,
        processor_type: str,
        enable_flag: bool,
        error_risk_level: RiskLevel,
    ) -> None:
        if not enable_flag or context.current_expression is None:
            return

        for processor in processors:
            processor_name = processor.__class__.__name__
            try:
                if processor_type == "transformer":
                    context.current_expression = processor.process(context.current_expression, context)
                else:
                    processor.process(context.current_expression, context)
            except Exception as e:
                # In strict mode, re-raise validation exceptions
                from sqlspec.exceptions import MissingParameterError
                from sqlspec.statement.pipelines.validators._parameter_style import (
                    MixedParameterStyleError,
                    UnsupportedParameterStyleError,
                )

                if not context.config.parse_errors_as_warnings and isinstance(
                    e, (MissingParameterError, MixedParameterStyleError, UnsupportedParameterStyleError)
                ):
                    raise

                error = ValidationError(
                    message=f"{processor_type.capitalize()} {processor_name} failed: {e}",
                    code=f"{processor_type}-failure",
                    risk_level=error_risk_level,
                    processor=processor_name,
                    expression=context.current_expression,
                )
                context.validation_errors.append(error)
                logger.exception("%s %s failed", processor_type.capitalize(), processor_name)
                if processor_type == "transformer":
                    break  # Stop further transformations if one fails

    def execute_pipeline(self, context: "SQLProcessingContext") -> "PipelineResult":
        """Executes the full pipeline (transform, validate, analyze) using the SQLProcessingContext."""
        CorrelationContext.get()
        if context.current_expression is None:
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

        # Run transformers
        if self.transformers:
            self._run_processors(
                self.transformers, context, "transformer", enable_flag=True, error_risk_level=RiskLevel.CRITICAL
            )

        # Run validators
        if self.validators:
            self._run_processors(
                self.validators, context, "validator", enable_flag=True, error_risk_level=RiskLevel.CRITICAL
            )

        # Run analyzers
        if self.analyzers:
            self._run_processors(
                self.analyzers, context, "analyzer", enable_flag=True, error_risk_level=RiskLevel.MEDIUM
            )

        return PipelineResult(expression=context.current_expression or exp.Select(), context=context)
