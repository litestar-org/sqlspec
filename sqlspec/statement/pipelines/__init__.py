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

from sqlspec.statement.pipelines import analyzers, transformers, validators
from sqlspec.statement.pipelines.analyzers import StatementAnalysis, StatementAnalyzer
from sqlspec.statement.pipelines.base import ProcessorProtocol, SQLValidator, StatementPipeline
from sqlspec.statement.pipelines.context import PipelineResult, SQLProcessingContext
from sqlspec.statement.pipelines.result_types import AnalysisFinding, TransformationLog, ValidationError
from sqlspec.statement.pipelines.transformers import (
    CommentRemover,
    ExpressionSimplifier,
    HintRemover,
    ParameterizeLiterals,
    SimplificationConfig,
)
from sqlspec.statement.pipelines.validators import (
    DMLSafetyConfig,
    DMLSafetyValidator,
    PerformanceConfig,
    PerformanceValidator,
    SecurityValidatorConfig,
)

__all__ = (
    # New Result Types
    "AnalysisFinding",
    # Concrete Transformers
    "CommentRemover",
    # Concrete Validators
    "DMLSafetyConfig",
    "DMLSafetyValidator",
    "ExpressionSimplifier",
    "HintRemover",
    "ParameterizeLiterals",
    "PerformanceConfig",
    "PerformanceValidator",
    # Core Pipeline Components
    "PipelineResult",
    "ProcessorProtocol",
    "SQLProcessingContext",
    # Base Validator
    "SQLValidator",
    "SecurityValidatorConfig",
    "SimplificationConfig",
    # Concrete Analyzers
    "StatementAnalysis",
    "StatementAnalyzer",
    # Core Pipeline & Context
    "StatementPipeline",
    "TransformationLog",
    "ValidationError",
    # Module exports
    "analyzers",
    "transformers",
    "validators",
)
