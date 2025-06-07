"""SQL Statement Processing Pipelines.

This module defines the framework for processing SQL statements through a series of
configurable stages: transformation, validation, and analysis.

Key Components:
- `SQLProcessingContext`: Holds shared data and state during pipeline execution.
- `StatementPipelineResult`: Encapsulates the final results of a pipeline run.
- `StatementPipeline`: The main orchestrator for executing the processing stages.
- `ProcessorProtocol`: The base protocol for all pipeline components (transformers,
  validators, analyzers).
- `ValidationResult`: Standardized result from validation components.
- `ValidationIssue`: Represents a single issue found during validation (to be defined,
  likely in `validators.base` or `base`).
"""

from sqlspec.statement.pipelines import analyzers, transformers, validators
from sqlspec.statement.pipelines.aggregator import AggregatedResults, ResultAggregator
from sqlspec.statement.pipelines.analyzers import (
    StatementAnalysis,
    StatementAnalyzer,
)
from sqlspec.statement.pipelines.base import (
    ProcessorProtocol,
    SQLValidator,
    StatementPipeline,
    ValidationResult,
)
from sqlspec.statement.pipelines.context import SQLProcessingContext, StatementPipelineResult
from sqlspec.statement.pipelines.transformers import (
    CommentRemover,
    HintRemover,
    ParameterizeLiterals,
)
from sqlspec.statement.pipelines.validators import (
    DMLSafetyConfig,
    DMLSafetyValidator,
    PerformanceConfig,
    PerformanceValidator,
)

__all__ = (
    # Result Aggregation
    "AggregatedResults",
    # Concrete Transformers
    "CommentRemover",
    # Concrete Validators (individual checks or groups)
    "DMLSafetyConfig",
    "DMLSafetyValidator",
    "HintRemover",
    "ParameterizeLiterals",
    "PerformanceConfig",
    "PerformanceValidator",
    "ProcessorProtocol",
    "ResultAggregator",
    "SQLProcessingContext",
    "SQLValidator",
    "StatementAnalysis",
    # Concrete Analyzers
    "StatementAnalyzer",
    # Core Pipeline & Context
    "StatementPipeline",
    "StatementPipelineResult",
    "ValidationResult",
    # Module exports
    "analyzers",
    "transformers",
    "validators",
)
