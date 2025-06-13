# Result Aggregation System for SQL Processing Pipeline
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.results import ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.analyzers import StatementAnalysis
    from sqlspec.statement.pipelines.results import ProcessorResult

__all__ = ("AggregatedResults", "ResultAggregator")


@dataclass
class AggregatedResults:
    """Aggregated results from all pipeline components.

    This class collects and consolidates results from transformers, validators,
    and analyzers into a unified view for easy access and reporting.
    """

    # Validation aggregation
    validation_result: "ValidationResult" = field(
        default_factory=lambda: ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)
    )
    validator_results: "dict[str, ValidationResult]" = field(default_factory=dict)

    # Analysis aggregation
    analysis_result: "Optional[StatementAnalysis]" = None
    analyzer_results: "dict[str, StatementAnalysis]" = field(default_factory=dict)

    # Transformation aggregation
    was_transformed: bool = False
    transformations_applied: "list[str]" = field(default_factory=list)
    parameters_extracted: int = 0
    comments_removed: int = 0

    # Metadata aggregation
    total_processors_run: int = 0
    failed_processors: "list[str]" = field(default_factory=list)
    processing_time_ms: float = 0.0

    # Component-specific metadata
    component_metadata: "dict[str, dict[str, Any]]" = field(default_factory=dict)

    def add_processor_result(self, processor_name: str, result: "ProcessorResult") -> None:
        """Add a generic processor result."""
        self.total_processors_run += 1

        if result.validation_result:
            self._add_validation_result(processor_name, result.validation_result)

        if result.analysis_result:
            self._add_analysis_result(processor_name, result.analysis_result)

        if result.metadata:
            if "transformer" in result.metadata.get("type", ""):
                self.add_transformation_info(processor_name, result.metadata)
            else:
                self.component_metadata[processor_name] = result.metadata

    def _add_validation_result(self, validator_name: str, result: "ValidationResult") -> None:
        """Add a validation result from a specific validator."""
        self.validator_results[validator_name] = result
        self.validation_result.merge(result)

    def _add_analysis_result(self, analyzer_name: str, analysis: "StatementAnalysis") -> None:
        """Add an analysis result from a specific analyzer."""
        self.analysis_result = analysis
        self.analyzer_results[analyzer_name] = analysis

        # Store key analysis metrics in component metadata
        self.component_metadata[analyzer_name] = {
            "tables": list(analysis.tables),
            "columns": list(analysis.columns),
            "operations": analysis.operations,
            "has_aggregation": analysis.has_aggregation,
            "has_window_functions": analysis.has_window_functions,
            "cte_count": analysis.cte_count,
            "complexity_score": analysis.complexity_score,
        }

    def add_transformation_info(self, transformer_name: str, info: "dict[str, Any]") -> None:
        """Add transformation information."""
        self.was_transformed = True
        if transformer_name not in self.transformations_applied:
            self.transformations_applied.append(transformer_name)

        if "parameters_extracted" in info:
            self.parameters_extracted += info.get("parameters_extracted", 0)
        if "comments_removed" in info:
            self.comments_removed += info.get("comments_removed", 0)

        self.component_metadata[transformer_name] = info

    def mark_processor_failed(self, processor_name: str, error: str) -> None:
        """Mark a processor as failed."""
        if processor_name not in self.failed_processors:
            self.failed_processors.append(processor_name)
        self.component_metadata[f"{processor_name}_error"] = {"error": error}

    def merge(self, other: "AggregatedResults") -> None:
        """Merge another AggregatedResults into this one."""
        self.validation_result.merge(other.validation_result)
        self.validator_results.update(other.validator_results)

        if other.analysis_result:
            self.analysis_result = other.analysis_result
        self.analyzer_results.update(other.analyzer_results)

        self.was_transformed = self.was_transformed or other.was_transformed
        self.transformations_applied.extend(
            t for t in other.transformations_applied if t not in self.transformations_applied
        )
        self.parameters_extracted += other.parameters_extracted
        self.comments_removed += other.comments_removed

        self.total_processors_run += other.total_processors_run
        self.failed_processors.extend(p for p in other.failed_processors if p not in self.failed_processors)
        self.processing_time_ms += other.processing_time_ms

        self.component_metadata.update(other.component_metadata)

    def get_summary(self) -> "dict[str, Any]":
        """Get a summary of all aggregated results."""
        summary = {
            "overall_risk_level": self.validation_result.risk_level.name,
            "is_safe": self.validation_result.is_safe,
            "total_issues": len(self.validation_result.issues),
            "total_warnings": len(self.validation_result.warnings),
            "was_transformed": self.was_transformed,
            "transformations_count": len(self.transformations_applied),
            "processors_run": self.total_processors_run,
            "processors_failed": len(self.failed_processors),
            "processing_time_ms": self.processing_time_ms,
        }
        if self.analysis_result:
            summary.update(
                {
                    "has_analysis": True,
                    "table_count": len(self.analysis_result.tables),
                    "join_count": self.analysis_result.join_count,
                    "complexity_score": self.analysis_result.complexity_score,
                }
            )
        else:
            summary.update(
                {
                    "has_analysis": False,
                    "table_count": 0,
                    "join_count": 0,
                    "complexity_score": 0,
                }
            )
        return summary

    def to_dict(self) -> "dict[str, Any]":
        """Convert aggregated results to a dictionary."""
        return {
            "summary": self.get_summary(),
            "validation": {
                "risk_level": self.validation_result.risk_level.name,
                "is_safe": self.validation_result.is_safe,
                "issues": self.validation_result.issues,
                "warnings": self.validation_result.warnings,
            },
            "analysis": {
                "has_analysis": bool(self.analysis_result),
                "details": self.component_metadata.get("StatementAnalyzer", {}),
            },
            "transformations": {
                "was_transformed": self.was_transformed,
                "applied": self.transformations_applied,
                "parameters_extracted": self.parameters_extracted,
                "comments_removed": self.comments_removed,
            },
            "processing": {
                "processors_run": self.total_processors_run,
                "failed_processors": self.failed_processors,
                "processing_time_ms": self.processing_time_ms,
            },
            "component_details": self.component_metadata,
        }


class ResultAggregator:
    """Aggregates results from multiple pipeline components."""

    def __init__(self) -> None:
        self._results = AggregatedResults()

    @property
    def results(self) -> AggregatedResults:
        """Get the aggregated results."""
        return self._results

    def aggregate_processor_results(self, results: "list[tuple[str, ProcessorResult]]") -> AggregatedResults:
        """Aggregate results from multiple processors."""
        for processor_name, result in results:
            self._results.add_processor_result(processor_name, result)
        return self._results

    def add_timing_info(self, processing_time_ms: float) -> None:
        """Add processing time information."""
        self._results.processing_time_ms = processing_time_ms

    def reset(self) -> None:
        """Reset the aggregator for a new aggregation."""
        self._results = AggregatedResults()

    def merge_with(self, other: "AggregatedResults") -> None:
        """Merge another aggregated result into this one."""
        self._results.merge(other)
