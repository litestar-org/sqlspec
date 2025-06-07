# Result Aggregation System for SQL Processing Pipeline
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import RiskLevel

if TYPE_CHECKING:
    from sqlspec.statement.pipelines import ValidationResult
    from sqlspec.statement.pipelines.analyzers import StatementAnalysis
    from sqlspec.statement.pipelines.validators.base import ProcessorResult

__all__ = ("AggregatedResults", "ResultAggregator")


@dataclass
class AggregatedResults:
    """Aggregated results from all pipeline components.

    This class collects and consolidates results from transformers, validators,
    and analyzers into a unified view for easy access and reporting.
    """

    # Validation aggregation
    overall_risk_level: RiskLevel = RiskLevel.SKIP
    is_safe: bool = True
    all_issues: "list[str]" = field(default_factory=list)
    all_warnings: "list[str]" = field(default_factory=list)
    validator_results: "dict[str, ValidationResult]" = field(default_factory=dict)

    # Analysis aggregation
    has_analysis: bool = False
    table_count: int = 0
    join_count: int = 0
    subquery_count: int = 0
    complexity_score: int = 0
    performance_issues: "list[dict[str, Any]]" = field(default_factory=list)
    security_issues: "list[dict[str, Any]]" = field(default_factory=list)

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

    def add_validation_result(self, validator_name: str, result: "ValidationResult") -> None:
        """Add a validation result from a specific validator.

        Args:
            validator_name: Name of the validator
            result: Validation result to add
        """
        self.validator_results[validator_name] = result

        # Update overall risk level (take the highest)
        if result.risk_level and result.risk_level.value > self.overall_risk_level.value:
            self.overall_risk_level = result.risk_level

        # Update safety status
        if not result.is_safe:
            self.is_safe = False

        # Collect issues and warnings
        if result.issues:
            self.all_issues.extend(result.issues)
        if result.warnings:
            self.all_warnings.extend(result.warnings)

    def add_analysis_result(self, analyzer_name: str, analysis: "StatementAnalysis") -> None:
        """Add an analysis result from a specific analyzer.

        Args:
            analyzer_name: Name of the analyzer
            analysis: Analysis result to add
        """
        self.has_analysis = True

        # Aggregate counts
        self.table_count = len(analysis.tables)
        self.join_count = analysis.join_count
        self.subquery_count = analysis.subquery_count

        # Store full analysis in metadata
        self.component_metadata[analyzer_name] = {
            "tables": list(analysis.tables),
            "columns": list(analysis.columns),
            "operations": analysis.operations,
            "has_aggregation": analysis.has_aggregation,
            "has_window_functions": analysis.has_window_functions,
            "cte_count": analysis.cte_count,
        }

    def add_transformation_info(self, transformer_name: str, info: "dict[str, Any]") -> None:
        """Add transformation information.

        Args:
            transformer_name: Name of the transformer
            info: Transformation details
        """
        self.was_transformed = True
        self.transformations_applied.append(transformer_name)

        # Extract common metrics
        if "parameters_extracted" in info:
            self.parameters_extracted += info["parameters_extracted"]
        if "comments_removed" in info:
            self.comments_removed += info["comments_removed"]

        # Store full info in metadata
        self.component_metadata[transformer_name] = info

    def add_processor_result(self, processor_name: str, result: "ProcessorResult") -> None:
        """Add a generic processor result.

        Args:
            processor_name: Name of the processor
            result: Processor result to add
        """
        self.total_processors_run += 1

        # Handle validation results
        if result.validation_result:
            self.add_validation_result(processor_name, result.validation_result)

        # Handle analysis results
        if result.analysis_result:
            self.add_analysis_result(processor_name, result.analysis_result)

        # Handle metadata
        if result.metadata:
            # Check for specific issue types
            if "performance_issues" in result.metadata:
                self.performance_issues.extend(result.metadata["performance_issues"])
            if "security_issues" in result.metadata:
                self.security_issues.extend(result.metadata["security_issues"])
            if "safety_issues" in result.metadata:
                self.security_issues.extend(result.metadata["safety_issues"])

            # Store all metadata
            self.component_metadata[processor_name] = result.metadata

    def mark_processor_failed(self, processor_name: str, error: str) -> None:
        """Mark a processor as failed.

        Args:
            processor_name: Name of the failed processor
            error: Error message
        """
        self.failed_processors.append(processor_name)
        self.component_metadata[f"{processor_name}_error"] = {"error": error}

    def get_summary(self) -> "dict[str, Any]":
        """Get a summary of all aggregated results.

        Returns:
            Dictionary containing summary information
        """
        return {
            "overall_risk_level": self.overall_risk_level.name,
            "is_safe": self.is_safe,
            "total_issues": len(self.all_issues),
            "total_warnings": len(self.all_warnings),
            "was_transformed": self.was_transformed,
            "transformations_count": len(self.transformations_applied),
            "has_analysis": self.has_analysis,
            "complexity_score": self.complexity_score,
            "performance_issues_count": len(self.performance_issues),
            "security_issues_count": len(self.security_issues),
            "processors_run": self.total_processors_run,
            "processors_failed": len(self.failed_processors),
            "processing_time_ms": self.processing_time_ms,
        }

    def get_recommendations(self) -> "list[str]":
        """Get all recommendations from aggregated results.

        Returns:
            List of unique recommendations
        """
        recommendations = set()

        # From performance issues
        for issue in self.performance_issues:
            if "recommendation" in issue:
                recommendations.add(issue["recommendation"])

        # From security issues
        for issue in self.security_issues:
            if "recommendation" in issue:
                recommendations.add(issue["recommendation"])

        # From component metadata
        for metadata in self.component_metadata.values():
            if (
                isinstance(metadata, dict)
                and "recommendations" in metadata
                and isinstance(metadata["recommendations"], list)
            ):
                recommendations.update(metadata["recommendations"])

        return sorted(recommendations)

    def to_dict(self) -> "dict[str, Any]":
        """Convert aggregated results to dictionary.

        Returns:
            Complete dictionary representation
        """
        return {
            "summary": self.get_summary(),
            "risk_assessment": {
                "overall_risk_level": self.overall_risk_level.name,
                "is_safe": self.is_safe,
                "issues": self.all_issues,
                "warnings": self.all_warnings,
            },
            "analysis": {
                "has_analysis": self.has_analysis,
                "table_count": self.table_count,
                "join_count": self.join_count,
                "subquery_count": self.subquery_count,
                "complexity_score": self.complexity_score,
            },
            "transformations": {
                "was_transformed": self.was_transformed,
                "applied": self.transformations_applied,
                "parameters_extracted": self.parameters_extracted,
                "comments_removed": self.comments_removed,
            },
            "issues": {
                "performance": self.performance_issues,
                "security": self.security_issues,
            },
            "recommendations": self.get_recommendations(),
            "processing": {
                "processors_run": self.total_processors_run,
                "failed_processors": self.failed_processors,
                "processing_time_ms": self.processing_time_ms,
            },
            "component_details": self.component_metadata,
        }


class ResultAggregator:
    """Aggregates results from multiple pipeline components.

    This class provides methods to collect and consolidate results from
    all pipeline processors into a unified view.
    """

    def __init__(self) -> None:
        """Initialize the result aggregator."""
        self._results = AggregatedResults()

    @property
    def results(self) -> AggregatedResults:
        """Get the aggregated results.

        Returns:
            The aggregated results object
        """
        return self._results

    def aggregate_processor_results(self, results: "list[tuple[str, ProcessorResult]]") -> AggregatedResults:
        """Aggregate results from multiple processors.

        Args:
            results: List of (processor_name, result) tuples

        Returns:
            Aggregated results
        """
        for processor_name, result in results:
            self._results.add_processor_result(processor_name, result)

        return self._results

    def add_timing_info(self, processing_time_ms: float) -> None:
        """Add processing time information.

        Args:
            processing_time_ms: Total processing time in milliseconds
        """
        self._results.processing_time_ms = processing_time_ms

    def reset(self) -> None:
        """Reset the aggregator for a new aggregation."""
        self._results = AggregatedResults()

    def merge_with(self, other: "AggregatedResults") -> None:
        """Merge another aggregated result into this one.

        Args:
            other: Other aggregated results to merge
        """
        # Merge risk levels (take highest)
        if other.overall_risk_level.value > self._results.overall_risk_level.value:
            self._results.overall_risk_level = other.overall_risk_level

        # Merge safety
        if not other.is_safe:
            self._results.is_safe = False

        # Merge lists
        self._results.all_issues.extend(other.all_issues)
        self._results.all_warnings.extend(other.all_warnings)
        self._results.transformations_applied.extend(other.transformations_applied)
        self._results.performance_issues.extend(other.performance_issues)
        self._results.security_issues.extend(other.security_issues)
        self._results.failed_processors.extend(other.failed_processors)

        # Merge counts
        self._results.parameters_extracted += other.parameters_extracted
        self._results.comments_removed += other.comments_removed
        self._results.total_processors_run += other.total_processors_run
        self._results.processing_time_ms += other.processing_time_ms

        # Merge dictionaries
        self._results.validator_results.update(other.validator_results)
        self._results.component_metadata.update(other.component_metadata)

        # Update flags
        if other.was_transformed:
            self._results.was_transformed = True
        if other.has_analysis:
            self._results.has_analysis = True
            # Take the analysis metrics from the latest
            self._results.table_count = other.table_count
            self._results.join_count = other.join_count
            self._results.subquery_count = other.subquery_count
            self._results.complexity_score = other.complexity_score
