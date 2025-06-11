from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.parameters import ParameterInfo
    from sqlspec.statement.pipelines.aggregator import AggregatedResults
    from sqlspec.statement.pipelines.analyzers import StatementAnalysis
    from sqlspec.statement.pipelines.base import ValidationResult
    from sqlspec.statement.sql import SQLConfig
    from sqlspec.typing import SQLParameterType

__all__ = ("SQLProcessingContext", "StatementPipelineResult")


@dataclass
class SQLProcessingContext:
    """Holds shared data and state during the execution of a StatementPipeline."""

    initial_sql_string: str
    """The original SQL string input by the user."""

    dialect: "Optional[DialectType]"
    """The SQL dialect to be used for parsing and generation."""

    config: "SQLConfig"
    """The configuration for SQL processing for this statement."""

    # --- Data populated after initial SQL object setup ---
    initial_parameters: "Optional[SQLParameterType]" = None
    """The initial parameters as provided to the SQL object (before merging with kwargs)."""
    initial_kwargs: "Optional[dict[str, Any]]" = None
    """The initial keyword arguments as provided to the SQL object."""
    merged_parameters: "SQLParameterType" = field(default_factory=list)
    """Parameters after merging initial_parameters and initial_kwargs."""
    parameter_info: "list[ParameterInfo]" = field(default_factory=list)
    """Information about identified parameters in the initial_sql_string."""

    # --- Data populated/modified during pipeline execution ---
    current_expression: Optional[exp.Expression] = None
    """The SQL expression, potentially modified by transformers. Initialized after parsing."""

    extracted_parameters_from_pipeline: list[Any] = field(default_factory=list)
    """List of parameters extracted by transformers (e.g., ParameterizeLiterals)."""

    validation_result: Optional["ValidationResult"] = None
    """Aggregated result from the validation stage."""

    analysis_result: Optional["StatementAnalysis"] = None
    """Result from the statement analysis stage."""

    aggregated_results: Optional["AggregatedResults"] = None
    """Aggregated results from all pipeline processors."""

    # --- Flags and metadata ---
    input_sql_had_placeholders: bool = False
    """Flag indicating if the initial_sql_string already contained placeholders."""

    statement_type: Optional[str] = None
    """The detected type of the SQL statement (e.g., SELECT, INSERT, DDL)."""

    extra_info: dict[str, Any] = field(default_factory=dict)
    """Extra information from parameter processing, including normalization state."""

    _extra_data: dict[str, Any] = field(default_factory=dict)
    """Allows processors to store arbitrary data if needed, use with caution."""

    def get_additional_data(self, key: str, default: Any = None) -> Any:
        return self._extra_data.get(key, default)

    def set_additional_data(self, key: str, value: Any) -> None:
        self._extra_data[key] = value


@dataclass
class StatementPipelineResult:
    """Holds the final results from a StatementPipeline execution."""

    final_expression: Optional[exp.Expression]
    """The SQL expression after all transformations."""

    merged_parameters: "SQLParameterType"
    """Final parameters, potentially including those extracted by the pipeline."""

    parameter_info: "list[ParameterInfo]"
    """Parameter info corresponding to the initial SQL and parameters."""

    validation_result: Optional["ValidationResult"]
    """The aggregated validation result."""

    analysis_result: Optional["StatementAnalysis"]
    """The result of statement analysis."""

    input_sql_had_placeholders: bool
    """Whether the original input SQL string had placeholders."""

    aggregated_results: Optional["AggregatedResults"] = None
    """Aggregated results from all pipeline processors."""

    # Add any other final state from the context that the SQL object needs
    # e.g., if ParameterizeLiterals modifies parameter_info, that should be reflected
