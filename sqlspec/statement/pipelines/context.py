from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.result_types import AnalysisFinding, TransformationLog, ValidationError

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.parameters import ParameterInfo
    from sqlspec.statement.sql import SQLConfig
    from sqlspec.typing import SQLParameterType

__all__ = ("PipelineResult", "SQLProcessingContext")


@dataclass
class SQLProcessingContext:
    """Carries expression through pipeline and collects all results."""

    # Input
    initial_sql_string: str
    """The original SQL string input by the user."""

    dialect: "DialectType"
    """The SQL dialect to be used for parsing and generation."""

    config: "SQLConfig"
    """The configuration for SQL processing for this statement."""

    # Initial state
    initial_expression: Optional[exp.Expression] = None
    """The initial parsed expression (for diffing/auditing)."""

    # Current state
    current_expression: Optional[exp.Expression] = None
    """The SQL expression, potentially modified by transformers."""

    # Parameters
    initial_parameters: "Optional[SQLParameterType]" = None
    """The initial parameters as provided to the SQL object (before merging with kwargs)."""
    initial_kwargs: "Optional[dict[str, Any]]" = None
    """The initial keyword arguments as provided to the SQL object."""
    merged_parameters: "SQLParameterType" = field(default_factory=list)
    """Parameters after merging initial_parameters and initial_kwargs."""
    parameter_info: "list[ParameterInfo]" = field(default_factory=list)
    """Information about identified parameters in the initial_sql_string."""
    extracted_parameters_from_pipeline: list[Any] = field(default_factory=list)
    """List of parameters extracted by transformers (e.g., ParameterizeLiterals)."""

    # Collected results (processors append to these)
    validation_errors: list[ValidationError] = field(default_factory=list)
    """Validation errors found during processing."""
    analysis_findings: list[AnalysisFinding] = field(default_factory=list)
    """Analysis findings discovered during processing."""
    transformations: list[TransformationLog] = field(default_factory=list)
    """Transformations applied during processing."""

    # General metadata
    metadata: dict[str, Any] = field(default_factory=dict)
    """General-purpose metadata store."""

    # Flags
    input_sql_had_placeholders: bool = False
    """Flag indicating if the initial_sql_string already contained placeholders."""
    statement_type: Optional[str] = None
    """The detected type of the SQL statement (e.g., SELECT, INSERT, DDL)."""
    extra_info: dict[str, Any] = field(default_factory=dict)
    """Extra information from parameter processing, including normalization state."""

    @property
    def has_errors(self) -> bool:
        """Check if any validation errors exist."""
        return bool(self.validation_errors)

    @property
    def risk_level(self) -> RiskLevel:
        """Calculate overall risk from validation errors."""
        if not self.validation_errors:
            return RiskLevel.SAFE
        return max(error.risk_level for error in self.validation_errors)


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
