from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.parameters import ParameterInfo, ParameterStyleConversionState
    from sqlspec.statement.sql import SQLConfig
    from sqlspec.typing import SQLParameterType

__all__ = ("AnalysisFinding", "SQLProcessingContext", "TransformationLog", "ValidationError")


class ValidationError:
    """A specific validation issue found during processing."""

    __slots__ = ("code", "expression", "message", "processor", "risk_level")

    def __init__(
        self,
        message: str,
        code: str,
        risk_level: "RiskLevel",
        processor: str,
        expression: "Optional[exp.Expression]" = None,
    ) -> None:
        self.message = message
        self.code = code
        self.risk_level = risk_level
        self.processor = processor
        self.expression = expression


class TransformationLog:
    """Record of a transformation applied."""

    __slots__ = ("after", "before", "description", "processor")

    def __init__(
        self, description: str, processor: str, before: Optional[str] = None, after: Optional[str] = None
    ) -> None:
        self.description = description
        self.processor = processor
        self.before = before
        self.after = after


class AnalysisFinding:
    """Metadata discovered during analysis."""

    __slots__ = ("key", "processor", "value")

    def __init__(self, key: str, value: Any, processor: str) -> None:
        self.key = key
        self.value = value
        self.processor = processor


class SQLProcessingContext:
    """Carries expression through pipeline and collects all results."""

    __slots__ = (
        "analysis_findings",
        "config",
        "current_expression",
        "dialect",
        "extra_info",
        "extracted_parameters_from_pipeline",
        "initial_expression",
        "initial_sql_string",
        "input_sql_had_placeholders",
        "merged_parameters",
        "metadata",
        "parameter_conversion",
        "parameter_info",
        "statement_type",
        "transformations",
        "validation_errors",
    )

    def __init__(
        self,
        initial_sql_string: str,
        dialect: "DialectType",
        config: "SQLConfig",
        initial_expression: Optional[exp.Expression] = None,
        current_expression: Optional[exp.Expression] = None,
        merged_parameters: Optional["SQLParameterType"] = None,
        parameter_info: Optional["list[ParameterInfo]"] = None,
        extracted_parameters_from_pipeline: Optional[list[Any]] = None,
        validation_errors: Optional[list[ValidationError]] = None,
        analysis_findings: Optional[list[AnalysisFinding]] = None,
        transformations: Optional[list[TransformationLog]] = None,
        metadata: Optional[dict[str, Any]] = None,
        input_sql_had_placeholders: bool = False,
        statement_type: Optional[str] = None,
        extra_info: Optional[dict[str, Any]] = None,
        parameter_conversion: "Optional[ParameterStyleConversionState]" = None,
    ) -> None:
        """Initialize SQLProcessingContext."""
        self.initial_sql_string = initial_sql_string
        """The original SQL string input by the user."""

        self.dialect = dialect
        """The SQL dialect to be used for parsing and generation."""

        self.config = config
        """The configuration for SQL processing for this statement."""

        self.initial_expression = initial_expression
        """The initial parsed expression (for diffing/auditing)."""

        self.current_expression = current_expression
        """The SQL expression, potentially modified by transformers."""

        self.merged_parameters = merged_parameters if merged_parameters is not None else []
        """Parameters after merging initial_parameters and initial_kwargs."""

        self.parameter_info = parameter_info if parameter_info is not None else []
        """Information about identified parameters in the initial_sql_string."""

        self.extracted_parameters_from_pipeline = (
            extracted_parameters_from_pipeline if extracted_parameters_from_pipeline is not None else []
        )
        """List of parameters extracted by transformers (e.g., ParameterizeLiterals)."""

        self.validation_errors = validation_errors if validation_errors is not None else []
        """Validation errors found during processing."""

        self.analysis_findings = analysis_findings if analysis_findings is not None else []
        """Analysis findings discovered during processing."""

        self.transformations = transformations if transformations is not None else []
        """Transformations applied during processing."""

        self.metadata = metadata if metadata is not None else {}
        """General-purpose metadata store."""

        self.input_sql_had_placeholders = input_sql_had_placeholders
        """Flag indicating if the initial_sql_string already contained placeholders."""

        self.statement_type = statement_type
        """The detected type of the SQL statement (e.g., SELECT, INSERT, DDL)."""

        self.extra_info = extra_info if extra_info is not None else {}
        """Extra information from parameter processing, including conversion state."""

        self.parameter_conversion = parameter_conversion
        """Single source of truth for parameter style conversion tracking."""

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
