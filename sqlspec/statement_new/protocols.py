"""Protocols and shared data classes for the statement module."""
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

from sqlglot import exp

from sqlspec.exceptions import RiskLevel

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement_new.parameters import ParameterInfo, ParameterStyleTransformation
    from sqlspec.statement_new.sql import SQLConfig
    from sqlspec.typing import SQLParameterType


__all__ = (
    "AnalysisFinding",
    "HasExpressionProtocol",
    "HasParameterBuilderProtocol",
    "HasSQLGlotExpressionProtocol",
    "HasToStatementProtocol",
    "ProcessorPhase",
    "SQLProcessingContext",
    "SQLProcessor",
    "TransformationLog",
    "ValidationError",
)


class ProcessorPhase(Enum):
    """Defines the execution order for processors."""

    VALIDATE = 1
    TRANSFORM = 2
    ANALYZE = 3


@dataclass
class ValidationError:
    """A specific validation issue found during processing."""

    message: str
    code: str
    risk_level: "RiskLevel"
    processor: str
    expression: "Optional[exp.Expression]" = None


@dataclass
class TransformationLog:
    """Record of a transformation applied."""

    description: str
    processor: str
    before: Optional[str] = None
    after: Optional[str] = None


@dataclass
class AnalysisFinding:
    """Metadata discovered during analysis."""

    key: str
    value: Any
    processor: str


@dataclass
class SQLProcessingContext:
    """Carries expression through pipeline and collects all results."""

    initial_sql_string: str
    dialect: "DialectType"
    config: "SQLConfig"
    initial_expression: Optional[exp.Expression] = None
    current_expression: Optional[exp.Expression] = None
    initial_parameters: "Optional[SQLParameterType]" = None
    initial_kwargs: "Optional[dict[str, Any]]" = None
    merged_parameters: "SQLParameterType" = field(default_factory=list)
    parameter_info: "list[ParameterInfo]" = field(default_factory=list)
    extracted_parameters_from_pipeline: list[Any] = field(default_factory=list)
    validation_errors: list[ValidationError] = field(default_factory=list)
    analysis_findings: list[AnalysisFinding] = field(default_factory=list)
    transformations: list[TransformationLog] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    input_sql_had_placeholders: bool = False
    statement_type: Optional[str] = None
    extra_info: dict[str, Any] = field(default_factory=dict)
    parameter_normalization: "Optional[ParameterStyleTransformation]" = None

    @property
    def has_errors(self) -> bool:
        return bool(self.validation_errors)

    @property
    def risk_level(self) -> RiskLevel:
        if not self.validation_errors:
            return RiskLevel.SAFE
        return max(error.risk_level for error in self.validation_errors)

    @property
    def highest_risk_level(self) -> RiskLevel:
        """Alias for risk_level for backward compatibility."""
        return self.risk_level


@runtime_checkable
class HasSQLGlotExpressionProtocol(Protocol):
    """Protocol for objects with a sqlglot_expression property."""

    @property
    def sqlglot_expression(self) -> "Optional[exp.Expression]":
        """Return the SQLGlot expression for this object."""
        ...


@runtime_checkable
class HasParameterBuilderProtocol(Protocol):
    """Protocol for objects that can add parameters."""

    def add_parameter(self, value: Any, name: "Optional[str]" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...


@runtime_checkable
class HasExpressionProtocol(Protocol):
    """Protocol for objects with an _expression attribute."""

    _expression: "Optional[exp.Expression]"


@runtime_checkable
class HasToStatementProtocol(Protocol):
    """Protocol for objects with a to_statement method."""

    def to_statement(self) -> Any:
        """Convert to SQL statement."""
        ...


class SQLProcessor(Protocol):
    """A unified interface for all pipeline processors."""

    phase: ProcessorPhase

    def process(self, context: SQLProcessingContext) -> SQLProcessingContext: ...
