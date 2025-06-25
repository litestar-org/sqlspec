"""Specific result types for the SQL processing pipeline."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from sqlglot import exp

    from sqlspec.exceptions import RiskLevel

__all__ = ("AnalysisFinding", "TransformationLog", "ValidationError")


@dataclass
class ValidationError:
    """A specific validation issue found during processing."""

    message: str
    code: str  # e.g., "risky-delete", "missing-where"
    risk_level: "RiskLevel"
    processor: str  # Which processor found it
    expression: "Optional[exp.Expression]" = None  # Problematic sub-expression


@dataclass
class AnalysisFinding:
    """Metadata discovered during analysis."""

    key: str  # e.g., "complexity_score", "table_count"
    value: Any
    processor: str


@dataclass
class TransformationLog:
    """Record of a transformation applied."""

    description: str
    processor: str
    before: Optional[str] = None  # SQL before transform
    after: Optional[str] = None  # SQL after transform
