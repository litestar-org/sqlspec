"""Result objects for the SQL processing pipeline."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sqlspec.exceptions import RiskLevel

__all__ = ("ValidationResult",)


class ValidationResult:
    """Result of SQL validation with detailed information."""

    __slots__ = ("is_safe", "issues", "risk_level", "transformed_sql", "warnings")

    def __init__(
        self,
        is_safe: bool,
        risk_level: "RiskLevel",
        issues: "Optional[list[str]]" = None,
        warnings: "Optional[list[str]]" = None,
        transformed_sql: "Optional[str]" = None,
    ) -> None:
        self.is_safe = is_safe
        self.risk_level = risk_level
        self.issues = list(issues) if issues is not None else []
        self.warnings = list(warnings) if warnings is not None else []
        self.transformed_sql = transformed_sql  # Though likely not used by validators

    def merge(self, other: "ValidationResult") -> None:
        """Merge another ValidationResult into this one."""
        if not other.is_safe:
            self.is_safe = False
        self.issues.extend(other.issues)
        self.warnings.extend(other.warnings)
        # Set risk level to the higher of the two
        if other.risk_level.value > self.risk_level.value:
            self.risk_level = other.risk_level

    def __bool__(self) -> bool:
        return self.is_safe
