from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig

__all__ = ("InjectionValidator",)


class InjectionValidator(SQLValidation):
    """Validates against common SQL injection patterns."""

    def __init__(
        self,
        risk_level: RiskLevel = RiskLevel.HIGH,
        min_risk_to_raise: RiskLevel | None = RiskLevel.HIGH,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        # Common SQL injection patterns (very basic, extend as needed)
        # These patterns are checked against the string representation of expression parts.
        self.injection_patterns = [
            r"(;|--|\/\*|\*\/)",  # SQL terminators/comments within literals or unquoted
            r"(\b(OR|AND)\b\s+\d+\s*=\s*\d+)",  # Basic tautologies like OR 1=1
            r"(\b(UNION|SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC)\b\s+.*\[\s*(SELECT|FROM|WHERE)\])",  # SQL keywords within other keywords, potentially indicative of stacked queries or complex injections
            r"(xp_cmdshell|sp_configure|dbms_lock.sleep)",  # Known dangerous procedures/functions
        ]

    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: SQLConfig,
        **kwargs: Any,
    ) -> ValidationResult:
        issues: list[str] = []
        warnings: list[str] = []

        # Check for patterns in string literals
        for literal_node in expression.find_all(exp.Literal):
            if literal_node.is_string:
                literal_value = literal_node.this
                issues.extend(
                    f"Potential SQL injection pattern '{pattern}' found in string literal: '{literal_value[:50]}...'"
                    for pattern in self.injection_patterns
                    if re.search(pattern, literal_value, re.IGNORECASE)
                )

        # Check for patterns in comments (if they weren't removed by a transformer)
        for comment_node in expression.find_all(exp.Comment):
            comment_text = comment_node.this
            warnings.extend(
                f"Potential SQL injection pattern '{pattern}' found in comment: '{comment_text[:50]}...'"
                for pattern in self.injection_patterns
                if re.search(pattern, comment_text, re.IGNORECASE)
            )

        # A more advanced check could involve looking for unparameterized, concatenated string literals
        # forming parts of the query structure, which is harder with just sqlglot expressions without context.

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues, warnings=warnings)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, warnings=warnings)
