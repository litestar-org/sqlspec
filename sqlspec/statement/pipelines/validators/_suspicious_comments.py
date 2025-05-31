import re
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig

__all__ = ("SuspiciousComments",)


class SuspiciousComments(SQLValidation):
    """Validates SQL comments for suspicious patterns."""

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.LOW,  # Comments are often benign
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.MEDIUM,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        # Patterns that might be suspicious in comments
        self.suspicious_comment_patterns = [
            r"(SELECT\s+.+FROM\s+.+WHERE.+);?",  # Full SELECT statements in comments
            r"(UNION\s+ALL\s+SELECT)",  # UNION SELECT
            r"(DROP\s+TABLE|DATABASE)",  # DROP statements
            r"(ALTER\s+TABLE)",  # ALTER statements
            r"(EXEC\s+[\[\(]?master..?xp_cmdshell[\]\)]?)",  # Exec master..xp_cmdshell
            r"(shutdown|reconfigure)",  # Dangerous commands
            # Comments used to bypass filters, e.g., /*! ... */ for MySQL version-specific execution
            r"(\/\*!\d{5}.*\*\/)",
            r"(--\s*\[ENDIF\])",  # SQLMap like comments
        ]

    def validate(
        self,
        expression: "exp.Expression",
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: "Any",
    ) -> "ValidationResult":
        warnings: list[str] = []  # Use warnings for comments, as they are less direct than code issues

        for comment_node in expression.find_all(exp.Comment):
            comment_text = comment_node.this
            warnings.extend(
                f"Suspicious pattern '{pattern}' found in SQL comment: '{comment_text[:70]}...'"
                for pattern in self.suspicious_comment_patterns
                if re.search(pattern, comment_text, re.IGNORECASE)
            )

        # If a CommentRemover transformer is used, this validator might not find many issues.
        # Its utility depends on the overall processing pipeline configuration.

        # For comments, we generally don't mark as unsafe unless a pattern is highly indicative of an attack.
        # We'll rely on the risk_level being LOW and strict_mode configuration for raising.
        if (
            warnings
        ):  # Even if there are warnings, the SQL itself might be safe. Let's consider it "safe" but with a warning.
            return ValidationResult(is_safe=True, risk_level=self.risk_level, warnings=warnings)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
