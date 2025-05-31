import re
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQLConfig


class SuspiciousKeywords(SQLValidation):
    """Validates against the use of suspicious keywords that might indicate security issues."""

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.MEDIUM,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.MEDIUM,
        suspicious_keywords: "Optional[list[str]]" = None,
        keyword_patterns: "Optional[list[str]]" = None,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.suspicious_keywords = suspicious_keywords or [
            "into outfile",
            "into dumpfile",
            "load_file",
            "information_schema",
            "sys.databases",
            "master.dbo",
            "msdb.dbo",
            "pg_user",
            "pg_shadow",
            "dual",  # Oracle
            "xp_",  # SQL Server extended procedures
            "sp_",  # SQL Server system procedures (some can be risky)
            "benchmark",
            "sleep",
            "waitfor delay",
            "pg_sleep",
            "dbms_lock.sleep",
        ]
        self.keyword_patterns = keyword_patterns or [
            r"union\s+select",  # Classic SQL injection pattern
            r"or\s+1\s*=\s*1",  # Classic bypass condition
            r"and\s+1\s*=\s*1",  # Classic bypass condition
            r"'\s*or\s*'[^']*'\s*=\s*'[^']*'",  # String-based injection
            r"admin'\s*--",  # Comment-based injection
            r"1'\s*and\s*extractvalue\(",  # XML-based injection
            r"1'\s*and\s*updatexml\(",  # XML-based injection
        ]

    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> ValidationResult:
        sql_text = expression.sql(dialect=dialect).lower()

        # Check for suspicious keywords
        issues = [
            f"Suspicious keyword found: {keyword}"
            for keyword in self.suspicious_keywords
            if keyword.lower() in sql_text
        ]

        # Check for suspicious patterns
        issues.extend(
            f"Suspicious pattern found: {pattern}"
            for pattern in self.keyword_patterns
            if re.search(pattern, sql_text, re.IGNORECASE)
        )

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
