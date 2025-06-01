from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig

__all__ = ("PreventInjection",)

# Compiled regex patterns for performance - focused on injection-specific patterns
STACKED_QUERY_PATTERN = re.compile(r";\s*(?:SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER)", re.IGNORECASE)
BYPASS_AUTHENTICATION_PATTERN = re.compile(r"(admin|administrator)\s*['\"]?\s*(?:--|\#|\/\*)", re.IGNORECASE)
UNION_INJECTION_PATTERN = re.compile(
    r"UNION\s+(?:ALL\s+)?SELECT.*(?:NULL|0x\w+|CHAR\(|CHR\()", re.IGNORECASE | re.DOTALL
)


class PreventInjection(SQLValidation):
    """Advanced SQL injection validator using SQLGlot AST analysis.

    This validator leverages SQLGlot's parsing to detect injection patterns that would be
    difficult to catch with regex alone. It focuses on structural anomalies in the AST
    that indicate potential injection rather than pattern matching.

    Key advantages of AST-based detection:
    - Detects injection even when obfuscated with comments or whitespace
    - Identifies structural inconsistencies
    - Catches multi-statement injection attempts
    - Validates parameter binding integrity

    Args:
        risk_level: The risk level of the validator.
        min_risk_to_raise: The minimum risk level to raise an issue.
        check_union_injection: Whether to check for UNION-based injection.
        check_stacked_queries: Whether to check for stacked query injection.
        max_union_selects: Maximum allowed UNION SELECT statements.
    """

    def __init__(
        self,
        risk_level: RiskLevel = RiskLevel.HIGH,
        min_risk_to_raise: RiskLevel | None = RiskLevel.HIGH,
        check_union_injection: bool = True,
        check_stacked_queries: bool = True,
        max_union_selects: int = 3,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.check_union_injection = check_union_injection
        self.check_stacked_queries = check_stacked_queries
        self.max_union_selects = max_union_selects

    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: SQLConfig,
        **kwargs: Any,
    ) -> ValidationResult:
        """Validate the expression for SQL injection patterns using AST analysis."""
        issues: list[str] = []
        warnings: list[str] = []

        # 1. Check for UNION injection using AST structure
        if self.check_union_injection:
            union_issues = self._check_union_injection(expression, dialect)
            issues.extend(union_issues)

        # 2. Check for stacked queries (multiple statements)
        if self.check_stacked_queries:
            stacked_issues = self._check_stacked_queries(expression, dialect)
            issues.extend(stacked_issues)

        # 3. Check for suspicious literal patterns in string values
        literal_issues = self._check_suspicious_literals(expression)
        issues.extend(literal_issues)

        # 4. Check for comment-based evasion in the AST
        comment_issues = self._check_comment_evasion(expression, dialect)
        issues.extend(comment_issues)

        # 5. Check for parameter binding anomalies
        binding_warnings = self._check_parameter_binding_anomalies(expression)
        warnings.extend(binding_warnings)

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues, warnings=warnings)

        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, warnings=warnings)

    def _check_union_injection(self, expression: exp.Expression, dialect: DialectType) -> list[str]:
        """Check for UNION-based injection patterns using AST analysis."""
        issues = []

        # Count UNION operations
        union_count = len(list(expression.find_all(exp.Union)))
        if union_count > self.max_union_selects:
            issues.append(f"Excessive UNION operations detected ({union_count}), potential injection")

        # Analyze UNION structure for injection patterns
        for union_expr in expression.find_all(exp.Union):
            # Check if UNION is followed by suspicious SELECT patterns
            if hasattr(union_expr, "expression") and isinstance(union_expr.expression, exp.Select):
                select_expr = union_expr.expression

                # Check for information_schema queries (common in injection)
                for table in select_expr.find_all(exp.Table):
                    table_name = str(table.this).lower() if table.this else ""
                    if any(schema in table_name for schema in ["information_schema", "sys.", "pg_"]):
                        issues.append(f"UNION query targeting system schema detected: {table_name}")

                # Check for NULL padding (common injection technique)
                select_exprs = select_expr.expressions
                if select_exprs and len(select_exprs) > 5:  # Arbitrary threshold
                    null_count = sum(1 for expr in select_exprs if isinstance(expr, exp.Null))
                    if null_count > len(select_exprs) // 2:  # More than half are NULLs
                        issues.append("UNION SELECT with excessive NULL padding detected (injection technique)")

        return issues

    def _check_stacked_queries(self, expression: exp.Expression, dialect: DialectType) -> list[str]:
        """Check for stacked query injection patterns."""
        issues = []

        # Convert to SQL and check for statement terminators followed by new statements
        sql_text = expression.sql(dialect=dialect)
        if STACKED_QUERY_PATTERN.search(sql_text):
            issues.append("Stacked query pattern detected (potential injection)")

        # Check if the expression contains multiple top-level statements
        # This might indicate successful injection of additional statements
        if isinstance(expression, (exp.Union, exp.Except, exp.Intersect)):
            # These can be legitimate, but check their structure
            pass
        elif hasattr(expression, "expressions") and len(expression.expressions or []) > 1:
            # Multiple expressions at top level might indicate stacked queries
            expr_types = [type(expr).__name__ for expr in expression.expressions[:3]]  # First 3 for brevity
            issues.append(f"Multiple top-level expressions detected: {', '.join(expr_types)}")

        return issues

    def _check_suspicious_literals(self, expression: exp.Expression) -> list[str]:
        """Check for suspicious patterns in string literals that might indicate injection."""
        issues = []

        for literal in expression.find_all(exp.Literal):
            if literal.is_string:
                literal_value = str(literal.this)

                # Check for SQL keywords in string literals (possible injection)
                sql_keywords = ["SELECT", "UNION", "INSERT", "UPDATE", "DELETE", "DROP", "ALTER"]
                if any(keyword.lower() in literal_value.lower() for keyword in sql_keywords):
                    issues.append(f"SQL keywords found in string literal: '{literal_value[:50]}...'")

                # Check for common injection payloads
                if BYPASS_AUTHENTICATION_PATTERN.search(literal_value):
                    issues.append(f"Authentication bypass pattern in literal: '{literal_value[:50]}...'")

                # Check for encoded payloads
                if len(literal_value) > 100 and all(c.isalnum() or c in "+/=" for c in literal_value):
                    issues.append(f"Potential base64 encoded payload in literal: '{literal_value[:50]}...'")

        return issues

    def _check_comment_evasion(self, expression: exp.Expression, dialect: DialectType) -> list[str]:
        """Check for comment-based evasion techniques specific to injection."""
        issues = []

        # Get SQL representation to check for comment-based evasion
        sql_text = expression.sql(dialect=dialect)

        # MySQL version-specific comments used for injection
        mysql_version_comments = re.findall(r"/\*!\d{5}([^*]+)\*/", sql_text, re.IGNORECASE)
        for comment_content in mysql_version_comments:
            if any(keyword in comment_content.upper() for keyword in ["UNION", "SELECT", "OR"]):
                issues.append(f"MySQL version comment contains injection keywords: {comment_content[:50]}...")

        # Check for comments that split SQL keywords (evasion technique)
        keyword_split_pattern = re.compile(r"(?:UN/\*\*/ION|SEL/\*\*/ECT|WH/\*\*/ERE)", re.IGNORECASE)
        if keyword_split_pattern.search(sql_text):
            issues.append("Comment-based keyword splitting detected (evasion technique)")

        return issues

    def _check_parameter_binding_anomalies(self, expression: exp.Expression) -> list[str]:
        """Check for anomalies in parameter binding that might indicate injection."""
        warnings = []

        # Count placeholders vs actual parameters
        placeholder_count = len(list(expression.find_all(exp.Placeholder)))

        # This is a basic check - in a real implementation, you'd compare with actual parameters
        if placeholder_count == 0:
            # Check if there are suspicious WHERE clauses without parameters
            for where_clause in expression.find_all(exp.Where):
                where_sql = where_clause.sql()
                if any(op in where_sql for op in ["=", "LIKE", "IN"]) and "NULL" not in where_sql:
                    # WHERE clause with comparison but no parameters - suspicious
                    warnings.append("WHERE clause with comparisons but no parameter placeholders")

        return warnings
