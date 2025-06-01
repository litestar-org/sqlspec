import re
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQLConfig

__all__ = ("SuspiciousKeywords",)

# Compiled regex patterns for performance - focused on system functions only
SYSTEM_FUNCTIONS_PATTERN = re.compile(
    r"\b(?:into\s+outfile|into\s+dumpfile|load_file|benchmark|sleep|waitfor\s+delay|pg_sleep|dbms_lock\.sleep)\b",
    re.IGNORECASE,
)

DATABASE_INTROSPECTION_PATTERN = re.compile(
    r"\b(?:information_schema\.|mysql\.|performance_schema\.|pg_catalog\.|pg_stat_|pg_proc|sys\.databases|master\.dbo|msdb\.dbo)\b",
    re.IGNORECASE,
)


class SuspiciousKeywords(SQLValidation):
    """Validates against the use of suspicious system functions and database introspection.

    This validator focuses specifically on system-level functions and database introspection
    that could be used for reconnaissance or privilege escalation. It complements but does
    not overlap with PreventInjection (structural) or SuspiciousComments (comment analysis).

    Args:
        risk_level: The risk level of the validator.
        min_risk_to_raise: The minimum risk level to raise an issue.
        allow_system_functions: Whether to allow system functions like SLEEP, BENCHMARK.
        allow_file_operations: Whether to allow file operations like INTO OUTFILE.
        allow_introspection: Whether to allow database introspection queries.
    """

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.MEDIUM,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.MEDIUM,
        allow_system_functions: bool = False,
        allow_file_operations: bool = False,
        allow_introspection: bool = False,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.allow_system_functions = allow_system_functions
        self.allow_file_operations = allow_file_operations
        self.allow_introspection = allow_introspection

    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> ValidationResult:
        """Validate the expression for suspicious system functions and introspection."""
        issues: list[str] = []
        warnings: list[str] = []

        # Check function calls for system functions
        self._check_function_calls(expression, issues, warnings)

        # Check table references for system schemas
        self._check_table_references(expression, issues, warnings)

        # Check for file operations in SQL text
        if not self.allow_file_operations:
            self._check_file_operations(expression, dialect, issues)

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues, warnings=warnings)

        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, warnings=warnings)

    def _check_function_calls(self, expression: exp.Expression, issues: list[str], warnings: list[str]) -> None:
        """Check function calls for suspicious system functions."""
        for func_expr in expression.find_all(exp.Func):
            func_name = func_expr.name.lower() if func_expr.name else ""

            # System timing functions (often used in timing attacks)
            if not self.allow_system_functions:
                timing_functions = ["sleep", "pg_sleep", "waitfor", "benchmark", "dbms_lock.sleep"]
                if any(timing_func in func_name for timing_func in timing_functions):
                    issues.append(f"Timing function detected: {func_name} (potential timing attack)")

            # File system functions
            if not self.allow_file_operations:
                file_functions = ["load_file", "into_outfile", "into_dumpfile"]
                if any(file_func in func_name for file_func in file_functions):
                    issues.append(f"File system function detected: {func_name}")

            # Database-specific introspection functions
            if not self.allow_introspection:
                introspection_functions = ["current_user", "user", "version", "database", "schema"]
                if func_name in introspection_functions:
                    warnings.append(f"Database introspection function: {func_name}")

    def _check_table_references(self, expression: exp.Expression, issues: list[str], warnings: list[str]) -> None:
        """Check table references for system schemas."""
        if self.allow_introspection:
            return

        for table_expr in expression.find_all(exp.Table):
            table_name = str(table_expr.this).lower() if table_expr.this else ""

            # System schema access
            system_schemas = [
                "information_schema",
                "mysql.user",
                "mysql.db",
                "performance_schema",
                "pg_catalog",
                "pg_stat_",
                "pg_proc",
                "sys.databases",
                "sys.tables",
                "sys.columns",
                "master.dbo",
                "msdb.dbo",
                "tempdb.dbo",
            ]

            for schema in system_schemas:
                if schema in table_name:
                    issues.append(f"System schema access detected: {table_name}")
                    break

    def _check_file_operations(self, expression: exp.Expression, dialect: DialectType, issues: list[str]) -> None:
        """Check for file operations in the SQL text."""
        sql_text = expression.sql(dialect=dialect).lower()

        # File operation patterns
        file_operations = [
            r"\binto\s+outfile\s+",
            r"\binto\s+dumpfile\s+",
            r"\bload\s+data\s+",
            r"\bload_file\s*\(",
        ]

        for pattern in file_operations:
            if re.search(pattern, sql_text, re.IGNORECASE):
                operation_name = pattern.replace(r"\b", "").replace(r"\s+", " ").replace(r"\s*\(", "(").strip()
                issues.append(f"File operation detected: {operation_name}")
