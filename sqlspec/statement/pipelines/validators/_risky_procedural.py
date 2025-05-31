import re
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQLConfig

__all__ = ("RiskyProceduralCode",)


class RiskyProceduralCode(SQLValidation):
    """Validates against the use of dangerous or suspicious SQL functions and procedures."""

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.HIGH,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.HIGH,
        banned_functions: "Optional[list[str]]" = None,
        suspicious_function_patterns: "Optional[list[str]]" = None,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.banned_functions = banned_functions or [
            "xp_cmdshell",
            "sp_configure",
            "sp_addextendedproc",
            "sp_dropextendedproc",
            "sp_oacreate",
            "sp_oamethod",
            "sp_send_dbmail",
            "utl_file.fopen",
            "dbms_lob.loadfromfile",
            "dbms_sql.execute",
            "dbms_xmlquery.newcontext",
            "load_file",  # MySQL
            "sys_exec",  # PostgreSQL, older versions
            "pg_sleep",  # Can be used for timing attacks or DoS
            "sleep",  # Generic sleep
            "benchmark",  # Can be used for timing attacks
        ]
        # Patterns for functions that might be risky depending on arguments or context
        self.suspicious_function_patterns = suspicious_function_patterns or [
            r"exec(ute)?_?sql",  # Functions like EXECSQL, EXECUTE_SQL
            r"eval",
            r"convert\(.*\,.*(char|binary|hex|ascii).*\)",  # Conversions that might be part of obfuscation
        ]

    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: "SQLConfig",
        **kwargs: Any,
    ) -> ValidationResult:
        issues = []

        for func_expr in expression.find_all(exp.Func):
            func_name = func_expr.name.lower()

            if func_name in self.banned_functions:
                issues.append(f"Use of banned SQL function: {func_name}")
            else:
                for pattern in self.suspicious_function_patterns:
                    if re.search(pattern, func_name, re.IGNORECASE):
                        arg_details = ""
                        if func_expr.args.get("expressions"):
                            first_arg = func_expr.args["expressions"][0]
                            if isinstance(first_arg, exp.Literal) and first_arg.is_string:
                                arg_details = f" (first argument is string literal: '{first_arg.this[:30]}...')"

                        issues.append(
                            f"Use of potentially suspicious SQL function pattern '{pattern}' matching '{func_name}'{arg_details}"
                        )

        # Check for EXEC or EXECUTE as keywords if not caught as exp.Func by sqlglot (dialect dependent)
        # Some dialects might parse EXEC sp_proc as exp.Command
        for command_expr in expression.find_all(exp.Command):
            command_verb = str(command_expr.this).upper()
            if command_verb in {"EXEC", "EXECUTE"}:
                # The expression following EXEC might be a procedure name or a string to execute
                # This is a simplified check; more detailed parsing of command_expr.expression might be needed.
                full_command_sql = command_expr.sql(dialect=dialect)
                issues.extend(
                    f"Use of banned function '{func_name}' within EXEC/EXECUTE command: {full_command_sql[:60]}..."
                    for func_name in self.banned_functions
                    if func_name in full_command_sql.lower()
                )
                issues.extend(
                    f"Use of suspicious function pattern '{pattern}' within EXEC/EXECUTE command: {full_command_sql[:60]}..."
                    for pattern in self.suspicious_function_patterns
                    if re.search(pattern, full_command_sql, re.IGNORECASE)
                )

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
