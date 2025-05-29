"""Provides the SQLStatement class for representing and manipulating SQL queries.

For adapter developers:
The SQLStatement.get_sql() method supports a `placeholder_style` parameter that allows
adapters to explicitly specify the placeholder format they need, regardless of the
underlying database dialect. This ensures compatibility across different drivers:

Example usage in adapters:
- ADBC: query.get_sql(placeholder_style="qmark")  # Always uses ? placeholders
- psycopg: query.get_sql(placeholder_style="pyformat_named")  # Uses %(name)s
- Other drivers can specify: "pyformat_positional" (%s), "named" (:name), "numeric" ($1)

If placeholder_style is not specified, the method falls back to dialect-based logic.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional, Union

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.exceptions import (
    RiskLevel,
    SQLInjectionError,
    SQLValidationError,
)

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.sql.statement import SQLStatement

# Define a type for SQL input
Statement = Union[str, exp.Expression, "SQLStatement"]

logger = logging.getLogger("sqlspec")

__all__ = (
    "SQLPreprocessor",
    "SQLTransformer",
    "SQLValidator",
    "ValidationResult",
    "validate_sql",
)


class ValidationResult:
    """Result of SQL validation with detailed information."""

    __slots__ = (
        "is_safe",
        "issues",
        "risk_level",
        "transformed_sql",
        "warnings",
    )

    def __init__(
        self,
        is_safe: bool,
        risk_level: RiskLevel,
        issues: Optional[list[str]] = None,
        warnings: Optional[list[str]] = None,
        transformed_sql: Optional[str] = None,
    ) -> None:
        self.is_safe = is_safe
        self.risk_level = risk_level
        self.issues = issues if issues is not None else []
        self.warnings = warnings if warnings is not None else []
        self.transformed_sql = transformed_sql

    def __bool__(self) -> bool:
        return self.is_safe


class UsesExpression:
    @staticmethod
    def get_expression(statement: "Statement", dialect: "DialectType" = None) -> exp.Expression:
        """Convert SQL input to expression.

        Returns:
            The parsed SQL expression.

        Raises:
            SQLValidationError: If SQL cannot be parsed.
        """
        if isinstance(statement, exp.Expression):
            return statement

        from sqlspec.sql.statement import SQLStatement

        if isinstance(statement, SQLStatement):
            expr = statement.expression
            if expr is not None:
                return expr
            return sqlglot.parse_one(statement.sql, read=dialect)

        if not statement or not statement.strip():
            return exp.Select()

        try:
            return sqlglot.parse_one(statement, read=dialect)
        except SQLGlotParseError as e:
            msg = f"SQL parsing failed: {e}"
            raise SQLValidationError(msg, statement, RiskLevel.HIGH) from e


class SQLPreprocessor(UsesExpression):
    """Base class for SQL preprocessors that can apply filters."""


@dataclass
class SQLTransformer(SQLPreprocessor):
    """SQL Sanitizer"""

    remove_comments: bool = False
    """Whether to allow SQL comments.  When false, comments are removed."""
    remove_hints: bool = False
    """Whether to allow SQL hints.  When false, hints are removed."""

    def transform(self, statement: "Statement", dialect: "DialectType" = None) -> "exp.Expression":
        """Sanitize SQL by removing dangerous constructs.

        Args:
            statement: The SQL string, expression, or Query object to sanitize
            dialect: SQL dialect for parsing

        Returns:
            transformed SQL expression
        """
        statement = self._remove_comments(self.get_expression(statement, dialect))
        return self._remove_hints(statement)

    def _remove_comments(self, expression: "exp.Expression") -> "exp.Expression":
        """Remove comments from the expression.

        Returns:
            The expression with comments removed.
        """
        transformed = expression.copy()
        if self.remove_comments:
            for comment in transformed.find_all(exp.Comment):
                comment.replace(exp.Anonymous())
        return transformed

    def _remove_hints(self, expression: "exp.Expression") -> "exp.Expression":
        """Remove hints from the expression.

        Returns:
            The expression with hints removed.
        """
        transformed = expression.copy()
        if self.remove_hints:
            for hint in transformed.find_all(exp.Hint):
                hint.replace(exp.Anonymous())
        return transformed


@dataclass
class SQLValidator(SQLPreprocessor):
    """SQL validator with security checks."""

    strict_mode: bool = True
    """Whether to use strict validation rules."""
    allow_dangerous_functions: bool = False
    """Whether to allow potentially dangerous functions."""
    dangerous_function: set[str] = field(
        default_factory=lambda: {
            "exec",
            "execute",
            "sp_",
            "xp_",
            "fn_",
            "load_file",
            "into_outfile",
            "into_dumpfile",
            "sleep",
            "waitfor",
            "delay",
            "dbms_",
        }
    )
    """Set of dangerous functions to check for in SQL."""
    allow_risky_dml: bool = False
    """Whether to allow DML statements without a WHERE clause."""
    max_sql_length: int = 10000
    """Maximum allowed SQL length."""
    allow_ddl: bool = False
    """Whether to allow DDL statements (CREATE, ALTER, DROP)."""
    allow_dml: bool = True
    """Whether to allow DML statements (INSERT, UPDATE, DELETE)."""
    allow_procedural_code: bool = True
    """Whether to allow procedural code (DECLARE, BEGIN, END, etc.)."""
    allowed_schemas: set[str] = field(default_factory=set)
    """Set of allowed database schemas."""
    min_risk_to_raise: RiskLevel = RiskLevel.HIGH
    """Minimum risk level that will trigger a SQLValidationError if encountered."""

    def validate(self, statement: "Statement", dialect: "DialectType" = None) -> "ValidationResult":
        """Validate SQL for security and safety.

        Args:
            statement: The SQL string, expression, or Query object to validate
            dialect: SQL dialect for parsing

        Returns:
            ValidationResult with detailed validation information
        """
        issues: list[str] = []
        warnings: list[str] = []
        risk_level = RiskLevel.SAFE

        try:
            expression = self.get_expression(statement, dialect)
        except SQLValidationError as e:
            issues.append(str(e))
            return ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=issues)

        try:
            injection_issues = self._check_injection_patterns(expression)
            if injection_issues:
                issues.extend(injection_issues)
                risk_level = RiskLevel(max(risk_level.value, RiskLevel.CRITICAL.value))

            unsafe_issues = self._check_unsafe_patterns(expression)
            if unsafe_issues:
                issues.extend(unsafe_issues)
                risk_level = RiskLevel(max(risk_level.value, RiskLevel.HIGH.value))

            risky_dml_issues = self._check_risky_dml(expression)
            if risky_dml_issues:
                issues.extend(risky_dml_issues)
                risk_level = RiskLevel(max(risk_level.value, RiskLevel.HIGH.value))

            stmt_issues = self._check_statement_type(expression)
            if stmt_issues:
                issues.extend(stmt_issues)
                risk_level = RiskLevel(max(risk_level.value, RiskLevel.MEDIUM.value))

            schema_issues = self._check_schema_access(expression)
            if schema_issues:
                warnings.extend(schema_issues)
                risk_level = RiskLevel(max(risk_level.value, RiskLevel.LOW.value))

        except (ValueError, TypeError, AttributeError) as e:
            issues.append(f"Validation error: {e}")
            risk_level = RiskLevel(max(risk_level.value, RiskLevel.HIGH.value))

        is_safe = not issues and risk_level.value in {RiskLevel.SAFE.value, RiskLevel.LOW.value}

        return ValidationResult(is_safe=is_safe, risk_level=risk_level, issues=issues, warnings=warnings)

    @staticmethod
    def _check_injection_patterns(expression: "exp.Expression") -> list[str]:
        """Check for injection patterns in the AST.

        Returns:
            List of detected injection issues.
        """
        issues = []
        if list(expression.find_all(exp.Union)):
            issues.append("Potential UNION-based injection detected")

        dangerous_functions = {"exec", "execute", "sp_", "xp_", "fn_", "sleep", "waitfor", "delay"}

        for func in expression.find_all(exp.Func):
            func_name = func.this.name if hasattr(func.this, "name") else str(func.this)
            if any(dangerous in func_name.lower() for dangerous in dangerous_functions):
                issues.append(f"Dangerous function detected: {func_name}")

        return issues

    @staticmethod
    def _check_unsafe_patterns(expression: "exp.Expression") -> list[str]:
        """Check for unsafe patterns in the AST.

        Returns:
            List of detected unsafe patterns.
        """
        issues = []

        file_functions = {"load_file", "into_outfile", "into_dumpfile"}
        for func in expression.find_all(exp.Func):
            func_name = func.this.name if hasattr(func.this, "name") else str(func.this)
            if func_name.lower() in file_functions:
                issues.append(f"File system operation detected: {func_name}")

        return issues

    def _check_statement_type(self, parsed: "exp.Expression") -> list[str]:
        issues = []
        if isinstance(parsed, (exp.Create, exp.Drop, exp.Alter)) and not self.allow_ddl:
            issues.append(f"DDL statement not allowed: {type(parsed).__name__}")
        if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete)) and not self.allow_dml:
            issues.append(f"DML statement not allowed: {type(parsed).__name__}")
        return issues

    def _check_schema_access(self, parsed: "exp.Expression") -> list[str]:
        warnings: list[str] = []
        if not self.allowed_schemas:
            return warnings
        for table in parsed.find_all(exp.Table):
            schema_name = getattr(table, "db", None)
            if schema_name:
                actual_schema_name = str(schema_name.name if hasattr(schema_name, "name") else schema_name).lower()
                if actual_schema_name and actual_schema_name not in self.allowed_schemas:
                    warnings.append(f"Access to schema '{actual_schema_name}' may be restricted")
        return warnings

    @staticmethod
    def _check_risky_dml(expression: "exp.Expression") -> list[str]:
        """Check for DML statements without a WHERE clause.

        Args:
            expression: The SQL expression to check.

        Returns:
            A list of issues found.
        """
        issues = []
        if isinstance(expression, (exp.Delete, exp.Update)) and not expression.args.get("where"):
            issues.append(f"{type(expression).__name__} statement without a WHERE clause is considered risky.")
        return issues

    def _check_dangerous_constructs(self, expression: exp.Expression) -> None:
        """Check for dangerous SQL constructs in the expression tree.

        Raises:
            SQLInjectionError: If dangerous constructs are found in strict mode.
        """
        dangerous_functions = {
            "exec",
            "execute",
            "sp_",
            "xp_",
            "fn_",
            "load_file",
            "into_outfile",
            "into_dumpfile",
            "sleep",
            "waitfor",
            "delay",
            "dbms_",
        }

        for func in expression.find_all(exp.Func):
            func_name = func.this.name if hasattr(func.this, "name") else str(func.this)
            if any(dangerous in func_name.lower() for dangerous in dangerous_functions):
                if self.strict_mode:
                    msg = f"Dangerous function detected: {func_name}"
                    raise SQLInjectionError(msg, str(expression), func_name)
                logger.warning("Potentially dangerous function found: %s", func_name)


def validate_sql(statement: "Statement", dialect: "DialectType" = None, strict: bool = True) -> "ValidationResult":
    """Validate SQL string for security and safety.

    Args:
        statement: SQL string, expression, or Query object to validate
        dialect: SQL dialect for parsing
        strict: Whether to use strict validation

    Returns:
        ValidationResult with detailed information
    """
    from sqlspec.sql.statement import SQLStatement

    if isinstance(statement, SQLStatement):
        config = statement.config
        if statement.config.strict_mode != strict:
            config.strict_mode = strict
        if dialect and statement.dialect != dialect:
            statement.dialect = dialect
        statement = statement.copy(config=config)
        return statement.validate()

    return SQLValidator(strict_mode=strict).validate(statement, dialect)
