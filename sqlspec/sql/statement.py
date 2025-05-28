# ruff: noqa: PLR0917, SLF001
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
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union
from weakref import WeakKeyDictionary

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.exceptions import (
    ParameterError,
    RiskLevel,
    SQLInjectionError,
    SQLValidationError,
)
from sqlspec.sql.filters import StatementFilter, apply_filter  # Add StatementFilter import

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.sql.filters import StatementFilter
    from sqlspec.sql.parameters import ParameterInfo, ParameterStyle
    from sqlspec.typing import StatementParameterType

__all__ = (
    "SQLSanitizer",
    "SQLStatement",
    "SQLValidator",
    "Statement",
    "ValidationResult",
    "is_sql_safe",
    "sanitize_sql",
    "validate_sql",
)

logger = logging.getLogger("sqlspec")

# Define a type for SQL input
Statement = Union[str, exp.Expression, "SQLStatement"]


class UsesExpression:
    @staticmethod
    def get_expression(sql: "Statement", dialect: "DialectType" = None) -> exp.Expression:
        """Convert SQL input to expression.

        Returns:
            The parsed SQL expression.

        Raises:
            SQLValidationError: If SQL cannot be parsed.
        """
        if isinstance(sql, exp.Expression):
            return sql
        if isinstance(sql, SQLStatement):
            expr = sql.expression
            if expr is not None:
                return expr
            # Fall back to parsing the original SQL if expression is None
            return sqlglot.parse_one(sql.sql, read=dialect)
        # str case
        sql_str = sql
        if not sql_str or not sql_str.strip():
            return exp.Select()

        try:
            return sqlglot.parse_one(sql_str, read=dialect)
        except SQLGlotParseError as e:
            msg = f"SQL parsing failed: {e}"
            raise SQLValidationError(msg, sql_str, RiskLevel.HIGH) from e


class ValidationResult(UsesExpression):
    """Result of SQL validation with detailed information."""

    def __init__(
        self,
        is_safe: bool,
        risk_level: RiskLevel,
        issues: Optional[list[str]] = None,
        warnings: Optional[list[str]] = None,
        sanitized_sql: Optional[str] = None,
    ) -> None:
        self.is_safe = is_safe
        self.risk_level = risk_level
        self.issues = issues if issues is not None else []
        self.warnings = warnings if warnings is not None else []
        self.sanitized_sql = sanitized_sql

    def __bool__(self) -> bool:
        return self.is_safe


@dataclass
class SQLSanitizer(UsesExpression):
    """High-performance SQL sanitizer with caching."""

    # Class-level cache for parsed expressions
    _cache: ClassVar[WeakKeyDictionary[str, exp.Expression]] = WeakKeyDictionary()

    strict_mode: bool = True
    """Whether to use strict validation rules."""

    allow_comments: bool = True
    """Whether to allow SQL comments."""

    max_sql_length: int = 10000
    """Maximum allowed SQL length."""

    def sanitize(self, sql: "Statement", dialect: "DialectType" = None) -> exp.Expression:
        """Sanitize SQL by removing dangerous constructs.

        Args:
            sql: The SQL string, expression, or Query object to sanitize
            dialect: SQL dialect for parsing

        Returns:
            Sanitized SQL expression
        """
        return self._sanitize_expression(self.get_expression(sql, dialect))

    def _sanitize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Sanitize a sqlglot expression.

        Returns:
            The sanitized expression.
        """
        sanitized = expression.copy()
        if not self.allow_comments:
            for comment in sanitized.find_all(exp.Comment):
                comment.replace(exp.Anonymous())
        self._check_dangerous_constructs(sanitized)
        return sanitized

    def _check_dangerous_constructs(self, expression: exp.Expression) -> None:
        """Check for dangerous SQL constructs in the expression tree.

        Raises:
            SQLInjectionError: If dangerous constructs are found in strict mode.
        """
        # Check for potentially dangerous functions or operations
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
        }

        for func in expression.find_all(exp.Func):
            func_name = func.this.name if hasattr(func.this, "name") else str(func.this)
            if any(dangerous in func_name.lower() for dangerous in dangerous_functions):
                if self.strict_mode:
                    msg = f"Dangerous function detected: {func_name}"
                    raise SQLInjectionError(msg, str(expression), func_name)
                logger.warning("Potentially dangerous function found: %s", func_name)


@dataclass
class SQLValidator(UsesExpression):
    """Comprehensive SQL validator with security checks."""

    strict_mode: bool = True
    """Whether to use strict validation rules."""

    allow_ddl: bool = False
    """Whether to allow DDL statements (CREATE, ALTER, DROP)."""

    allow_dml: bool = True
    """Whether to allow DML statements (INSERT, UPDATE, DELETE)."""

    allow_procedural_code: bool = True
    """Whether to allow procedural code (DECLARE, BEGIN, END, etc.)."""

    allowed_schemas: frozenset[str] = field(default_factory=frozenset)
    """Set of allowed database schemas."""

    min_risk_to_raise: RiskLevel = RiskLevel.HIGH
    """Minimum risk level that will trigger a SQLValidationError if encountered."""

    def validate(self, sql: "Statement", dialect: "DialectType" = None) -> "ValidationResult":
        """Validate SQL for security and safety.

        Args:
            sql: The SQL string, expression, or Query object to validate
            dialect: SQL dialect for parsing

        Returns:
            ValidationResult with detailed validation information
        """
        issues: list[str] = []
        warnings: list[str] = []
        risk_level = RiskLevel.SAFE

        try:
            expression = self.get_expression(sql, dialect)
        except SQLValidationError as e:
            issues.append(str(e))
            return ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=issues)

        try:
            injection_issues = self._check_injection_patterns_ast(expression)
            if injection_issues:
                issues.extend(injection_issues)
                risk_level = RiskLevel(max(risk_level.value, RiskLevel.CRITICAL.value))

            unsafe_issues = self._check_unsafe_patterns_ast(expression)
            if unsafe_issues:
                issues.extend(unsafe_issues)
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
    def _check_injection_patterns_ast(expression: exp.Expression) -> list[str]:
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
    def _check_unsafe_patterns_ast(expression: exp.Expression) -> list[str]:
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

    def _check_statement_type(self, parsed: exp.Expression) -> list[str]:
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


def sanitize_sql(sql: "Statement", dialect: "DialectType" = None, strict: bool = True) -> "exp.Expression":
    """Sanitize SQL string for safe execution.

    Args:
        sql: SQL string, expression, or Query object to sanitize
        dialect: SQL dialect for parsing
        strict: Whether to use strict sanitization

    Returns:
        Sanitized SQL string
    """
    return SQLSanitizer(strict_mode=strict).sanitize(sql, dialect)


def validate_sql(sql: Statement, dialect: "DialectType" = None, strict: bool = True) -> ValidationResult:
    """Validate SQL string for security and safety.

    Args:
        sql: SQL string, expression, or Query object to validate
        dialect: SQL dialect for parsing
        strict: Whether to use strict validation

    Returns:
        ValidationResult with detailed information
    """
    return SQLValidator(strict_mode=strict).validate(sql, dialect)


def is_sql_safe(sql: "Statement", dialect: "DialectType" = None) -> bool:
    """Quick check if SQL is safe for execution.

    Args:
        sql: SQL string, expression, or Query object to check
        dialect: SQL dialect for parsing

    Returns:
        True if SQL is safe, False otherwise
    """
    try:
        # Validate with strict=True by default for safety checks
        result = validate_sql(sql, dialect, strict=True)
    except SQLValidationError:
        return False
    except Exception:  # noqa: BLE001
        logger.debug("is_sql_safe encountered an unexpected exception during validation.", exc_info=True)
        return False
    else:
        return result.is_safe


@dataclass
class StatementConfig:
    """Configuration for SQLStatement behavior."""

    enable_parsing: bool = True
    """Whether to enable SQLglot parsing for validation and transformation."""

    enable_validation: bool = True
    """Whether to enable SQL validation and security checks."""

    enable_sanitization: bool = True
    """Whether to enable SQL sanitization."""

    strict_mode: bool = True
    """Whether to use strict validation and sanitization rules."""

    allow_mixed_parameters: bool = False
    """Whether to allow mixing args and kwargs when parsing is disabled."""

    cache_parsed_expression: bool = True
    """Whether to cache the parsed expression for performance."""


class SQLStatement:
    """Represents a SQL statement with parameters and validation.

    This class provides a unified interface for SQL statements with automatic parameter
    binding, validation, and sanitization. It supports multiple parameter styles and
    can work with raw SQL strings, sqlglot expressions, or query builder objects.

    Key Features:
    - Intelligent parameter binding from args, kwargs, or explicit parameters
    - Security-focused validation and sanitization
    - Support for different placeholder styles for database drivers
    - Filter composition from sqlspec.sql.filters
    - Performance optimizations with caching
    - Configurable behavior for different use cases

    Example usage:
        >>> stmt = SQLStatement(
        ...     "SELECT * FROM users WHERE id = ?", [123]
        ... )
        >>> sql, params = stmt.get_sql(), stmt.get_parameters()

        >>> stmt = SQLStatement(
        ...     "SELECT * FROM users WHERE name = :name", name="John"
        ... )
        >>> sql = stmt.get_sql(
        ...     placeholder_style="pyformat_named"
        ... )  # %(name)s

        >>> from sqlspec.sql.filters import SearchFilter
        >>> stmt = stmt.append_filter(SearchFilter("name", "John"))
    """

    def __init__(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        config: Optional[StatementConfig] = None,
        validator: Optional[SQLValidator] = None,
        sanitizer: Optional[SQLSanitizer] = None,
    ) -> None:
        """Initialize SQLStatement.

        Args:
            sql: SQL string, expression, or SQLStatement to wrap
            parameters: Primary parameters (dict, list, tuple, or scalar)
            args: Positional arguments for parameters
            kwargs: Keyword arguments for parameters
            dialect: SQL dialect for parsing and transformation
            config: Configuration for statement behavior
            validator: Custom SQL validator (uses default if None)
            sanitizer: Custom SQL sanitizer (uses default if None)

        """
        # Handle recursive SQLStatement wrapping
        if isinstance(sql, SQLStatement):
            self._copy_from_existing(sql, parameters, args, kwargs, dialect, config, validator, sanitizer)
            return

        # Store configuration
        self._config = config or StatementConfig()
        self._dialect = dialect
        self._validator = validator or SQLValidator(strict_mode=self._config.strict_mode)
        self._sanitizer = sanitizer or SQLSanitizer(strict_mode=self._config.strict_mode)

        # Store original SQL for reference
        self._original_sql = str(sql) if not isinstance(sql, exp.Expression) else None

        # Initialize parameter converter
        from sqlspec.sql.parameters import ParameterConverter

        self._parameter_converter = ParameterConverter()

        # Process parameters and SQL
        self._initialize_statement(sql, parameters, args, kwargs)

    def _copy_from_existing(
        self,
        existing: "SQLStatement",
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
        dialect: "Optional[DialectType]",
        config: Optional[StatementConfig],
        validator: Optional[SQLValidator],
        sanitizer: Optional[SQLSanitizer],
    ) -> None:
        """Copy from existing SQLStatement, optionally overriding values."""
        self._config = config or existing._config
        self._dialect = dialect or existing._dialect
        self._validator = validator or existing._validator
        self._sanitizer = sanitizer or existing._sanitizer
        self._original_sql = existing._original_sql
        self._parameter_converter = existing._parameter_converter

        # If new parameters provided, re-process with existing SQL
        if parameters is not None or args is not None or kwargs is not None:
            # Use the original expression from the existing statement
            original_expr = existing._raw_expression
            self._initialize_statement(original_expr, parameters, args, kwargs)
        else:
            # Copy all processed state
            self._raw_expression = existing._raw_expression
            self._parsed_expression = existing._parsed_expression
            self._parameter_info = existing._parameter_info
            self._merged_parameters = existing._merged_parameters
            self._placeholder_map = existing._placeholder_map
            self._transformed_sql = existing._transformed_sql
            self._validation_result = existing._validation_result

    def _initialize_statement(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> None:
        """Initialize the statement with SQL and parameters.

        Raises:
            SQLValidationError: If SQL parsing fails when parsing is enabled.
        """
        # Store raw expression (could be string, Expression, or other)
        self._raw_expression = sql

        # Convert to sqlglot expression if parsing enabled
        if self._config.enable_parsing:
            if isinstance(sql, exp.Expression):
                self._parsed_expression = sql
            else:
                self._parsed_expression = self._parse_sql(str(sql))
        else:
            self._parsed_expression = None

        # Process parameters
        if self._config.enable_parsing and self._parsed_expression is not None:
            # Use sqlglot-based parameter processing
            self._process_parameters_with_parsing(str(sql), parameters, args, kwargs)
        else:
            # Use regex-based parameter processing (no sqlglot)
            self._process_parameters_without_parsing(str(sql), parameters, args, kwargs)

        # Validate if enabled
        if self._config.enable_validation:
            self._validation_result = self._validator.validate(self._parsed_expression or str(sql), self._dialect)
            if not self._validation_result.is_safe and self._config.strict_mode:
                msg = f"SQL validation failed: {', '.join(self._validation_result.issues)}"
                raise SQLValidationError(msg, str(sql), self._validation_result.risk_level)
        else:
            self._validation_result = None

    def _parse_sql(self, sql_str: str) -> exp.Expression:
        """Parse SQL string to sqlglot expression.

        Returns:
            Parsed sqlglot expression.

        Raises:
            SQLValidationError: If SQL parsing fails in strict mode.
        """
        try:
            return UsesExpression.get_expression(sql_str, self._dialect)
        except SQLValidationError:
            if self._config.strict_mode:
                raise
            logger.warning("Failed to parse SQL, continuing without parsed expression: %s", sql_str)
            return exp.Select()  # Return empty select as fallback

    def _process_parameters_with_parsing(
        self,
        sql_str: str,
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> None:
        """Process parameters using sqlglot parsing.

        Raises:
            ParameterError: If parameter processing fails.
            TypeError: If parameter types are invalid.
            ValueError: If parameter values are invalid.
        """
        try:
            (
                self._transformed_sql,
                self._parameter_info,
                self._merged_parameters,
                self._placeholder_map,
            ) = self._parameter_converter.convert_parameters(
                sql_str,
                parameters,
                args,
                kwargs,
                validate=self._config.enable_validation,
            )
        except (ParameterError, ValueError, TypeError) as e:
            if self._config.strict_mode:
                raise
            logger.warning("Parameter processing failed, using basic merge: %s", e)
            self._parameter_info = []
            self._merged_parameters = self._parameter_converter.merge_parameters(parameters, args, kwargs)
            self._placeholder_map = {}
            self._transformed_sql = sql_str

    def _process_parameters_without_parsing(
        self,
        sql_str: str,
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> None:
        """Process parameters without sqlglot parsing (regex-based).

        Raises:
            ParameterError: If mixed parameters are used when not allowed.
        """
        # Check for mixed parameters if not allowed
        if not self._config.allow_mixed_parameters and args and kwargs:
            msg = "Cannot mix args and kwargs when parsing is disabled"
            raise ParameterError(msg, sql_str)

        # Simple parameter merging
        self._merged_parameters = self._parameter_converter.merge_parameters(parameters, args, kwargs)
        self._parameter_info = []  # No detailed info without parsing
        self._placeholder_map = {}
        self._transformed_sql = sql_str

    @property
    def sql(self) -> str:
        """Get the original SQL string."""
        return self._original_sql or str(self._raw_expression)

    @property
    def expression(self) -> Optional[exp.Expression]:
        """Get the parsed sqlglot expression if available."""
        return self._parsed_expression

    @property
    def parameters(self) -> "StatementParameterType":
        """Get the merged parameters."""
        return self._merged_parameters

    @property
    def parameter_info(self) -> "list[ParameterInfo]":
        """Get detailed parameter information."""
        return self._parameter_info

    @property
    def placeholder_map(self) -> "dict[str, Union[str, int]]":
        """Get the placeholder mapping for internal use."""
        return self._placeholder_map

    @property
    def validation_result(self) -> Optional[ValidationResult]:
        """Get the validation result if validation was performed."""
        return self._validation_result

    @property
    def is_safe(self) -> bool:
        """Check if the statement is safe for execution."""
        if self._validation_result is None:
            return True  # Assume safe if not validated
        return self._validation_result.is_safe

    def get_sql(
        self,
        placeholder_style: "Optional[Union[str, ParameterStyle]]" = None,
        statement_separator: str = ";",
        include_statement_separator: bool = False,
    ) -> str:
        """Get SQL string with specified placeholder style.

        Args:
            placeholder_style: Target placeholder style for the SQL output.
                Can be a string ('qmark', 'named', 'pyformat_named', etc.) or ParameterStyle enum.
                If None, uses dialect-appropriate default.
            statement_separator: Optional statement separator to use (default is ';').
            include_statement_separator: Whether to ensure SQL ends with statement separator.

        Returns:
            SQL string with placeholders in the requested style.

        Example:
            >>> stmt = SQLStatement(
            ...     "SELECT * FROM users WHERE id = ?", [123]
            ... )
            >>> stmt.get_sql()  # Returns original format
            'SELECT * FROM users WHERE id = ?'
            >>> stmt.get_sql(placeholder_style="named")
            'SELECT * FROM users WHERE id = :param_0'
        """
        if placeholder_style is None:
            sql = self._original_sql or str(self._raw_expression)
        else:
            # Convert placeholder style and transform SQL
            sql = self._transform_sql_placeholders(placeholder_style)

        if include_statement_separator and not sql.rstrip().endswith(";"):
            sql = sql.rstrip() + ";"

        return sql

    def _transform_sql_placeholders(self, target_style: "Union[str, ParameterStyle]") -> str:
        """Transform SQL placeholders to target style.

        Returns:
            SQL string with placeholders transformed to the target style.
        """
        from sqlspec.sql.parameters import ParameterStyle

        # Convert string to enum
        if isinstance(target_style, str):
            style_map = {
                "qmark": ParameterStyle.QMARK,
                "named": ParameterStyle.NAMED_COLON,
                "named_colon": ParameterStyle.NAMED_COLON,
                "named_at": ParameterStyle.NAMED_AT,
                "named_dollar": ParameterStyle.NAMED_DOLLAR,
                "numeric": ParameterStyle.NUMERIC,
                "pyformat_named": ParameterStyle.PYFORMAT_NAMED,
                "pyformat_positional": ParameterStyle.PYFORMAT_POSITIONAL,
                "static": ParameterStyle.STATIC,
            }
            target_style = style_map.get(target_style.lower(), ParameterStyle.QMARK)

        # If no parsing was done, return original SQL
        if not self._config.enable_parsing or not self._parsed_expression:
            return self._original_sql or str(self._raw_expression)

        # For STATIC style, replace all placeholders with actual values
        if target_style == ParameterStyle.STATIC:
            return self._generate_static_sql()

        # Transform using sqlglot dialect transpilation
        try:
            # Convert the parsed expression to target dialect format
            dialect_map = {
                ParameterStyle.QMARK: "sqlite",
                ParameterStyle.NAMED_COLON: "oracle",
                ParameterStyle.NAMED_AT: "tsql",
                ParameterStyle.NAMED_DOLLAR: "postgres",
                ParameterStyle.NUMERIC: "postgres",
                ParameterStyle.PYFORMAT_NAMED: "mysql",
                ParameterStyle.PYFORMAT_POSITIONAL: "mysql",
            }

            target_dialect = dialect_map.get(target_style, "sqlite")
            return self._parsed_expression.sql(dialect=target_dialect)
        except (SQLGlotParseError, ValueError, AttributeError) as e:
            logger.warning("Failed to transform placeholders to %s: %s", target_style, e)
            return self._original_sql or str(self._raw_expression)

    def _generate_static_sql(self) -> str:
        """Generate SQL with parameters substituted as literals.

        Returns:
            SQL string with parameter values directly substituted.

        Note:
            This method creates SQL with embedded values, which should only be used
            for debugging, logging, or testing purposes. It is NOT recommended for
            actual database execution as it bypasses parameterized queries.
        """
        if not self._merged_parameters:
            return self._original_sql or str(self._raw_expression)

        # Use the parameter infrastructure to do proper substitution
        # If we have parameter_info and placeholder_map, use that for precision
        if self._parameter_info and self._placeholder_map:
            return self._generate_static_sql_with_parsing()
        # Fallback to simple approach for basic cases
        return self._generate_static_sql_simple()

    def _generate_static_sql_with_parsing(self) -> str:
        """Generate static SQL using parameter info from parsing.

        Returns:
            SQL string with parameter values substituted using precise positioning.
        """
        # Start with the original SQL
        sql = self._original_sql or str(self._raw_expression)

        # Sort parameters by position in reverse order so we don't mess up positions
        # when doing replacements
        sorted_params = sorted(self._parameter_info, key=lambda p: p.position, reverse=True)

        for param_info in sorted_params:
            # Get the parameter value
            if param_info.name and isinstance(self._merged_parameters, dict):
                value = self._merged_parameters.get(param_info.name)
            elif isinstance(self._merged_parameters, (list, tuple)):
                # For positional parameters, use ordinal index
                if param_info.ordinal < len(self._merged_parameters):
                    value = self._merged_parameters[param_info.ordinal]
                else:
                    value = None
            else:
                value = None

            # Escape the value for SQL
            escaped_value = self._escape_value(value)

            # Replace the exact placeholder text at the exact position
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)
            sql = sql[:start_pos] + escaped_value + sql[end_pos:]

        return sql

    def _generate_static_sql_simple(self) -> str:
        """Simple static SQL generation for fallback cases.

        Returns:
            SQL string with parameter values substituted using simple replacement.
        """
        sql = self._original_sql or str(self._raw_expression)

        if isinstance(self._merged_parameters, dict):
            # Replace named parameters
            for name, value in self._merged_parameters.items():
                escaped_value = self._escape_value(value)
                # Replace different named parameter styles
                sql = sql.replace(f":{name}", escaped_value)
                sql = sql.replace(f"@{name}", escaped_value)
                sql = sql.replace(f"${{{name}}}", escaped_value)
                sql = sql.replace(f"%({name})s", escaped_value)
        elif isinstance(self._merged_parameters, (list, tuple)):
            # Replace positional placeholders in order
            for value in self._merged_parameters:
                escaped_value = self._escape_value(value)
                # Replace first occurrence of positional placeholders
                if "?" in sql:
                    sql = sql.replace("?", escaped_value, 1)
                elif "%s" in sql:
                    sql = sql.replace("%s", escaped_value, 1)
                else:
                    break  # No more placeholders to replace

        return sql

    @staticmethod
    def _escape_value(value: Any) -> str:
        """Escape a value for safe inclusion in SQL (basic implementation).

        Args:
            value: The value to escape for SQL inclusion.

        Returns:
            Escaped string representation safe for SQL.
        """
        if value is None:
            return "NULL"
        if isinstance(value, str):
            # Basic SQL string escaping
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        # For other types, convert to string and escape
        str_value = str(value).replace("'", "''")
        return f"'{str_value}'"

    def get_parameters(self, style: "Optional[Union[str, ParameterStyle]]" = None) -> "StatementParameterType":
        """Get parameters in the specified format.

        Args:
            style: Target parameter style. If None, returns merged parameters as-is.
                  Can be 'dict', 'list', 'tuple', or a ParameterStyle enum.

        Returns:
            Parameters in the requested format.

        Note:
            Currently supports basic format conversion between dict/list/tuple.
            For complex parameter style transformations (e.g., named to positional),
            use get_sql() with the appropriate placeholder_style parameter.
        """
        if style is None:
            return self._merged_parameters

        # Convert string style names to formats
        if isinstance(style, str):
            style_lower = style.lower()
            if style_lower in {"dict", "named"}:
                return self._convert_to_dict_parameters()
            if style_lower in {"list", "positional"}:
                return self._convert_to_list_parameters()
            if style_lower == "tuple":
                params = self._convert_to_list_parameters()
                return tuple(params) if isinstance(params, list) else params

        # For ParameterStyle enum, convert to appropriate format
        from sqlspec.sql.parameters import ParameterStyle

        if isinstance(style, ParameterStyle):
            if style in {
                ParameterStyle.NAMED_COLON,
                ParameterStyle.NAMED_AT,
                ParameterStyle.NAMED_DOLLAR,
                ParameterStyle.PYFORMAT_NAMED,
            }:
                return self._convert_to_dict_parameters()
            if style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.PYFORMAT_POSITIONAL}:
                return self._convert_to_list_parameters()

        return self._merged_parameters

    def _convert_to_dict_parameters(self) -> dict[str, Any]:
        """Convert parameters to dict format.

        Returns:
            Dictionary with parameter names as keys and values as values.
        """
        if isinstance(self._merged_parameters, dict):
            return self._merged_parameters.copy()
        if isinstance(self._merged_parameters, (list, tuple)):
            # Convert positional to named using parameter info if available
            if self._parameter_info:
                result = {}
                for i, param_info in enumerate(self._parameter_info):
                    if param_info.name and i < len(self._merged_parameters):
                        result[param_info.name] = self._merged_parameters[i]
                return result
            # Fallback: create generic names
            return {f"param_{i}": value for i, value in enumerate(self._merged_parameters)}
        return {"param_0": self._merged_parameters}

    def _convert_to_list_parameters(self) -> list[Any]:
        """Convert parameters to list format.

        Returns:
            List of parameter values in positional order.
        """
        if isinstance(self._merged_parameters, (list, tuple)):
            return list(self._merged_parameters)
        if isinstance(self._merged_parameters, dict):
            # Convert named to positional using parameter info order if available
            if self._parameter_info:
                return [
                    self._merged_parameters[param_info.name]
                    for param_info in sorted(self._parameter_info, key=lambda p: p.ordinal)
                    if param_info.name and param_info.name in self._merged_parameters
                ]
            # Fallback: just return values in order
            return list(self._merged_parameters.values())
        return [self._merged_parameters]

    def copy(
        self,
        sql: Optional[Statement] = None,
        parameters: "Optional[StatementParameterType]" = None,
        *,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        config: Optional[StatementConfig] = None,
        validator: Optional[SQLValidator] = None,
        sanitizer: Optional[SQLSanitizer] = None,
    ) -> "SQLStatement":
        """Create a copy of the statement, optionally overriding attributes.

        Args:
            sql: New SQL string, expression, or SQLStatement.
            parameters: New primary parameters.
            args: New positional arguments for parameters.
            kwargs: New keyword arguments for parameters.
            dialect: New SQL dialect.
            config: New statement configuration.
            validator: New SQL validator.
            sanitizer: New SQL sanitizer.

        Returns:
            A new SQLStatement instance.
        """
        # Determine which attributes to use from the existing instance or new arguments
        # The logic here is to prioritize newly provided arguments, then existing._raw_expression if sql is None
        # and finally self attributes as the ultimate fallback.

        # If new sql is provided, it's the base for the new statement.
        # If not, but new parameters/args/kwargs are, use existing SQL with new params.
        # Otherwise, it's a shallow copy or a copy with other attributes (dialect, config, etc.) changed.

        final_sql = sql if sql is not None else self._raw_expression
        final_parameters = parameters
        final_args = args
        final_kwargs = kwargs

        # If no new SQL and no new parameters, it's a direct copy of parameter-related state
        # unless other config items like dialect, config, validator, sanitizer are changing.
        if sql is None and parameters is None and args is None and kwargs is None:
            # This path is for when only config-like items are changed, or it's a true shallow copy
            # We still need to pass the existing parameters to the constructor if they exist
            final_parameters = self._merged_parameters  # or self._parameter_info depending on how __init__ consumes it
            # The constructor will handle None for args/kwargs if final_parameters is already merged

        return SQLStatement(
            sql=final_sql if final_sql is not None else self.sql,  # Fallback to self.sql if _raw_expression was None
            parameters=final_parameters,
            args=final_args,
            kwargs=final_kwargs,
            dialect=dialect if dialect is not None else self._dialect,
            config=config if config is not None else self._config,
            validator=validator if validator is not None else self._validator,
            sanitizer=sanitizer if sanitizer is not None else self._sanitizer,
        )

    def append_filter(self, filter_to_apply: "StatementFilter") -> "SQLStatement":
        """Applies a filter to the statement and returns a new SQLStatement.

        Args:
            filter_to_apply: The filter object to apply.

        Returns:
            A new SQLStatement instance with the filter applied.
        """
        return apply_filter(self, filter_to_apply)

    def validate(self) -> ValidationResult:
        """Validate the statement and return detailed results.

        Returns:
            ValidationResult with validation details.
        """
        if self._validation_result is None:
            self._validation_result = self._validator.validate(self._parsed_expression or self.sql, self._dialect)
        return self._validation_result

    def sanitize(self) -> "SQLStatement":
        """Return a sanitized version of the statement.

        Returns:
            New SQLStatement with sanitized SQL.

        Raises:
            SQLValidationError: If SQL sanitization fails.
        """
        if not self._config.enable_sanitization:
            return self

        try:
            sanitized_expr = self._sanitizer.sanitize(self._parsed_expression or self.sql, self._dialect)
            return SQLStatement(
                sanitized_expr,
                self._merged_parameters,
                dialect=self._dialect,
                config=self._config,
                validator=self._validator,
                sanitizer=self._sanitizer,
            )
        except (SQLValidationError, ValueError, TypeError) as e:
            msg = f"SQL sanitization failed: {e}"
            raise SQLValidationError(msg, self.sql, RiskLevel.HIGH) from e

    def __str__(self) -> str:
        """String representation showing SQL.

        Returns:
            SQL string for display.
        """
        return self.sql

    def __repr__(self) -> str:
        """Detailed string representation.

        Returns:
            Detailed string representation including SQL and parameters.
        """
        params_repr = f", parameters={self._merged_parameters!r}" if self._merged_parameters else ""
        return f"SQLStatement({self.sql!r}{params_repr})"
