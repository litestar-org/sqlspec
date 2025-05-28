# ruff: noqa: PLR0917, SLF001, PLR0904
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
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.exceptions import (
    ParameterError,
    RiskLevel,
    SQLInjectionError,
    SQLParsingError,
    SQLSpecError,
    SQLValidationError,
)
from sqlspec.sql.filters import apply_filter
from sqlspec.sql.parameters import ParameterConverter, ParameterStyle, ParameterValidator

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions import Condition

    from sqlspec.sql.filters import StatementFilter
    from sqlspec.sql.parameters import ParameterInfo
    from sqlspec.typing import StatementParameterType

__all__ = (
    "SQLSanitizer",
    "SQLStatement",
    "SQLValidator",
    "Statement",
    "UsesExpression",
    "ValidationResult",
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


class ValidationResult:
    """Result of SQL validation with detailed information."""

    __slots__ = (
        "is_safe",
        "issues",
        "risk_level",
        "sanitized_sql",
        "warnings",
    )

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


class SQLPreprocessor(UsesExpression):
    """Base class for SQL preprocessors that can apply filters."""


@dataclass
class SQLSanitizer(SQLPreprocessor):
    """SQL Sanitizer"""

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
class SQLValidator(SQLPreprocessor):
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
    def _check_injection_patterns_ast(expression: "exp.Expression") -> list[str]:
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
    def _check_unsafe_patterns_ast(expression: "exp.Expression") -> list[str]:
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


def validate_sql(sql: "Statement", dialect: "DialectType" = None, strict: bool = True) -> "ValidationResult":
    """Validate SQL string for security and safety.

    Args:
        sql: SQL string, expression, or Query object to validate
        dialect: SQL dialect for parsing
        strict: Whether to use strict validation

    Returns:
        ValidationResult with detailed information
    """
    return SQLValidator(strict_mode=strict).validate(sql, dialect)


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

    validator: SQLValidator = field(default_factory=lambda: SQLValidator(strict_mode=True))
    """SQL validator to use. Defaults to strict mode validator."""

    sanitizer: SQLSanitizer = field(default_factory=lambda: SQLSanitizer(strict_mode=True))
    """SQL sanitizer to use. Defaults to strict mode sanitizer."""

    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    """Parameter converter to use for parameter processing."""

    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    """Parameter validator to use for parameter validation."""


class SQLStatement:
    """Represents a SQL statement with parameters and validation.

    This class provides a unified interface for SQL statements with automatic parameter
    binding, validation, and sanitization. It supports multiple parameter styles and
    can work with raw SQL strings, sqlglot expressions, or query builder objects.
    It is designed to be immutable; methods that modify the statement return a new instance.

    Key Features:
    - Intelligent parameter binding from args, kwargs, or explicit parameters
    - Security-focused validation and sanitization
    - Support for different placeholder styles for database drivers
    - Filter composition from sqlspec.sql.filters
    - Performance optimizations with caching
    - Configurable behavior for different use cases
    - Immutability: Modification methods return new instances.

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

    __slots__ = (
        "_dialect",
        "_merged_parameters",
        "_original_input",
        "_parameter_info",
        "_parsed_expression",
        "_statement_config",
        "_validation_result",
    )

    def __init__(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        statement_config: Optional[StatementConfig] = None,
        _existing_statement_copy_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize a SQLStatement instance."""
        _existing_statement_copy_data = _existing_statement_copy_data or {}
        statement_config = statement_config or StatementConfig()
        if isinstance(sql, SQLStatement):
            self._copy_from_existing(
                existing=sql,
                parameters=parameters,
                args=args,
                kwargs=kwargs,
                dialect=dialect,
                statement_config=statement_config,
            )
            return

        self._statement_config = _existing_statement_copy_data.get("_statement_config", statement_config)
        self._dialect = _existing_statement_copy_data.get("_dialect", dialect)
        self._original_input = _existing_statement_copy_data.get("_original_input", sql)
        self._parsed_expression = _existing_statement_copy_data.get("_parsed_expression", None)
        self._parameter_info = _existing_statement_copy_data.get("_parameter_info", [])
        self._merged_parameters = _existing_statement_copy_data.get("_merged_parameters", [])
        self._validation_result = _existing_statement_copy_data.get("_validation_result", None)
        if not _existing_statement_copy_data:
            self._initialize_statement(self._original_input, parameters, args, kwargs)

    def _copy_from_existing(
        self,
        existing: "SQLStatement",
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
        dialect: "Optional[DialectType]",
        statement_config: Optional[StatementConfig],
    ) -> None:
        """Copy from existing SQLStatement, optionally overriding values."""
        self._statement_config = statement_config if statement_config is not None else existing._statement_config
        self._dialect = dialect if dialect is not None else existing._dialect
        self._original_input = existing._original_input

        # If new parameters provided, re-process with existing SQL
        if parameters is not None or args is not None or kwargs is not None:
            # Use the raw expression from the existing statement
            current_sql_source = existing._original_input
            self._initialize_statement(current_sql_source, parameters, args, kwargs)
        else:
            self._original_input = existing._original_input
            self._parsed_expression = existing._parsed_expression
            self._parameter_info = existing._parameter_info
            self._merged_parameters = existing._merged_parameters
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
        self._original_input = sql

        if self._statement_config.enable_parsing:
            if isinstance(sql, exp.Expression):
                self._parsed_expression = sql
            else:
                self._parsed_expression = self._parse_sql(str(sql))
        else:
            self._parsed_expression = None

        if self._statement_config.enable_parsing and self._parsed_expression is not None:
            # Check if we have placeholders but no parameters - in this case, be lenient during initialization
            has_any_parameters = parameters is not None or args is not None or kwargs is not None
            if not has_any_parameters:
                temp_config = replace(self._statement_config, enable_validation=False)
                original_config = self._statement_config
                self._statement_config = temp_config
                try:
                    self._process_parameters_with_parsing(str(sql), parameters, args, kwargs)
                finally:
                    # Restore original config
                    self._statement_config = original_config
            else:
                self._process_parameters_with_parsing(str(sql), parameters, args, kwargs)
        else:
            self._process_parameters_without_parsing(str(sql), parameters, args, kwargs)

        # Sanitize if enabled and parsing is enabled
        if (
            self._statement_config.enable_parsing
            and self._statement_config.enable_sanitization
            and self._parsed_expression
        ):
            try:
                # Ensure the dialect is passed to the sanitizer
                sanitized_expr = self._statement_config.sanitizer.sanitize(
                    self._parsed_expression, self._dialect
                )
                # Update the original input if sanitization changes the expression
                # and the original input was an expression itself or parsing is enabled.
                # This ensures that .sql property reflects the sanitized version.
                if sanitized_expr is not self._parsed_expression:
                    self._parsed_expression = sanitized_expr
                    # If the original input was a string, we don't update it directly.
                    # The .sql property will generate from _parsed_expression.
                    # If original input was an expression, update it to keep them in sync.
                    if isinstance(self._original_input, exp.Expression):
                        self._original_input = self._parsed_expression

            except (SQLValidationError, ValueError, TypeError) as e:
                if self._statement_config.strict_mode:
                    msg = "SQL sanitization failed"
                    raise SQLValidationError(
                        msg, str(sql), self._validation_result.risk_level if self._validation_result else RiskLevel.LOW
                    ) from e
                logger.warning("SQL sanitization failed during initialization: %s", e)

        if self._statement_config.enable_validation:
            self._validation_result = self._statement_config.validator.validate(
                self._parsed_expression or str(sql), self._dialect
            )
            if (
                self._validation_result is not None
                and not self._validation_result.is_safe
                and self._statement_config.strict_mode
            ):
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
            if self._statement_config.strict_mode:
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
            _, self._parameter_info, self._merged_parameters, _ = (
                self._statement_config.parameter_converter.convert_parameters(
                    sql_str, parameters, args, kwargs, validate=self._statement_config.enable_validation
                )
            )
        except (ParameterError, ValueError, TypeError) as e:
            if self._statement_config.strict_mode:
                raise
            logger.warning("Parameter processing failed, using basic merge: %s", e)
            self._parameter_info = []
            self._merged_parameters = self._statement_config.parameter_converter.merge_parameters(
                parameters, args, kwargs
            )

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
        if not self._statement_config.allow_mixed_parameters and args and kwargs:
            msg = "Cannot mix args and kwargs when parsing is disabled"
            raise ParameterError(msg, sql_str)

        # When parsing is disabled and mixed parameters are allowed, store as tuple
        if args and kwargs and self._statement_config.allow_mixed_parameters:
            self._merged_parameters = (list(args) if args else [], dict(kwargs) if kwargs else {})
        else:
            # Simple parameter merging for other cases
            self._merged_parameters = self._statement_config.parameter_converter.merge_parameters(
                parameters, args, kwargs
            )
        self._parameter_info = []

    @property
    def sql(self) -> str:
        """Get the SQL string if provided as string, or generated SQL from expression."""
        # If parsing is enabled and we have a parsed (and possibly sanitized) expression,
        # generate the SQL from it. Otherwise, return the original input string.
        if self._statement_config.enable_parsing and self._parsed_expression is not None:
            return self.get_sql()  # Calls the method that uses _parsed_expression
        return str(self._original_input)

    @property
    def config(self) -> "StatementConfig":
        """Get the statement configuration."""
        return self._statement_config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """Get the parsed and potentially sanitized sqlglot expression if available and parsing enabled."""
        if not self._statement_config.enable_parsing:
            return None
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
    def validation_result(self) -> "Optional[ValidationResult]":
        """Get the validation result if validation was performed."""
        return self._validation_result

    def validate(self) -> "ValidationResult":
        """Validate the statement and return detailed results.

        Returns:
            ValidationResult with validation details.
        """
        if self._validation_result is None:
            if self._statement_config.enable_validation:
                return self._statement_config.validator.validate(
                    self.expression if self.expression is not None else self.sql, self._dialect
                )
            return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
        return self._validation_result

    @property
    def is_safe(self) -> bool:
        """Check if the statement is safe based on validation results.

        Returns:
            True if the statement is safe, False otherwise.
        """
        if self._validation_result is None:
            return True
        return self._validation_result.is_safe

    def get_sql(
        self,
        placeholder_style: "Optional[Union[str, ParameterStyle]]" = None,
        statement_separator: str = ";",
        include_statement_separator: bool = False,
        dialect: "Optional[DialectType]" = None,
    ) -> str:
        """Get SQL string with specified placeholder style.

        Args:
            placeholder_style: The target placeholder style.
                Can be a string ('qmark', 'named', 'pyformat_named', etc.) or ParameterStyle enum.
                If None, uses dialect-appropriate default or existing SQL if parsing disabled.
            statement_separator: The statement separator to use.
            include_statement_separator: Whether to include the statement separator.
            dialect: The SQL dialect to use for SQL generation.

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
        target_dialect = dialect if dialect is not None else self._dialect

        if not self._statement_config.enable_parsing and self.expression is None:
            sql = str(self._original_input)
            if include_statement_separator and not sql.rstrip().endswith(statement_separator):
                sql = sql.rstrip() + statement_separator
            return sql

        if self.expression is not None:
            if placeholder_style is None:
                sql = self.expression.sql(dialect=target_dialect)
            else:
                sql = self._transform_sql_placeholders(placeholder_style, self.expression, target_dialect)
        else:
            sql = str(self._original_input)

        if include_statement_separator and not sql.rstrip().endswith(statement_separator):
            sql = sql.rstrip() + statement_separator

        return sql

    def _transform_sql_placeholders(
        self,
        target_style: "Union[str, ParameterStyle]",
        expression_to_render: "exp.Expression",
        dialect: "Optional[DialectType]" = None,
    ) -> str:
        """Transform SQL placeholders to target style using the provided expression.

        Args:
            target_style: The target placeholder style.
            expression_to_render: The sqlglot expression to render.
            dialect: The SQL dialect to use for rendering.

        Returns:
            SQL string with placeholders transformed to the target style.
        """
        target_dialect = dialect if dialect is not None else self._dialect

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
            try:
                target_style_enum = style_map[target_style.lower()]
            except KeyError:
                logger.warning("Unknown placeholder_style '%s', defaulting to qmark.", target_style)
                target_style_enum = ParameterStyle.QMARK
        else:
            target_style_enum = target_style

        # For STATIC style, replace all placeholders with actual values
        if target_style_enum == ParameterStyle.STATIC:
            return self._generate_static_sql(expression_to_render)

        # Get the SQL with current placeholders
        sql = expression_to_render.sql(dialect=target_dialect)

        # Convert placeholders to target style
        return self._convert_placeholder_style(sql, target_style_enum)

    def _convert_placeholder_style(self, sql: str, target_style: "ParameterStyle") -> str:
        """Convert placeholder style in SQL string.

        Args:
            sql: SQL string with current placeholders
            target_style: Target placeholder style

        Returns:
            SQL string with converted placeholders
        """

        parameters_info = self.config.parameter_validator.extract_parameters(sql)

        if not parameters_info:
            return sql

        # Build the converted SQL by replacing parameters in reverse order (to preserve positions)
        result_sql = sql
        for param_info in reversed(parameters_info):
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)

            # Generate the new placeholder based on target style
            if target_style == ParameterStyle.QMARK:
                new_placeholder = "?"
            elif target_style == ParameterStyle.NAMED_COLON:
                new_placeholder = f":{param_info.name}" if param_info.name else f":param_{param_info.ordinal}"
            elif target_style == ParameterStyle.NAMED_DOLLAR:
                new_placeholder = f"${param_info.name}" if param_info.name else f"$param_{param_info.ordinal}"
            elif target_style == ParameterStyle.NUMERIC:
                new_placeholder = f":{param_info.ordinal + 1}"  # 1-based numbering
            elif target_style == ParameterStyle.NAMED_AT:
                new_placeholder = f"@{param_info.name}" if param_info.name else f"@param_{param_info.ordinal}"
            elif target_style == ParameterStyle.PYFORMAT_NAMED:
                new_placeholder = f"%({param_info.name})s" if param_info.name else f"%(param_{param_info.ordinal})s"
            elif target_style == ParameterStyle.PYFORMAT_POSITIONAL:
                new_placeholder = "%s"
            else:
                # For unsupported styles, keep original
                new_placeholder = param_info.placeholder_text

            # Replace the placeholder in the SQL
            result_sql = result_sql[:start_pos] + new_placeholder + result_sql[end_pos:]

        return result_sql

    def _generate_static_sql(self, expression_to_render: exp.Expression) -> str:
        """Generate SQL with parameters substituted as literals from the given expression.

        Args:
            expression_to_render: The sqlglot expression to use for generating static SQL.

        Returns:
            SQL string with parameter values directly substituted.
        """
        if not self._merged_parameters:
            return expression_to_render.sql(dialect=self._dialect)

        if self._parameter_info and self._merged_parameters:
            return self._generate_static_sql_with_parsing(expression_to_render)
        return self._generate_static_sql_simple(expression_to_render)

    def _generate_static_sql_with_parsing(self, expression_to_render: exp.Expression) -> str:
        """Generate static SQL using parameter info from parsing, applied to the given expression.

        Args:
            expression_to_render: The sqlglot expression to use as the base.

        Returns:
            SQL string with parameter values substituted using precise positioning.
        """
        sql = expression_to_render.sql(dialect=self._dialect)

        sorted_params = sorted(self._parameter_info, key=lambda p: p.position, reverse=True)

        for param_info in sorted_params:
            if param_info.name and isinstance(self._merged_parameters, dict):
                value = self._merged_parameters.get(param_info.name)
            elif isinstance(self._merged_parameters, (list, tuple)):
                if param_info.ordinal < len(self._merged_parameters):
                    value = self._merged_parameters[param_info.ordinal]
                else:
                    value = None
            else:
                value = None

            escaped_value = self._escape_value(value)

            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)
            sql = sql[:start_pos] + escaped_value + sql[end_pos:]

        return sql

    def _generate_static_sql_simple(self, expression_to_render: exp.Expression) -> str:
        """Generate static SQL with robust parameter substitution using parameter parsing.

        This method uses the same robust parameter detection as _convert_placeholder_style
        to avoid false positives when replacing placeholders with literal values.

        Args:
            expression_to_render: The sqlglot expression to use as the base.

        Returns:
            SQL string with parameter values substituted using precise positioning.
        """

        sql = expression_to_render.sql(dialect=self._dialect)

        if not self._merged_parameters:
            return sql

        parameters_info = self.config.parameter_validator.extract_parameters(sql)

        if not parameters_info:
            return sql

        # Replace parameters in reverse order to preserve positions
        result_sql = sql
        for param_info in reversed(parameters_info):
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)
            value = self._get_parameter_value_for_substitution(param_info)
            escaped_value = self._escape_value(value)
            result_sql = result_sql[:start_pos] + escaped_value + result_sql[end_pos:]

        return result_sql

    def _get_parameter_value_for_substitution(self, param_info: "ParameterInfo") -> Any:
        """Get the parameter value for substitution based on ParameterInfo.

        Args:
            param_info: ParameterInfo containing parameter details.

        Returns:
            The parameter value to substitute, or None if not found.
        """
        if not self._merged_parameters:
            return None

        # Named parameter - look up by name
        if param_info.name:
            if isinstance(self._merged_parameters, dict):
                return self._merged_parameters.get(param_info.name)
            # If name exists but we have a list/tuple, can't match by name
            return None

        # Positional parameter - handle different parameter container types
        if isinstance(self._merged_parameters, (list, tuple)):
            # Direct positional access by ordinal
            if 0 <= param_info.ordinal < len(self._merged_parameters):
                return self._merged_parameters[param_info.ordinal]
            return None

        if isinstance(self._merged_parameters, dict):
            # Mixed parameter style - positional parameters get names like _arg_{ordinal}
            generated_name = f"_arg_{param_info.ordinal}"
            return self._merged_parameters.get(generated_name)

        # Scalar parameter - only valid for single parameter (ordinal 0)
        if param_info.ordinal == 0:
            return self._merged_parameters

        return None

    @staticmethod
    def _escape_value(value: Any) -> str:
        """Escape a value for safe inclusion in SQL (basic implementation).

        TODO: Enhance with proper SQL escaping for different data types.

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
            if style_lower in {"list", "positional", "qmark"}:
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
        # If _merged_parameters is None or a single scalar value
        if self._merged_parameters is None:
            return {}
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

    def get_parameters_for_style(self, style: "Union[str, ParameterStyle]") -> "StatementParameterType":
        """Get parameters in the specified format.

        This is an alias for get_parameters() for backward compatibility.

        Args:
            style: Target parameter style. Can be 'dict', 'list', 'tuple', 'qmark',
                  'named', or a ParameterStyle enum.

        Returns:
            Parameters in the requested format.
        """
        return self.get_parameters(style)

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

        final_sql = sql if sql is not None else self._original_input
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
        elif sql is not None and parameters is None and args is None and kwargs is None:
            # When copying with new SQL but no new parameters, preserve existing parameters
            final_parameters = self._merged_parameters

        return SQLStatement(
            sql=final_sql if final_sql is not None else self.sql,  # Fallback to self.sql if _raw_expression was None
            parameters=final_parameters,
            args=final_args,
            kwargs=final_kwargs,
            dialect=dialect if dialect is not None else self._dialect,
            statement_config=config if config is not None else self._statement_config,
        )

    def append_filter(self, filter_to_apply: "StatementFilter") -> "SQLStatement":
        """Applies a filter to the statement and returns a new SQLStatement.

        Args:
            filter_to_apply: The filter object to apply.

        Returns:
            A new SQLStatement instance with the filter applied.
        """
        return apply_filter(self, filter_to_apply)

    def sanitize(self) -> "SQLStatement":
        """Return a sanitized version of the statement.

        Returns:
            New SQLStatement with sanitized SQL.

        Raises:
            SQLValidationError: If SQL sanitization fails.
        """
        if not self._statement_config.enable_sanitization or not self.expression:
            return self

        try:
            return self.copy(sql=self._statement_config.sanitizer.sanitize(self.expression, self._dialect))
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
        current_sql_for_repr = self.get_sql()

        # Always show parameters if they were explicitly provided (even if empty)
        params_repr = ""
        if self._merged_parameters is not None:
            params_repr = f", parameters={self._merged_parameters!r}"

        config_repr = f", _config={self._statement_config!r}" if self._statement_config else ""
        return f"SQLStatement(sql={current_sql_for_repr!r}{params_repr}{config_repr})"

    def _get_current_expression_for_modification(self) -> exp.Expression:
        if not self._statement_config.enable_parsing:
            msg = "Cannot modify expression if parsing is disabled."
            raise SQLSpecError(msg)

        if self.expression is None:
            logger.debug("No existing expression to modify, starting with a new Select.")
            return exp.Select()
        return self.expression.copy()

    def where(self, *conditions: "Union[Condition, str]") -> "SQLStatement":
        """Applies WHERE conditions and returns a new SQLStatement.

        Args:
            *conditions: One or more condition strings or sqlglot Condition expressions.

        Raises:
            SQLParsingError: If the condition cannot be parsed.
            TypeError: If the condition is not a string or sqlglot Condition.

        Returns:
            A new SQLStatement instance with the conditions applied.
        """
        new_expr = self._get_current_expression_for_modification()

        for cond_item in conditions:
            condition_expression: Condition
            if isinstance(cond_item, str):
                try:
                    parsed_node = sqlglot.parse_one(cond_item, read=self._dialect)
                    if not isinstance(parsed_node, exp.Condition):
                        condition_expression = exp.condition(parsed_node)  # type: ignore
                    else:
                        condition_expression = parsed_node
                except Exception as e:
                    msg = f"Failed to parse string condition: '{cond_item}'. Error: {e}"
                    raise SQLParsingError(msg) from e
            elif isinstance(cond_item, exp.Condition):
                condition_expression = cond_item
            else:
                try:
                    condition_expression = exp.condition(cond_item)  # type: ignore
                except Exception as e:
                    msg = f"Invalid condition type: {type(cond_item)}. Must be str or sqlglot.exp.Condition. Error: {e}"
                    raise TypeError(msg) from e

            new_expr = new_expr.where(condition_expression)  # type: ignore[attr-defined] # sqlglot's where appends

        return self.copy(sql=new_expr)

    def limit(self, limit_value: int, use_parameter: bool = False) -> "SQLStatement":
        """Applies a LIMIT clause and returns a new SQLStatement.

        Args:
            limit_value: The limit value.
            use_parameter: If True, treats limit_value as a parameter name (str) or adds a new parameter.

        Returns:
            A new SQLStatement instance with the limit applied.
        """
        new_expr = self._get_current_expression_for_modification()
        if use_parameter:
            param_name = self.get_unique_parameter_name("limit_val")
            new_stmt = self.add_named_parameter(param_name, limit_value)
            expr_with_param = new_stmt._get_current_expression_for_modification()
            expr_with_param = expr_with_param.limit(exp.Placeholder(this=param_name))  # type: ignore[attr-defined]
            return new_stmt.copy(sql=expr_with_param)  # Copy again with the modified expression

        new_expr = new_expr.limit(limit_value)  # type: ignore[attr-defined]
        return self.copy(sql=new_expr)

    def offset(self, offset_value: int, use_parameter: bool = False) -> "SQLStatement":
        """Applies an OFFSET clause and returns a new SQLStatement.

        Args:
            offset_value: The offset value.
            use_parameter: If True, treats offset_value as a parameter name (str) or adds a new parameter.

        Returns:
            A new SQLStatement instance with the offset applied.
        """
        new_expr = self._get_current_expression_for_modification()
        if use_parameter:
            param_name = self.get_unique_parameter_name("offset_val")
            new_stmt = self.add_named_parameter(param_name, offset_value)
            expr_with_param = new_stmt._get_current_expression_for_modification()
            expr_with_param = expr_with_param.offset(exp.Placeholder(this=param_name))  # type: ignore[attr-defined]
            return new_stmt.copy(sql=expr_with_param)

        new_expr = new_expr.offset(offset_value)  # type: ignore[attr-defined]
        return self.copy(sql=new_expr)

    def order_by(self, *order_expressions: "Union[str, exp.Order, exp.Ordered]") -> "SQLStatement":
        """Applies ORDER BY clauses and returns a new SQLStatement.

        Args:
            *order_expressions: Column names (str) or sqlglot Order/Ordered expressions.

        Raises:
            TypeError: If the order expression is not a string or sqlglot Order/Ordered.

        Returns:
            A new SQLStatement instance with ordering applied.
        """
        new_expr = self._get_current_expression_for_modification()
        parsed_orders = []
        for o_expr in order_expressions:
            if isinstance(o_expr, str):
                # Basic parsing for "col asc", "col desc", "col"
                parts = o_expr.strip().lower().split()
                col_name = parts[0]
                direction = "asc"
                if len(parts) > 1 and parts[1] in {"asc", "desc"}:
                    direction = parts[1]

                order_exp = exp.column(col_name)
                if direction == "desc":
                    parsed_orders.append(order_exp.desc())
                else:
                    parsed_orders.append(order_exp.asc())

            elif isinstance(o_expr, (exp.Order, exp.Ordered)):
                parsed_orders.append(o_expr)
            else:
                msg = f"Unsupported order_by type: {type(o_expr)}"
                raise TypeError(msg)

        if parsed_orders:
            new_expr = new_expr.order_by(*parsed_orders)  # type: ignore[attr-defined]
        return self.copy(sql=new_expr)

    def add_named_parameter(self, name: str, value: Any) -> "SQLStatement":
        """Adds a named parameter and returns a new SQLStatement.

        Args:
            name: The name of the parameter.
            value: The value of the parameter.

        Returns:
            A new SQLStatement instance with the parameter added.
        """
        current_params_dict = self._convert_to_dict_parameters()
        current_params_dict[name] = value

        # Create a temporary config with validation disabled but parsing enabled for building operations
        from dataclasses import replace

        building_config = replace(self._statement_config, enable_validation=False, enable_parsing=True)

        return self.copy(parameters=current_params_dict, config=building_config)

    def get_unique_parameter_name(self, base_name: str) -> str:
        """Generates a unique parameter name based on the current parameters.

        Args:
            base_name: The desired base name for the parameter.

        Returns:
            A unique parameter name (e.g., "base_name", "base_name_1", etc.).
        """
        params_dict = self._convert_to_dict_parameters()
        if base_name not in params_dict:
            return base_name
        i = 1
        while True:
            name = f"{base_name}_{i}"
            if name not in params_dict:
                return name
            i += 1
