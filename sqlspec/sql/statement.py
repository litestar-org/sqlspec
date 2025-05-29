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
    SQLParsingError,
    SQLSpecError,
    SQLTransformationError,
    SQLValidationError,
)
from sqlspec.sql.filters import StatementFilter, apply_filter
from sqlspec.sql.parameters import ParameterConverter, ParameterStyle, ParameterValidator

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions import Condition

    from sqlspec.sql.parameters import ParameterInfo
    from sqlspec.sql.preprocessors import SQLTransformer, SQLValidator, ValidationResult
    from sqlspec.typing import StatementParameterType

__all__ = (
    "SQLStatement",
    "Statement",
)

logger = logging.getLogger("sqlspec")

# Define a type for SQL input
Statement = Union[str, exp.Expression, "SQLStatement"]


def _default_validator() -> "SQLValidator":
    from sqlspec.sql.preprocessors import SQLValidator

    return SQLValidator(strict_mode=True)


def _default_transformer() -> "SQLTransformer":
    from sqlspec.sql.preprocessors import SQLTransformer

    return SQLTransformer()


@dataclass
class StatementConfig:
    """Configuration for SQLStatement behavior."""

    enable_parsing: bool = True
    """Whether to enable SQLglot parsing for validation and transformation."""

    enable_validation: bool = True
    """Whether to enable SQL validation and security checks."""

    enable_transformations: bool = True
    """Whether to enable SQL transformer."""

    strict_mode: bool = True
    """Whether to use strict validation of rules."""

    allow_mixed_parameters: bool = False
    """Whether to allow mixing args and kwargs when parsing is disabled."""

    cache_parsed_expression: bool = True
    """Whether to cache the parsed expression for performance."""

    validator: "SQLValidator" = field(default_factory=_default_validator)
    """SQL validator to use. Defaults to strict mode validator."""

    transformer: "SQLTransformer" = field(default_factory=_default_transformer)
    """SQL transformer to use. Defaults to strict mode sanitizer."""

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
        "_parameter_info",
        "_parsed_expression",
        "_sql",
        "_statement_config",
        "_validation_result",
    )

    def __init__(
        self,
        statement: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        _existing_statement_copy_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize a SQLStatement instance."""
        _existing_statement_copy_data = _existing_statement_copy_data or {}
        statement_config = statement_config or StatementConfig()
        if isinstance(statement, SQLStatement):
            self._copy_from_existing(
                existing=statement,
                parameters=parameters,
                args=args,
                kwargs=kwargs,
                dialect=dialect,
                statement_config=statement_config,
            )
            if filters:
                self._apply_filters_in_place(filters)
            return

        self._statement_config = _existing_statement_copy_data.get("_statement_config", statement_config)
        self._dialect = _existing_statement_copy_data.get("_dialect", dialect)
        self._sql = _existing_statement_copy_data.get("_original_input", statement)
        self._parsed_expression = _existing_statement_copy_data.get("_parsed_expression", None)
        self._parameter_info = _existing_statement_copy_data.get("_parameter_info", [])
        self._merged_parameters = _existing_statement_copy_data.get("_merged_parameters", [])
        self._validation_result = _existing_statement_copy_data.get("_validation_result", None)

        if not _existing_statement_copy_data:
            self._initialize_statement(self._sql, parameters, args, kwargs)

        if filters:
            self._apply_filters_in_place(filters)

    def _copy_from_existing(
        self,
        existing: "SQLStatement",
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
        dialect: "Optional[DialectType]",
        statement_config: "Optional[StatementConfig]",
    ) -> None:
        """Copy from existing SQLStatement, optionally overriding values."""
        self._statement_config = statement_config if statement_config is not None else existing._statement_config
        self._dialect = dialect if dialect is not None else existing._dialect
        self._sql = existing._sql

        if parameters is not None or args is not None or kwargs is not None:
            current_sql_source = existing._sql
            self._initialize_statement(current_sql_source, parameters, args, kwargs)
        else:
            self._sql = existing._sql
            self._parsed_expression = existing._parsed_expression
            self._parameter_info = existing._parameter_info
            self._merged_parameters = existing._merged_parameters
            self._validation_result = existing._validation_result

    def _initialize_statement(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> None:
        """Initialize the statement with SQL and parameters.

        Raises:
            SQLTransformationError: If SQL transformations fail in strict mode.
            SQLValidationError: If SQL validation fails in strict mode.

        """
        self._sql = statement

        if self._statement_config.enable_parsing:
            if isinstance(statement, exp.Expression):
                self._parsed_expression = statement
            else:
                self._parsed_expression = self.to_expression(str(statement), self._dialect)
        else:
            self._parsed_expression = None

        self._parameter_info, self._merged_parameters = self._process_parameters(
            str(statement), parameters, args, kwargs
        )

        if (
            self._statement_config.enable_parsing
            and self._statement_config.enable_transformations
            and self._parsed_expression
        ):
            try:
                transformed_expr = self._statement_config.transformer.transform(self._parsed_expression, self._dialect)
                if transformed_expr is not self._parsed_expression:
                    self._parsed_expression = transformed_expr
                    if isinstance(self._sql, exp.Expression):
                        self._sql = self._parsed_expression

            except (SQLTransformationError, ValueError, TypeError) as e:
                if self._statement_config.strict_mode:
                    msg = "SQL transformations failed"
                    raise SQLTransformationError(msg, str(statement)) from e
                logger.warning("SQL transformation failed during initialization: %s", e)

        if self._statement_config.enable_validation:
            self._validation_result = self._statement_config.validator.validate(
                self._parsed_expression or str(statement), self._dialect
            )
            if (
                self._validation_result is not None
                and not self._validation_result.is_safe
                and self._statement_config.strict_mode
            ):
                msg = f"SQL validation failed: {', '.join(self._validation_result.issues)}"
                raise SQLValidationError(msg, str(statement), self._validation_result.risk_level)
        else:
            self._validation_result = None

    def _process_parameters(
        self,
        sql_str: str,
        parameters: "Optional[StatementParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> tuple["list[ParameterInfo]", "StatementParameterType"]:
        if self._statement_config.enable_parsing:
            try:
                _, parameter_info, merged_parameters, _ = self._statement_config.parameter_converter.convert_parameters(
                    sql_str, parameters, args, kwargs, validate=self._statement_config.enable_validation
                )

            except (ParameterError, ValueError, TypeError) as e:
                if self._statement_config.strict_mode:
                    raise
                logger.warning("Parameter processing failed, using basic merge: %s", e)
                return [], self._statement_config.parameter_converter.merge_parameters(parameters, args, kwargs)
            return parameter_info, merged_parameters
        if not self._statement_config.allow_mixed_parameters and args and kwargs:
            msg = "Cannot mix args and kwargs when parsing is disabled"
            raise ParameterError(msg, sql_str)
        merged_parameters = self._statement_config.parameter_converter.merge_parameters(parameters, args, kwargs)
        return [], merged_parameters

    @staticmethod
    def to_expression(statement: "Statement", dialect: "DialectType" = None) -> exp.Expression:
        """Convert SQL input to expression.

        Returns:
            The parsed SQL expression.

        Raises:
            SQLValidationError: If SQL cannot be parsed.
        """
        if isinstance(statement, exp.Expression):
            return statement
        if isinstance(statement, SQLStatement):
            expr = statement.expression
            if expr is not None:
                return expr
            return sqlglot.parse_one(statement.sql, read=dialect)
        # str case
        sql_str = statement
        if not sql_str or not sql_str.strip():
            return exp.Select()

        try:
            return sqlglot.parse_one(sql_str, read=dialect)
        except SQLGlotParseError as e:
            msg = f"SQL parsing failed: {e}"
            raise SQLValidationError(msg, sql_str, RiskLevel.HIGH) from e

    @property
    def sql(self) -> str:
        """The SQL string, potentially modified by sanitization or filters."""
        if self._statement_config.enable_parsing and self._parsed_expression is not None:
            return self.to_sql(dialect=self._dialect)
        return str(self._sql)

    @property
    def dialect(self) -> "DialectType":
        """Get the SQL dialect."""
        return self._dialect

    @dialect.setter
    def dialect(self, dialect: "DialectType") -> None:
        """Set the SQL dialect."""
        self._dialect = dialect

    @property
    def config(self) -> "StatementConfig":
        """Get the statement configuration."""
        return self._statement_config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """Get the parsed and potentially transformed sqlglot expression if available and parsing enabled."""
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

    @property
    def is_safe(self) -> bool:
        """Check if the statement is safe based on validation results.

        Returns:
            True if the statement is safe, False otherwise.
        """
        if self._validation_result is None:
            return True
        return self._validation_result.is_safe

    def to_sql(
        self,
        placeholder_style: "Optional[Union[str, ParameterStyle]]" = None,
        dialect: "Optional[DialectType]" = None,
        statement_separator: str = ";",
        include_statement_separator: bool = False,
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
            >>> stmt.get_sql()
            'SELECT * FROM users WHERE id = ?'
            >>> stmt.get_sql(placeholder_style="named")
            'SELECT * FROM users WHERE id = :param_0'
        """
        target_dialect = dialect if dialect is not None else self._dialect

        if not self._statement_config.enable_parsing and self.expression is None:
            sql = str(self._sql)
            if include_statement_separator and not sql.rstrip().endswith(statement_separator):
                sql = sql.rstrip() + statement_separator
            return sql

        if self.expression is not None:
            if placeholder_style is None:
                sql = self.expression.sql(dialect=target_dialect)
            else:
                sql = self._transform_sql_placeholders(placeholder_style, self.expression, target_dialect)
        else:
            sql = str(self._sql)

        if include_statement_separator and not sql.rstrip().endswith(statement_separator):
            sql = sql.rstrip() + statement_separator

        return sql

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

        if isinstance(style, str):
            style_lower = style.lower()
            if style_lower in {"dict", "named"}:
                return self._convert_to_dict_parameters()
            if style_lower in {"list", "positional", "qmark"}:
                return self._convert_to_list_parameters()
            if style_lower == "tuple":
                params = self._convert_to_list_parameters()
                return tuple(params) if isinstance(params, list) else params

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

    def validate(self) -> "ValidationResult":
        """Perform validation on the statement, update the internal validation result,
        and raise SQLValidationError if the configuration and result warrant it.
        The validation is run if not already cached or if cache is considered stale
        (e.g., after filters have been applied).

        Returns:
            The ValidationResult instance.

        Raises:
            SQLValidationError: If validation is enabled, the statement is not safe,
                                and the risk level meets or exceeds min_risk_to_raise.
        """
        if not self._statement_config.enable_validation:
            if self._validation_result is None:
                from sqlspec.sql.preprocessors import ValidationResult

                self._validation_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)
            return self._validation_result
        if self._validation_result is None:
            source_to_validate = self.expression if self.expression is not None else self.sql
            self._validation_result = self._statement_config.validator.validate(source_to_validate, self._dialect)

        if (
            self._validation_result is not None
            and not self._validation_result.is_safe
            and self._statement_config.validator.min_risk_to_raise is not None
            and self._validation_result.risk_level is not None
            and self._validation_result.risk_level.value >= self._statement_config.validator.min_risk_to_raise.value
        ):
            error_msg = f"SQL validation failed with risk level {self._validation_result.risk_level}:\n"
            error_msg += "Issues:\n" + "\n".join([f"- {issue}" for issue in self._validation_result.issues or []])
            if self._validation_result.warnings:
                error_msg += "\nWarnings:\n" + "\n".join([f"- {warn}" for warn in self._validation_result.warnings])
            raise SQLValidationError(error_msg, self.to_sql(), self._validation_result.risk_level)

        return self._validation_result

    def _transform_sql_placeholders(
        self,
        target_style: "Union[str, ParameterStyle]",
        expression_to_render: "exp.Expression",
        dialect: "Optional[DialectType]" = None,
    ) -> str:
        target_dialect = dialect if dialect is not None else self._dialect

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

        if target_style_enum == ParameterStyle.STATIC:
            return self._render_static_sql(expression_to_render)

        sql = expression_to_render.sql(dialect=target_dialect)
        return self._convert_placeholder_style(sql, target_style_enum)

    def _convert_placeholder_style(self, sql: str, target_style: "ParameterStyle") -> str:
        parameters_info = self._statement_config.parameter_validator.extract_parameters(sql)

        if not parameters_info:
            return sql

        result_sql = sql
        for param_info in reversed(parameters_info):
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)
            new_placeholder = self._get_placeholder_for_style(target_style, param_info)
            result_sql = result_sql[:start_pos] + new_placeholder + result_sql[end_pos:]
        return result_sql

    @staticmethod
    def _get_placeholder_for_style(target_style: "ParameterStyle", param_info: "ParameterInfo") -> str:
        if target_style == ParameterStyle.QMARK:
            return "?"
        if target_style == ParameterStyle.NAMED_COLON:
            return f":{param_info.name}" if param_info.name else f":param_{param_info.ordinal}"
        if target_style == ParameterStyle.NAMED_DOLLAR:
            return f"${param_info.name}" if param_info.name else f"$param_{param_info.ordinal}"
        if target_style == ParameterStyle.NUMERIC:
            return f":{param_info.ordinal + 1}"  # 1-based numbering
        if target_style == ParameterStyle.NAMED_AT:
            return f"@{param_info.name}" if param_info.name else f"@param_{param_info.ordinal}"
        if target_style == ParameterStyle.PYFORMAT_NAMED:
            return f"%({param_info.name})s" if param_info.name else f"%(param_{param_info.ordinal})s"
        if target_style == ParameterStyle.PYFORMAT_POSITIONAL:
            return "%s"
        return param_info.placeholder_text

    def _render_static_sql(self, expression: "exp.Expression") -> str:
        if not self._merged_parameters:
            return expression.sql(dialect=self._dialect)

        sql = expression.sql(dialect=self._dialect)
        parameters_info = self._statement_config.parameter_validator.extract_parameters(sql)

        if not parameters_info:
            return sql

        result_sql = sql
        for param_info in reversed(parameters_info):
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)
            value = self._get_parameter_value_for_substitution(param_info)
            escaped_value = self._escape_value(value)
            result_sql = result_sql[:start_pos] + escaped_value + result_sql[end_pos:]
        return result_sql

    def _get_parameter_value_for_substitution(self, param_info: "ParameterInfo") -> Any:
        if not self._merged_parameters:
            return None

        if param_info.name:
            if isinstance(self._merged_parameters, dict):
                return self._merged_parameters.get(param_info.name)
            return None

        if isinstance(self._merged_parameters, (list, tuple)):
            if 0 <= param_info.ordinal < len(self._merged_parameters):
                return self._merged_parameters[param_info.ordinal]
            return None

        if isinstance(self._merged_parameters, dict):
            generated_name = f"_arg_{param_info.ordinal}"
            return self._merged_parameters.get(generated_name)

        if param_info.ordinal == 0:
            return self._merged_parameters
        return None

    @staticmethod
    def _escape_value(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        str_value = str(value).replace("'", "''")
        return f"'{str_value}'"

    def _convert_to_dict_parameters(self) -> dict[str, Any]:
        if isinstance(self._merged_parameters, dict):
            return self._merged_parameters.copy()
        if isinstance(self._merged_parameters, (list, tuple)):
            if self._parameter_info:
                result = {}
                for i, param_info in enumerate(self._parameter_info):
                    if param_info.name and i < len(self._merged_parameters):
                        result[param_info.name] = self._merged_parameters[i]
                return result
            return {f"param_{i}": value for i, value in enumerate(self._merged_parameters)}
        if self._merged_parameters is None:
            return {}
        return {"param_0": self._merged_parameters}

    def _convert_to_list_parameters(self) -> list[Any]:
        if isinstance(self._merged_parameters, (list, tuple)):
            return list(self._merged_parameters)
        if isinstance(self._merged_parameters, dict):
            if self._parameter_info:
                return [
                    self._merged_parameters[param_info.name]
                    for param_info in sorted(self._parameter_info, key=lambda p: p.ordinal)
                    if param_info.name and param_info.name in self._merged_parameters
                ]
            return list(self._merged_parameters.values())
        return [self._merged_parameters]

    def copy(
        self,
        statement: "Optional[Statement]" = None,
        parameters: "Optional[StatementParameterType]" = None,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[StatementConfig]" = None,
        *filters: "StatementFilter",
    ) -> "SQLStatement":
        """Create a copy of the statement, optionally overriding attributes.

        Args:
            statement: New SQL string, expression, or SQLStatement.
            parameters: New primary parameters.
            args: New positional arguments for parameters.
            kwargs: New keyword arguments for parameters.
            dialect: New SQL dialect.
            config: New statement configuration.
            *filters: Statement filters to apply to the new copy.

        Returns:
            A new SQLStatement instance.
        """
        sql = statement if statement is not None else self._sql

        if parameters is None and args is None and kwargs is None:
            if sql is self._sql:
                effective_parameters = self._merged_parameters
                effective_args = None
                effective_kwargs = None
            else:
                # SQL is changing, or new SQL is provided, let constructor handle params from scratch
                effective_parameters = None
                effective_args = None
                effective_kwargs = None
        else:
            effective_parameters = parameters
            effective_args = args
            effective_kwargs = kwargs

        # Data to potentially pass for optimized copying if underlying SQL/Expression is an SQLStatement
        # This helps avoid re-parsing or re-validating unnecessarily if only minor things change.
        # However, our _copy_from_existing handles the SQLStatement case primarily.
        # For a general copy, we create a new one, letting it initialize.

        copied_statement = SQLStatement(
            statement=sql,
            parameters=effective_parameters,
            # Filters are passed here directly for initial application by __init__ if it's a fresh build
            # Or, if copying an SQLStatement, __init__ handles it via _copy_from_existing.
            # The main thing is that filters passed to *this* copy method are applied *after* this new instance is formed.
            args=effective_args,
            kwargs=effective_kwargs,
            dialect=dialect if dialect is not None else self._dialect,
            statement_config=config if config is not None else self._statement_config,
            # We are not using _existing_statement_copy_data here because we want a potentially fresh state
            # based on overrides, and then apply filters specifically passed to this copy method.
        )

        if filters:
            copied_statement._apply_filters_in_place(filters)

        return copied_statement

    def append_filter(self, filter_to_apply: "StatementFilter") -> "SQLStatement":
        """Applies a filter to the statement and returns a new SQLStatement.

        Args:
            filter_to_apply: The filter object to apply.

        Returns:
            A new SQLStatement instance with the filter applied.
        """
        return apply_filter(self, filter_to_apply)

    def transform(self) -> "SQLStatement":
        """Return a transformed version of the statement.

        Returns:
            New SQLStatement with transformed SQL.

        Raises:
            SQLTransformationError: If SQL sanitization fails.
        """
        if not self._statement_config.enable_transformations or not self.expression:
            return self

        try:
            return self.copy(statement=self._statement_config.transformer.transform(self.expression, self._dialect))
        except (SQLTransformationError, ValueError, TypeError) as e:
            msg = f"SQL sanitization failed: {e}"
            raise SQLTransformationError(msg, self.sql) from e

    def _apply_filters_in_place(self, filters_to_apply: "Sequence[StatementFilter]") -> None:
        if not filters_to_apply:
            return

        if not self._statement_config.enable_parsing:
            msg = "Filters are not supported when parsing is disabled"
            raise ParameterError(msg)

        current_stmt_for_filtering = self
        for f in filters_to_apply:
            # apply_filter returns a new instance. We need to update self.
            # Create a temporary "snapshot" config for filtering to avoid unintended validation
            temp_config = replace(
                current_stmt_for_filtering._statement_config,
                enable_validation=False,  # Defer validation until all filters applied
            )
            temp_stmt = current_stmt_for_filtering.copy(config=temp_config)
            filtered_stmt = apply_filter(temp_stmt, f)

            # Update the current instance's attributes from the filtered_stmt
            self._sql = filtered_stmt._sql  # The original SQL might change if a filter alters the base expression
            self._parsed_expression = filtered_stmt._parsed_expression
            self._merged_parameters = filtered_stmt._merged_parameters
            self._parameter_info = filtered_stmt._parameter_info
            # _validation_result should be re-evaluated after all filters if needed,
            # or cleared if filters potentially invalidate it. For now, let's clear it
            # to ensure it's re-evaluated by a subsequent .validate() call.
            self._validation_result = None

            # Update current_stmt_for_filtering for the next iteration
            current_stmt_for_filtering = self

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

        return self.copy(statement=new_expr, parameters=self._merged_parameters)

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
            # Preserve parameters from new_stmt by copying its internal state
            return new_stmt.copy(statement=expr_with_param, parameters=new_stmt._merged_parameters)

        new_expr = new_expr.limit(limit_value)  # type: ignore[attr-defined]
        return self.copy(statement=new_expr)

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
            # Preserve parameters from new_stmt by copying its internal state
            return new_stmt.copy(statement=expr_with_param, parameters=new_stmt._merged_parameters)

        new_expr = new_expr.offset(offset_value)  # type: ignore[attr-defined]
        return self.copy(statement=new_expr)

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
        parsed_orders: list[exp.Ordered] = []
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

            elif isinstance(o_expr, exp.Ordered):
                parsed_orders.append(o_expr)
            elif isinstance(o_expr, exp.Order):
                # Convert Order to Ordered if needed
                parsed_orders.append(o_expr)  # type: ignore[arg-type]
            else:
                msg = f"Unsupported order_by type: {type(o_expr)}"
                raise TypeError(msg)

        if parsed_orders:
            new_expr = new_expr.order_by(*parsed_orders)  # type: ignore[attr-defined]
        return self.copy(statement=new_expr)

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

    def _get_current_expression_for_modification(self) -> exp.Expression:
        if not self._statement_config.enable_parsing:
            msg = "Cannot modify expression if parsing is disabled."
            raise SQLSpecError(msg)

        if self.expression is None:
            logger.debug("No existing expression to modify, starting with a new Select.")
            return exp.Select()
        return self.expression.copy()

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
        current_sql_for_repr = self.to_sql()
        return f"SQLStatement(sql={current_sql_for_repr!r}{', parameters=...' if self._merged_parameters is not None else ''}{f', _statement_config={self._statement_config!r}' if self._statement_config else ''})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SQLStatement):
            return NotImplemented
        return (
            str(self._sql) == str(other._sql)
            and self._merged_parameters == other._merged_parameters
            and self._dialect == other._dialect
            and self._statement_config == other._statement_config
        )

    def __hash__(self) -> int:
        hashable_params: tuple[Any, ...]
        if isinstance(self._merged_parameters, list):
            hashable_params = tuple(self._merged_parameters)
        elif isinstance(self._merged_parameters, dict):
            hashable_params = tuple(sorted(self._merged_parameters.items()))
        elif isinstance(self._merged_parameters, tuple):
            hashable_params = self._merged_parameters
        elif self._merged_parameters is None:
            hashable_params = ()
        else:
            hashable_params = (self._merged_parameters,)

        return hash((
            str(self._sql),
            hashable_params,
            self._dialect,
            self._statement_config,
        ))
