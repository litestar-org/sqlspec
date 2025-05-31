# ruff: noqa: SLF001, PLR0904
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

import sqlglot  # Restore
from sqlglot import exp
from sqlglot.errors import ParseError as SQLGlotParseError  # Restore

from sqlspec.exceptions import (
    ParameterError,  # Restore
    RiskLevel,  # Restore
    SQLParsingError,
    SQLSpecError,
    SQLTransformationError,
    SQLValidationError,
)

# Updated imports for pipeline components
from sqlspec.statement.filters import StatementFilter, apply_filter
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle, ParameterValidator
from sqlspec.statement.pipelines import ValidationResult

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions import Condition
    from sqlglot.schema import Schema as SQLGlotSchema

    from sqlspec.statement.parameters import ParameterInfo
    from sqlspec.statement.pipelines import ProcessorProtocol, SQLValidator, TransformerPipeline
    from sqlspec.statement.pipelines.analyzers import StatementAnalysis, StatementAnalyzer
    from sqlspec.typing import SQLParameterType

__all__ = (
    "SQL",
    "Statement",
)

logger = logging.getLogger("sqlspec")

# Define a type for SQL input
Statement = Union[str, exp.Expression, "SQL"]


def _default_validator() -> "SQLValidator":
    from sqlspec.statement.pipelines import SQLValidator
    from sqlspec.statement.pipelines.validators import (
        InjectionValidator,
        PreventDDL,
        RiskyDML,
        RiskyProceduralCode,
        SuspiciousComments,
        SuspiciousKeywords,
        TautologyConditions,
    )

    default_pipeline_validators = [
        InjectionValidator(),
        RiskyDML(),
        PreventDDL(),
        RiskyProceduralCode(),
        TautologyConditions(),
        SuspiciousKeywords(risk_level=RiskLevel.MEDIUM),
        SuspiciousComments(risk_level=RiskLevel.LOW),
    ]
    return SQLValidator(validators=default_pipeline_validators)


@dataclass
class SQLConfig:
    """Configuration for SQLStatement behavior."""

    enable_parsing: bool = True
    """Whether to enable SQLglot parsing for validation and transformation."""

    enable_validation: bool = True
    """Whether to enable SQL validation and security checks."""

    enable_transformations: bool = True
    """Whether to enable SQL transformer."""

    enable_analysis: bool = False
    """Whether to enable SQL statement analysis for metadata extraction."""

    strict_mode: bool = True
    """Whether to use strict validation of rules."""

    allow_mixed_parameters: bool = False
    """Whether to allow mixing args and kwargs when parsing is disabled."""

    cache_parsed_expression: bool = True
    """Whether to cache the parsed expression for performance."""

    processing_pipeline_components: "list[ProcessorProtocol[exp.Expression]]" = field(default_factory=list)
    """List of processing components (transformers, validators) for the pipeline."""

    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    """Parameter converter to use for parameter processing."""

    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    """Parameter validator to use for parameter validation."""

    sqlglot_schema: "Optional[SQLGlotSchema]" = None
    """Optional sqlglot schema for schema-aware transformations."""

    analysis_cache_size: int = 1000
    """Maximum number of analysis results to cache."""

    def get_pipeline(self) -> "TransformerPipeline":
        """Constructs and returns a TransformerPipeline from the configured components.
        If no components are specified, it will add default ones based on flags.

        Returns:
            A TransformerPipeline instance.
        """
        from sqlspec.statement.pipelines import TransformerPipeline

        components_to_use = list(self.processing_pipeline_components)  # Make a copy

        # Add default analyzer if analysis is enabled and no components specified
        if not components_to_use and self.enable_analysis:
            from sqlspec.statement.pipelines.analyzers import StatementAnalyzer

            components_to_use.append(StatementAnalyzer(cache_size=self.analysis_cache_size))

        # Add default validator if validation is enabled and no components specified
        if not components_to_use and self.enable_validation:
            components_to_use.append(_default_validator())

        return TransformerPipeline(components=components_to_use)


class SQL:
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
        "_analysis_result",
        "_builder_result_type",
        "_cached_sql_string",
        "_config",
        "_dialect",
        "_merged_parameters",
        "_parameter_info",
        "_parsed_expression",
        "_sql",
        "_statement_analyzer",
        "_validation_result",
    )

    def __init__(
        self,
        statement: Statement,
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        _builder_result_type: Optional[type] = None,
        _existing_statement_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize a SQLStatement instance."""
        _existing_statement_data = _existing_statement_data or {}
        config = config or SQLConfig()
        if isinstance(statement, SQL):
            self._copy_from_existing(
                existing=statement,
                parameters=parameters,
                args=args,
                kwargs=kwargs,
                dialect=dialect,
                config=config,
            )
            if filters:
                self._apply_filters(filters)
            return

        self._config: SQLConfig = _existing_statement_data.get("_config", config)
        self._dialect: DialectType = _existing_statement_data.get("_dialect", dialect)
        self._sql: Statement = _existing_statement_data.get("_original_input", statement)
        self._parsed_expression: Optional[exp.Expression] = _existing_statement_data.get("_parsed_expression", None)
        self._parameter_info: list[ParameterInfo] = _existing_statement_data.get("_parameter_info", [])
        self._merged_parameters: SQLParameterType = _existing_statement_data.get("_merged_parameters", [])
        self._validation_result: Optional[ValidationResult] = _existing_statement_data.get("_validation_result", None)
        self._builder_result_type: Optional[type] = _existing_statement_data.get(
            "_builder_result_type", _builder_result_type
        )
        self._analysis_result: Optional[StatementAnalysis] = _existing_statement_data.get("_analysis_result", None)
        self._statement_analyzer: Optional[StatementAnalyzer] = None

        if not _existing_statement_data:
            self._initialize_statement(self._sql, parameters, args, kwargs)

        if filters:
            self._apply_filters(filters)

    def _copy_from_existing(
        self,
        existing: "SQL",
        parameters: "Optional[SQLParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
        dialect: "Optional[DialectType]",
        config: "Optional[SQLConfig]",
    ) -> None:
        """Copy from existing SQLStatement, optionally overriding values."""
        self._config = config if config is not None else existing._config
        self._dialect = dialect if dialect is not None else existing._dialect
        self._sql = existing._sql
        self._builder_result_type = existing._builder_result_type
        self._analysis_result = existing._analysis_result
        self._statement_analyzer = existing._statement_analyzer

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
        parameters: "Optional[SQLParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> None:
        """Initialize the statement with SQL and parameters.

        Raises:
            SQLValidationError: If SQL validation fails in strict mode.

        """
        self._sql = statement

        if self._config.enable_parsing:
            if isinstance(statement, exp.Expression):
                self._parsed_expression = statement
            else:
                self._parsed_expression = self.to_expression(str(statement), self._dialect)
        else:
            self._parsed_expression = None

        self._parameter_info, self._merged_parameters = self._process_parameters(
            str(statement), parameters, args, kwargs
        )

        # Process with pipeline if parsing is enabled
        if self._config.enable_parsing and self._parsed_expression:
            pipeline = self._config.get_pipeline()
            transformed_expr, validation_res_from_pipeline = pipeline.execute(
                self._parsed_expression,
                self._dialect,
                self._config,  # Pass config for context to processors
            )
            self._parsed_expression = transformed_expr
            self._validation_result = validation_res_from_pipeline

            # If SQL was an expression, update it with the transformed one
            if isinstance(self._sql, exp.Expression):
                self._sql = self._parsed_expression

            # Handle strict mode for validation results from pipeline
            # The pipeline itself doesn't raise; individual components might if they are validators
            # and configured to do so with strict_mode from SQLConfig.
            # However, SQLStatement should still check the final aggregated result.
            if self._validation_result is not None and not self._validation_result.is_safe and self._config.strict_mode:
                # Check against the min_risk_to_raise of the *default* validator if no specific one is targeted.
                # This part might need refinement if multiple validators with different min_risk levels are used.
                # For now, assume the strict_mode check is sufficient based on the pipeline's output.
                # Or, SQLValidator component within the pipeline should have already raised if its conditions were met.
                # Let's rely on the SQLValidator component to raise if strict_mode is on.
                # If it didn't raise, but the result is still unsafe, it means it was a lower risk or strict_mode was off for that validator.
                # The pipeline aggregates, so if any validator configured to be strict fails, it raises.
                # If no validator raises, but the final result is not safe, and strict_mode is ON for SQLConfig,
                # then we should raise here based on *some* threshold. Default to SQLConfig's implied validator config.
                default_validator_for_threshold = _default_validator()  # Get a default validator to check its min_risk
                if (
                    default_validator_for_threshold.min_risk_to_raise is not None
                    and self._validation_result.risk_level.value
                    >= default_validator_for_threshold.min_risk_to_raise.value
                ):
                    msg = (
                        f"SQL processing pipeline failed with strict mode: {', '.join(self._validation_result.issues)}"
                    )
                    raise SQLValidationError(msg, str(self._sql), self._validation_result.risk_level)

        elif self._config.enable_validation:  # Parsing disabled, but validation requested (basic string checks)
            # This path is for when full parsing is off, so pipeline won't run on an expression.
            # We might need a separate, simpler validation path if enable_parsing is false.
            # For now, if parsing is off, the full pipeline (and thus SQLValidator as a component) won't run.
            default_validator = _default_validator()
            self._validation_result = default_validator.validate(str(statement), self._dialect)
            if self._validation_result is not None and not self._validation_result.is_safe and self._config.strict_mode:  # noqa: SIM102
                if (
                    default_validator.min_risk_to_raise is not None
                    and self._validation_result.risk_level.value >= default_validator.min_risk_to_raise.value
                ):
                    msg = f"SQL validation failed (parsing disabled): {', '.join(self._validation_result.issues)}"
                    raise SQLValidationError(msg, str(statement), self._validation_result.risk_level)
        elif not self._parsed_expression:  # If parsing was disabled, ensure validation_result is None
            self._validation_result = None

    def _process_parameters(
        self,
        sql_str: str,
        parameters: "Optional[SQLParameterType]",
        args: "Optional[Sequence[Any]]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> tuple["list[ParameterInfo]", "SQLParameterType"]:
        if self._config.enable_parsing:
            try:
                _, parameter_info, merged_parameters, _ = self._config.parameter_converter.convert_parameters(
                    sql_str, parameters, args, kwargs, validate=self._config.enable_validation
                )

            except (ParameterError, ValueError, TypeError) as e:
                if self._config.strict_mode:
                    raise
                logger.warning("Parameter processing failed, using basic merge: %s", e)
                return [], self._config.parameter_converter.merge_parameters(parameters, args, kwargs)
            return parameter_info, merged_parameters
        if not self._config.allow_mixed_parameters and args and kwargs:
            msg = "Cannot mix args and kwargs when parsing is disabled"
            raise ParameterError(msg, sql_str)
        merged_parameters = self._config.parameter_converter.merge_parameters(parameters, args, kwargs)
        return [], merged_parameters

    def _get_sql_string_for_processing(self) -> str:
        """Get SQL string for parameter processing, using cached version when possible."""
        # Cache the SQL string representation to avoid repeated expression->string conversions
        if not hasattr(self, "_cached_sql_string"):
            if self._config.enable_parsing and self._parsed_expression is not None:
                self._cached_sql_string = self.to_sql(dialect=self._dialect)
            else:
                self._cached_sql_string = str(self._sql)
        return self._cached_sql_string

    def _invalidate_sql_cache(self) -> None:
        """Invalidate cached SQL string when expression changes."""
        if hasattr(self, "_cached_sql_string"):
            delattr(self, "_cached_sql_string")

    def _invalidate_validation_cache(self) -> None:
        """Invalidate validation cache when SQL structure changes."""
        self._validation_result = None

    def _apply_filters(self, filters_to_apply: "Sequence[StatementFilter]") -> None:
        if not filters_to_apply:
            return

        if not self._config.enable_parsing:
            msg = "Filters are not supported when parsing is disabled"
            raise ParameterError(msg)

        current_stmt_for_filtering = self
        for f in filters_to_apply:
            # apply_filter returns a new instance. We need to update self.
            # Create a temporary "snapshot" config for filtering to avoid unintended validation
            temp_config = replace(
                current_stmt_for_filtering._config,
                enable_validation=False,  # Defer validation until all filters applied
            )
            temp_stmt = current_stmt_for_filtering.copy(config=temp_config)
            filtered_stmt = apply_filter(temp_stmt, f)

            # Update the current instance's attributes from the filtered_stmt
            self._sql = filtered_stmt._sql  # The original SQL might change if a filter alters the base expression
            self._parsed_expression = filtered_stmt._parsed_expression
            self._merged_parameters = filtered_stmt._merged_parameters
            self._parameter_info = filtered_stmt._parameter_info
            # Clear validation and SQL cache since structure changed
            self._invalidate_validation_cache()
            self._invalidate_sql_cache()

            # Update current_stmt_for_filtering for the next iteration
            current_stmt_for_filtering = self

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
        if isinstance(statement, SQL):
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
        if self._config.enable_parsing and self._parsed_expression is not None:
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
    def config(self) -> "SQLConfig":
        """Get the statement configuration."""
        return self._config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """Get the parsed and potentially transformed sqlglot expression if available and parsing enabled."""
        if not self._config.enable_parsing:
            return None
        return self._parsed_expression

    @property
    def parameters(self) -> "SQLParameterType":
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

    @property
    def expected_result_type(self) -> Optional[type]:
        """Get the expected result type from the builder.

        Returns:
            The expected result type if set by a builder, None otherwise.
        """
        return self._builder_result_type

    @property
    def analysis_result(self) -> "Optional[StatementAnalysis]":
        """Get the analysis result if analysis was performed.

        Returns:
            StatementAnalysis if analysis is available, None otherwise.
        """
        return self._analysis_result

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

        if not self._config.enable_parsing and self.expression is None:
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

    def get_parameters(self, style: "Optional[Union[str, ParameterStyle]]" = None) -> "SQLParameterType":
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
        if not self._config.enable_validation:
            if self._validation_result is None:
                self._validation_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)
            return self._validation_result

        # If validation result is already computed and current, return it.
        # The result is considered current if it exists and no modifications have occurred
        # that would invalidate it (like applying filters).
        if self._validation_result is not None:
            # Check for strict mode raising based on the existing result
            self._check_and_raise_for_strict_mode()
            return self._validation_result

        # If no validation result yet, run validation (but avoid re-running transformation pipeline)
        if self._config.enable_parsing and self.expression is not None:
            # Only run validation components, not the full pipeline which might include transformers
            validator_only_pipeline = self._get_validation_only_pipeline()
            _, validation_res = validator_only_pipeline.execute(self.expression, self._dialect, self._config)
            self._validation_result = validation_res
        elif self._config.enable_parsing and self.expression is None:
            # Attempt to parse first if not already an expression
            try:
                parsed_expr = self.to_expression(self.sql, self._dialect)
                validator_only_pipeline = self._get_validation_only_pipeline()
                _, validation_res = validator_only_pipeline.execute(parsed_expr, self._dialect, self._config)
                self._validation_result = validation_res
            except SQLValidationError as e:
                self._validation_result = ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=[str(e)])
        else:  # Parsing disabled, do basic string validation
            validator = _default_validator()
            self._validation_result = validator.validate(self.sql, self._dialect)

        # Final check for strict mode after running validation
        self._check_and_raise_for_strict_mode()
        return self._validation_result

    def _get_validation_only_pipeline(self) -> "TransformerPipeline":
        """Get a pipeline with only validation components, no transformers."""
        from sqlspec.statement.pipelines import TransformerPipeline

        # Extract only validation components from the main pipeline

        # First, try to get components from existing config
        # Check if component is a validator (implements SQLValidator pattern or is a validator)
        validation_components = [
            component
            for component in self._config.processing_pipeline_components
            if hasattr(component, "validators") or component.__class__.__name__.endswith("Validator")
        ]

        # If no validation components found, use defaults
        if not validation_components:
            validation_components.append(_default_validator())

        return TransformerPipeline(components=validation_components)

    def _check_and_raise_for_strict_mode(self) -> None:
        if self._validation_result is not None and not self._validation_result.is_safe and self._config.strict_mode:
            validator_for_threshold_check = _default_validator()
            if (
                validator_for_threshold_check.min_risk_to_raise is not None
                and self._validation_result.risk_level is not None
                and self._validation_result.risk_level.value >= validator_for_threshold_check.min_risk_to_raise.value
            ):
                error_msg = f"SQL validation failed with risk level {self._validation_result.risk_level}:\n"
                error_msg += "Issues:\n" + "\n".join([f"- {issue}" for issue in self._validation_result.issues or []])
                if self._validation_result.warnings:
                    error_msg += "\nWarnings:\n" + "\n".join([f"- {warn}" for warn in self._validation_result.warnings])
                raise SQLValidationError(error_msg, self.to_sql(), self._validation_result.risk_level)

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
        parameters_info = self._config.parameter_validator.extract_parameters(sql)

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
        parameters_info = self._config.parameter_validator.extract_parameters(sql)

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
        parameters: "Optional[SQLParameterType]" = None,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        *filters: "StatementFilter",
    ) -> "SQL":
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

        copied_statement = SQL(
            statement=sql,
            parameters=effective_parameters,
            # Filters are passed here directly for initial application by __init__ if it's a fresh build
            # Or, if copying an SQLStatement, __init__ handles it via _copy_from_existing.
            # The main thing is that filters passed to *this* copy method are applied *after* this new instance is formed.
            args=effective_args,
            kwargs=effective_kwargs,
            dialect=dialect if dialect is not None else self._dialect,
            config=config if config is not None else self._config,
            # We are not using _existing_statement_copy_data here because we want a potentially fresh state
            # based on overrides, and then apply filters specifically passed to this copy method.
        )

        if filters:
            copied_statement._apply_filters(filters)

        return copied_statement

    def append_filter(self, filter_to_apply: "StatementFilter") -> "SQL":
        """Applies a filter to the statement and returns a new SQLStatement.

        Args:
            filter_to_apply: The filter object to apply.

        Returns:
            A new SQLStatement instance with the filter applied.
        """
        return apply_filter(self, filter_to_apply)

    def transform(self) -> "SQL":
        """Return a transformed version of the statement using the configured pipeline.

        Returns:
            New SQLStatement with transformed SQL.

        Raises:
            SQLTransformationError: If SQL transformation within the pipeline fails and strict_mode is on.
            SQLSpecError: If parsing is disabled, as transformations require a parsed expression.
        """
        if not self._config.enable_parsing:
            msg = "Cannot transform SQL if parsing is disabled."
            raise SQLSpecError(msg)

        if not self.expression:
            # If there's no expression (e.g. from an empty initial SQL string),
            # attempt to parse it first or return self if still no expression.
            try:
                parsed_expr_for_transform = self.to_expression(self.sql, self._dialect)
                if not parsed_expr_for_transform:
                    return self  # No actual content to transform
            except SQLValidationError:
                return self  # Cannot parse, cannot transform
        else:
            parsed_expr_for_transform = self.expression

        if not self._config.enable_transformations:
            return self  # Transformations are globally disabled

        pipeline = self._config.get_pipeline()
        try:
            # The pipeline's execute method returns (transformed_expression, validation_result)
            # We are interested in the transformed_expression here.
            transformed_expr, _ = pipeline.execute(parsed_expr_for_transform, self._dialect, self._config)
            # Create a new SQL instance with the transformed expression.
            # The parameters remain the same. Validation will be re-run by the new instance's __init__.
            return self.copy(statement=transformed_expr)
        except SQLValidationError as e:
            # If a validator in the pipeline raises due to strict mode during transformation attempt
            msg = f"SQL transformation failed due to validation error during pipeline execution: {e}"
            raise SQLTransformationError(msg, self.sql) from e
        except Exception as e:  # Catch other potential errors from transformers
            msg = f"SQL transformation pipeline failed: {e}"
            raise SQLTransformationError(msg, self.sql) from e

    def where(self, *conditions: "Union[Condition, str]") -> "SQL":
        """Applies WHERE conditions and returns a new SQLStatement.

        Args:
            *conditions: One or more condition strings or sqlglot Condition expressions.

        Raises:
            SQLParsingError: If the condition cannot be parsed.

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
                        condition_expression = exp.condition(parsed_node)
                    else:
                        condition_expression = parsed_node
                except Exception as e:
                    msg = f"Failed to parse string condition: '{cond_item}'. Error: {e}"
                    raise SQLParsingError(msg) from e
            elif isinstance(cond_item, exp.Condition):
                condition_expression = cond_item
            else:
                msg = f"Invalid condition type: {type(cond_item)}"
                raise SQLParsingError(msg)

            # Ensure we have a Select expression that supports where()
            if hasattr(new_expr, "where") and callable(new_expr.where):  # pyright: ignore
                new_expr = new_expr.where(condition_expression)  # pyright: ignore
            else:
                # Convert to Select if not already selectable
                if not isinstance(new_expr, exp.Select):
                    logger.warning("Converting non-Select expression to Select for WHERE clause")
                    new_expr = exp.Select().from_(new_expr)
                new_expr = new_expr.where(condition_expression)

        return self.copy(statement=new_expr, parameters=self._merged_parameters)

    def limit(self, limit_value: int, use_parameter: bool = False) -> "SQL":
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

            # Ensure expression supports limit()
            if hasattr(expr_with_param, "limit") and callable(expr_with_param.limit):  # pyright: ignore
                expr_with_param = expr_with_param.limit(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                logger.warning("Expression does not support limit(), converting to Select")
                if not isinstance(expr_with_param, exp.Select):
                    expr_with_param = exp.Select().from_(expr_with_param)
                expr_with_param = expr_with_param.limit(exp.Placeholder(this=param_name))

            return new_stmt.copy(statement=expr_with_param, parameters=new_stmt._merged_parameters)

        # Direct limit without parameter
        if hasattr(new_expr, "limit") and callable(new_expr.limit):  # pyright: ignore
            new_expr = new_expr.limit(limit_value)  # pyright: ignore
        else:
            logger.warning("Expression does not support limit(), converting to Select")
            if not isinstance(new_expr, exp.Select):
                new_expr = exp.Select().from_(new_expr)
            new_expr = new_expr.limit(limit_value)

        return self.copy(statement=new_expr)

    def offset(self, offset_value: int, use_parameter: bool = False) -> "SQL":
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

            # Ensure expression supports offset()
            if hasattr(expr_with_param, "offset") and callable(expr_with_param.offset):  # pyright: ignore
                expr_with_param = expr_with_param.offset(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                logger.warning("Expression does not support offset(), converting to Select")
                if not isinstance(expr_with_param, exp.Select):
                    expr_with_param = exp.Select().from_(expr_with_param)
                expr_with_param = expr_with_param.offset(exp.Placeholder(this=param_name))

            return new_stmt.copy(statement=expr_with_param, parameters=new_stmt._merged_parameters)

        # Direct offset without parameter
        if hasattr(new_expr, "offset") and callable(new_expr.offset):  # pyright: ignore
            new_expr = new_expr.offset(offset_value)  # pyright: ignore
        else:
            logger.warning("Expression does not support offset(), converting to Select")
            if not isinstance(new_expr, exp.Select):
                new_expr = exp.Select().from_(new_expr)
            new_expr = new_expr.offset(offset_value)

        return self.copy(statement=new_expr)

    def order_by(self, *order_expressions: "Union[str, exp.Order, exp.Ordered]") -> "SQL":
        """Applies ORDER BY clauses and returns a new SQLStatement.

        Args:
            *order_expressions: Column names (str) or sqlglot Order/Ordered expressions.

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
                # Convert Order to Ordered by extracting the expression and desc flag
                if hasattr(o_expr, "this") and hasattr(o_expr, "desc"):
                    if o_expr.desc:
                        parsed_orders.append(o_expr.this.desc())
                    else:
                        parsed_orders.append(o_expr.this.asc())
                else:
                    # Fallback: treat as ordered expression
                    parsed_orders.append(o_expr)  # type: ignore[arg-type]

        if parsed_orders:
            # Ensure expression supports order_by()
            if hasattr(new_expr, "order_by") and callable(new_expr.order_by):  # pyright: ignore
                new_expr = new_expr.order_by(*parsed_orders)  # pyright: ignore
            else:
                logger.warning("Expression does not support order_by(), converting to Select")
                if not isinstance(new_expr, exp.Select):
                    new_expr = exp.Select().from_(new_expr)
                new_expr = new_expr.order_by(*parsed_orders)

        return self.copy(statement=new_expr)

    def add_named_parameter(self, name: str, value: Any) -> "SQL":
        """Adds a named parameter and returns a new SQLStatement.

        Args:
            name: The name of the parameter.
            value: The value of the parameter.

        Returns:
            A new SQLStatement instance with the parameter added.
        """
        current_params_dict = self._convert_to_dict_parameters()
        current_params_dict[name] = value
        building_config = replace(self._config, enable_validation=False, enable_parsing=True)
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
        if not self._config.enable_parsing:
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
        return f"SQLStatement(statement={current_sql_for_repr!r}{', parameters=...' if self._merged_parameters is not None else ''}{f', _config={self._config!r}' if self._config else ''})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SQL):
            return NotImplemented
        return (
            str(self._sql) == str(other._sql)
            and self._merged_parameters == other._merged_parameters
            and self._dialect == other._dialect
            and self._config == other._config
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

        return hash(
            (
                str(self._sql),
                hashable_params,
                self._dialect,
                self._config,
            )
        )

    def analyze(self) -> "StatementAnalysis":
        """Analyze the SQL statement and return analysis metadata.

        This method will create or reuse a StatementAnalyzer to extract
        metadata about the SQL statement, such as table names, complexity
        score, join count, etc.

        Returns:
            StatementAnalysis with extracted metadata.

        Raises:
            SQLSpecError: If parsing is disabled, as analysis requires a parsed expression.
        """
        if not self._config.enable_parsing:
            msg = "Cannot analyze SQL if parsing is disabled."
            raise SQLSpecError(msg)

        # Create analyzer if not exists
        if self._statement_analyzer is None:
            from sqlspec.statement.pipelines.analyzers import StatementAnalyzer

            self._statement_analyzer = StatementAnalyzer(cache_size=self._config.analysis_cache_size)

        # Use cached result if available
        if self._analysis_result is not None:
            return self._analysis_result

        # Perform analysis
        if self.expression is not None:
            self._analysis_result = self._statement_analyzer.analyze_expression(self.expression)
        else:
            self._analysis_result = self._statement_analyzer.analyze_statement(self.sql, self._dialect)

        return self._analysis_result
