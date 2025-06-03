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

# Updated imports for pipeline components
from sqlspec.statement.filters import StatementFilter, apply_filter
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle, ParameterValidator
from sqlspec.statement.pipelines import ValidationResult
from sqlspec.statement.pipelines.context import SQLProcessingContext

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.expressions import Condition
    from sqlglot.schema import Schema as SQLGlotSchema

    from sqlspec.statement.parameters import ParameterInfo
    from sqlspec.statement.pipelines import ProcessorProtocol, SQLValidator
    from sqlspec.statement.pipelines.analyzers import StatementAnalysis
    from sqlspec.statement.pipelines.base import StatementPipeline
    from sqlspec.typing import SQLParameterType

__all__ = (
    "SQL",
    "Statement",
)

logger = logging.getLogger("sqlspec")

Statement = Union[str, exp.Expression, "SQL"]


def _default_validator() -> "SQLValidator":
    from sqlspec.statement.pipelines import SQLValidator

    # Return an empty SQLValidator since individual validators are now handled
    # as ProcessorProtocol instances in the pipeline
    return SQLValidator(validators=[])


@dataclass
class SQLConfig:
    """Configuration for SQLStatement behavior."""

    # Behavior flags
    enable_parsing: bool = True
    enable_validation: bool = True
    enable_transformations: bool = True
    enable_analysis: bool = False
    strict_mode: bool = True
    cache_parsed_expression: bool = True

    # Component lists for explicit staging
    transformers: "Optional[list[ProcessorProtocol[exp.Expression]]]" = None
    validators: "Optional[list[ProcessorProtocol[exp.Expression]]]" = None  # Or specific ValidatorProtocol
    analyzers: "Optional[list[ProcessorProtocol[exp.Expression]]]" = None  # Or specific AnalyzerProtocol

    # Fallback for old-style mixed component list (for smoother transition)
    processing_pipeline_components: "Optional[list[ProcessorProtocol[exp.Expression]]]" = None

    # Other configs
    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    sqlglot_schema: "Optional[SQLGlotSchema]" = None
    analysis_cache_size: int = 1000
    input_sql_had_placeholders: bool = False  # Populated by SQL.__init__

    def get_statement_pipeline(self) -> "StatementPipeline":  # Renamed
        """Constructs and returns a StatementPipeline from the configured components.
        Prioritizes explicit transformer/validator/analyzer lists if provided.
        Otherwise, uses default components based on enable_* flags or processing_pipeline_components.
        """
        from sqlspec.statement.pipelines import StatementPipeline  # Renamed
        from sqlspec.statement.pipelines.analyzers import StatementAnalyzer
        from sqlspec.statement.pipelines.transformers import CommentRemover, ParameterizeLiterals
        from sqlspec.statement.pipelines.validators import (
            PreventDDL,
            PreventInjection,
            RiskyDML,
            SuspiciousKeywords,
            TautologyConditions,
        )

        # Determine components for each stage
        # If explicit lists are given, use them
        # Otherwise, use defaults or the old processing_pipeline_components list

        final_transformers: list[ProcessorProtocol[exp.Expression]] = []
        final_validators: list[ProcessorProtocol[exp.Expression]] = []
        final_analyzers: list[ProcessorProtocol[exp.Expression]] = []

        if self.transformers is not None:
            final_transformers.extend(self.transformers)
        elif self.processing_pipeline_components is not None:
            # Filter from old list - this is for transition
            final_transformers.extend(
                [
                    p
                    for p in self.processing_pipeline_components
                    if not (hasattr(p, "validate") or hasattr(p, "analyze"))
                ]
            )  # Basic heuristic
        elif self.enable_transformations:
            final_transformers.extend([CommentRemover(), ParameterizeLiterals()])

        if self.validators is not None:
            final_validators.extend(self.validators)
        elif self.processing_pipeline_components is not None:
            final_validators.extend(
                [p for p in self.processing_pipeline_components if hasattr(p, "validate") and not hasattr(p, "analyze")]
            )  # Basic heuristic
        elif self.enable_validation:
            # Use the new ProcessorProtocol-based validators directly
            final_validators.extend(
                [
                    PreventInjection(),
                    RiskyDML(),
                    PreventDDL(),
                    TautologyConditions(),
                    SuspiciousKeywords(risk_level=RiskLevel.MEDIUM),
                ]
            )

        if self.analyzers is not None:
            final_analyzers.extend(self.analyzers)
        elif self.processing_pipeline_components is not None:
            final_analyzers.extend(
                [p for p in self.processing_pipeline_components if hasattr(p, "analyze")]
            )  # Basic heuristic
        elif self.enable_analysis:
            final_analyzers.append(StatementAnalyzer(cache_size=self.analysis_cache_size))

        return StatementPipeline(
            transformers=final_transformers, validators=final_validators, analyzers=final_analyzers
        )


class SQL:
    """Represents a SQL statement with parameters and validation.

    This class provides a unified interface for SQL statements with automatic parameter
    binding, validation, and sanitization. It supports multiple parameter styles and
    can work with raw SQL strings, sqlglot expressions, or query builder objects.
    It is designed to be immutable; methods that modify the statement return a new instance.

    Key Features:
    - Intelligent parameter binding from parameters and kwargs
    - Security-focused validation and sanitization
    - Support for different placeholder styles for database drivers
    - Filter composition from sqlspec.sql.filters
    - Performance optimizations with caching
    - Configurable behavior for different use cases
    - Immutability: Modification methods return new instances.

    Parameter binding is the merging of parameters, filters, and kwargs.

    Example usage:
        >>> stmt = SQL(
        ...     "SELECT * FROM users WHERE id = ?", parameters=[123]
        ... )
        >>> sql, params = stmt.to_sql(), stmt.get_parameters()

        >>> stmt = SQL(
        ...     "SELECT * FROM users WHERE name = :name", name="John"
        ... )
        >>> sql = stmt.to_sql(
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
        "_is_many",
        "_is_script",
        "_merged_parameters",
        "_parameter_info",
        "_parsed_expression",
        "_sql",
        "_validation_result",
    )

    def __init__(
        self,
        statement: Statement,
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        _builder_result_type: Optional[type] = None,
        _existing_statement_data: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a SQLStatement instance."""
        _existing_statement_data = _existing_statement_data or {}
        config = config or SQLConfig()

        # Handle the case where the second positional argument might be a filter instead of parameters
        actual_filters = list(filters)
        actual_parameters = parameters

        # If parameters looks like a StatementFilter, move it to filters
        # Check if it has the append_to_statement method (duck typing for StatementFilter protocol)
        if (
            parameters is not None
            and hasattr(parameters, "append_to_statement")
            and callable(getattr(parameters, "append_to_statement", None))
            and not isinstance(parameters, (dict, list, tuple, str, int, float, bool))
        ):
            # The "parameters" argument is actually a filter
            actual_filters.insert(0, parameters)
            actual_parameters = None

        if isinstance(statement, SQL):
            self._copy_from_existing(
                existing=statement,
                parameters=actual_parameters,
                kwargs=kwargs,
                dialect=dialect,
                config=config,
            )
            if actual_filters:
                self._apply_filters(actual_filters)
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
        self._is_many: bool = _existing_statement_data.get("_is_many", False)
        self._is_script: bool = _existing_statement_data.get("_is_script", False)

        if not _existing_statement_data:
            self._initialize_statement(self._sql, actual_parameters, kwargs)

        if actual_filters:
            self._apply_filters(actual_filters)

    def _copy_from_existing(
        self,
        existing: "SQL",
        parameters: "Optional[SQLParameterType]",
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

        if parameters is not None or (kwargs is not None and kwargs):
            current_sql_source = existing._sql
            self._initialize_statement(current_sql_source, parameters, kwargs)
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
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> None:
        """Initialize the statement with SQL, parameters, and run the processing pipeline."""
        self._sql = statement  # Store original input

        from sqlspec.statement.pipelines.context import SQLProcessingContext

        context = SQLProcessingContext(
            initial_sql_string=str(statement),
            dialect=self._dialect,
            config=self._config,
            initial_parameters=parameters,
            initial_kwargs=dict(kwargs) if kwargs else {},
        )

        if isinstance(statement, str):
            try:
                param_validator = self._config.parameter_validator
                existing_params_info = param_validator.extract_parameters(statement)
                if existing_params_info:
                    context.input_sql_had_placeholders = True
                    self._config.input_sql_had_placeholders = True
            except Exception:
                context.input_sql_had_placeholders = False
                self._config.input_sql_had_placeholders = False
        elif isinstance(statement, SQL):
            context.input_sql_had_placeholders = statement.config.input_sql_had_placeholders
            self._config.input_sql_had_placeholders = statement.config.input_sql_had_placeholders
        else:
            context.input_sql_had_placeholders = False
            self._config.input_sql_had_placeholders = False

        context.parameter_info, context.merged_parameters = self._process_parameters(
            context.initial_sql_string, context.initial_parameters, context.initial_kwargs or None
        )
        self._parameter_info = context.parameter_info
        self._merged_parameters = context.merged_parameters

        if self._config.enable_parsing:
            if isinstance(statement, exp.Expression):
                context.current_expression = statement
            else:
                try:
                    is_script_statement = getattr(self, "_is_script", False)
                    context.current_expression = self.to_expression(
                        str(statement), self._dialect, is_script=is_script_statement
                    )
                except SQLValidationError as e:
                    self._parsed_expression = None
                    self._validation_result = ValidationResult(is_safe=False, risk_level=e.risk_level, issues=[str(e)])
                    self._check_and_raise_for_strict_mode()
                    return
        else:
            context.current_expression = None

        self._parsed_expression = context.current_expression

        pipeline_result = None
        if self._config.enable_parsing or isinstance(statement, exp.Expression):
            if context.current_expression is not None or self._config.enable_parsing:
                pipeline = self._config.get_statement_pipeline()
                pipeline_result = pipeline.execute_pipeline(context)

                self._parsed_expression = pipeline_result.final_expression
                self._validation_result = pipeline_result.validation_result
                self._analysis_result = pipeline_result.analysis_result

                if context.extracted_parameters_from_pipeline:
                    self._merge_extracted_parameters(context.extracted_parameters_from_pipeline)

                if isinstance(self._sql, exp.Expression) and self._parsed_expression is not None:
                    self._sql = self._parsed_expression
        elif self._config.enable_validation:
            default_validator = _default_validator()
            if hasattr(default_validator, "validate") and callable(default_validator.validate):
                self._validation_result = default_validator.validate(str(statement), self._dialect, self.config)
            else:
                self._validation_result = ValidationResult(
                    is_safe=True,
                    risk_level=RiskLevel.SKIP,
                    issues=["Validation enabled but default validator structure unexpected."],
                )

        else:
            self._validation_result = ValidationResult(
                is_safe=True, risk_level=RiskLevel.SKIP, issues=["Parsing and Validation disabled"]
            )

        self._check_and_raise_for_strict_mode()

    def _merge_extracted_parameters(self, extracted_params: list[Any]) -> None:
        """Merge parameters extracted by transformers with existing parameters.

        Args:
            extracted_params: List of parameter values extracted by transformers
        """
        if not extracted_params:
            return

        # Convert existing parameters to list format for merging
        if self._merged_parameters is None:
            self._merged_parameters = extracted_params
        elif isinstance(self._merged_parameters, (list, tuple)):
            # Extend existing list with extracted parameters
            merged_list = list(self._merged_parameters) + extracted_params
            self._merged_parameters = merged_list
        elif isinstance(self._merged_parameters, dict):
            # For dict parameters, we need to be more careful about merging
            # Convert extracted params to dict format using ordinal-based names
            param_dict = dict(self._merged_parameters)
            for param_value in extracted_params:
                # Find the next available ordinal-based key
                ordinal = len(param_dict)
                while f"_extracted_{ordinal}" in param_dict:
                    ordinal += 1
                param_dict[f"_extracted_{ordinal}"] = param_value
            self._merged_parameters = param_dict
        else:
            # Single parameter case - convert to list and add extracted params
            self._merged_parameters = [self._merged_parameters, *extracted_params]

    def _process_parameters(
        self,
        sql_str: str,
        parameters: "Optional[SQLParameterType]",
        kwargs: "Optional[Mapping[str, Any]]",
    ) -> tuple["list[ParameterInfo]", "SQLParameterType"]:
        if self._config.enable_parsing:
            try:
                param_info = self._config.parameter_validator.extract_parameters(sql_str)
                has_positional = any(p.name is None for p in param_info)
                has_named = any(p.name is not None for p in param_info)
                has_mixed = has_positional and has_named

                if has_mixed and parameters is not None and kwargs:
                    args = parameters if isinstance(parameters, (list, tuple)) else [parameters]
                    merged = self._config.parameter_converter._merge_mixed_parameters(param_info, args, kwargs)
                    return param_info, merged
                _, param_info, merged, _ = self._config.parameter_converter.convert_parameters(  # type: ignore[assignment]
                    sql_str, parameters, None, kwargs, validate=self._config.enable_validation
                )

            except (ParameterError, ValueError, TypeError):
                if self._config.strict_mode:
                    raise
                return [], self._config.parameter_converter.merge_parameters(parameters, None, kwargs)
            return param_info, merged
        merged = self._config.parameter_converter.merge_parameters(parameters, None, kwargs)  # type: ignore[assignment]
        return [], merged

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
    def to_expression(statement: "Statement", dialect: "DialectType" = None, is_script: bool = False) -> exp.Expression:
        """Convert SQL input to expression.

        Args:
            statement: The SQL statement to convert
            dialect: The SQL dialect
            is_script: Whether this is a multi-statement script

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
            # If no expression, fall through to parse the SQL string
            statement = statement.sql  # Convert to string for parsing below
        # str case
        sql_str = statement
        if not sql_str or not sql_str.strip():
            return exp.Select()

        try:
            # Auto-detect scripts by checking for multiple statements (semicolons)
            auto_detect_script = False
            if not is_script and isinstance(sql_str, str):
                clean_sql = sql_str.strip()
                semicolon_positions = [i for i, c in enumerate(clean_sql) if c == ";"]
                if semicolon_positions:
                    for pos in semicolon_positions[:-1]:
                        remaining = clean_sql[pos + 1 :].strip()
                        if remaining:
                            auto_detect_script = True
                            break

            if is_script or auto_detect_script:
                parsed_statements = sqlglot.parse(sql_str, read=dialect)
                if not parsed_statements:
                    return exp.Select()
                if len(parsed_statements) == 1:
                    first_stmt = parsed_statements[0]
                    return first_stmt if first_stmt is not None else exp.Select()
                valid_statements = [stmt for stmt in parsed_statements if stmt is not None]
                if not valid_statements:
                    return exp.Select()
                return exp.Command(this="SCRIPT", expressions=valid_statements)
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
        """Get the analysis result for this statement.

        Returns:
            The analysis result if analysis was enabled and performed, None otherwise.
        """
        return self._analysis_result

    @property
    def is_many(self) -> bool:
        """Whether this statement should be executed as a batch operation.

        Returns:
            True if this is a batch operation, False otherwise.
        """
        return self._is_many

    @property
    def is_script(self) -> bool:
        """Whether this statement should be executed as a script.

        Returns:
            True if this is a script operation, False otherwise.
        """
        return self._is_script

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
        sql: str

        if not self._config.enable_parsing and self.expression is None:
            sql = str(self._sql)
            if include_statement_separator and not sql.rstrip().endswith(statement_separator):
                sql = sql.rstrip() + statement_separator
            return sql

        current_expression = self.expression

        if current_expression is not None:
            # 1. Always serialize SCRIPT commands to a multi-statement SQL string first.
            if (
                isinstance(current_expression, exp.Command)
                and hasattr(current_expression, "this")
                and str(current_expression.this) == "SCRIPT"
                and hasattr(current_expression, "expressions")
            ):
                if placeholder_style is not None:
                    # TODO(@litestar): For placeholder transformation on scripts, apply the transformation to each sub-statement in the script individually (not just to the combined script string). Then join the transformed statements with semicolons.
                    # TODO(@litestar): Placeholder should be configurable by the driver.  Perhaps the means that it should be an attribute on the SQL class?
                    transformed_parts = [
                        self._transform_sql_placeholders(placeholder_style, stmt_expr, target_dialect)
                        for stmt_expr in current_expression.expressions
                        if stmt_expr is not None
                    ]
                    sql = ";\n".join(transformed_parts)
                    if sql and not sql.rstrip().endswith(";"):
                        sql += ";"
                else:
                    script_parts = [
                        stmt_expr.sql(dialect=target_dialect)
                        for stmt_expr in current_expression.expressions
                        if stmt_expr is not None
                    ]
                    sql = ";\n".join(script_parts)
                    if sql and not sql.rstrip().endswith(";"):
                        sql += ";"
            # 2. Non-script expressions: apply placeholder transformation if needed, else just render.
            elif placeholder_style is not None:
                sql = self._transform_sql_placeholders(placeholder_style, current_expression, target_dialect)
            else:
                sql = current_expression.sql(dialect=target_dialect)
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
            pipeline_result = validator_only_pipeline.execute_pipeline(
                SQLProcessingContext(
                    initial_sql_string=str(self.expression),
                    dialect=self._dialect,
                    config=self._config,
                    current_expression=self.expression,
                )
            )
            self._validation_result = pipeline_result.validation_result
        elif self._config.enable_parsing and self.expression is None:
            # Attempt to parse first if not already an expression
            try:
                parsed_expr = self.to_expression(self.sql, self._dialect)
                validator_only_pipeline = self._get_validation_only_pipeline()
                pipeline_result = validator_only_pipeline.execute_pipeline(
                    SQLProcessingContext(
                        initial_sql_string=self.sql,
                        dialect=self._dialect,
                        config=self._config,
                        current_expression=parsed_expr,
                    )
                )
                self._validation_result = pipeline_result.validation_result
            except SQLValidationError as e:
                self._validation_result = ValidationResult(is_safe=False, risk_level=RiskLevel.HIGH, issues=[str(e)])
        else:  # Parsing disabled, do basic string validation
            validator = _default_validator()
            self._validation_result = validator.validate(self.sql, self._dialect, self.config)

        # Final check for strict mode after running validation
        self._check_and_raise_for_strict_mode()
        # Ensure we always return a ValidationResult
        if self._validation_result is None:
            self._validation_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)
        return self._validation_result

    def _get_validation_only_pipeline(self) -> "StatementPipeline":
        """Get a pipeline with only validation components, no transformers or analyzers."""
        from sqlspec.statement.pipelines import StatementPipeline

        # Get the configured validators from the main pipeline configuration
        main_pipeline_config = self.config.get_statement_pipeline()
        configured_validators = main_pipeline_config.validators

        if not configured_validators and self.config.enable_validation:
            # If no validators configured but validation is enabled, use default.
            return StatementPipeline(validators=[_default_validator()])

        return StatementPipeline(validators=configured_validators, transformers=[], analyzers=[])

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
        target_style_enum: ParameterStyle

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
        elif isinstance(target_style, ParameterStyle):
            target_style_enum = target_style
        else:
            # Should not happen with proper typing, but as a fallback:
            logger.error("Invalid target_style type: %s. Defaulting to qmark.", type(target_style))
            target_style_enum = ParameterStyle.QMARK

        if target_style_enum == ParameterStyle.STATIC:
            # For static rendering, we need to handle scripts by rendering sub-expressions
            if (
                isinstance(expression_to_render, exp.Command)
                and str(getattr(expression_to_render, "this", "")) == "SCRIPT"
            ):
                script_parts = [
                    self._render_static_sql(stmt_expr)
                    for stmt_expr in getattr(expression_to_render, "expressions", [])
                    if stmt_expr is not None
                ]
                rendered_script = ";\n".join(script_parts)
                if rendered_script and not rendered_script.rstrip().endswith(";"):
                    rendered_script += ";"
                return rendered_script
            return self._render_static_sql(expression_to_render)

        # For other placeholder styles, first get the SQL string of the expression.
        # If it's a SCRIPT command, this will correctly render the multi-statement string.
        # The _convert_placeholder_style method will then operate on this rendered string.
        current_sql_str: str
        if (
            isinstance(expression_to_render, exp.Command)
            and hasattr(expression_to_render, "this")
            and str(expression_to_render.this) == "SCRIPT"
            and hasattr(expression_to_render, "expressions")
        ):
            script_parts = [
                stmt_expr.sql(dialect=target_dialect)
                for stmt_expr in expression_to_render.expressions
                if stmt_expr is not None
            ]
            current_sql_str = ";\n".join(script_parts)
            if current_sql_str and not current_sql_str.rstrip().endswith(";"):
                current_sql_str += ";"
        else:
            current_sql_str = expression_to_render.sql(dialect=target_dialect)

        return self._convert_placeholder_style(current_sql_str, target_style_enum)

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
            return f"${param_info.ordinal + 1}"  # PostgreSQL-style $1, $2, etc.
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
        kwargs: "Optional[Mapping[str, Any]]" = None,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        *filters: "StatementFilter",
        **additional_kwargs: Any,
    ) -> "SQL":
        """Create a copy of the statement, optionally overriding attributes.

        Args:
            statement: New SQL string, expression, or SQLStatement.
            parameters: New primary parameters.
            kwargs: New keyword arguments for parameters.
            dialect: New SQL dialect.
            config: New statement configuration.
            *filters: Statement filters to apply to the new copy.
            **additional_kwargs: Additional keyword arguments for parameters.

        Returns:
            A new SQLStatement instance.
        """
        sql = statement if statement is not None else self._sql

        # Merge kwargs and additional_kwargs
        merged_kwargs: dict[str, Any] = {}
        if kwargs:
            merged_kwargs.update(kwargs)
        if additional_kwargs:
            merged_kwargs.update(additional_kwargs)
        final_kwargs = merged_kwargs or None

        if parameters is None and final_kwargs is None:
            if sql is self._sql:
                effective_parameters = self._merged_parameters
                effective_kwargs = None
            else:
                # SQL is changing, or new SQL is provided, let constructor handle params from scratch
                effective_parameters = None
                effective_kwargs = None
        else:
            effective_parameters = parameters
            effective_kwargs = final_kwargs

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
            dialect=dialect if dialect is not None else self._dialect,
            config=config if config is not None else self._config,
            # We are not using _existing_statement_copy_data here because we want a potentially fresh state
            # based on overrides, and then apply filters specifically passed to this copy method.
            **(effective_kwargs or {}),
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
        This method now re-runs a transformation-focused part of the pipeline.
        """
        if not self._config.enable_parsing or not self._config.enable_transformations:
            return self  # No parsing or no transformations enabled

        expr_to_transform = self.expression
        if not expr_to_transform:
            try:
                expr_to_transform = self.to_expression(self.sql, self._dialect)
            except SQLValidationError:
                return self  # Cannot parse, cannot transform

        from sqlspec.statement.pipelines import StatementPipeline
        from sqlspec.statement.pipelines.context import SQLProcessingContext

        # Create a context for this specific transformation run
        # It inherits most things from the current SQL object's state
        transform_context = SQLProcessingContext(
            initial_sql_string=str(self._sql),  # Add missing required parameter
            dialect=self._dialect,
            config=self._config,  # Use current config, which has enable_transformations=True
            current_expression=expr_to_transform,
            initial_parameters=self._merged_parameters,  # Pass current params
            merged_parameters=self._merged_parameters,
            parameter_info=self._parameter_info,
            input_sql_had_placeholders=self._config.input_sql_had_placeholders,
            # Validation and Analysis results are not strictly needed for transformation-only pass
        )

        # Get only transformers from the configured pipeline
        current_pipeline_config = self.config.get_statement_pipeline()
        transform_pipeline = StatementPipeline(
            transformers=current_pipeline_config.transformers, validators=[], analyzers=[]
        )

        try:
            pipeline_result = transform_pipeline.execute_pipeline(transform_context)
            if pipeline_result.final_expression is not expr_to_transform:
                # Create a new SQL instance with the transformed expression and current parameters.
                # The new instance's __init__ will re-run full validation/analysis if enabled.
                return self.copy(
                    statement=pipeline_result.final_expression, parameters=pipeline_result.merged_parameters
                )
        except SQLValidationError as e:
            msg = f"SQL transformation failed due to validation error during pipeline execution: {e}"
            raise SQLTransformationError(msg, self.sql) from e
        except Exception as e:
            msg = f"SQL transformation pipeline failed: {e}"
            raise SQLTransformationError(msg, self.sql) from e
        return self

    def where(self, *conditions: "Union[Condition, str]") -> "SQL":
        """Applies WHERE conditions and returns a new SQLStatement.

        Args:
            *conditions: One or more condition strings or sqlglot Condition expressions.

        Raises:
            SQLParsingError: If the condition cannot be parsed.

        Returns:
            A new SQLStatement instance with the conditions applied.
        """
        expr = self._get_current_expression_for_modification()

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
            if hasattr(expr, "where") and callable(getattr(expr, "where", None)):
                expr = expr.where(condition_expression)  # pyright: ignore
            else:
                if not isinstance(expr, exp.Select):
                    expr = exp.Select().from_(expr)
                expr = expr.where(condition_expression)  # pyright: ignore

        return self.copy(statement=expr, parameters=self._merged_parameters)

    def limit(self, limit_value: int, use_parameter: bool = False) -> "SQL":
        """Applies a LIMIT clause and returns a new SQLStatement.

        Args:
            limit_value: The limit value.
            use_parameter: If True, treats limit_value as a parameter name (str) or adds a new parameter.

        Returns:
            A new SQLStatement instance with the limit applied.
        """
        expr = self._get_current_expression_for_modification()

        if use_parameter:
            param_name = self.get_unique_parameter_name("limit_val")
            new_stmt = self.add_named_parameter(param_name, limit_value)
            expr_with_param = new_stmt._get_current_expression_for_modification()

            if hasattr(expr_with_param, "limit") and callable(getattr(expr_with_param, "limit", None)):
                expr_with_param = expr_with_param.limit(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                if not isinstance(expr_with_param, exp.Select):
                    expr_with_param = exp.Select().from_(expr_with_param)
                expr_with_param = expr_with_param.limit(exp.Placeholder(this=param_name))
            return new_stmt.copy(statement=expr_with_param, parameters=new_stmt._merged_parameters)

        if hasattr(expr, "limit") and callable(getattr(expr, "limit", None)):
            expr = expr.limit(limit_value)  # pyright: ignore
        else:
            if not isinstance(expr, exp.Select):
                expr = exp.Select().from_(expr)
            expr = expr.limit(limit_value)
        return self.copy(statement=expr)

    def offset(self, offset_value: int, use_parameter: bool = False) -> "SQL":
        """Applies an OFFSET clause and returns a new SQLStatement.

        Args:
            offset_value: The offset value.
            use_parameter: If True, treats offset_value as a parameter name (str) or adds a new parameter.

        Returns:
            A new SQLStatement instance with the offset applied.
        """
        expr = self._get_current_expression_for_modification()

        if use_parameter:
            param_name = self.get_unique_parameter_name("offset_val")
            new_stmt = self.add_named_parameter(param_name, offset_value)
            expr_with_param = new_stmt._get_current_expression_for_modification()

            if hasattr(expr_with_param, "offset") and callable(getattr(expr_with_param, "offset", None)):
                expr_with_param = expr_with_param.offset(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                if not isinstance(expr_with_param, exp.Select):
                    expr_with_param = exp.Select().from_(expr_with_param)
                expr_with_param = expr_with_param.offset(exp.Placeholder(this=param_name))

            return new_stmt.copy(statement=expr_with_param, parameters=new_stmt._merged_parameters)

        if hasattr(expr, "offset") and callable(getattr(expr, "offset", None)):
            expr = expr.offset(offset_value)  # pyright: ignore
        else:
            if not isinstance(expr, exp.Select):
                expr = exp.Select().from_(expr)
            expr = expr.offset(offset_value)

        return self.copy(statement=expr)

    def order_by(self, *order_expressions: "Union[str, exp.Order, exp.Ordered]") -> "SQL":
        """Applies ORDER BY clauses and returns a new SQLStatement.

        Args:
            *order_expressions: Column names (str) or sqlglot Order/Ordered expressions.

        Returns:
            A new SQLStatement instance with ordering applied.
        """
        expr = self._get_current_expression_for_modification()
        parsed_orders: list[exp.Ordered] = []

        for o_expr in order_expressions:
            if isinstance(o_expr, str):
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
                if hasattr(o_expr, "this") and hasattr(o_expr, "desc"):
                    ordered = o_expr.this.desc() if getattr(o_expr, "desc", False) else o_expr.this.asc()
                    if isinstance(ordered, exp.Ordered):
                        parsed_orders.append(ordered)

        if parsed_orders:
            if not isinstance(expr, exp.Select):
                expr = exp.Select().from_(expr)
            expr = expr.order_by(*parsed_orders)

        return self.copy(statement=expr)

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
        def make_hashable(obj: Any) -> Any:
            """Convert unhashable types to hashable equivalents."""
            if isinstance(obj, list):
                return tuple(make_hashable(item) for item in obj)
            if isinstance(obj, dict):
                return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
            if isinstance(obj, set):
                return tuple(sorted(make_hashable(item) for item in obj))
            return obj

        hashable_params: tuple[Any, ...]
        if isinstance(self._merged_parameters, list):
            hashable_params = tuple(self._merged_parameters)
        elif isinstance(self._merged_parameters, dict):
            hashable_params = make_hashable(self._merged_parameters)
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
                hash(str(self._config)),
            )
        )

    def as_many(self, parameters: "Optional[SQLParameterType]" = None) -> "SQL":
        """Create a copy of this SQL statement marked for batch execution.

        Args:
            parameters: Optional list of parameter sets for executemany operations.
                       Each item should be a list/tuple of parameters for one execution.
                       Example: [["John"], ["Jane"], ["Bob"]] for INSERT statements.

        Returns:
            A new SQL instance with is_many=True and the provided parameters.

        Example:
            >>> stmt = SQL("INSERT INTO users (name) VALUES (?)").as_many(
            ...     [
            ...         ["John"],
            ...         ["Jane"],
            ...         ["Bob"],
            ...     ]
            ... )
            >>> # This creates a statement ready for executemany with 3 parameter sets
        """
        # Use provided parameters or keep existing ones
        many_parameters = parameters if parameters is not None else self._merged_parameters

        # Create a config that completely disables all processing for executemany operations
        # since validation should happen at execution time in the drivers
        executemany_config = replace(
            self._config,
            enable_validation=False,  # Disable validation for executemany
            enable_parsing=False,  # Disable parsing to avoid parameter processing
            enable_transformations=False,  # Disable transformations
            enable_analysis=False,  # Disable analysis
            strict_mode=False,  # Disable strict mode to prevent exceptions
        )

        # Create complete existing statement data to bypass all initialization
        existing_data = {
            "_config": executemany_config,
            "_dialect": self._dialect,
            "_original_input": self._sql,
            "_parsed_expression": self._parsed_expression,
            "_parameter_info": self._parameter_info,
            "_merged_parameters": many_parameters,  # Set executemany parameters directly
            "_validation_result": ValidationResult(
                is_safe=True, risk_level=RiskLevel.SKIP, issues=["executemany validation skipped"]
            ),
            "_builder_result_type": self._builder_result_type,
            "_analysis_result": self._analysis_result,
            "_is_many": True,  # Mark as executemany
            "_is_script": False,
        }

        # Create the new SQL instance with complete existing statement data to avoid any processing
        return SQL(
            statement=self._sql,
            parameters=None,  # Don't pass parameters to avoid processing
            dialect=self._dialect,
            config=executemany_config,
            _builder_result_type=self._builder_result_type,
            _existing_statement_data=existing_data,
        )

    def as_script(self) -> "SQL":
        """Create a copy of this SQL statement marked for script execution.

        Returns:
            A new SQL instance with is_script=True.
        """
        return SQL(
            statement=self._sql,
            parameters=self._merged_parameters,
            dialect=self._dialect,
            config=self._config,
            _builder_result_type=self._builder_result_type,
            _existing_statement_data={
                **self._get_existing_data(),
                "_is_script": True,
            },
        )

    def _get_existing_data(self) -> dict[str, Any]:
        """Get existing statement data for copying."""
        return {
            "_config": self._config,
            "_dialect": self._dialect,
            "_original_input": self._sql,
            "_parsed_expression": self._parsed_expression,
            "_parameter_info": self._parameter_info,
            "_merged_parameters": self._merged_parameters,
            "_validation_result": self._validation_result,
            "_builder_result_type": self._builder_result_type,
            "_analysis_result": self._analysis_result,
            "_is_many": self._is_many,
            "_is_script": self._is_script,
        }
