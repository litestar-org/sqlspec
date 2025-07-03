"""SQL statement handling with centralized parameter management."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Union

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle, ParameterValidator
from sqlspec.statement.pipelines import SQLProcessingContext, StatementPipeline
from sqlspec.statement.pipelines.transformers import CommentAndHintRemover, ParameterizeLiterals
from sqlspec.statement.pipelines.validators import DMLSafetyValidator, ParameterStyleValidator
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import (
    can_append_to_statement,
    can_extract_parameters,
    has_parameter_value,
    has_risk_level,
    is_dict,
    supports_limit,
    supports_offset,
    supports_order_by,
    supports_where,
)

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("SQL", "SQLConfig", "Statement")

logger = get_logger("sqlspec.statement")

Statement = Union[str, exp.Expression, "SQL"]


@dataclass
class _ProcessedState:
    """Cached state from pipeline processing."""

    processed_expression: exp.Expression
    processed_sql: str
    merged_parameters: Any
    validation_errors: list[Any] = field(default_factory=list)
    analysis_results: dict[str, Any] = field(default_factory=dict)
    transformation_results: dict[str, Any] = field(default_factory=dict)


@dataclass
class SQLConfig:
    """Configuration for SQL statement behavior.

    Uses conservative defaults that prioritize compatibility and robustness
    over strict enforcement, making it easier to work with diverse SQL dialects
    and complex queries.

    Component Lists:
        transformers: Optional list of SQL transformers for explicit staging
        validators: Optional list of SQL validators for explicit staging
        analyzers: Optional list of SQL analyzers for explicit staging

    Configuration Options:
        parameter_converter: Handles parameter style conversions
        parameter_validator: Validates parameter usage and styles
        analysis_cache_size: Cache size for analysis results
        input_sql_had_placeholders: Populated by SQL.__init__ to track original SQL state
        dialect: SQL dialect to use for parsing and generation

    Parameter Style Configuration:
        allowed_parameter_styles: Allowed parameter styles (e.g., ('qmark', 'named_colon'))
        target_parameter_style: Target parameter style for SQL generation
        allow_mixed_parameter_styles: Whether to allow mixing parameter styles in same query
    """

    enable_parsing: bool = True
    enable_validation: bool = True
    enable_transformations: bool = True
    enable_analysis: bool = False
    enable_normalization: bool = True
    strict_mode: bool = False
    cache_parsed_expression: bool = True
    parse_errors_as_warnings: bool = True

    transformers: list[Any] | None = None
    validators: list[Any] | None = None
    analyzers: list[Any] | None = None

    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    analysis_cache_size: int = 1000
    input_sql_had_placeholders: bool = False
    dialect: DialectType | None = None

    allowed_parameter_styles: tuple[str, ...] | None = None
    target_parameter_style: str | None = None
    allow_mixed_parameter_styles: bool = False

    def validate_parameter_style(self, style: ParameterStyle | str) -> bool:
        """Check if a parameter style is allowed.

        Args:
            style: Parameter style to validate (can be ParameterStyle enum or string)

        Returns:
            True if the style is allowed, False otherwise
        """
        if self.allowed_parameter_styles is None:
            return True  # No restrictions
        style_str = str(style)
        return style_str in self.allowed_parameter_styles

    def get_statement_pipeline(self) -> StatementPipeline:
        """Get the configured statement pipeline.

        Returns:
            StatementPipeline configured with transformers, validators, and analyzers
        """
        transformers = []
        if self.transformers is not None:
            transformers = list(self.transformers)
        elif self.enable_transformations:
            placeholder_style = self.target_parameter_style or "?"
            transformers = [CommentAndHintRemover(), ParameterizeLiterals(placeholder_style=placeholder_style)]

        validators = []
        if self.validators is not None:
            validators = list(self.validators)
        elif self.enable_validation:
            validators = [ParameterStyleValidator(fail_on_violation=self.strict_mode), DMLSafetyValidator()]

        analyzers = []
        if self.analyzers is not None:
            analyzers = list(self.analyzers)
        elif self.enable_analysis:
            analyzers = []

        return StatementPipeline(transformers=transformers, validators=validators, analyzers=analyzers)


class SQL:
    """Immutable SQL statement with centralized parameter management.

    The SQL class is the single source of truth for:
    - SQL expression/statement
    - Positional parameters
    - Named parameters
    - Applied filters

    All methods that modify state return new SQL instances.
    """

    __slots__ = (
        "_builder_result_type",
        "_config",
        "_dialect",
        "_filters",
        "_is_many",
        "_is_script",
        "_named_params",
        "_original_parameters",
        "_original_sql",
        "_placeholder_mapping",
        "_positional_params",
        "_processed_state",
        "_processing_context",
        "_raw_sql",
        "_statement",
    )

    def __init__(
        self,
        statement: str | exp.Expression | SQL,
        *parameters: Any | StatementFilter | list[Any | StatementFilter],
        _dialect: DialectType = None,
        _config: SQLConfig | None = None,
        _builder_result_type: type | None = None,
        _existing_state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL with centralized parameter management."""
        if "config" in kwargs and _config is None:
            _config = kwargs.pop("config")
        self._config = _config or SQLConfig()
        self._dialect = _dialect or (self._config.dialect if self._config else None)
        self._builder_result_type = _builder_result_type
        self._processed_state: _ProcessedState | None = None
        self._processing_context: SQLProcessingContext | None = None
        self._positional_params: list[Any] = []
        self._named_params: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression
        self._raw_sql: str = ""
        self._original_parameters: Any = None
        self._original_sql: str = ""
        self._placeholder_mapping: dict[str, str | int] = {}
        self._is_many: bool = False
        self._is_script: bool = False

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement, _dialect, _config, _builder_result_type)
        else:
            self._init_from_str_or_expression(statement)

        if _existing_state:
            self._load_from_existing_state(_existing_state)

        if not isinstance(statement, SQL) and not _existing_state:
            self._set_original_parameters(*parameters)

        self._process_parameters(*parameters, **kwargs)

    def _init_from_sql_object(
        self, statement: SQL, dialect: DialectType, config: SQLConfig | None, builder_result_type: type | None
    ) -> None:
        """Initialize attributes from an existing SQL object."""
        self._statement = statement._statement
        self._dialect = dialect or statement._dialect
        self._config = config or statement._config
        self._builder_result_type = builder_result_type or statement._builder_result_type
        self._is_many = statement._is_many
        self._is_script = statement._is_script
        self._raw_sql = statement._raw_sql
        self._original_parameters = statement._original_parameters
        self._original_sql = statement._original_sql
        self._placeholder_mapping = statement._placeholder_mapping.copy()
        self._positional_params.extend(statement._positional_params)
        self._named_params.update(statement._named_params)
        self._filters.extend(statement._filters)

    def _init_from_str_or_expression(self, statement: str | exp.Expression) -> None:
        """Initialize attributes from a SQL string or expression."""
        if isinstance(statement, str):
            self._raw_sql = statement
            self._statement = self._to_expression(statement)
        else:
            self._raw_sql = statement.sql(dialect=self._dialect)  # pyright: ignore
            self._statement = statement

    def _load_from_existing_state(self, existing_state: dict[str, Any]) -> None:
        """Load state from a dictionary (used by copy)."""
        self._positional_params = list(existing_state.get("positional_params", self._positional_params))
        self._named_params = dict(existing_state.get("named_params", self._named_params))
        self._filters = list(existing_state.get("filters", self._filters))
        self._is_many = existing_state.get("is_many", self._is_many)
        self._is_script = existing_state.get("is_script", self._is_script)
        self._raw_sql = existing_state.get("raw_sql", self._raw_sql)
        self._original_parameters = existing_state.get("original_parameters", self._original_parameters)

    def _set_original_parameters(self, *parameters: Any) -> None:
        """Store the original parameters for compatibility."""
        if len(parameters) == 0:
            self._original_parameters = None
        elif len(parameters) == 1 and isinstance(parameters[0], StatementFilter):
            # Don't store filters as parameters
            self._original_parameters = None
        elif len(parameters) == 1 and isinstance(parameters[0], (list, tuple)):
            self._original_parameters = parameters[0]
        else:
            self._original_parameters = parameters

    def _process_parameters(self, *parameters: Any, **kwargs: Any) -> None:
        """Process positional and keyword arguments for parameters and filters."""
        for param in parameters:
            self._process_parameter_item(param)

        if "parameters" in kwargs:
            param_value = kwargs.pop("parameters")
            if isinstance(param_value, (list, tuple)):
                self._positional_params.extend(param_value)
            elif isinstance(param_value, dict):
                self._named_params.update(param_value)
            else:
                self._positional_params.append(param_value)

        for key, value in kwargs.items():
            if not key.startswith("_"):
                self._named_params[key] = value

    def _process_parameter_item(self, item: Any) -> None:
        """Process a single item from the parameters list."""
        if isinstance(item, StatementFilter):
            self._filters.append(item)
            pos_params, named_params = self._extract_filter_parameters(item)
            self._positional_params.extend(pos_params)
            self._named_params.update(named_params)
        elif isinstance(item, list):
            for sub_item in item:
                self._process_parameter_item(sub_item)
        elif isinstance(item, dict):
            self._named_params.update(item)
        elif isinstance(item, tuple):
            self._positional_params.extend(item)
        else:
            self._positional_params.append(item)

    def _ensure_processed(self) -> None:
        """Ensure the SQL has been processed through the pipeline (lazy initialization).

        This method implements the facade pattern with lazy processing.
        It's called by public methods that need processed state.
        """
        if self._processed_state is not None:
            return

        final_expr, final_params = self._build_final_state()
        has_placeholders = self._detect_placeholders()
        initial_sql_for_context, final_params = self._prepare_context_sql(final_expr, final_params)

        context = self._create_processing_context(initial_sql_for_context, final_expr, final_params, has_placeholders)
        result = self._run_pipeline(context)

        processed_sql, merged_params = self._process_pipeline_result(result, final_params, context)

        self._finalize_processed_state(result, processed_sql, merged_params)

    def _detect_placeholders(self) -> bool:
        """Detect if the raw SQL has placeholders."""
        if self._raw_sql:
            validator = self._config.parameter_validator
            raw_param_info = validator.extract_parameters(self._raw_sql)
            has_placeholders = bool(raw_param_info)
            # Update the config so transformers can see it
            if has_placeholders:
                self._config.input_sql_had_placeholders = True
            return has_placeholders
        return self._config.input_sql_had_placeholders

    def _prepare_context_sql(self, final_expr: exp.Expression, final_params: Any) -> tuple[str, Any]:
        """Prepare SQL string and parameters for context."""
        initial_sql_for_context = self._raw_sql or final_expr.sql(dialect=self._dialect or self._config.dialect)

        if hasattr(final_expr, "sql") and self._placeholder_mapping:
            # We have normalized SQL - use the expression's SQL for consistency
            initial_sql_for_context = final_expr.sql(dialect=self._dialect or self._config.dialect)
            # Also update merged parameters to use the normalized placeholder names
            if self._placeholder_mapping:
                final_params = self._normalize_parameters(final_params)

        return initial_sql_for_context, final_params

    def _normalize_parameters(self, final_params: Any) -> Any:
        """Normalize parameters based on placeholder mapping."""
        if isinstance(final_params, dict):
            # Convert Oracle-style parameters to normalized parameter names
            normalized_params = {}
            for placeholder_key, original_name in self._placeholder_mapping.items():
                if str(original_name) in final_params:
                    normalized_params[placeholder_key] = final_params[str(original_name)]
            # Keep any non-Oracle parameters as-is
            non_oracle_params = {
                key: value
                for key, value in final_params.items()
                if key not in {str(name) for name in self._placeholder_mapping.values()}
            }
            normalized_params.update(non_oracle_params)
            return normalized_params
        if isinstance(final_params, (list, tuple)):
            # For list parameters with mixed styles, convert to dict using placeholder mapping
            validator = self._config.parameter_validator
            param_info = validator.extract_parameters(self._raw_sql)

            # Check if all parameters are Oracle numeric style
            all_numeric = all(p.name and p.name.isdigit() for p in param_info)

            if all_numeric:
                # For Oracle numeric parameters, when a list is provided:
                # The convention is that list[0] maps to :1, list[1] maps to :2, etc.
                # regardless of the order they appear in the SQL
                # e.g., SQL ":2, :1" with ["john", 42"] maps to {"1": "john", "2": 42}
                normalized_params = {}

                # Find the minimum parameter number to handle both 0-based and 1-based
                min_param_num = min(int(p.name) for p in param_info if p.name)

                for i, param in enumerate(final_params):
                    # Map list index to parameter number
                    param_num = str(i + min_param_num)
                    normalized_params[param_num] = param

                return normalized_params
            # For other cases, map by ordinal (order of appearance)
            normalized_params = {}
            for i, param in enumerate(final_params):
                if i < len(param_info):
                    # Use the normalized placeholder name
                    placeholder_key = f"param_{param_info[i].ordinal}"
                    normalized_params[placeholder_key] = param
            return normalized_params
        return final_params

    def _create_processing_context(
        self, initial_sql_for_context: str, final_expr: exp.Expression, final_params: Any, has_placeholders: bool
    ) -> SQLProcessingContext:
        """Create SQL processing context."""
        context = SQLProcessingContext(
            initial_sql_string=initial_sql_for_context,
            dialect=self._dialect or self._config.dialect,
            config=self._config,
            initial_expression=final_expr,
            current_expression=final_expr,
            merged_parameters=final_params,
            input_sql_had_placeholders=has_placeholders or self._config.input_sql_had_placeholders,
        )

        # Add placeholder mapping to extra_info if available
        if self._placeholder_mapping:
            context.extra_info["placeholder_map"] = self._placeholder_mapping

        validator = self._config.parameter_validator
        context.parameter_info = validator.extract_parameters(context.initial_sql_string)

        return context

    def _run_pipeline(self, context: SQLProcessingContext) -> Any:
        """Run the SQL processing pipeline."""
        pipeline = self._config.get_statement_pipeline()
        result = pipeline.execute_pipeline(context)
        self._processing_context = result.context
        return result

    def _process_pipeline_result(
        self, result: Any, final_params: Any, context: SQLProcessingContext
    ) -> tuple[str, Any]:
        """Process the result from the pipeline."""
        processed_expr = result.expression

        if isinstance(processed_expr, exp.Anonymous):
            processed_sql = self._raw_sql or context.initial_sql_string
        else:
            processed_sql = processed_expr.sql(dialect=self._dialect or self._config.dialect, comments=False)
            logger.debug("Processed expression SQL: '%s'", processed_sql)

            if self._placeholder_mapping and self._original_sql:
                processed_sql, result = self._denormalize_sql(processed_sql, result)

        # Merge parameters from pipeline
        merged_params = self._merge_pipeline_parameters(result, final_params)

        return processed_sql, merged_params

    def _denormalize_sql(self, processed_sql: str, result: Any) -> tuple[str, Any]:
        """Denormalize SQL back to original parameter style."""
        from sqlspec.statement.parameters import ParameterStyle

        original_sql = self._original_sql
        param_info = self._config.parameter_validator.extract_parameters(original_sql)
        target_styles = {p.style for p in param_info}

        logger.debug(
            "Denormalizing SQL: before='%s', original='%s', styles=%s", processed_sql, original_sql, target_styles
        )

        if ParameterStyle.POSITIONAL_PYFORMAT in target_styles:
            processed_sql = self._config.parameter_converter._denormalize_sql(
                processed_sql, param_info, ParameterStyle.POSITIONAL_PYFORMAT
            )
            logger.debug("Denormalized SQL to: '%s'", processed_sql)
        elif ParameterStyle.NAMED_PYFORMAT in target_styles:
            processed_sql = self._config.parameter_converter._denormalize_sql(
                processed_sql, param_info, ParameterStyle.NAMED_PYFORMAT
            )
            logger.debug("Denormalized SQL to: '%s'", processed_sql)
        elif ParameterStyle.POSITIONAL_COLON in target_styles:
            # Check if we already have named placeholders from parameter extraction
            processed_param_info = self._config.parameter_validator.extract_parameters(processed_sql)
            has_param_placeholders = any(p.name and p.name.startswith("param_") for p in processed_param_info)

            if has_param_placeholders:
                # Skip denormalization when we have param_N placeholders from literal extraction
                logger.debug("Skipping denormalization for param_N placeholders")
            else:
                processed_sql = self._config.parameter_converter._denormalize_sql(
                    processed_sql, param_info, ParameterStyle.POSITIONAL_COLON
                )
                logger.debug("Denormalized SQL to: '%s'", processed_sql)
            # Also denormalize parameters back to Oracle numeric format
            if (
                self._placeholder_mapping
                and result.context.merged_parameters
                and isinstance(result.context.merged_parameters, dict)
            ):
                result.context.merged_parameters = self._denormalize_oracle_params(result.context.merged_parameters)
        else:
            logger.debug(
                "No denormalization needed: mapping=%s, original=%s",
                bool(self._placeholder_mapping),
                bool(self._original_sql),
            )

        return processed_sql, result

    def _denormalize_oracle_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Denormalize Oracle parameters back to numeric format."""
        denormalized_params = {}
        for placeholder_key, original_name in self._placeholder_mapping.items():
            if placeholder_key in params:
                denormalized_params[str(original_name)] = params[placeholder_key]
        # Keep any non-normalized parameters as-is
        non_normalized_params = {key: value for key, value in params.items() if not key.startswith("param_")}
        denormalized_params.update(non_normalized_params)
        return denormalized_params

    def _merge_pipeline_parameters(self, result: Any, final_params: Any) -> Any:
        """Merge parameters from the pipeline processing."""
        merged_params = result.context.merged_parameters

        # If the pipeline didn't update merged_parameters, fall back to original behavior
        if merged_params == final_params and result.context.extracted_parameters_from_pipeline:
            # Original behavior - merge extracted parameters
            merged_params = final_params
            if result.context.extracted_parameters_from_pipeline:
                if isinstance(merged_params, dict):
                    # For named parameters, add extracted params with generated names
                    for i, param in enumerate(result.context.extracted_parameters_from_pipeline):
                        param_name = f"param_{i}"
                        merged_params[param_name] = param
                elif isinstance(merged_params, list):
                    # For positional parameters, extend the list
                    merged_params.extend(result.context.extracted_parameters_from_pipeline)
                elif merged_params is None:
                    # No user parameters, use extracted ones
                    merged_params = result.context.extracted_parameters_from_pipeline
                else:
                    # Single value, convert to list and add extracted params
                    merged_params = [merged_params, *list(result.context.extracted_parameters_from_pipeline)]

        return merged_params

    def _finalize_processed_state(self, result: Any, processed_sql: str, merged_params: Any) -> None:
        """Finalize the processed state."""
        self._processed_state = _ProcessedState(
            processed_expression=result.expression,
            processed_sql=processed_sql,
            merged_parameters=merged_params,
            validation_errors=list(result.context.validation_errors),
            analysis_results={},  # Can be populated from analysis_findings if needed
            transformation_results={},  # Can be populated from transformations if needed
        )

        if self._config.strict_mode and self._processed_state.validation_errors:
            highest_risk_error = max(
                self._processed_state.validation_errors, key=lambda e: e.risk_level.value if has_risk_level(e) else 0
            )
            raise SQLValidationError(
                message=highest_risk_error.message,
                sql=self._raw_sql or processed_sql,
                risk_level=getattr(highest_risk_error, "risk_level", RiskLevel.HIGH),
            )

    def _to_expression(self, statement: str | exp.Expression) -> exp.Expression:
        """Convert string to sqlglot expression."""
        if isinstance(statement, exp.Expression):
            return statement

        if not statement or not statement.strip():
            return exp.Select()

        if not self._config.enable_parsing:
            return exp.Anonymous(this=statement)

        from sqlspec.statement.parameters import ParameterStyle

        validator = self._config.parameter_validator
        param_info = validator.extract_parameters(statement)

        has_pyformat = any(
            p.style in {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT} for p in param_info
        )
        has_oracle = any(p.style == ParameterStyle.POSITIONAL_COLON for p in param_info)

        normalized_sql = statement
        placeholder_mapping: dict[str, Any] = {}

        if has_pyformat or has_oracle:
            # Normalize pyformat placeholders to named placeholders for SQLGlot
            converter = self._config.parameter_converter
            normalized_sql, placeholder_mapping = converter._transform_sql_for_parsing(statement, param_info)
            self._original_sql = statement
            self._placeholder_mapping = placeholder_mapping

        try:
            # Parse with sqlglot
            expressions = sqlglot.parse(normalized_sql, dialect=self._dialect)  # pyright: ignore
            if not expressions:
                return exp.Anonymous(this=statement)
            first_expr = expressions[0]
            if first_expr is None:
                # Could not parse
                return exp.Anonymous(this=statement)

        except ParseError as e:
            if getattr(self._config, "parse_errors_as_warnings", False):
                logger.warning(
                    "Failed to parse SQL, returning Anonymous expression.", extra={"sql": statement, "error": str(e)}
                )
                return exp.Anonymous(this=statement)
            from sqlspec.exceptions import SQLParsingError

            msg = f"Failed to parse SQL: {statement}"
            raise SQLParsingError(msg) from e
        return first_expr

    @staticmethod
    def _extract_filter_parameters(filter_obj: StatementFilter) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters from a filter object."""
        if can_extract_parameters(filter_obj):
            return filter_obj.extract_parameters()
        return [], {}

    def copy(
        self,
        statement: str | exp.Expression | None = None,
        parameters: Any | None = None,
        dialect: DialectType = None,
        config: SQLConfig | None = None,
        **kwargs: Any,
    ) -> SQL:
        """Create a copy with optional modifications.

        This is the primary method for creating modified SQL objects.
        """
        existing_state = {
            "positional_params": list(self._positional_params),
            "named_params": dict(self._named_params),
            "filters": list(self._filters),
            "is_many": self._is_many,
            "is_script": self._is_script,
            "raw_sql": self._raw_sql,
        }
        # Always include original_parameters in existing_state
        existing_state["original_parameters"] = self._original_parameters

        new_statement = statement if statement is not None else self._statement
        new_dialect = dialect if dialect is not None else self._dialect
        new_config = config if config is not None else self._config

        # If parameters are explicitly provided, they replace existing ones
        if parameters is not None:
            existing_state["positional_params"] = []
            existing_state["named_params"] = {}
            return SQL(
                new_statement,
                parameters,
                _dialect=new_dialect,
                _config=new_config,
                _builder_result_type=self._builder_result_type,
                _existing_state=None,  # Don't use existing state
                **kwargs,
            )

        return SQL(
            new_statement,
            _dialect=new_dialect,
            _config=new_config,
            _builder_result_type=self._builder_result_type,
            _existing_state=existing_state,
            **kwargs,
        )

    def add_named_parameter(self, name: str, value: Any) -> SQL:
        """Add a named parameter and return a new SQL instance."""
        new_obj = self.copy()
        new_obj._named_params[name] = value
        return new_obj

    def get_unique_parameter_name(
        self, base_name: str, namespace: str | None = None, preserve_original: bool = False
    ) -> str:
        """Generate a unique parameter name.

        Args:
            base_name: The base parameter name
            namespace: Optional namespace prefix (e.g., 'cte', 'subquery')
            preserve_original: If True, try to preserve the original name

        Returns:
            A unique parameter name
        """
        # Check both positional and named params
        all_param_names = set(self._named_params.keys())

        candidate = f"{namespace}_{base_name}" if namespace else base_name

        # If preserve_original and the name is unique, use it
        if preserve_original and candidate not in all_param_names:
            return candidate

        if candidate not in all_param_names:
            return candidate

        # Generate unique name with counter
        counter = 1
        while True:
            new_candidate = f"{candidate}_{counter}"
            if new_candidate not in all_param_names:
                return new_candidate
            counter += 1

    def where(self, condition: str | exp.Expression | exp.Condition) -> SQL:
        """Apply WHERE clause and return new SQL instance."""
        condition_expr = self._to_expression(condition) if isinstance(condition, str) else condition

        if supports_where(self._statement):
            new_statement = self._statement.where(condition_expr)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).where(condition_expr)  # pyright: ignore

        return self.copy(statement=new_statement)

    def filter(self, filter_obj: StatementFilter) -> SQL:
        """Apply a filter and return a new SQL instance."""
        new_obj = self.copy()
        new_obj._filters.append(filter_obj)
        pos_params, named_params = self._extract_filter_parameters(filter_obj)
        new_obj._positional_params.extend(pos_params)
        new_obj._named_params.update(named_params)
        return new_obj

    def as_many(self, parameters: list[Any] | None = None) -> SQL:
        """Mark for executemany with optional parameters."""
        new_obj = self.copy()
        new_obj._is_many = True
        if parameters is not None:
            new_obj._positional_params = []
            new_obj._named_params = {}
            new_obj._original_parameters = parameters
        return new_obj

    def as_script(self) -> SQL:
        """Mark as script for execution."""
        new_obj = self.copy()
        new_obj._is_script = True
        return new_obj

    def _build_final_state(self) -> tuple[exp.Expression, Any]:
        """Build final expression and parameters after applying filters."""
        final_expr = self._statement

        for filter_obj in self._filters:
            if can_append_to_statement(filter_obj):
                temp_sql = SQL(final_expr, config=self._config, dialect=self._dialect)
                temp_sql._positional_params = list(self._positional_params)
                temp_sql._named_params = dict(self._named_params)
                result = filter_obj.append_to_statement(temp_sql)
                final_expr = result._statement if isinstance(result, SQL) else result

        final_params: Any
        if self._named_params and not self._positional_params:
            final_params = dict(self._named_params)
        elif self._positional_params and not self._named_params:
            final_params = list(self._positional_params)
        elif self._positional_params and self._named_params:
            # Mixed - merge into dict
            final_params = dict(self._named_params)
            for i, param in enumerate(self._positional_params):
                param_name = f"arg_{i}"
                while param_name in final_params:
                    param_name = f"arg_{i}_{id(param)}"
                final_params[param_name] = param
        else:
            final_params = None

        return final_expr, final_params

    @property
    def sql(self) -> str:
        """Get SQL string."""
        if not self._raw_sql or (self._raw_sql and not self._raw_sql.strip()):
            return ""

        # For scripts, always return the raw SQL to preserve multi-statement scripts
        if self._is_script and self._raw_sql:
            return self._raw_sql
        # If parsing is disabled, return the raw SQL
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql

        self._ensure_processed()
        assert self._processed_state is not None
        return self._processed_state.processed_sql

    @property
    def expression(self) -> exp.Expression | None:
        """Get the final expression."""
        if not self._config.enable_parsing:
            return None
        self._ensure_processed()
        assert self._processed_state is not None
        return self._processed_state.processed_expression

    @property
    def parameters(self) -> Any:
        """Get merged parameters."""
        if self._is_many and self._original_parameters is not None:
            return self._original_parameters

        # If original parameters were passed as multiple args, return as tuple
        if (
            self._original_parameters is not None
            and isinstance(self._original_parameters, tuple)
            and not self._named_params
        ):
            return self._original_parameters

        self._ensure_processed()
        assert self._processed_state is not None
        params = self._processed_state.merged_parameters
        if params is None:
            return {}
        return params

    @property
    def is_many(self) -> bool:
        """Check if this is for executemany."""
        return self._is_many

    @property
    def is_script(self) -> bool:
        """Check if this is a script."""
        return self._is_script

    @property
    def dialect(self) -> DialectType | None:
        """Get the SQL dialect."""
        return self._dialect

    def to_sql(self, placeholder_style: str | None = None) -> str:
        """Convert to SQL string with given placeholder style."""
        if self._is_script:
            return self.sql
        sql, _ = self.compile(placeholder_style=placeholder_style)
        return sql

    def get_parameters(self, style: str | None = None) -> Any:
        """Get parameters in the requested style."""
        _, params = self.compile(placeholder_style=style)
        return params

    def compile(self, placeholder_style: str | None = None) -> tuple[str, Any]:
        """Compile to SQL and parameters."""
        if self._is_script:
            return self.sql, None

        # For executemany operations with original parameters, handle specially
        if self._is_many and self._original_parameters is not None:
            sql = self.sql  # This will ensure processing if needed

            # Ensure processing happens to get extracted parameters
            self._ensure_processed()

            # Start with original parameters
            params = self._original_parameters

            # If there are extracted parameters from the pipeline, add them to each parameter set
            extracted_params = []
            if self._processed_state and self._processed_state.merged_parameters:
                # Check if merged_parameters contains extracted parameters (TypedParameter objects)
                # We need to distinguish between extracted parameters and the original parameter list
                merged = self._processed_state.merged_parameters
                if isinstance(merged, list):
                    # For execute_many, merged_parameters might be the original parameter list
                    # Extracted parameters should be TypedParameter objects or scalars, not tuples
                    if merged and not isinstance(merged[0], (tuple, list)):
                        # This looks like extracted parameters (not parameter sets)
                        extracted_params = merged
                elif (
                    hasattr(self, "_processing_context")
                    and self._processing_context
                    and self._processing_context.extracted_parameters_from_pipeline
                ):
                    extracted_params = self._processing_context.extracted_parameters_from_pipeline

            if extracted_params:
                # Merge extracted parameters with each parameter set
                enhanced_params = []
                for param_set in params:
                    if isinstance(param_set, (list, tuple)):
                        # Convert TypedParameter objects to their values
                        extracted_values = []
                        for extracted in extracted_params:
                            if has_parameter_value(extracted):
                                extracted_values.append(extracted.value)
                            else:
                                extracted_values.append(extracted)
                        # Add extracted parameters to this parameter set
                        enhanced_set = list(param_set) + extracted_values
                        enhanced_params.append(tuple(enhanced_set))
                    else:
                        # Single parameter - treat as tuple and add extracted
                        extracted_values = []
                        for extracted in extracted_params:
                            if has_parameter_value(extracted):
                                extracted_values.append(extracted.value)
                            else:
                                extracted_values.append(extracted)
                        enhanced_params.append((param_set, *extracted_values))
                params = enhanced_params

            if placeholder_style:
                sql, params = self._convert_placeholder_style(sql, params, placeholder_style)

            return sql, params

        # If parsing is disabled, return raw SQL without transformation
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql, self._raw_parameters

        self._ensure_processed()

        assert self._processed_state is not None
        sql = self._processed_state.processed_sql
        params = self._processed_state.merged_parameters

        if params is not None and hasattr(self, "_processing_context") and self._processing_context:
            parameter_mapping = self._processing_context.metadata.get("parameter_position_mapping")
            if parameter_mapping:
                params = self._reorder_parameters(params, parameter_mapping)

        # Unwrap TypedParameter objects before returning
        params = self._unwrap_typed_parameters(params)

        # If no placeholder style requested, return as-is
        if placeholder_style is None:
            return sql, params

        if placeholder_style:
            # For Oracle positional colon style, we need the original merged params
            # with TypedParameter objects intact to properly order parameters
            if placeholder_style == "positional_colon" and self._processed_state:
                original_params = self._processed_state.merged_parameters
                sql, params = self._convert_placeholder_style(sql, original_params, placeholder_style)
                # Now unwrap the result
                params = self._unwrap_typed_parameters(params)
            else:
                sql, params = self._convert_placeholder_style(sql, params, placeholder_style)

        return sql, params

    @staticmethod
    def _unwrap_typed_parameters(params: Any) -> Any:
        """Unwrap TypedParameter objects to their actual values.

        Args:
            params: Parameters that may contain TypedParameter objects

        Returns:
            Parameters with TypedParameter objects unwrapped to their values
        """
        if params is None:
            return None

        if isinstance(params, dict):
            unwrapped_dict = {}
            for key, value in params.items():
                if has_parameter_value(value):
                    unwrapped_dict[key] = value.value
                else:
                    unwrapped_dict[key] = value
            return unwrapped_dict

        if isinstance(params, (list, tuple)):
            unwrapped_list = []
            for value in params:
                if has_parameter_value(value):
                    unwrapped_list.append(value.value)
                else:
                    unwrapped_list.append(value)
            return type(params)(unwrapped_list)

        # Single parameter
        if has_parameter_value(params):
            return params.value

        return params

    @staticmethod
    def _reorder_parameters(params: Any, mapping: dict[int, int]) -> Any:
        """Reorder parameters based on the position mapping.

        Args:
            params: Original parameters (list, tuple, or dict)
            mapping: Dict mapping new positions to original positions

        Returns:
            Reordered parameters in the same format as input
        """
        if isinstance(params, (list, tuple)):
            reordered_list = [None] * len(params)  # pyright: ignore
            for new_pos, old_pos in mapping.items():
                if old_pos < len(params):
                    reordered_list[new_pos] = params[old_pos]  # pyright: ignore

            for i, val in enumerate(reordered_list):
                if val is None and i < len(params) and i not in mapping:
                    # If position wasn't mapped, try to use original
                    reordered_list[i] = params[i]  # pyright: ignore

            return tuple(reordered_list) if isinstance(params, tuple) else reordered_list

        if isinstance(params, dict):
            # For dict parameters, we need to handle differently
            # If keys are like param_0, param_1, we can reorder them
            if all(key.startswith("param_") and key[6:].isdigit() for key in params):
                reordered_dict: dict[str, Any] = {}
                for new_pos, old_pos in mapping.items():
                    old_key = f"param_{old_pos}"
                    new_key = f"param_{new_pos}"
                    if old_key in params:
                        reordered_dict[new_key] = params[old_key]

                for key, value in params.items():
                    if key not in reordered_dict and key.startswith("param_"):
                        idx = int(key[6:])
                        if idx not in mapping:
                            reordered_dict[key] = value

                return reordered_dict
            # Can't reorder named parameters, return as-is
            return params
        # Single value or unknown format, return as-is
        return params

    def _convert_placeholder_style(self, sql: str, params: Any, placeholder_style: str) -> tuple[str, Any]:
        """Convert SQL and parameters to the requested placeholder style.

        Args:
            sql: The SQL string to convert
            params: The parameters to convert
            placeholder_style: Target placeholder style

        Returns:
            Tuple of (converted_sql, converted_params)
        """
        if self._is_many and isinstance(params, list) and params and isinstance(params[0], (list, tuple)):
            converter = self._config.parameter_converter
            param_info = converter.validator.extract_parameters(sql)

            if param_info:
                from sqlspec.statement.parameters import ParameterStyle

                target_style = (
                    ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
                )
                sql = self._replace_placeholders_in_sql(sql, param_info, target_style)

            return sql, params

        # Always extract parameter info from the current SQL to ensure we catch
        # any parameters added by transformers (like ParameterizeLiterals)
        converter = self._config.parameter_converter
        param_info = converter.validator.extract_parameters(sql)

        if not param_info:
            return sql, params

        from sqlspec.statement.parameters import ParameterStyle

        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, params, param_info)

        if param_info and all(p.style == target_style for p in param_info):
            converted_params = self._convert_parameters_format(params, param_info, target_style)
            return sql, converted_params

        sql = self._replace_placeholders_in_sql(sql, param_info, target_style)

        params = self._convert_parameters_format(params, param_info, target_style)

        return sql, params

    def _embed_static_parameters(self, sql: str, params: Any, param_info: list[Any]) -> tuple[str, Any]:
        """Embed parameter values directly into SQL for STATIC style.

        This is used for scripts and other cases where parameters need to be
        embedded directly in the SQL string rather than passed separately.

        Args:
            sql: The SQL string with placeholders
            params: The parameter values
            param_info: List of parameter information from extraction

        Returns:
            Tuple of (sql_with_embedded_values, None)
        """
        param_list: list[Any] = []
        if isinstance(params, dict):
            for p in param_info:
                if p.name and p.name in params:
                    param_list.append(params[p.name])
                elif f"param_{p.ordinal}" in params:
                    param_list.append(params[f"param_{p.ordinal}"])
                elif f"arg_{p.ordinal}" in params:
                    param_list.append(params[f"arg_{p.ordinal}"])
                else:
                    param_list.append(params.get(str(p.ordinal), None))
        elif isinstance(params, (list, tuple)):
            param_list = list(params)
        elif params is not None:
            param_list = [params]

        sorted_params = sorted(param_info, key=lambda p: p.position, reverse=True)

        for p in sorted_params:
            if p.ordinal < len(param_list):
                value = param_list[p.ordinal]

                if has_parameter_value(value):
                    value = value.value

                if value is None:
                    literal_str = "NULL"
                elif isinstance(value, bool):
                    literal_str = "TRUE" if value else "FALSE"
                elif isinstance(value, str):
                    literal_expr = sqlglot.exp.Literal.string(value)
                    literal_str = literal_expr.sql(dialect=self._dialect)
                elif isinstance(value, (int, float)):
                    literal_expr = sqlglot.exp.Literal.number(value)
                    literal_str = literal_expr.sql(dialect=self._dialect)
                else:
                    literal_expr = sqlglot.exp.Literal.string(str(value))
                    literal_str = literal_expr.sql(dialect=self._dialect)

                start = p.position
                end = start + len(p.placeholder_text)
                sql = sql[:start] + literal_str + sql[end:]

        return sql, None

    def _replace_placeholders_in_sql(self, sql: str, param_info: list[Any], target_style: ParameterStyle) -> str:
        """Replace placeholders in SQL string with target style placeholders.

        Args:
            sql: The SQL string
            param_info: List of parameter information
            target_style: Target parameter style

        Returns:
            SQL string with replaced placeholders
        """
        sorted_params = sorted(param_info, key=lambda p: p.position, reverse=True)

        for p in sorted_params:
            new_placeholder = self._generate_placeholder(p, target_style)
            start = p.position
            end = start + len(p.placeholder_text)
            sql = sql[:start] + new_placeholder + sql[end:]

        return sql

    @staticmethod
    def _generate_placeholder(param: Any, target_style: ParameterStyle) -> str:
        """Generate a placeholder string for the given parameter style.

        Args:
            param: Parameter information object
            target_style: Target parameter style

        Returns:
            Placeholder string
        """
        if target_style in {ParameterStyle.STATIC, ParameterStyle.QMARK}:
            return "?"
        if target_style == ParameterStyle.NUMERIC:
            # Use 1-based numbering for numeric style
            return f"${param.ordinal + 1}"
        if target_style == ParameterStyle.NAMED_COLON:
            # Use original name if available, otherwise generate one
            # Oracle doesn't like underscores at the start of parameter names
            if param.name and not param.name.isdigit():
                return f":{param.name}"
            # Generate a new name for numeric placeholders or missing names
            return f":arg_{param.ordinal}"
        if target_style == ParameterStyle.NAMED_AT:
            # Use @ prefix for BigQuery style
            # BigQuery requires parameter names to start with a letter, not underscore
            return f"@{param.name or f'param_{param.ordinal}'}"
        if target_style == ParameterStyle.POSITIONAL_COLON:
            # Use :1, :2, etc. for Oracle positional style
            return f":{param.ordinal + 1}"
        if target_style == ParameterStyle.POSITIONAL_PYFORMAT:
            # Use %s for positional pyformat
            return "%s"
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            # Use %(name)s for named pyformat
            return f"%({param.name or f'arg_{param.ordinal}'})s"
        return str(param.placeholder_text)

    def _convert_parameters_format(self, params: Any, param_info: list[Any], target_style: ParameterStyle) -> Any:
        """Convert parameters to the appropriate format for the target style.

        Args:
            params: Original parameters
            param_info: List of parameter information
            target_style: Target parameter style

        Returns:
            Converted parameters
        """
        if target_style == ParameterStyle.POSITIONAL_COLON:
            return self._convert_to_positional_colon_format(params, param_info)
        if target_style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT}:
            return self._convert_to_positional_format(params, param_info)
        if target_style == ParameterStyle.NAMED_COLON:
            return self._convert_to_named_colon_format(params, param_info)
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            return self._convert_to_named_pyformat_format(params, param_info)
        return params

    def _convert_list_to_oracle_dict(
        self, params: list[Any] | tuple[Any, ...], param_info: list[Any]
    ) -> dict[str, Any]:
        """Convert list/tuple parameters to Oracle dict format."""
        result_dict: dict[str, Any] = {}

        if param_info:
            all_numeric = all(p.name and p.name.isdigit() for p in param_info)
            if all_numeric:
                # For Oracle numeric parameters, list position maps to parameter number
                for i, value in enumerate(params):
                    result_dict[str(i + 1)] = value
            else:
                # Non-numeric names, map by ordinal
                for i, value in enumerate(params):
                    if i < len(param_info):
                        param_name = param_info[i].name or str(i + 1)
                        result_dict[param_name] = value
                    else:
                        result_dict[str(i + 1)] = value
        else:
            for i, value in enumerate(params):
                result_dict[str(i + 1)] = value

        return result_dict

    def _convert_single_value_to_oracle_dict(self, params: Any, param_info: list[Any]) -> dict[str, Any]:
        """Convert single value parameter to Oracle dict format."""
        result_dict: dict[str, Any] = {}
        if param_info and param_info[0].name and param_info[0].name.isdigit():
            result_dict[param_info[0].name] = params
        else:
            result_dict["1"] = params
        return result_dict

    def _process_mixed_oracle_params(self, params: dict[str, Any], param_info: list[Any]) -> dict[str, Any]:
        """Process mixed Oracle numeric and normalized parameters."""
        result_dict: dict[str, Any] = {}

        # Separate different types of parameters
        extracted_params = []
        user_oracle_params = {}
        extracted_keys_sorted = []

        for key, value in params.items():
            if has_parameter_value(value):
                extracted_params.append((key, value))
            elif key.isdigit():
                user_oracle_params[key] = value
            elif key.startswith("param_") and key[6:].isdigit():
                param_idx = int(key[6:])
                oracle_key = str(param_idx + 1)
                if oracle_key not in user_oracle_params:
                    extracted_keys_sorted.append((param_idx, key, value))
            else:
                extracted_params.append((key, value))

        # Sort extracted keys by index
        extracted_keys_sorted.sort(key=lambda x: x[0])
        for _, key, value in extracted_keys_sorted:
            extracted_params.append((key, value))

        # Track which extracted params we've used
        used_extracted = set()

        # Process each parameter based on its position in the SQL
        for p in sorted(param_info, key=lambda x: x.ordinal):
            oracle_key = str(p.ordinal + 1)

            if p.name is None:
                # Anonymous placeholder from literal extraction
                for key, value in extracted_params:
                    if key not in used_extracted:
                        used_extracted.add(key)
                        if has_parameter_value(value):
                            result_dict[oracle_key] = value.value
                        else:
                            result_dict[oracle_key] = value
                        break
            elif p.name == oracle_key:
                # Oracle numeric parameter
                if oracle_key in user_oracle_params:
                    result_dict[oracle_key] = user_oracle_params[oracle_key]
                else:
                    for key, value in extracted_params:
                        if key not in used_extracted:
                            used_extracted.add(key)
                            if has_parameter_value(value):
                                result_dict[oracle_key] = value.value
                            else:
                                result_dict[oracle_key] = value
                            break
            else:
                # Named parameter
                for key, value in extracted_params:
                    if key == p.name and key not in used_extracted:
                        used_extracted.add(key)
                        if has_parameter_value(value):
                            result_dict[oracle_key] = value.value
                        else:
                            result_dict[oracle_key] = value
                        break

        return result_dict

    def _convert_to_positional_colon_format(self, params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for Oracle positional colon style.

        Oracle's positional colon style uses :1, :2, etc. placeholders and expects
        parameters as a dict with string keys "1", "2", etc.

        For execute_many operations, returns a list of parameter sets.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            Dict of parameters with string keys "1", "2", etc., or list for execute_many
        """
        if self._is_many and isinstance(params, list) and params and isinstance(params[0], (list, tuple)):
            return params

        if isinstance(params, (list, tuple)):
            return self._convert_list_to_oracle_dict(params, param_info)

        if not is_dict(params) and param_info:
            return self._convert_single_value_to_oracle_dict(params, param_info)

        if is_dict(params):
            if all(key.isdigit() for key in params):
                return params

            if all(key.startswith("param_") for key in params):
                result_dict: dict[str, Any] = {}
                # Map normalized params back to Oracle numeric format using param_info
                for i, p in enumerate(sorted(param_info, key=lambda x: x.ordinal)):
                    if p.name and p.name.isdigit():
                        normalized_key = f"param_{i}"
                        if normalized_key in params:
                            result_dict[p.name] = params[normalized_key]
                    else:
                        normalized_key = f"param_{i}"
                        if normalized_key in params:
                            result_dict[str(i + 1)] = params[normalized_key]
                return result_dict

            # Special handling for mixed Oracle numeric + normalized params
            # This happens when literals are extracted from SQL like "VALUES (1, :1)"
            has_oracle_numeric = any(key.isdigit() for key in params)
            has_param_normalized = any(key.startswith("param_") for key in params)
            has_typed_params = any(has_parameter_value(v) for v in params.values())

            if (has_oracle_numeric and has_param_normalized) or has_typed_params:
                return self._process_mixed_oracle_params(params, param_info)

            # Standard case - handle direct name matching and fallback patterns
            result_dict: dict[str, Any] = {}

            # Try direct name matching first
            if param_info:
                for p in sorted(param_info, key=lambda x: x.ordinal):
                    oracle_key = str(p.ordinal + 1)
                    if p.name and p.name in params:
                        value = params[p.name]
                        if has_parameter_value(value):
                            result_dict[oracle_key] = value.value
                        else:
                            result_dict[oracle_key] = value

                # If we didn't get all params, try alternative patterns
                if len(result_dict) < len(param_info):
                    for p in sorted(param_info, key=lambda x: x.ordinal):
                        oracle_key = str(p.ordinal + 1)
                        if oracle_key not in result_dict:
                            # Try different key patterns
                            value = None
                            if f"param_{p.ordinal}" in params:
                                value = params[f"param_{p.ordinal}"]
                            elif f"arg_{p.ordinal}" in params:
                                value = params[f"arg_{p.ordinal}"]

                            if value is not None:
                                if has_parameter_value(value):
                                    value = value.value
                                result_dict[oracle_key] = value

            return result_dict

        return params

    @staticmethod
    def _convert_to_positional_format(params: Any, param_info: list[Any]) -> Any:
        """Convert to list format for positional parameter styles.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            List of parameters
        """
        result_list: list[Any] = []
        if is_dict(params):
            # Create a mapping of all parameter values by ordinal for easier lookup
            param_values_by_ordinal: dict[int, Any] = {}

            # First, map named parameters that exist in the dict
            for p in param_info:
                if p.name and p.name in params:
                    param_values_by_ordinal[p.ordinal] = params[p.name]

            # Then, map unnamed parameters using arg_N or param_N naming
            for p in param_info:
                if p.name is None and p.ordinal not in param_values_by_ordinal:
                    arg_key = f"arg_{p.ordinal}"
                    param_key = f"param_{p.ordinal}"
                    if arg_key in params:
                        param_values_by_ordinal[p.ordinal] = params[arg_key]
                    elif param_key in params:
                        param_values_by_ordinal[p.ordinal] = params[param_key]

            # Finally, try to match any remaining parameters by semantic naming
            # This handles parameters created by ParameterizeLiterals with semantic names
            remaining_params = {
                k: v
                for k, v in params.items()
                if k not in {p.name for p in param_info if p.name} and not k.startswith(("arg_", "param_"))
            }

            unmatched_ordinals = [p.ordinal for p in param_info if p.ordinal not in param_values_by_ordinal]

            # Match remaining parameters with unmatched ordinals by order
            for ordinal, (_key, value) in zip(unmatched_ordinals, remaining_params.items()):
                param_values_by_ordinal[ordinal] = value

            # Build result list in ordinal order
            for p in param_info:
                val = param_values_by_ordinal.get(p.ordinal)
                if val is not None:
                    if has_parameter_value(val):
                        result_list.append(val.value)
                    else:
                        result_list.append(val)
                else:
                    result_list.append(None)

            return result_list
        if isinstance(params, (list, tuple)):
            for param in params:
                if has_parameter_value(param):
                    result_list.append(param.value)
                else:
                    result_list.append(param)
            return result_list
        return params

    @staticmethod
    def _convert_to_named_colon_format(params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for named colon style.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            Dict of parameters with generated names
        """
        result_dict: dict[str, Any] = {}
        if is_dict(params):
            if all(p.name in params for p in param_info if p.name):
                return params
            for p in param_info:
                if p.name and p.name in params:
                    result_dict[p.name] = params[p.name]
                elif f"param_{p.ordinal}" in params:
                    # Oracle doesn't like underscores at the start of parameter names
                    result_dict[p.name or f"arg_{p.ordinal}"] = params[f"param_{p.ordinal}"]
            return result_dict
        if isinstance(params, (list, tuple)):
            # Ensure we process all parameters, not just up to len(param_info)
            for i, value in enumerate(params):
                # Unwrap TypedParameter if needed
                if has_parameter_value(value):
                    value = value.value

                if i < len(param_info):
                    p = param_info[i]
                    # Oracle doesn't like underscores at the start of parameter names
                    param_name = p.name or f"arg_{i}"
                    result_dict[param_name] = value
                else:
                    # Handle extra parameters beyond param_info
                    param_name = f"arg_{i}"
                    result_dict[param_name] = value
            return result_dict
        return params

    @staticmethod
    def _convert_to_named_pyformat_format(params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for named pyformat style.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            Dict of parameters with names
        """
        if isinstance(params, (list, tuple)):
            result_dict: dict[str, Any] = {}
            for i, p in enumerate(param_info):
                if i < len(params):
                    param_name = p.name or f"param_{i}"
                    result_dict[param_name] = params[i]
            return result_dict
        return params

    @property
    def validation_errors(self) -> list[Any]:
        """Get validation errors."""
        if not self._config.enable_validation:
            return []
        self._ensure_processed()
        assert self._processed_state
        return self._processed_state.validation_errors

    @property
    def has_errors(self) -> bool:
        """Check if there are validation errors."""
        return bool(self.validation_errors)

    @property
    def is_safe(self) -> bool:
        """Check if statement is safe."""
        return not self.has_errors

    def validate(self) -> list[Any]:
        """Validate the SQL statement and return validation errors."""
        return self.validation_errors

    @property
    def parameter_info(self) -> list[Any]:
        """Get parameter information from the SQL statement.

        Returns the original parameter info before any normalization.
        """
        # Always extract from raw SQL to get original parameter info
        validator = self._config.parameter_validator
        if self._raw_sql:
            return validator.extract_parameters(self._raw_sql)

        # Fallback to processed state if no raw SQL
        self._ensure_processed()

        if hasattr(self, "_processing_context") and self._processing_context:
            return self._processing_context.parameter_info

        return []

    @property
    def _raw_parameters(self) -> Any:
        """Get raw parameters for compatibility."""
        return self._original_parameters

    @property
    def _sql(self) -> str:
        """Get SQL string for compatibility."""
        return self.sql

    @property
    def _expression(self) -> exp.Expression | None:
        """Get expression for compatibility."""
        return self.expression

    @property
    def statement(self) -> exp.Expression:
        """Get statement for compatibility."""
        return self._statement

    def limit(self, count: int, use_parameter: bool = False) -> SQL:
        """Add LIMIT clause."""
        if use_parameter:
            param_name = self.get_unique_parameter_name("limit")
            result = self
            result = result.add_named_parameter(param_name, count)
            if supports_limit(result._statement):
                new_statement = result._statement.limit(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                new_statement = exp.Select().from_(result._statement).limit(exp.Placeholder(this=param_name))  # pyright: ignore
            return result.copy(statement=new_statement)
        if supports_limit(self._statement):
            new_statement = self._statement.limit(count)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).limit(count)  # pyright: ignore
        return self.copy(statement=new_statement)

    def offset(self, count: int, use_parameter: bool = False) -> SQL:
        """Add OFFSET clause."""
        if use_parameter:
            param_name = self.get_unique_parameter_name("offset")
            result = self
            result = result.add_named_parameter(param_name, count)
            if supports_offset(result._statement):
                new_statement = result._statement.offset(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                new_statement = exp.Select().from_(result._statement).offset(exp.Placeholder(this=param_name))  # pyright: ignore
            return result.copy(statement=new_statement)
        if supports_offset(self._statement):
            new_statement = self._statement.offset(count)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).offset(count)  # pyright: ignore
        return self.copy(statement=new_statement)

    def order_by(self, expression: exp.Expression) -> SQL:
        """Add ORDER BY clause."""
        if supports_order_by(self._statement):
            new_statement = self._statement.order_by(expression)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).order_by(expression)  # pyright: ignore
        return self.copy(statement=new_statement)
