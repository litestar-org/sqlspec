"""SQL statement handling with centralized parameter management."""

import operator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from typing_extensions import TypeAlias

from sqlspec.exceptions import RiskLevel, SQLParsingError, SQLValidationError
from sqlspec.statement.cache import sql_cache
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.parameters import (
    SQLGLOT_INCOMPATIBLE_STYLES,
    ParameterConverter,
    ParameterStyle,
    ParameterValidator,
)
from sqlspec.statement.pipelines import SQLProcessingContext, StatementPipeline
from sqlspec.statement.pipelines.transformers import CommentAndHintRemover, ParameterizeLiterals
from sqlspec.statement.pipelines.validators import DMLSafetyValidator, ParameterStyleValidator, SecurityValidator
from sqlspec.utils.logging import get_logger
from sqlspec.utils.statement_hashing import hash_sql_statement
from sqlspec.utils.type_guards import (
    can_append_to_statement,
    can_extract_parameters,
    has_parameter_value,
    has_risk_level,
    is_dict,
    is_expression,
    is_statement_filter,
    supports_where,
)

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.parameters import ParameterStyleTransformationState

__all__ = ("SQL", "SQLConfig", "Statement")

logger = get_logger("sqlspec.statement")

Statement: TypeAlias = Union[str, exp.Expression, "SQL"]

# Parameter naming constants
PARAM_PREFIX = "param_"
POS_PARAM_PREFIX = "pos_param_"
KW_POS_PARAM_PREFIX = "kw_pos_param_"
ARG_PREFIX = "arg_"

# Cache and limit constants
DEFAULT_CACHE_SIZE = 1000

# Oracle/Colon style parameter constants
COLON_PARAM_ONE = "1"
COLON_PARAM_MIN_INDEX = 1


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

    Uses conservative defaults that prioritize compatibility and robustness,
    making it easier to work with diverse SQL dialects and complex queries.

    Pipeline Configuration:
        enable_parsing: Parse SQL strings using sqlglot (default: True)
        enable_validation: Run SQL validators to check for safety issues (default: True)
        enable_transformations: Apply SQL transformers like literal parameterization (default: True)
        enable_analysis: Run SQL analyzers for metadata extraction (default: False)
        enable_expression_simplification: Apply expression simplification transformer (default: False)
        enable_parameter_type_wrapping: Wrap parameters with type information (default: True)
        parse_errors_as_warnings: Treat parse errors as warnings instead of failures (default: True)
        enable_caching: Cache processed SQL statements (default: True)

    Component Lists (Advanced):
        transformers: Optional list of SQL transformers for explicit staging
        validators: Optional list of SQL validators for explicit staging
        analyzers: Optional list of SQL analyzers for explicit staging

    Internal Configuration:
        parameter_converter: Handles parameter style conversions
        parameter_validator: Validates parameter usage and styles
        input_sql_had_placeholders: Populated by SQL.__init__ to track original SQL state
        dialect: SQL dialect to use for parsing and generation

    Parameter Style Configuration:
        allowed_parameter_styles: Allowed parameter styles (e.g., ('qmark', 'named_colon'))
        default_parameter_style: Target parameter style for SQL generation
        allow_mixed_parameter_styles: Whether to allow mixing parameter styles in same query
    """

    enable_parsing: bool = True
    enable_validation: bool = True
    enable_transformations: bool = True
    enable_analysis: bool = False
    enable_expression_simplification: bool = False
    enable_parameter_type_wrapping: bool = True
    parse_errors_as_warnings: bool = True
    enable_caching: bool = True

    transformers: "Optional[list[Any]]" = None
    validators: "Optional[list[Any]]" = None
    analyzers: "Optional[list[Any]]" = None

    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    input_sql_had_placeholders: bool = False
    dialect: "Optional[DialectType]" = None

    allowed_parameter_styles: "Optional[tuple[str, ...]]" = None
    default_parameter_style: "Optional[str]" = None
    allow_mixed_parameter_styles: bool = False
    analyzer_output_handler: "Optional[Callable[[Any], None]]" = None

    def validate_parameter_style(self, style: "Union[ParameterStyle, str]") -> bool:
        """Check if a parameter style is allowed.

        Args:
            style: Parameter style to validate (can be ParameterStyle enum or string)

        Returns:
            True if the style is allowed, False otherwise
        """
        if self.allowed_parameter_styles is None:
            return True
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
            placeholder_style = self.default_parameter_style or "?"
            transformers = [CommentAndHintRemover(), ParameterizeLiterals(placeholder_style=placeholder_style)]
            if self.enable_expression_simplification:
                from sqlspec.statement.pipelines.transformers import ExpressionSimplifier

                transformers.append(ExpressionSimplifier())

        validators = []
        if self.validators is not None:
            validators = list(self.validators)
        elif self.enable_validation:
            validators = [
                ParameterStyleValidator(fail_on_violation=not self.parse_errors_as_warnings),
                DMLSafetyValidator(),
                SecurityValidator(),
            ]

        analyzers = []
        if self.analyzers is not None:
            analyzers = list(self.analyzers)
        elif self.enable_analysis:
            from sqlspec.statement.pipelines.analyzers import StatementAnalyzer

            analyzers = [StatementAnalyzer()]

        return StatementPipeline(transformers=transformers, validators=validators, analyzers=analyzers)  # pyright: ignore


def default_analysis_handler(analysis: Any) -> None:
    """Default handler that logs analysis to debug."""
    logger.debug("SQL Analysis: %s", analysis)


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
        "_parameter_conversion_state",
        "_placeholder_mapping",
        "_positional_params",
        "_processed_state",
        "_processing_context",
        "_raw_sql",
        "_statement",
    )

    def __init__(
        self,
        statement: "Union[str, exp.Expression, 'SQL']",
        *parameters: "Union[Any, StatementFilter, list[Union[Any, StatementFilter]]]",
        _dialect: "DialectType" = None,
        _config: "Optional[SQLConfig]" = None,
        _builder_result_type: "Optional[type]" = None,
        _existing_state: "Optional[dict[str, Any]]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL with centralized parameter management."""
        if "config" in kwargs and _config is None:
            _config = kwargs.pop("config")
        self._config = _config or SQLConfig()
        self._dialect = _dialect or self._config.dialect
        self._builder_result_type = _builder_result_type
        self._processed_state: Optional[_ProcessedState] = None
        self._processing_context: Optional[SQLProcessingContext] = None
        self._positional_params: list[Any] = []
        self._named_params: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression
        self._raw_sql: str = ""
        self._original_parameters: Any = None
        self._original_sql: str = ""
        self._placeholder_mapping: dict[str, Union[str, int]] = {}
        self._parameter_conversion_state: Optional[ParameterStyleTransformationState] = None
        self._is_many: bool = False
        self._is_script: bool = False

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement, _dialect, _config or SQLConfig(), _builder_result_type)
        else:
            self._init_from_str_or_expression(statement)

        if _existing_state:
            self._load_from_existing_state(_existing_state)

        if not isinstance(statement, SQL) and not _existing_state:
            self._set_original_parameters(*parameters)

        self._process_parameters(*parameters, **kwargs)

    def _init_from_sql_object(
        self, statement: "SQL", dialect: "DialectType", config: "SQLConfig", builder_result_type: "Optional[type]"
    ) -> None:
        """Initialize from an existing SQL object."""
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
        self._parameter_conversion_state = statement._parameter_conversion_state
        self._positional_params.extend(statement._positional_params)
        self._named_params.update(statement._named_params)
        self._filters.extend(statement._filters)

    def _init_from_str_or_expression(self, statement: "Union[str, exp.Expression]") -> None:
        """Initialize from a string or expression."""
        if isinstance(statement, str):
            self._raw_sql = statement
            self._statement = self._to_expression(statement)
        else:
            self._raw_sql = statement.sql(dialect=self._dialect)
            self._statement = statement

    def _load_from_existing_state(self, existing_state: "dict[str, Any]") -> None:
        """Load state from a dictionary (used by copy)."""
        self._positional_params = list(existing_state.get("positional_params", self._positional_params))
        self._named_params = dict(existing_state.get("named_params", self._named_params))
        self._filters = list(existing_state.get("filters", self._filters))
        self._is_many = existing_state.get("is_many", self._is_many)
        self._is_script = existing_state.get("is_script", self._is_script)
        self._raw_sql = existing_state.get("raw_sql", self._raw_sql)
        self._original_parameters = existing_state.get("original_parameters", self._original_parameters)

    def _set_original_parameters(self, *parameters: Any) -> None:
        """Set the original parameters."""
        if not parameters or (len(parameters) == 1 and is_statement_filter(parameters[0])):
            self._original_parameters = None
        elif len(parameters) == 1 and isinstance(parameters[0], (list, tuple)):
            self._original_parameters = parameters[0]
        else:
            self._original_parameters = parameters

    def _process_parameters(self, *parameters: Any, **kwargs: Any) -> None:
        """Process and categorize parameters."""
        for param in parameters:
            self._process_parameter_item(param)

        if "parameters" in kwargs:
            param_value = kwargs.pop("parameters")
            if isinstance(param_value, (list, tuple)):
                self._positional_params.extend(param_value)
            elif is_dict(param_value):
                self._named_params.update(param_value)
            else:
                self._positional_params.append(param_value)

        self._named_params.update({k: v for k, v in kwargs.items() if not k.startswith("_")})

    def _cache_key(self) -> str:
        """Generate a cache key for the current SQL state."""
        return hash_sql_statement(self)

    def _process_parameter_item(self, item: Any) -> None:
        """Process a single item from the parameters list."""
        if is_statement_filter(item):
            self._filters.append(item)
            pos_params, named_params = self._extract_filter_parameters(item)
            self._positional_params.extend(pos_params)
            self._named_params.update(named_params)
        elif isinstance(item, list):
            for sub_item in item:
                self._process_parameter_item(sub_item)
        elif is_dict(item):
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

        # Check cache first if caching is enabled
        cache_key = None
        if self._config.enable_caching:
            cache_key = self._cache_key()
            cached_state = sql_cache.get(cache_key)

            if cached_state is not None:
                self._processed_state = cached_state
                return

        final_expr, final_params = self._build_final_state()
        has_placeholders = self._detect_placeholders()
        initial_sql_for_context, final_params = self._prepare_context_sql(final_expr, final_params)

        context = self._create_processing_context(initial_sql_for_context, final_expr, final_params, has_placeholders)
        result = self._run_pipeline(context)

        processed_sql, merged_params = self._process_pipeline_result(result, final_params, context)

        self._finalize_processed_state(result, processed_sql, merged_params)

        # Store in cache if caching is enabled
        if self._config.enable_caching and cache_key is not None and self._processed_state is not None:
            sql_cache.set(cache_key, self._processed_state)

    def _detect_placeholders(self) -> bool:
        """Detect if the raw SQL has placeholders."""
        if self._raw_sql:
            validator = self._config.parameter_validator
            raw_param_info = validator.extract_parameters(self._raw_sql)
            has_placeholders = bool(raw_param_info)
            if has_placeholders:
                self._config.input_sql_had_placeholders = True
            return has_placeholders
        return self._config.input_sql_had_placeholders

    def _prepare_context_sql(self, final_expr: exp.Expression, final_params: Any) -> tuple[str, Any]:
        """Prepare SQL string and parameters for context."""
        initial_sql_for_context = self._raw_sql or final_expr.sql(dialect=self._dialect or self._config.dialect)

        if is_expression(final_expr) and self._placeholder_mapping:
            initial_sql_for_context = final_expr.sql(dialect=self._dialect or self._config.dialect)
            if self._placeholder_mapping:
                final_params = self._convert_parameters(final_params)

        return initial_sql_for_context, final_params

    def _convert_parameters(self, final_params: Any) -> Any:
        """Convert parameters based on placeholder mapping."""
        if is_dict(final_params):
            converted_params = {}
            for placeholder_key, original_name in self._placeholder_mapping.items():
                if str(original_name) in final_params:
                    converted_params[placeholder_key] = final_params[str(original_name)]
            non_oracle_params = {
                key: value
                for key, value in final_params.items()
                if key not in {str(name) for name in self._placeholder_mapping.values()}
            }
            converted_params.update(non_oracle_params)
            return converted_params
        if isinstance(final_params, (list, tuple)):
            validator = self._config.parameter_validator
            param_info = validator.extract_parameters(self._raw_sql)

            all_numeric = all(p.name and p.name.isdigit() for p in param_info)

            if all_numeric:
                converted_params = {}

                min_param_num = min(int(p.name) for p in param_info if p.name)

                for i, param in enumerate(final_params):
                    param_num = str(i + min_param_num)
                    converted_params[param_num] = param

                return converted_params
            converted_params = {}
            for i, param in enumerate(final_params):
                if i < len(param_info):
                    placeholder_key = f"{PARAM_PREFIX}{param_info[i].ordinal}"
                    converted_params[placeholder_key] = param
            return converted_params
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

        if self._placeholder_mapping:
            context.extra_info["placeholder_map"] = self._placeholder_mapping

        # Set conversion state if available
        if self._parameter_conversion_state:
            context.parameter_conversion = self._parameter_conversion_state

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
            # Use the initial expression that includes filters, not the processed one
            # The processed expression may have lost LIMIT/OFFSET during pipeline processing
            if hasattr(context, "initial_expression") and context.initial_expression != processed_expr:
                # Check if LIMIT/OFFSET was stripped during processing
                has_limit_in_initial = (
                    context.initial_expression is not None
                    and hasattr(context.initial_expression, "args")
                    and "limit" in context.initial_expression.args
                )
                has_limit_in_processed = hasattr(processed_expr, "args") and "limit" in processed_expr.args

                if has_limit_in_initial and not has_limit_in_processed:
                    # Restore LIMIT/OFFSET from initial expression
                    processed_expr = context.initial_expression

            processed_sql = (
                processed_expr.sql(dialect=self._dialect or self._config.dialect, comments=False)
                if processed_expr
                else ""
            )
            if self._placeholder_mapping and self._original_sql:
                processed_sql, result = self._denormalize_sql(processed_sql, result)

        merged_params = self._merge_pipeline_parameters(result, final_params)

        return processed_sql, merged_params

    def _denormalize_sql(self, processed_sql: str, result: Any) -> tuple[str, Any]:
        """Denormalize SQL back to original parameter style."""

        original_sql = self._original_sql
        param_info = self._config.parameter_validator.extract_parameters(original_sql)
        target_styles = {p.style for p in param_info}
        if ParameterStyle.POSITIONAL_PYFORMAT in target_styles:
            processed_sql = self._config.parameter_converter._convert_sql_placeholders(
                processed_sql, param_info, ParameterStyle.POSITIONAL_PYFORMAT
            )
        elif ParameterStyle.NAMED_PYFORMAT in target_styles:
            processed_sql = self._config.parameter_converter._convert_sql_placeholders(
                processed_sql, param_info, ParameterStyle.NAMED_PYFORMAT
            )
            if (
                self._placeholder_mapping
                and result.context.merged_parameters
                and is_dict(result.context.merged_parameters)
            ):
                result.context.merged_parameters = self._denormalize_pyformat_params(result.context.merged_parameters)
        elif ParameterStyle.POSITIONAL_COLON in target_styles:
            processed_param_info = self._config.parameter_validator.extract_parameters(processed_sql)
            has_param_placeholders = any(p.name and p.name.startswith(PARAM_PREFIX) for p in processed_param_info)

            if not has_param_placeholders:
                processed_sql = self._config.parameter_converter._convert_sql_placeholders(
                    processed_sql, param_info, ParameterStyle.POSITIONAL_COLON
                )
            if (
                self._placeholder_mapping
                and result.context.merged_parameters
                and is_dict(result.context.merged_parameters)
            ):
                result.context.merged_parameters = self._denormalize_colon_params(result.context.merged_parameters)

        return processed_sql, result

    def _denormalize_colon_params(self, params: "dict[str, Any]") -> "dict[str, Any]":
        """Denormalize colon-style parameters back to numeric format."""
        # For positional colon style, all params should have numeric keys
        # Just return the params as-is if they already have the right format
        if all(key.isdigit() for key in params):
            return params

        # For positional colon, we need ALL parameters in the final result
        # This includes both user parameters and extracted literals
        # We should NOT filter out extracted parameters (param_0, param_1, etc)
        # because they need to be included in the final parameter conversion
        return params

    def _denormalize_pyformat_params(self, params: "dict[str, Any]") -> "dict[str, Any]":
        """Denormalize pyformat parameters back to their original names."""
        deconverted_params = {}
        for placeholder_key, original_name in self._placeholder_mapping.items():
            if placeholder_key in params:
                # For pyformat, the original_name is the actual parameter name (e.g., 'max_value')
                deconverted_params[str(original_name)] = params[placeholder_key]
        # Include any parameters that weren't converted
        non_converted_params = {key: value for key, value in params.items() if not key.startswith(PARAM_PREFIX)}
        deconverted_params.update(non_converted_params)
        return deconverted_params

    def _merge_pipeline_parameters(self, result: Any, final_params: Any) -> Any:
        """Merge parameters from the pipeline processing."""
        merged_params = result.context.merged_parameters

        # If we have extracted parameters from the pipeline, only merge them if:
        # 1. We don't already have parameters in merged_params, OR
        # 2. The original params were None and we need to use the extracted ones
        if result.context.extracted_parameters_from_pipeline:
            if merged_params is None:
                # No existing parameters - use the extracted ones
                merged_params = result.context.extracted_parameters_from_pipeline
            elif merged_params == final_params and final_params is None:
                # Both are None, use extracted parameters
                merged_params = result.context.extracted_parameters_from_pipeline
            elif merged_params != result.context.extracted_parameters_from_pipeline:
                # Only merge if the extracted parameters are different from what we already have
                # This prevents the duplication issue where the same parameters get added twice
                if is_dict(merged_params):
                    for i, param in enumerate(result.context.extracted_parameters_from_pipeline):
                        param_name = f"{PARAM_PREFIX}{i}"
                        merged_params[param_name] = param
                elif isinstance(merged_params, (list, tuple)):
                    # Only extend if we don't already have these parameters
                    # Convert to list and extend with extracted parameters
                    if isinstance(merged_params, tuple):
                        merged_params = list(merged_params)
                    merged_params.extend(result.context.extracted_parameters_from_pipeline)
                else:
                    # Single parameter case - convert to list with original + extracted
                    merged_params = [merged_params, *list(result.context.extracted_parameters_from_pipeline)]

        return merged_params

    def _finalize_processed_state(self, result: Any, processed_sql: str, merged_params: Any) -> None:
        """Finalize the processed state."""
        # Wrap parameters with type information if enabled
        if self._config.enable_parameter_type_wrapping and merged_params is not None:
            # Get parameter info from the processed SQL
            validator = self._config.parameter_validator
            param_info = validator.extract_parameters(processed_sql)

            # Wrap parameters with type information
            converter = self._config.parameter_converter
            merged_params = converter.wrap_parameters_with_types(merged_params, param_info)

        # Extract analyzer results from context metadata
        analysis_results = (
            {key: value for key, value in result.context.metadata.items() if key.endswith("Analyzer")}
            if result.context.metadata
            else {}
        )

        # If analyzer output handler is configured, call it with the analysis
        if self._config.analyzer_output_handler and analysis_results:
            # Create a structured analysis object from the metadata

            # Extract the main analyzer results
            analyzer_metadata = analysis_results.get("StatementAnalyzer", {})
            if analyzer_metadata:
                # Create a simplified analysis object for the handler
                analysis = {
                    "statement_type": analyzer_metadata.get("statement_type"),
                    "complexity_score": analyzer_metadata.get("complexity_score"),
                    "table_count": analyzer_metadata.get("table_count"),
                    "has_subqueries": analyzer_metadata.get("has_subqueries"),
                    "join_count": analyzer_metadata.get("join_count"),
                    "duration_ms": analyzer_metadata.get("duration_ms"),
                }
                self._config.analyzer_output_handler(analysis)

        self._processed_state = _ProcessedState(
            processed_expression=result.expression,
            processed_sql=processed_sql,
            merged_parameters=merged_params,
            validation_errors=list(result.context.validation_errors),
            analysis_results=analysis_results,
            transformation_results={},
        )

        if not self._config.parse_errors_as_warnings and self._processed_state.validation_errors:
            highest_risk_error = max(
                self._processed_state.validation_errors, key=lambda e: e.risk_level.value if has_risk_level(e) else 0
            )
            raise SQLValidationError(
                message=highest_risk_error.message,
                sql=self._raw_sql or processed_sql,
                risk_level=getattr(highest_risk_error, "risk_level", RiskLevel.HIGH),
            )

    def _to_expression(self, statement: "Union[str, exp.Expression]") -> exp.Expression:
        """Convert string to sqlglot expression."""
        if is_expression(statement):
            return statement

        if not statement or (isinstance(statement, str) and not statement.strip()):
            return exp.Select()

        if not self._config.enable_parsing:
            return exp.Anonymous(this=statement)

        if not isinstance(statement, str):
            return exp.Anonymous(this="")
        validator = self._config.parameter_validator
        param_info = validator.extract_parameters(statement)

        # Check if conversion is needed
        needs_conversion = any(p.style in SQLGLOT_INCOMPATIBLE_STYLES for p in param_info)

        converted_sql = statement
        placeholder_mapping: dict[str, Any] = {}

        if needs_conversion:
            converter = self._config.parameter_converter
            converted_sql, placeholder_mapping = converter._transform_sql_for_parsing(statement, param_info)
            self._original_sql = statement
            self._placeholder_mapping = placeholder_mapping

            # Create conversion state
            from sqlspec.statement.parameters import ParameterStyleTransformationState

            self._parameter_conversion_state = ParameterStyleTransformationState(
                was_transformed=True,
                original_styles=list({p.style for p in param_info}),
                transformation_style=ParameterStyle.NAMED_COLON,
                placeholder_map=placeholder_mapping,
                original_param_info=param_info,
            )
        else:
            self._parameter_conversion_state = None

        try:
            expressions = sqlglot.parse(converted_sql, dialect=self._dialect)  # pyright: ignore
            if not expressions:
                return exp.Anonymous(this=statement)
            first_expr = expressions[0]
            if first_expr is None:
                return exp.Anonymous(this=statement)

        except ParseError as e:
            if getattr(self._config, "parse_errors_as_warnings", False):
                logger.warning(
                    "Failed to parse SQL, returning Anonymous expression.", extra={"sql": statement, "error": str(e)}
                )
                return exp.Anonymous(this=statement)

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
        statement: "Optional[Union[str, exp.Expression]]" = None,
        parameters: "Optional[Any]" = None,
        dialect: "DialectType" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQL":
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
        existing_state["original_parameters"] = self._original_parameters

        new_statement = statement if statement is not None else self._statement
        new_dialect = dialect if dialect is not None else self._dialect
        new_config = config if config is not None else self._config

        if parameters is not None:
            existing_state["positional_params"] = []
            existing_state["named_params"] = {}
            return SQL(
                new_statement,
                parameters,
                _dialect=new_dialect,
                _config=new_config,
                _builder_result_type=self._builder_result_type,
                _existing_state=None,
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

    def add_named_parameter(self, name: "str", value: Any) -> "SQL":
        """Add a named parameter and return a new SQL instance."""
        new_obj = self.copy()
        new_obj._named_params[name] = value
        return new_obj

    def get_unique_parameter_name(
        self, base_name: "str", namespace: "Optional[str]" = None, preserve_original: bool = False
    ) -> str:
        """Generate a unique parameter name.

        Args:
            base_name: The base parameter name
            namespace: Optional namespace prefix (e.g., 'cte', 'subquery')
            preserve_original: If True, try to preserve the original name

        Returns:
            A unique parameter name
        """
        all_param_names = set(self._named_params.keys())

        candidate = f"{namespace}_{base_name}" if namespace else base_name

        if preserve_original and candidate not in all_param_names:
            return candidate

        if candidate not in all_param_names:
            return candidate

        counter = 1
        while True:
            new_candidate = f"{candidate}_{counter}"
            if new_candidate not in all_param_names:
                return new_candidate
            counter += 1

    def where(self, condition: "Union[str, exp.Expression, exp.Condition]") -> "SQL":
        """Apply WHERE clause and return new SQL instance."""
        condition_expr = self._to_expression(condition) if isinstance(condition, str) else condition

        if supports_where(self._statement):
            new_statement = self._statement.where(condition_expr)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).where(condition_expr)  # pyright: ignore

        return self.copy(statement=new_statement)

    def filter(self, filter_obj: StatementFilter) -> "SQL":
        """Apply a filter and return a new SQL instance."""
        new_obj = self.copy()
        new_obj._filters.append(filter_obj)
        pos_params, named_params = self._extract_filter_parameters(filter_obj)
        new_obj._positional_params.extend(pos_params)
        new_obj._named_params.update(named_params)
        return new_obj

    def as_many(self, parameters: "Optional[list[Any]]" = None) -> "SQL":
        """Mark for executemany with optional parameters."""
        new_obj = self.copy()
        new_obj._is_many = True
        if parameters is not None:
            new_obj._positional_params = []
            new_obj._named_params = {}
            new_obj._original_parameters = parameters
        return new_obj

    def as_script(self) -> "SQL":
        """Mark as script for execution."""
        new_obj = self.copy()
        new_obj._is_script = True
        return new_obj

    def _build_final_state(self) -> tuple[exp.Expression, Any]:
        """Build final expression and parameters after applying filters."""
        final_expr = self._statement

        # Accumulate parameters from both the original SQL and filters
        accumulated_positional = list(self._positional_params)
        accumulated_named = dict(self._named_params)

        for filter_obj in self._filters:
            if can_append_to_statement(filter_obj):
                temp_sql = SQL(final_expr, config=self._config, dialect=self._dialect)
                temp_sql._positional_params = list(accumulated_positional)
                temp_sql._named_params = dict(accumulated_named)
                result = filter_obj.append_to_statement(temp_sql)

                if isinstance(result, SQL):
                    # Extract the modified expression
                    final_expr = result._statement
                    # Also preserve any parameters added by the filter
                    accumulated_positional = list(result._positional_params)
                    accumulated_named = dict(result._named_params)
                else:
                    final_expr = result

        final_params: Any
        if accumulated_named and not accumulated_positional:
            final_params = dict(accumulated_named)
        elif accumulated_positional and not accumulated_named:
            final_params = list(accumulated_positional)
        elif accumulated_positional and accumulated_named:
            final_params = dict(accumulated_named)
            for i, param in enumerate(accumulated_positional):
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

        if self._is_script and self._raw_sql:
            return self._raw_sql
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql

        self._ensure_processed()
        if self._processed_state is None:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        return self._processed_state.processed_sql

    @property
    def config(self) -> "SQLConfig":
        """Get the SQL configuration."""
        return self._config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """Get the final expression."""
        if not self._config.enable_parsing:
            return None
        self._ensure_processed()
        if self._processed_state is None:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        return self._processed_state.processed_expression

    @property
    def parameters(self) -> Any:
        """Get merged parameters."""
        if self._is_many and self._original_parameters is not None:
            return self._original_parameters

        if (
            self._original_parameters is not None
            and isinstance(self._original_parameters, tuple)
            and not self._named_params
        ):
            return self._original_parameters

        self._ensure_processed()
        if self._processed_state is None:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
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
    def dialect(self) -> "Optional[DialectType]":
        """Get the SQL dialect."""
        return self._dialect

    def to_sql(self, placeholder_style: "Optional[str]" = None) -> "str":
        """Convert to SQL string with given placeholder style."""
        if self._is_script:
            return self.sql
        sql, _ = self.compile(placeholder_style=placeholder_style)
        return sql

    def get_parameters(self, style: "Optional[str]" = None) -> Any:
        """Get parameters in the requested style."""
        _, params = self.compile(placeholder_style=style)
        return params

    def _compile_execute_many(self, placeholder_style: "Optional[str]") -> "tuple[str, Any]":
        """Compile for execute_many operations.

        The pipeline processed the first parameter set to extract literals.
        Now we need to apply those extracted literals to all parameter sets.
        """
        sql = self.sql
        self._ensure_processed()

        # Get the original parameter sets
        param_sets = self._original_parameters or []

        # Get any literals extracted during pipeline processing
        if self._processed_state and self._processing_context:
            extracted_literals = self._processing_context.extracted_parameters_from_pipeline

            if extracted_literals:
                # Apply extracted literals to each parameter set
                enhanced_params: list[Any] = []
                for param_set in param_sets:
                    if isinstance(param_set, (list, tuple)):
                        # Add extracted literals to the parameter tuple
                        enhanced_set = list(param_set) + [
                            p.value if hasattr(p, "value") else p for p in extracted_literals
                        ]
                        enhanced_params.append(tuple(enhanced_set))
                    elif isinstance(param_set, dict):
                        # For dict params, add extracted literals with generated names
                        enhanced_dict = dict(param_set)
                        for i, literal in enumerate(extracted_literals):
                            param_name = f"_literal_{i}"
                            enhanced_dict[param_name] = literal.value if hasattr(literal, "value") else literal
                        enhanced_params.append(enhanced_dict)
                    else:
                        # Single parameter - convert to tuple with literals
                        literals = [p.value if hasattr(p, "value") else p for p in extracted_literals]
                        enhanced_params.append((param_set, *literals))
                param_sets = enhanced_params

        if placeholder_style:
            sql, param_sets = self._convert_placeholder_style(sql, param_sets, placeholder_style)

        return sql, param_sets

    def _get_extracted_parameters(self) -> "list[Any]":
        """Get extracted parameters from pipeline processing."""
        extracted_params = []
        if self._processed_state and self._processed_state.merged_parameters:
            merged = self._processed_state.merged_parameters
            if isinstance(merged, list):
                if merged and not isinstance(merged[0], (tuple, list)):
                    extracted_params = merged
            elif self._processing_context and self._processing_context.extracted_parameters_from_pipeline:
                extracted_params = self._processing_context.extracted_parameters_from_pipeline
        return extracted_params

    def _merge_extracted_params_with_sets(self, params: Any, extracted_params: "list[Any]") -> "list[tuple[Any, ...]]":
        """Merge extracted parameters with each parameter set."""
        enhanced_params = []
        for param_set in params:
            if isinstance(param_set, (list, tuple)):
                extracted_values = []
                for extracted in extracted_params:
                    if has_parameter_value(extracted):
                        extracted_values.append(extracted.value)
                    else:
                        extracted_values.append(extracted)
                enhanced_set = list(param_set) + extracted_values
                enhanced_params.append(tuple(enhanced_set))
            else:
                extracted_values = []
                for extracted in extracted_params:
                    if has_parameter_value(extracted):
                        extracted_values.append(extracted.value)
                    else:
                        extracted_values.append(extracted)
                enhanced_params.append((param_set, *extracted_values))
        return enhanced_params

    def compile(self, placeholder_style: "Optional[str]" = None) -> "tuple[str, Any]":
        """Compile to SQL and parameters."""
        if self._is_script:
            return self.sql, None

        if self._is_many and self._original_parameters is not None:
            return self._compile_execute_many(placeholder_style)

        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql, self._raw_parameters

        self._ensure_processed()

        if self._processed_state is None:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        sql = self._processed_state.processed_sql
        params = self._processed_state.merged_parameters

        if params is not None and self._processing_context:
            parameter_mapping = self._processing_context.metadata.get("parameter_position_mapping")
            if parameter_mapping:
                params = self._reorder_parameters(params, parameter_mapping)

        # Handle deconversion if needed
        if self._processing_context and self._processing_context.parameter_conversion:
            norm_state = self._processing_context.parameter_conversion

            # If original SQL had incompatible styles, denormalize back to the original style
            # when no specific style requested OR when the requested style matches the original
            if norm_state.was_transformed and norm_state.original_styles:
                original_style = norm_state.original_styles[0]
                should_denormalize = placeholder_style is None or (
                    placeholder_style and ParameterStyle(placeholder_style) == original_style
                )

                if should_denormalize and original_style in SQLGLOT_INCOMPATIBLE_STYLES:
                    # Denormalize SQL back to original style
                    sql = self._config.parameter_converter._convert_sql_placeholders(
                        sql, norm_state.original_param_info, original_style
                    )
                    # Also deConvert parameters if needed
                    if original_style == ParameterStyle.POSITIONAL_COLON and is_dict(params):
                        params = self._denormalize_colon_params(params)

        params = self._unwrap_typed_parameters(params)

        if placeholder_style is None:
            return sql, params

        if placeholder_style:
            sql, params = self._apply_placeholder_style(sql, params, placeholder_style)

        return sql, params

    def _apply_placeholder_style(self, sql: "str", params: Any, placeholder_style: "str") -> "tuple[str, Any]":
        """Apply placeholder style conversion to SQL and parameters."""
        # Just use the params passed in - they've already been processed
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

        if is_dict(params):
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
                    reordered_list[i] = params[i]  # pyright: ignore

            return tuple(reordered_list) if isinstance(params, tuple) else reordered_list

        if is_dict(params):
            if all(key.startswith(PARAM_PREFIX) and key[len(PARAM_PREFIX) :].isdigit() for key in params):
                reordered_dict: dict[str, Any] = {}
                for new_pos, old_pos in mapping.items():
                    old_key = f"{PARAM_PREFIX}{old_pos}"
                    new_key = f"{PARAM_PREFIX}{new_pos}"
                    if old_key in params:
                        reordered_dict[new_key] = params[old_key]

                for key, value in params.items():
                    if key not in reordered_dict and key.startswith(PARAM_PREFIX):
                        idx = int(key[6:])
                        if idx not in mapping:
                            reordered_dict[key] = value

                return reordered_dict
            return params
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
                target_style = (
                    ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
                )
                sql = self._replace_placeholders_in_sql(sql, param_info, target_style)

            return sql, params

        converter = self._config.parameter_converter

        # For POSITIONAL_COLON style, use original parameter info if available to preserve numeric identifiers
        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
        if (
            target_style == ParameterStyle.POSITIONAL_COLON
            and self._processing_context
            and self._processing_context.parameter_conversion
            and self._processing_context.parameter_conversion.original_param_info
        ):
            param_info = self._processing_context.parameter_conversion.original_param_info
        else:
            param_info = converter.validator.extract_parameters(sql)

        # CRITICAL FIX: For POSITIONAL_COLON, we need to ensure param_info reflects
        # all placeholders in the current SQL, not just the original ones.
        # This handles cases where transformers (like ParameterizeLiterals) add new placeholders.
        if target_style == ParameterStyle.POSITIONAL_COLON and param_info:
            # Re-extract from current SQL to get all placeholders
            current_param_info = converter.validator.extract_parameters(sql)
            if len(current_param_info) > len(param_info):
                # More placeholders in current SQL means transformers added some
                # Use the current info to ensure all placeholders get parameters
                param_info = current_param_info

        if not param_info:
            return sql, params

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
        if is_dict(params):
            for p in param_info:
                if p.name and p.name in params:
                    param_list.append(params[p.name])
                elif f"{PARAM_PREFIX}{p.ordinal}" in params:
                    param_list.append(params[f"{PARAM_PREFIX}{p.ordinal}"])
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
            return f"${param.ordinal + 1}"
        if target_style == ParameterStyle.NAMED_COLON:
            if param.name and not param.name.isdigit():
                return f":{param.name}"
            return f":arg_{param.ordinal}"
        if target_style == ParameterStyle.NAMED_AT:
            return f"@{param.name or f'param_{param.ordinal}'}"
        if target_style == ParameterStyle.POSITIONAL_COLON:
            # For Oracle positional colon, preserve the original numeric identifier if it was already :N style
            if (
                hasattr(param, "style")
                and param.style == ParameterStyle.POSITIONAL_COLON
                and hasattr(param, "name")
                and param.name
                and param.name.isdigit()
            ):
                return f":{param.name}"
            return f":{param.ordinal + 1}"
        if target_style == ParameterStyle.POSITIONAL_PYFORMAT:
            return "%s"
        if target_style == ParameterStyle.NAMED_PYFORMAT:
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

    def _convert_list_to_colon_dict(
        self, params: "Union[list[Any], tuple[Any, ...]]", param_info: "list[Any]"
    ) -> "dict[str, Any]":
        """Convert list/tuple parameters to colon-style dict format."""
        result_dict: dict[str, Any] = {}

        if param_info:
            all_numeric = all(p.name and p.name.isdigit() for p in param_info)
            if all_numeric:
                for i, value in enumerate(params):
                    result_dict[str(i + 1)] = value
            else:
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

    def _convert_single_value_to_colon_dict(self, params: Any, param_info: "list[Any]") -> "dict[str, Any]":
        """Convert single value parameter to colon-style dict format."""
        result_dict: dict[str, Any] = {}
        if param_info and param_info[0].name and param_info[0].name.isdigit():
            result_dict[param_info[0].name] = params
        else:
            result_dict["1"] = params
        return result_dict

    def _process_mixed_colon_params(self, params: "dict[str, Any]", param_info: "list[Any]") -> "dict[str, Any]":
        """Process mixed colon-style numeric and converted parameters."""
        result_dict: dict[str, Any] = {}

        # When we have mixed parameters (extracted literals + user oracle params),
        # we need to be careful about the ordering. The extracted literals should
        # fill positions based on where they appear in the SQL, not based on
        # matching parameter names.

        # Separate extracted parameters and user oracle parameters
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

        extracted_keys_sorted.sort(key=operator.itemgetter(0))
        for _, key, value in extracted_keys_sorted:
            extracted_params.append((key, value))

        # Build lists of parameter values in order
        extracted_values = []
        for _, value in extracted_params:
            if has_parameter_value(value):
                extracted_values.append(value.value)
            else:
                extracted_values.append(value)

        user_values = [user_oracle_params[key] for key in sorted(user_oracle_params.keys(), key=int)]

        # Now assign parameters based on position
        # Extracted parameters go first (they were literals in original positions)
        # User parameters follow
        all_values = extracted_values + user_values

        for i, p in enumerate(sorted(param_info, key=lambda x: x.ordinal)):
            oracle_key = str(p.ordinal + 1)
            if i < len(all_values):
                result_dict[oracle_key] = all_values[i]

        return result_dict

    def _convert_to_positional_colon_format(self, params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for positional colon style.

        Positional colon style uses :1, :2, etc. placeholders and expects
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
            return self._convert_list_to_colon_dict(params, param_info)

        if not is_dict(params) and param_info:
            return self._convert_single_value_to_colon_dict(params, param_info)

        if is_dict(params):
            if all(key.isdigit() for key in params):
                return params

            if all(key.startswith("param_") for key in params):
                param_result_dict: dict[str, Any] = {}
                for p in sorted(param_info, key=lambda x: x.ordinal):
                    # Use the parameter's ordinal to find the converted key
                    converted_key = f"param_{p.ordinal}"
                    if converted_key in params:
                        if p.name and p.name.isdigit():
                            # For Oracle numeric parameters, preserve the original number
                            param_result_dict[p.name] = params[converted_key]
                        else:
                            # For other cases, use sequential numbering
                            param_result_dict[str(p.ordinal + 1)] = params[converted_key]
                return param_result_dict

            has_oracle_numeric = any(key.isdigit() for key in params)
            has_param_converted = any(key.startswith("param_") for key in params)
            has_typed_params = any(has_parameter_value(v) for v in params.values())

            if (has_oracle_numeric and has_param_converted) or has_typed_params:
                return self._process_mixed_colon_params(params, param_info)

            result_dict: dict[str, Any] = {}

            if param_info:
                # Process all parameters in order of their ordinals
                for p in sorted(param_info, key=lambda x: x.ordinal):
                    oracle_key = str(p.ordinal + 1)
                    value = None

                    # Try different ways to find the parameter value
                    if p.name and (
                        p.name in params
                        or (p.name.isdigit() and p.name in params)
                        or (p.name.startswith("param_") and p.name in params)
                    ):
                        value = params[p.name]

                    # If not found by name, try by ordinal-based keys
                    if value is None:
                        # Try param_N format (common for pipeline parameters)
                        param_key = f"param_{p.ordinal}"
                        if param_key in params:
                            value = params[param_key]
                        # Try arg_N format
                        elif f"arg_{p.ordinal}" in params:
                            value = params[f"arg_{p.ordinal}"]
                        # For positional colon, also check if there's a numeric key
                        # that matches the ordinal position
                        elif str(p.ordinal + 1) in params:
                            value = params[str(p.ordinal + 1)]

                    # Unwrap TypedParameter if needed
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
            param_values_by_ordinal: dict[int, Any] = {}

            for p in param_info:
                if p.name and p.name in params:
                    param_values_by_ordinal[p.ordinal] = params[p.name]

            for p in param_info:
                if p.name is None and p.ordinal not in param_values_by_ordinal:
                    arg_key = f"arg_{p.ordinal}"
                    param_key = f"param_{p.ordinal}"
                    if arg_key in params:
                        param_values_by_ordinal[p.ordinal] = params[arg_key]
                    elif param_key in params:
                        param_values_by_ordinal[p.ordinal] = params[param_key]

            remaining_params = {
                k: v
                for k, v in params.items()
                if k not in {p.name for p in param_info if p.name} and not k.startswith(("arg_", "param_"))
            }

            unmatched_ordinals = [p.ordinal for p in param_info if p.ordinal not in param_values_by_ordinal]

            for ordinal, (_, value) in zip(unmatched_ordinals, remaining_params.items()):
                param_values_by_ordinal[ordinal] = value

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
            # Special case: if params is empty, preserve it (don't create None values)
            # This is important for execute_many with empty parameter lists
            if not params:
                return params

            # Handle mixed parameter styles correctly
            # For mixed styles, assign parameters in order of appearance, not by numeric reference
            if param_info and any(p.style == ParameterStyle.NUMERIC for p in param_info):
                # Create mapping from ordinal to parameter value
                param_mapping: dict[int, Any] = {}

                # Sort parameter info by position to get order of appearance
                sorted_params = sorted(param_info, key=lambda p: p.position)

                # Assign parameters sequentially in order of appearance
                for i, param_info_item in enumerate(sorted_params):
                    if i < len(params):
                        param_mapping[param_info_item.ordinal] = params[i]

                # Build result list ordered by original ordinal values
                for i in range(len(param_info)):
                    val = param_mapping.get(i)
                    if val is not None:
                        if has_parameter_value(val):
                            result_list.append(val.value)
                        else:
                            result_list.append(val)
                    else:
                        result_list.append(None)

                return result_list

            # Standard conversion for non-mixed styles
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
                    result_dict[p.name or f"arg_{p.ordinal}"] = params[f"param_{p.ordinal}"]
            return result_dict
        if isinstance(params, (list, tuple)):
            for i, value in enumerate(params):
                if has_parameter_value(value):
                    value = value.value

                if i < len(param_info):
                    p = param_info[i]
                    param_name = p.name or f"arg_{i}"
                    result_dict[param_name] = value
                else:
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
        if not self._processed_state:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
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

        Returns the original parameter info before any conversion.
        """
        validator = self._config.parameter_validator
        if self._raw_sql:
            return validator.extract_parameters(self._raw_sql)

        self._ensure_processed()

        if self._processing_context:
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
    def _expression(self) -> "Optional[exp.Expression]":
        """Get expression for compatibility."""
        return self.expression

    @property
    def statement(self) -> exp.Expression:
        """Get statement for compatibility."""
        return self._statement
