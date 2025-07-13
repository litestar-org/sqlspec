"""SQL statement handling with centralized parameter management."""

import operator
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from typing_extensions import TypeAlias

from sqlspec.exceptions import RiskLevel, SQLParsingError, SQLValidationError
from sqlspec.statement.cache import ast_fragment_cache, base_statement_cache, sql_cache
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.parameters import (
    SQLGLOT_INCOMPATIBLE_STYLES,
    ParameterConverter,
    ParameterStyle,
    ParameterValidator,
)
from sqlspec.statement.pipeline import (
    SQLTransformContext,
    compose_pipeline,
    normalize_step,
    optimize_step,
    parameterize_literals_step,
    remove_comments_step,
    validate_dml_safety_step,
    validate_parameter_style_step,
    validate_step,
)
from sqlspec.typing import Empty
from sqlspec.utils.logging import get_logger
from sqlspec.utils.statement_hashing import hash_sql_statement
from sqlspec.utils.type_guards import (
    can_append_to_statement,
    can_extract_parameters,
    expression_has_limit,
    get_param_style_and_name,
    get_value_attribute,
    has_parameter_value,
    has_risk_level,
    is_dict,
    is_expression,
    is_statement_filter,
    supports_where,
)

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.parameters import ParameterStyleConversionState

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
PROCESSED_STATE_SLOTS = (
    "analysis_results",
    "merged_parameters",
    "processed_expression",
    "processed_sql",
    "transformation_results",
    "validation_errors",
)
# SQLConfig slots definition for mypyc compatibility
SQL_CONFIG_SLOTS = (
    "allow_mixed_parameter_styles",
    "allowed_parameter_styles",
    "analyzer_output_handler",
    "analyzers",
    "custom_pipeline_steps",
    "default_parameter_style",
    "dialect",
    "enable_analysis",
    "enable_caching",
    "enable_expression_simplification",
    "enable_parameter_type_wrapping",
    "enable_parsing",
    "enable_transformations",
    "enable_validation",
    "input_sql_had_placeholders",
    "parameter_converter",
    "parameter_validator",
    "parse_errors_as_warnings",
    "transformers",
    "validators",
)


class _ProcessedState:
    """Cached state from pipeline processing."""

    __slots__ = PROCESSED_STATE_SLOTS

    def __hash__(self) -> int:
        """Hash based on processed SQL and expression."""
        return hash(
            (
                self.processed_sql,
                str(self.processed_expression),  # Convert expression to string for hashing
                len(self.validation_errors) if self.validation_errors else 0,
            )
        )

    def __init__(
        self,
        processed_expression: exp.Expression,
        processed_sql: str,
        merged_parameters: Any,
        validation_errors: "Optional[list[Any]]" = None,
        analysis_results: "Optional[dict[str, Any]]" = None,
        transformation_results: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.processed_expression = processed_expression
        self.processed_sql = processed_sql
        self.merged_parameters = merged_parameters
        self.validation_errors = validation_errors or []
        self.analysis_results = analysis_results or {}
        self.transformation_results = transformation_results or {}

    def replace(self, **changes: Any) -> "_ProcessedState":
        """Create a new _ProcessedState with specified changes."""
        # Validate that all changes correspond to valid slots

        for key in changes:
            if key not in PROCESSED_STATE_SLOTS:
                msg = f"{key!r} is not a field in {type(self).__name__}"
                raise TypeError(msg)

        # Build the keyword arguments for the new instance
        kwargs = {slot: getattr(self, slot) for slot in PROCESSED_STATE_SLOTS}
        kwargs.update(changes)

        return type(self)(**kwargs)

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        field_strs = []
        for slot in PROCESSED_STATE_SLOTS:
            value = getattr(self, slot)
            field_strs.append(f"{slot}={value!r}")
        return f"{self.__class__.__name__}({', '.join(field_strs)})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison compatible with dataclass.__eq__."""
        if not isinstance(other, type(self)):
            return False
        # mypyc removes __slots__ at runtime, so we hardcode the comparison
        return (
            self.processed_expression == other.processed_expression
            and self.processed_sql == other.processed_sql
            and self.merged_parameters == other.merged_parameters
            and self.validation_errors == other.validation_errors
            and self.analysis_results == other.analysis_results
            and self.transformation_results == other.transformation_results
        )


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

    __slots__ = SQL_CONFIG_SLOTS

    def __hash__(self) -> int:
        """Hash based on key configuration settings."""
        return hash(
            (
                self.enable_parsing,
                self.enable_validation,
                self.enable_transformations,
                self.enable_analysis,
                self.enable_expression_simplification,
                self.enable_parameter_type_wrapping,
                self.enable_caching,
                self.dialect,
                self.default_parameter_style,
                tuple(self.allowed_parameter_styles) if self.allowed_parameter_styles else None,
            )
        )

    def __init__(
        self,
        enable_parsing: bool = True,
        enable_validation: bool = True,
        enable_transformations: bool = True,
        enable_analysis: bool = False,
        enable_expression_simplification: bool = False,
        enable_parameter_type_wrapping: bool = True,
        parse_errors_as_warnings: bool = True,
        enable_caching: bool = True,
        transformers: "Optional[list[Any]]" = None,
        validators: "Optional[list[Any]]" = None,
        analyzers: "Optional[list[Any]]" = None,
        parameter_converter: "Optional[ParameterConverter]" = None,
        parameter_validator: "Optional[ParameterValidator]" = None,
        input_sql_had_placeholders: bool = False,
        dialect: "Optional[DialectType]" = None,
        allowed_parameter_styles: "Optional[tuple[str, ...]]" = None,
        default_parameter_style: "Optional[str]" = None,
        allow_mixed_parameter_styles: bool = False,
        analyzer_output_handler: "Optional[Callable[[Any], None]]" = None,
        custom_pipeline_steps: "Optional[list[Any]]" = None,
    ) -> None:
        self.enable_parsing = enable_parsing
        self.enable_validation = enable_validation
        self.enable_transformations = enable_transformations
        self.enable_analysis = enable_analysis
        self.enable_expression_simplification = enable_expression_simplification
        self.enable_parameter_type_wrapping = enable_parameter_type_wrapping
        self.parse_errors_as_warnings = parse_errors_as_warnings
        self.enable_caching = enable_caching
        self.transformers = transformers
        self.validators = validators
        self.analyzers = analyzers
        self.parameter_converter = parameter_converter or ParameterConverter()
        self.parameter_validator = parameter_validator or ParameterValidator()
        self.input_sql_had_placeholders = input_sql_had_placeholders
        self.dialect = dialect
        self.allowed_parameter_styles = allowed_parameter_styles
        self.default_parameter_style = default_parameter_style
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
        self.analyzer_output_handler = analyzer_output_handler
        self.custom_pipeline_steps = custom_pipeline_steps

    def replace(self, **changes: Any) -> "SQLConfig":
        """Create a new SQLConfig with specified changes.

        This replaces the dataclass replace() functionality.
        """
        # Validate that all changes correspond to valid slots
        for key in changes:
            if key not in SQL_CONFIG_SLOTS:
                msg = f"{key!r} is not a field in {type(self).__name__}"
                raise TypeError(msg)

        # Build the keyword arguments for the new instance
        kwargs = {slot: getattr(self, slot) for slot in SQL_CONFIG_SLOTS}
        kwargs.update(changes)

        return type(self)(**kwargs)

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        field_strs = []
        for slot in SQL_CONFIG_SLOTS:
            value = getattr(self, slot)
            field_strs.append(f"{slot}={value!r}")
        return f"{self.__class__.__name__}({', '.join(field_strs)})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison compatible with dataclass.__eq__."""
        if not isinstance(other, type(self)):
            return False
        return all(getattr(self, slot) == getattr(other, slot) for slot in SQL_CONFIG_SLOTS)

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

    def get_pipeline_steps(self) -> list:
        """Get the configured pipeline steps.

        Returns:
            List of pipeline steps to execute
        """
        steps = []

        # Add custom pipeline steps first (e.g., ADBC transformer)
        if self.custom_pipeline_steps:
            steps.extend(self.custom_pipeline_steps)

        if self.enable_transformations:
            steps.extend([remove_comments_step, normalize_step, parameterize_literals_step])
            if self.enable_expression_simplification:
                steps.append(optimize_step)

        if self.enable_validation:
            steps.extend([validate_dml_safety_step, validate_parameter_style_step, validate_step])

        return steps


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
        "_base_statement_key",
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
        self._dialect = self._normalize_dialect(_dialect or self._config.dialect)
        self._builder_result_type = _builder_result_type
        self._processed_state: Any = Empty  # Use Any to avoid mypyc Optional issues
        self._processing_context: Optional[SQLTransformContext] = None
        self._positional_params: list[Any] = []
        self._named_params: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression
        self._raw_sql: str = ""
        self._original_parameters: Any = None
        self._original_sql: str = ""
        self._placeholder_mapping: dict[str, Union[str, int]] = {}
        self._parameter_conversion_state: Optional[ParameterStyleConversionState] = None
        self._is_many: bool = False
        self._is_script: bool = False
        self._base_statement_key: Optional[tuple[str, str]] = None

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement, _dialect, _config or SQLConfig(), _builder_result_type)
        else:
            self._init_from_str_or_expression(statement)

        if _existing_state:
            self._load_from_existing_state(_existing_state)

        if not isinstance(statement, SQL) and not _existing_state:
            self._set_original_parameters(*parameters)

        self._process_parameters(*parameters, **kwargs)

    @staticmethod
    def _normalize_dialect(dialect: "DialectType") -> "Optional[str]":
        """Normalize dialect to string representation."""
        if dialect is None:
            return None
        if isinstance(dialect, str):
            return dialect
        try:
            return dialect.__class__.__name__.lower()
        except AttributeError:
            return str(dialect)

    def _init_from_sql_object(
        self, statement: "SQL", dialect: "DialectType", config: "SQLConfig", builder_result_type: "Optional[type]"
    ) -> None:
        """Initialize from an existing SQL object."""
        self._statement = statement._statement
        self._dialect = self._normalize_dialect(dialect or statement._dialect)
        self._config = config or statement._config
        self._builder_result_type = builder_result_type or statement._builder_result_type
        self._is_many = statement._is_many
        self._is_script = statement._is_script
        self._raw_sql = statement._raw_sql
        self._original_parameters = statement._original_parameters
        self._original_sql = statement._original_sql
        self._placeholder_mapping = statement._placeholder_mapping.copy()
        self._parameter_conversion_state = statement._parameter_conversion_state
        self._base_statement_key = statement._base_statement_key
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
        # Check if already processed
        if self._processed_state is not Empty:
            return

        # Check cache first if caching is enabled
        cache_key = None
        if self._config.enable_caching:
            cache_key = self._cache_key()
            cached_state = sql_cache.get(cache_key)

            if cached_state is not None:
                # Cast to work around mypyc Optional type checking
                self._processed_state = cast("_ProcessedState", cached_state)
                return

        final_expr, final_params = self._build_final_state()
        has_placeholders = self._detect_placeholders()
        initial_sql_for_context, final_params = self._prepare_context_sql(final_expr, final_params)

        context = self._create_processing_context(initial_sql_for_context, final_expr, final_params, has_placeholders)
        original_context = context  # Keep a reference to the original context
        result_context = self._run_pipeline(context)

        processed_sql, merged_params = self._process_pipeline_result(result_context, final_params, original_context)

        self._finalize_processed_state(result_context, processed_sql, merged_params)

        # Store in cache if caching is enabled
        if self._config.enable_caching and cache_key is not None:
            # We know _processed_state is not Empty after _finalize_processed_state
            # Use cast to work around mypyc type checking
            sql_cache.set(cache_key, cast("_ProcessedState", self._processed_state))

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
    ) -> SQLTransformContext:
        """Create SQL processing context."""
        # Convert parameters to dict format for the context
        param_dict = {}
        param_info = None  # Initialize to avoid unbound variable
        if final_params:
            if is_dict(final_params):
                param_dict = final_params
            elif isinstance(final_params, (list, tuple)):
                # Extract parameter info to check if we have numeric placeholders
                validator = self._config.parameter_validator
                param_info = validator.extract_parameters(initial_sql_for_context)

                # Check if SQL has numeric placeholders ($1, $2, etc.)
                has_numeric_placeholders = any(p.style == ParameterStyle.NUMERIC for p in param_info)

                if has_numeric_placeholders:
                    # For numeric placeholders, map parameters by their numeric value
                    # $1 -> key "1", $2 -> key "2", etc.
                    for i, param in enumerate(final_params):
                        param_dict[str(i + 1)] = param
                else:
                    # For other styles, use param_0, param_1, etc.
                    for i, param in enumerate(final_params):
                        param_dict[f"param_{i}"] = param
            else:
                param_dict["param_0"] = final_params

        context = SQLTransformContext(
            current_expression=final_expr,
            original_expression=final_expr,
            parameters=param_dict,
            dialect=str(self._dialect or self._config.dialect or ""),
        )

        # Store additional metadata
        context.metadata["initial_sql"] = initial_sql_for_context
        context.metadata["has_placeholders"] = has_placeholders or self._config.input_sql_had_placeholders

        if self._placeholder_mapping:
            context.metadata["placeholder_map"] = self._placeholder_mapping

        # Store parameter conversion state
        if self._parameter_conversion_state:
            context.metadata["parameter_conversion"] = self._parameter_conversion_state

        # Extract and store parameter info (reuse if already extracted above)
        if final_params and isinstance(final_params, (list, tuple)) and param_info is not None:
            # We already extracted it above
            context.metadata["parameter_info"] = param_info
        else:
            validator = self._config.parameter_validator
            context.metadata["parameter_info"] = validator.extract_parameters(initial_sql_for_context)

        return context

    def _run_pipeline(self, context: SQLTransformContext) -> SQLTransformContext:
        """Run the SQL processing pipeline."""
        steps = self._config.get_pipeline_steps()

        if steps:
            pipeline = compose_pipeline(steps)
            context = pipeline(context)

        # Store the context for later reference
        self._processing_context = context
        return context

    def _process_pipeline_result(
        self, context: SQLTransformContext, final_params: Any, original_context: SQLTransformContext
    ) -> tuple[str, Any]:
        """Process the result from the pipeline."""
        processed_expr = context.current_expression

        if isinstance(processed_expr, exp.Anonymous):
            processed_sql = self._raw_sql or context.metadata.get("initial_sql", "")
        else:
            # Use the initial expression that includes filters, not the processed one
            # The processed expression may have lost LIMIT/OFFSET during pipeline processing
            initial_expr = original_context.original_expression
            if initial_expr and initial_expr != processed_expr:
                # Check if LIMIT/OFFSET was stripped during processing
                has_limit_in_initial = expression_has_limit(initial_expr)
                has_limit_in_processed = expression_has_limit(processed_expr)

                if has_limit_in_initial and not has_limit_in_processed:
                    # Restore LIMIT/OFFSET from initial expression
                    processed_expr = initial_expr

            processed_sql = (
                processed_expr.sql(dialect=self._dialect or self._config.dialect, comments=False)
                if processed_expr
                else ""
            )
            if self._placeholder_mapping and self._original_sql:
                processed_sql, context = self._denormalize_sql(processed_sql, context)

        merged_params = self._merge_pipeline_parameters(context, final_params)

        return processed_sql, merged_params

    def _denormalize_sql(self, processed_sql: str, context: SQLTransformContext) -> tuple[str, SQLTransformContext]:
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
            if self._placeholder_mapping and context.parameters and is_dict(context.parameters):
                # For mypyc: create new variable after type narrowing
                dict_params = context.parameters  # Type narrowed to dict[str, Any]
                context.parameters = self._denormalize_pyformat_params(dict_params)
        elif ParameterStyle.POSITIONAL_COLON in target_styles:
            processed_param_info = self._config.parameter_validator.extract_parameters(processed_sql)
            has_param_placeholders = any(p.name and p.name.startswith(PARAM_PREFIX) for p in processed_param_info)

            if not has_param_placeholders:
                processed_sql = self._config.parameter_converter._convert_sql_placeholders(
                    processed_sql, param_info, ParameterStyle.POSITIONAL_COLON
                )
            if self._placeholder_mapping and context.parameters and is_dict(context.parameters):
                # For mypyc: create new variable after type narrowing
                dict_params = context.parameters  # Type narrowed to dict[str, Any]
                context.parameters = self._denormalize_colon_params(dict_params)

        return processed_sql, context

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

    def _merge_pipeline_parameters(self, context: SQLTransformContext, final_params: Any) -> Any:
        """Merge parameters from the pipeline processing."""
        # Get parameters from context in appropriate format
        merged_params = context.merged_parameters

        # Check if we have extracted literals from parameterize_literals_step
        if context.metadata.get("literals_parameterized") and context.parameters:
            # Count how many parameters were there before literal extraction
            original_param_count = 0
            for key in context.parameters:
                if not key.startswith("param_") or not key[6:].isdigit():
                    original_param_count += 1

            # If we started with no parameters, use the extracted ones
            if final_params is None and original_param_count < len(context.parameters):
                # We have extracted literals, return them in the appropriate format
                if self._config.dialect in {"mysql", "sqlite"}:
                    # Return as list for positional parameter styles
                    if isinstance(context.parameters, dict):
                        return list(context.parameters.values())
                    if isinstance(context.parameters, (list, tuple)):
                        return list(context.parameters)
                    return []
                # Return as dict for named parameter styles
                if isinstance(context.parameters, dict):
                    return dict(context.parameters)
                return {}

        # If we had original parameters, they should already be in the context
        return merged_params

    def _finalize_processed_state(self, context: SQLTransformContext, processed_sql: str, merged_params: Any) -> None:
        """Finalize the processed state."""
        # Wrap parameters with type information if enabled
        if self._config.enable_parameter_type_wrapping and merged_params is not None:
            # Get parameter info from the processed SQL
            validator = self._config.parameter_validator
            param_info = validator.extract_parameters(processed_sql)

            # Check if literals were parameterized - if so, force wrapping
            literals_parameterized = (
                context.metadata.get("literals_parameterized", False) if context.metadata else False
            )

            # Wrap parameters with type information
            converter = self._config.parameter_converter
            merged_params = converter.wrap_parameters_with_types(merged_params, param_info, literals_parameterized)

        # Extract analyzer results from context metadata
        analysis_results = (
            {key: value for key, value in context.metadata.items() if key.endswith("Analyzer")}
            if context.metadata
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

        # Get validation errors from metadata
        validation_errors = []
        if context.metadata.get("validation_issues"):
            validation_errors.extend(context.metadata["validation_issues"])
        if context.metadata.get("validation_warnings"):
            validation_errors.extend(context.metadata["validation_warnings"])

        self._processed_state = _ProcessedState(
            processed_expression=context.current_expression,
            processed_sql=processed_sql,
            merged_parameters=merged_params,
            validation_errors=validation_errors,
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
                # Use try/except for mypyc compatibility
                risk_level=highest_risk_error.risk_level if highest_risk_error else RiskLevel.HIGH,
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
            from sqlspec.statement.parameters import ParameterStyleConversionState

            self._parameter_conversion_state = ParameterStyleConversionState(
                was_transformed=True,
                original_styles=list({p.style for p in param_info}),
                transformation_style=ParameterStyle.NAMED_COLON,
                placeholder_map=placeholder_mapping,
                original_param_info=param_info,
            )
        else:
            self._parameter_conversion_state = None

        # Check if this is a base statement without parameters
        # Only use base_statement_cache for raw SQL without any parameters
        use_base_cache = not needs_conversion and not param_info and isinstance(statement, str)

        if use_base_cache:
            # Set the base statement key for later caching operations
            self._base_statement_key = (statement.strip(), str(self._dialect) if self._dialect else "default")
            try:
                # Use base_statement_cache for huge performance boost
                return base_statement_cache.get_or_parse(statement, str(self._dialect) if self._dialect else None)
            except ParseError:
                # Let the error handling below take care of this
                pass
        else:
            # Try to get from AST cache first
            cached_expr = ast_fragment_cache.parse_with_cache(
                converted_sql, fragment_type="QUERY", dialect=self._dialect
            )

            if cached_expr:
                return cached_expr

        # Fall back to regular parsing if not cached
        try:
            expressions = sqlglot.parse(converted_sql, dialect=self._dialect)  # pyright: ignore
            if not expressions:
                return exp.Anonymous(this=statement)
            first_expr = expressions[0]
            if first_expr is None:
                return exp.Anonymous(this=statement)

            # Only cache in ast_fragment_cache if not using base_statement_cache
            if not use_base_cache:
                # Cache the successfully parsed expression
                ast_fragment_cache.set_fragment(
                    sql=converted_sql,
                    expression=first_expr,
                    fragment_type="QUERY",
                    dialect=self._dialect,
                    parameter_count=len(param_info),
                )

        except ParseError as e:
            # Use direct attribute access for mypyc compatibility
            if self._config.parse_errors_as_warnings:
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
        """Get SQL string with default QMARK placeholder style."""
        if not self._raw_sql or (self._raw_sql and not self._raw_sql.strip()):
            return ""

        if self._is_script and self._raw_sql:
            return self._raw_sql
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql

        # For execute_many, avoid recursion by using processed SQL directly
        if self._is_many:
            self._ensure_processed()
            if self._processed_state is Empty:
                msg = "Failed to process SQL statement"
                raise RuntimeError(msg)
            # Apply QMARK style conversion to the processed SQL
            processed_sql = cast("_ProcessedState", self._processed_state).processed_sql
            processed_params = cast("_ProcessedState", self._processed_state).merged_parameters
            if processed_params:
                sql, _ = self._apply_placeholder_style(processed_sql, processed_params, "qmark")
                return sql
            return processed_sql

        # Check if literals were parameterized - if so, use QMARK style for consistency
        self._ensure_processed()
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)

        # If literals were parameterized AND new parameters were actually extracted,
        # convert to QMARK style for test compatibility
        if (
            self._processing_context
            and self._processing_context.metadata
            and self._processing_context.metadata.get("literals_parameterized", False)
            and self._processing_context.metadata.get("parameter_count", 0) > 0
        ):
            sql, _ = self.compile(placeholder_style="qmark")
            return sql

        # Otherwise, return processed SQL as-is to preserve original parameter style
        return cast("_ProcessedState", self._processed_state).processed_sql

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
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        return cast("_ProcessedState", self._processed_state).processed_expression

    @property
    def parameters(self) -> Any:
        """Get merged parameters with TypedParameter objects preserved."""
        if self._is_many and self._original_parameters is not None:
            return self._original_parameters

        if (
            self._original_parameters is not None
            and isinstance(self._original_parameters, tuple)
            and not self._named_params
        ):
            return self._original_parameters

        self._ensure_processed()
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        params = self._processed_state.merged_parameters
        if params is None:
            return {}

        # Convert dict params to list format in two cases:
        # 1. When literals were parameterized AND SQL was converted to QMARK style
        # 2. When original SQL used positional placeholders (?) - preserve list format

        should_convert_to_list = False

        if (
            isinstance(params, dict)
            and params
            and all(key.startswith("param_") for key in params)
            and (
                (
                    self._processing_context
                    and self._processing_context.metadata
                    and self._processing_context.metadata.get("literals_parameterized", False)
                    and self._processing_context.metadata.get("parameter_count", 0) > 0
                )
                or (
                    self._original_parameters is not None
                    and isinstance(self._original_parameters, tuple)
                    and self._raw_sql
                    and "?" in self._raw_sql
                )
            )
        ):
            should_convert_to_list = True

        if should_convert_to_list:
            # Convert param_0, param_1, etc. to list format
            sorted_params = []
            for i in range(len(params)):
                key = f"param_{i}"
                if key in params:
                    sorted_params.append(params[key])
            return sorted_params

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
        if self._processed_state is not Empty and self._processing_context:
            # Get extracted literals from metadata if available
            extracted_literals = self._processing_context.metadata.get("extracted_literals", [])

            if extracted_literals:
                # Apply extracted literals to each parameter set
                enhanced_params: list[Any] = []
                for param_set in param_sets:
                    if isinstance(param_set, (list, tuple)):
                        # Add extracted literals to the parameter tuple
                        literal_values = [get_value_attribute(p) for p in extracted_literals]
                        enhanced_set = list(param_set) + literal_values
                        enhanced_params.append(tuple(enhanced_set))
                    elif isinstance(param_set, dict):
                        # For dict params, add extracted literals with generated names
                        enhanced_dict = dict(param_set)
                        for i, literal in enumerate(extracted_literals):
                            param_name = f"_literal_{i}"
                            enhanced_dict[param_name] = get_value_attribute(literal)
                        enhanced_params.append(enhanced_dict)
                    else:
                        # Single parameter - convert to tuple with literals
                        literals = [get_value_attribute(p) for p in extracted_literals]
                        enhanced_params.append((param_set, *literals))
                param_sets = enhanced_params

        if placeholder_style:
            sql, param_sets = self._convert_placeholder_style(sql, param_sets, placeholder_style)

        return sql, param_sets

    def _get_extracted_parameters(self) -> "list[Any]":
        """Get extracted parameters from pipeline processing."""
        extracted_params = []
        if self._processed_state is not Empty and self._processed_state.merged_parameters:
            merged = self._processed_state.merged_parameters
            if isinstance(merged, list):
                if merged and not isinstance(merged[0], (tuple, list)):
                    extracted_params = merged
            elif self._processing_context and self._processing_context.metadata.get("extracted_literals"):
                extracted_params = self._processing_context.metadata.get("extracted_literals", [])
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

        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        sql = self._processed_state.processed_sql
        params = self._processed_state.merged_parameters

        if params is not None and self._processing_context:
            parameter_mapping = self._processing_context.metadata.get("parameter_position_mapping")
            if parameter_mapping:
                params = self._reorder_parameters(params, parameter_mapping)

        # Handle deconversion if needed
        if self._processing_context and self._processing_context.metadata.get("parameter_conversion"):
            norm_state = self._processing_context.metadata["parameter_conversion"]

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

            for i in range(len(reordered_list)):
                if reordered_list[i] is None and i < len(params) and i not in mapping:
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
            and self._processing_context.metadata.get("parameter_conversion")
            and self._processing_context.metadata["parameter_conversion"].original_param_info
        ):
            param_info = self._processing_context.metadata["parameter_conversion"].original_param_info
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

        # For pyformat styles, escape literal % characters after placeholder replacement
        if target_style in {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT}:
            # We need to escape % that are not part of our placeholders
            # Since we just replaced placeholders, we know %s and %(name)s are our placeholders
            # So we escape any % that is not followed by 's' or '('
            result = []
            i = 0
            while i < len(sql):
                if sql[i] == "%":
                    # Check if this is a placeholder we just added
                    if i + 1 < len(sql) and (sql[i + 1] == "s" or sql[i + 1] == "("):
                        # This is a placeholder, don't escape
                        result.append(sql[i])
                    else:
                        # This is a literal %, escape it
                        result.append("%%")
                else:
                    result.append(sql[i])
                i += 1
            sql = "".join(result)

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
            return f":param_{param.ordinal}"
        if target_style == ParameterStyle.NAMED_AT:
            return f"@{param.name or f'param_{param.ordinal}'}"
        if target_style == ParameterStyle.POSITIONAL_COLON:
            # For Oracle positional colon, preserve the original numeric identifier if it was already :N style
            style, name = get_param_style_and_name(param)
            if style == ParameterStyle.POSITIONAL_COLON and name and name.isdigit():
                return f":{name}"
            return f":{param.ordinal + 1}"
        if target_style == ParameterStyle.POSITIONAL_PYFORMAT:
            return "%s"
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            return f"%({param.name or f'param_{param.ordinal}'})s"
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
            # Check if we have mixed parameter styles
            has_numeric = any(p.style == ParameterStyle.NUMERIC for p in param_info)
            has_other_styles = any(p.style != ParameterStyle.NUMERIC for p in param_info)

            if has_numeric and has_other_styles:
                # Mixed parameter styles: assign parameters in order of appearance in SQL
                sorted_params = sorted(param_info, key=lambda p: p.position)
                for i, _ in enumerate(sorted_params):
                    if i < len(params):
                        # Use sequential numbering for all parameters in mixed mode
                        # This ensures first appearance gets "1", second gets "2", etc.
                        result_dict[str(i + 1)] = params[i]
                return result_dict

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
        if is_dict(params):
            return SQL._convert_dict_to_positional(params, param_info)
        if isinstance(params, (list, tuple)):
            return SQL._convert_sequence_to_positional(params, param_info)
        return params

    @staticmethod
    def _convert_dict_to_positional(params: dict[str, Any], param_info: list[Any]) -> list[Any]:
        """Convert dictionary parameters to positional format."""
        if all(k.isdigit() for k in params):
            return SQL._convert_numeric_dict_to_positional(params, param_info)

        return SQL._convert_named_dict_to_positional(params, param_info)

    @staticmethod
    def _convert_numeric_dict_to_positional(params: dict[str, Any], param_info: list[Any]) -> list[Any]:
        """Convert numeric dictionary keys to positional format."""
        result_list: list[Any] = []
        sorted_params = sorted(param_info, key=lambda p: p.position)

        for i, _ in enumerate(sorted_params):
            key = str(i + 1)
            if key in params:
                val = params[key]
                result_list.append(val.value if has_parameter_value(val) else val)
            else:
                result_list.append(None)
        return result_list

    @staticmethod
    def _convert_named_dict_to_positional(params: dict[str, Any], param_info: list[Any]) -> list[Any]:
        """Convert named dictionary parameters to positional format."""
        result_list: list[Any] = []
        param_values_by_ordinal: dict[int, Any] = {}

        # Map named parameters
        for p in param_info:
            if p.name and p.name in params:
                param_values_by_ordinal[p.ordinal] = params[p.name]

        # Map unnamed parameters with arg_/param_ prefixes
        for p in param_info:
            if p.name is None and p.ordinal not in param_values_by_ordinal:
                arg_key = f"arg_{p.ordinal}"
                param_key = f"param_{p.ordinal}"
                if arg_key in params:
                    param_values_by_ordinal[p.ordinal] = params[arg_key]
                elif param_key in params:
                    param_values_by_ordinal[p.ordinal] = params[param_key]

        # Handle remaining parameters
        remaining_params = {
            k: v
            for k, v in params.items()
            if k not in {p.name for p in param_info if p.name} and not k.startswith(("arg_", "param_"))
        }

        unmatched_ordinals = [p.ordinal for p in param_info if p.ordinal not in param_values_by_ordinal]
        for ordinal, (_, value) in zip(unmatched_ordinals, remaining_params.items()):
            param_values_by_ordinal[ordinal] = value

        # Build result list
        for p in param_info:
            val = param_values_by_ordinal.get(p.ordinal)
            if val is not None:
                result_list.append(val.value if has_parameter_value(val) else val)
            else:
                result_list.append(None)

        return result_list

    @staticmethod
    def _convert_sequence_to_positional(params: "list[Any] | tuple[Any, ...]", param_info: list[Any]) -> Any:
        """Convert sequence parameters to positional format."""
        # Special case: if params is empty, preserve it (don't create None values)
        if not params:
            return params

        # Handle mixed parameter styles correctly
        if param_info and any(p.style == ParameterStyle.NUMERIC for p in param_info):
            return SQL._handle_mixed_style_conversion(params, param_info)

        # Standard conversion for non-mixed styles
        result_list: list[Any] = [param.value if has_parameter_value(param) else param for param in params]
        return result_list

    @staticmethod
    def _handle_mixed_style_conversion(params: "list[Any] | tuple[Any, ...]", param_info: list[Any]) -> list[Any]:
        """Handle mixed parameter style conversion."""
        sorted_params = sorted(param_info, key=lambda p: p.position)
        param_values_by_final_position = {}

        # Assign parameters in order of appearance in SQL to sequential final positions
        for final_pos, _ in enumerate(sorted_params):
            if final_pos < len(params):
                val = params[final_pos]
                param_values_by_final_position[final_pos] = val.value if has_parameter_value(val) else val

        # Build result list in final positional order
        result_list: list[Any] = [param_values_by_final_position.get(i) for i in range(len(sorted_params))]

        return result_list

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
                    result_dict[p.name or f"param_{p.ordinal}"] = params[f"param_{p.ordinal}"]
            return result_dict
        if isinstance(params, (list, tuple)):
            # Sort param_info by position (order in SQL) to ensure correct parameter mapping
            # This is critical: params list should be ordered by SQL appearance
            sorted_param_info = sorted(param_info, key=lambda p: getattr(p, "position", getattr(p, "ordinal", 0)))

            for i, value in enumerate(params):
                if has_parameter_value(value):
                    value = value.value

                if i < len(sorted_param_info):
                    p = sorted_param_info[i]
                    param_name = p.name or f"param_{getattr(p, 'ordinal', i)}"
                    result_dict[param_name] = value
                else:
                    param_name = f"param_{i}"
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
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        return cast("_ProcessedState", self._processed_state).validation_errors

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
            return self._processing_context.metadata.get("parameter_info", [])  # type: ignore[no-any-return]

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
