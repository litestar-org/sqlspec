"""SQL statement handling with centralized parameter management."""

from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

import sqlglot
import sqlglot.expressions as exp
from mypy_extensions import mypyc_attr
from sqlglot.errors import ParseError
from sqlglot.tokens import TokenType
from typing_extensions import TypeAlias

from sqlspec.parameters import (
    SQLGLOT_INCOMPATIBLE_STYLES,
    ParameterConverter,
    ParameterStyle,
    ParameterStyleConfig,
    ParameterStyleConversionState,
    ParameterValidator,
)
from sqlspec.statement.cache import ast_fragment_cache, base_statement_cache
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.pipeline import (
    SQLTransformContext,
    compose_pipeline,
    metadata_extraction_step,
    optimize_step,
    parameter_analysis_step,
    parameterize_literals_step,
    returns_rows_analysis_step,
    validate_dml_safety_step,
    validate_step,
)
from sqlspec.typing import Empty
from sqlspec.utils.logging import get_logger
from sqlspec.utils.statement_hashing import hash_sql_statement
from sqlspec.utils.type_guards import (
    can_append_to_statement,
    can_extract_parameters,
    expression_has_limit,
    get_value_attribute,
    has_parameter_value,
    is_dict,
    is_expression,
    is_statement_filter,
    supports_where,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.typing import StatementParameters

__all__ = ("SQL", "Statement", "StatementConfig")

logger = get_logger("sqlspec.statement")

Statement: TypeAlias = Union[str, exp.Expression, "SQL"]

ROW_RETURNING_TOKENS = {
    TokenType.SELECT,
    TokenType.WITH,
    TokenType.VALUES,
    TokenType.TABLE,
    TokenType.SHOW,
    TokenType.DESCRIBE,
    TokenType.PRAGMA,
}

PROCESSED_STATE_SLOTS = (
    "analysis_results",
    "merged_parameters",
    "processed_expression",
    "processed_sql",
    "transformation_results",
    "validation_errors",
)
SQL_CONFIG_SLOTS = (
    "pre_process_steps",
    "post_process_steps",
    "dialect",
    "enable_analysis",
    "enable_caching",
    "enable_expression_simplification",
    "enable_parameter_type_wrapping",
    "enable_parsing",
    "enable_transformations",
    "enable_validation",
    "output_transformer",
    "parameter_config",
    "parameter_converter",
    "parameter_validator",
)


class _ProcessedState:
    """Cached state from pipeline processing."""

    __slots__ = PROCESSED_STATE_SLOTS

    def __hash__(self) -> int:
        """Hash based on processed SQL and expression."""
        return hash(
            (
                self.processed_sql,
                str(self.processed_expression),
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
        for key in changes:
            if key not in PROCESSED_STATE_SLOTS:
                msg = f"{key!r} is not a field in {type(self).__name__}"
                raise TypeError(msg)

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
        return (
            self.processed_expression == other.processed_expression
            and self.processed_sql == other.processed_sql
            and self.merged_parameters == other.merged_parameters
            and self.validation_errors == other.validation_errors
            and self.analysis_results == other.analysis_results
            and self.transformation_results == other.transformation_results
        )


@mypyc_attr(allow_interpreted_subclasses=True)
class StatementConfig:
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
        Parse errors are handled gracefully by returning Anonymous expressions
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
        parameter_config: Required ParameterStyleConfig instance containing all parameter style settings
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
            )
        )

    def __init__(
        self,
        parameter_config: "Optional[ParameterStyleConfig]" = None,
        enable_parsing: bool = True,
        enable_validation: bool = True,
        enable_transformations: bool = True,
        enable_analysis: bool = False,
        enable_expression_simplification: bool = False,
        enable_parameter_type_wrapping: bool = True,
        enable_caching: bool = True,
        parameter_converter: "Optional[ParameterConverter]" = None,
        parameter_validator: "Optional[ParameterValidator]" = None,
        dialect: "Optional[DialectType]" = None,
        pre_process_steps: "Optional[list[Any]]" = None,
        post_process_steps: "Optional[list[Any]]" = None,
        output_transformer: "Optional[Callable[[str, Any], tuple[str, Any]]]" = None,
    ) -> None:
        self.enable_parsing = enable_parsing
        self.enable_validation = enable_validation
        self.enable_transformations = enable_transformations
        self.enable_analysis = enable_analysis
        self.enable_expression_simplification = enable_expression_simplification
        self.enable_parameter_type_wrapping = enable_parameter_type_wrapping
        self.enable_caching = enable_caching
        self.parameter_converter = parameter_converter or ParameterConverter()
        self.parameter_validator = parameter_validator or ParameterValidator()
        self.parameter_config = parameter_config or ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )

        self.dialect = dialect
        self.pre_process_steps = pre_process_steps
        self.post_process_steps = post_process_steps
        self.output_transformer = output_transformer

    def replace(self, **changes: Any) -> "StatementConfig":
        """Create a new StatementConfig with specified changes."""
        for key in changes:
            if key not in SQL_CONFIG_SLOTS:
                msg = f"{key!r} is not a field in {type(self).__name__}"
                raise TypeError(msg)
        kwargs = {slot: getattr(self, slot) for slot in SQL_CONFIG_SLOTS}
        kwargs.update(changes)
        return type(self)(**kwargs)

    def __repr__(self) -> str:
        """String representation of the StatementConfig instance."""
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

    def get_pipeline_steps(self) -> list:
        """Get configured pipeline steps in execution order.

        Returns:
            List of pipeline steps in execution order
        """
        steps = []

        if self.pre_process_steps:
            steps.extend(self.pre_process_steps)

        if self.enable_transformations:
            steps.append(parameterize_literals_step)
            if self.enable_expression_simplification:
                steps.append(optimize_step)

        if self.post_process_steps:
            steps.extend(self.post_process_steps)

        if self.enable_validation:
            steps.extend([validate_dml_safety_step, validate_step])

        if self.enable_analysis:
            steps.extend(self._get_analysis_steps())

        return steps

    def _get_analysis_steps(self) -> list:
        """Get configured analysis steps.

        Returns:
            List of analysis pipeline steps
        """
        from sqlspec.statement.pipeline import with_analysis_caching

        if self.enable_caching:
            return [
                with_analysis_caching(metadata_extraction_step, "metadata"),
                with_analysis_caching(returns_rows_analysis_step, "returns_rows"),
                with_analysis_caching(parameter_analysis_step, "parameters"),
            ]

        return [metadata_extraction_step, returns_rows_analysis_step, parameter_analysis_step]


def default_analysis_handler(analysis: Any) -> None:
    """Default handler that logs analysis to debug."""
    logger.debug("SQL Analysis: %s", analysis)


@mypyc_attr(allow_interpreted_subclasses=True)
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
        "statement_config",
    )

    def __init__(
        self,
        statement: "Union[str, exp.Expression, 'SQL']",
        *parameters: "Union[Any, StatementFilter, list[Union[Any, StatementFilter]]]",
        _dialect: "DialectType" = None,
        statement_config: "Optional[StatementConfig]" = None,
        _builder_result_type: "Optional[type]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL with centralized parameter management."""
        if statement_config is None:
            from sqlspec.parameters import ParameterStyle
            from sqlspec.parameters.config import ParameterStyleConfig

            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self._dialect = self._normalize_dialect(_dialect or self.statement_config.dialect)
        self._builder_result_type = _builder_result_type
        self._processed_state: Any = Empty
        self._processing_context: Optional[SQLTransformContext] = None
        self._positional_params: list[Any] = []
        self._named_params: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression
        self._raw_sql: str = ""
        self._original_parameters: Any = None
        self._original_sql: str = ""
        self._placeholder_mapping: dict[str, Any] = {}
        self._parameter_conversion_state: Optional[ParameterStyleConversionState] = None
        self._is_many: bool = False
        self._is_script: bool = False
        self._base_statement_key: Optional[tuple[str, str]] = None

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement, _dialect, statement_config or StatementConfig(), _builder_result_type)
        else:
            self._init_from_str_or_expression(statement)

        if not isinstance(statement, SQL):
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
        self, statement: "SQL", dialect: "DialectType", config: "StatementConfig", builder_result_type: "Optional[type]"
    ) -> None:
        """Initialize from an existing SQL object."""
        self._statement = statement._statement
        self._dialect = self._normalize_dialect(dialect or statement._dialect)
        self.statement_config = config or statement.statement_config
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
        self._processing_context = statement._processing_context

    def _init_from_str_or_expression(self, statement: "Union[str, exp.Expression]") -> None:
        """Initialize from a string or expression."""
        if isinstance(statement, str):
            self._raw_sql = statement
            self._statement = self._to_expression(statement)
        else:
            self._raw_sql = statement.sql(dialect=self._dialect)
            self._statement = statement

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
        if self._is_many:
            params = list(parameters)
            if "parameters" in kwargs:
                param_value = kwargs.pop("parameters")
                params.extend(param_value) if isinstance(param_value, list) else params.append(param_value)
            self._process_batch_parameters(params)
        else:
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

    def _process_batch_parameters(self, parameters: list[Any]) -> None:
        """Process parameters for batch execution (execute_many).

        Preserves tuple structure for parameter sets while still handling
        statement filters and other special cases.
        """
        for item in parameters:
            if is_statement_filter(item):
                self._filters.append(item)
                pos_params, named_params = self._extract_filter_parameters(item)
                self._positional_params.extend(pos_params)
                self._named_params.update(named_params)
            elif isinstance(item, (tuple, list)) or is_dict(item):
                self._positional_params.append(item)
            else:
                self._positional_params.append(item)

    def _ensure_processed(self) -> None:
        """Ensure the SQL has been processed through the pipeline (lazy initialization).

        This method implements the facade pattern with lazy processing.
        It's called by public methods that need processed state.
        """
        if self._processed_state is not Empty:
            return

        final_expr, final_params = self._build_final_state()
        has_placeholders = self._detect_placeholders()
        initial_sql_for_context, final_params = self._prepare_context_sql(final_expr, final_params)

        context = self._create_processing_context(initial_sql_for_context, final_expr, final_params, has_placeholders)
        original_context = context
        result_context = self._run_pipeline(context)

        processed_sql, merged_params = self._process_pipeline_result(result_context, final_params, original_context)

        self._finalize_processed_state(result_context, processed_sql, merged_params)

    def _detect_placeholders(self) -> bool:
        """Detect if the raw SQL has placeholders."""
        if self._raw_sql:
            validator = self.statement_config.parameter_validator
            raw_param_info = validator.extract_parameters(self._raw_sql)
            return bool(raw_param_info)
        return False

    def _prepare_context_sql(self, final_expr: exp.Expression, final_params: Any) -> tuple[str, Any]:
        """Prepare SQL string and parameters for context."""
        initial_sql_for_context = self._raw_sql or final_expr.sql(
            dialect=self._dialect or self.statement_config.dialect
        )

        if is_expression(final_expr) and self._placeholder_mapping:
            initial_sql_for_context = final_expr.sql(dialect=self._dialect or self.statement_config.dialect)
            if self._placeholder_mapping:
                converter = self.statement_config.parameter_converter
                converted_result = converter.convert_parameters(
                    initial_sql_for_context, final_params, None, None, validate=False
                )
                final_params = converted_result.merged_parameters

        return initial_sql_for_context, final_params

    def _create_processing_context(
        self, initial_sql_for_context: str, final_expr: exp.Expression, final_params: Any, has_placeholders: bool
    ) -> SQLTransformContext:
        """Create SQL processing context."""
        param_dict: dict[str, Any] = {}
        param_info = None

        if final_params:
            converter = self.statement_config.parameter_converter
            param_info = converter.validator.extract_parameters(initial_sql_for_context)

            if is_dict(final_params):
                param_dict = final_params
            elif isinstance(final_params, (list, tuple)):
                has_numeric_placeholders = (
                    any(p.style == ParameterStyle.NUMERIC for p in param_info) if param_info else False
                )
                has_mixed_styles = len({p.style for p in param_info}) > 1

                param_dict = (
                    converter.convert_mixed_parameters_to_dict(final_params, param_info)
                    if has_mixed_styles or has_numeric_placeholders
                    else {f"param_{i}": param for i, param in enumerate(final_params)}
                )
            else:
                param_dict = {"param_0": final_params}

        context = SQLTransformContext(
            current_expression=final_expr,
            original_expression=final_expr,
            parameters=param_dict,
            dialect=str(self._dialect or self.statement_config.dialect or ""),
        )

        self._populate_context_metadata(context, initial_sql_for_context, has_placeholders, param_info, final_params)
        return context

    def _populate_context_metadata(
        self,
        context: SQLTransformContext,
        initial_sql_for_context: str,
        has_placeholders: bool,
        param_info: Optional[list[Any]],
        final_params: Any,
    ) -> None:
        """Populate context metadata."""
        context.metadata["initial_sql"] = initial_sql_for_context
        context.metadata["has_placeholders"] = has_placeholders

        if self._placeholder_mapping:
            context.metadata["placeholder_map"] = self._placeholder_mapping

        if self._parameter_conversion_state:
            context.metadata["parameter_conversion"] = self._parameter_conversion_state

        if final_params and isinstance(final_params, (list, tuple)) and param_info is not None:
            context.metadata["parameter_info"] = param_info
        else:
            validator = self.statement_config.parameter_validator
            context.metadata["parameter_info"] = validator.extract_parameters(initial_sql_for_context)

    def _run_pipeline(self, context: SQLTransformContext) -> SQLTransformContext:
        """Run the SQL processing pipeline."""
        steps = self.statement_config.get_pipeline_steps()

        if steps:
            pipeline = compose_pipeline(steps)
            context = pipeline(context)

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
            initial_expr = original_context.original_expression
            if initial_expr and initial_expr != processed_expr:
                has_limit_in_initial = expression_has_limit(initial_expr)
                has_limit_in_processed = expression_has_limit(processed_expr)

                if has_limit_in_initial and not has_limit_in_processed:
                    processed_expr = initial_expr

            processed_sql = (
                processed_expr.sql(dialect=self._dialect or self.statement_config.dialect, comments=False)
                if processed_expr
                else ""
            )
            if self._placeholder_mapping and self._original_sql:
                processed_sql, context = self._transform_parameter_style(processed_sql, context)

        merged_params = self._merge_pipeline_parameters(context, final_params)

        return processed_sql, merged_params

    def _transform_parameter_style(
        self, processed_sql: str, context: SQLTransformContext
    ) -> tuple[str, SQLTransformContext]:
        """Denormalize SQL back to original parameter style using ParameterConverter."""
        if not self._original_sql:
            return processed_sql, context

        original_param_info = self.statement_config.parameter_validator.extract_parameters(self._original_sql)
        target_styles = {p.style for p in original_param_info}

        if ParameterStyle.POSITIONAL_PYFORMAT in target_styles:
            processed_sql = self.statement_config.parameter_converter.convert_placeholders(
                processed_sql, ParameterStyle.POSITIONAL_PYFORMAT
            )
        elif ParameterStyle.NAMED_PYFORMAT in target_styles:
            processed_sql = self.statement_config.parameter_converter.convert_placeholders(
                processed_sql, ParameterStyle.NAMED_PYFORMAT
            )
            if self._placeholder_mapping and context.parameters and is_dict(context.parameters):
                context.parameters = self.statement_config.parameter_converter._convert_to_named_pyformat_format(
                    context.parameters, original_param_info
                )
        elif ParameterStyle.POSITIONAL_COLON in target_styles:
            processed_sql = self.statement_config.parameter_converter.convert_placeholders(
                processed_sql, ParameterStyle.POSITIONAL_COLON
            )
            if self._placeholder_mapping and context.parameters and is_dict(context.parameters):
                context.parameters = self.statement_config.parameter_converter._convert_to_positional_colon_format(
                    context.parameters, original_param_info
                )

        return processed_sql, context

    def _merge_pipeline_parameters(self, context: SQLTransformContext, final_params: Any) -> Any:
        """Merge parameters from the pipeline processing."""
        merged_params = context.merged_parameters

        if context.metadata.get("literals_parameterized") and context.parameters:
            original_param_count = 0
            for key in context.parameters:
                if not key.startswith("param_") or not key[6:].isdigit():
                    original_param_count += 1

            if final_params is None and original_param_count < len(context.parameters):
                if self.statement_config.dialect in {"mysql", "sqlite"}:
                    if isinstance(context.parameters, dict):
                        return list(context.parameters.values())
                    if isinstance(context.parameters, (list, tuple)):
                        return list(context.parameters)
                    return []
                if isinstance(context.parameters, dict):
                    return dict(context.parameters)
                return {}

        return merged_params

    def _finalize_processed_state(self, context: SQLTransformContext, processed_sql: str, merged_params: Any) -> None:
        """Finalize the processed state."""
        if self.statement_config.enable_parameter_type_wrapping and merged_params is not None:
            validator = self.statement_config.parameter_validator
            param_info = validator.extract_parameters(processed_sql)

            literals_parameterized = (
                context.metadata.get("literals_parameterized", False) if context.metadata else False
            )

            converter = self.statement_config.parameter_converter
            merged_params = converter.wrap_parameters_with_types(merged_params, param_info, literals_parameterized)

        analysis_results = context.metadata.copy() if context.metadata else {}

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

        if self._processed_state.validation_errors:
            for error in self._processed_state.validation_errors:
                logger.warning("SQL validation issue: %s", str(error))

    def _to_expression(self, statement: "Union[str, exp.Expression]") -> exp.Expression:
        """Convert string to sqlglot expression."""
        if is_expression(statement):
            return statement

        if not statement or (isinstance(statement, str) and not statement.strip()):
            return exp.Select()

        if not self.statement_config.enable_parsing:
            return exp.Anonymous(this=statement)

        if not isinstance(statement, str):
            return exp.Anonymous(this="")
        validator = self.statement_config.parameter_validator
        param_info = validator.extract_parameters(statement)

        needs_conversion = any(p.style in SQLGLOT_INCOMPATIBLE_STYLES for p in param_info)

        converted_sql = statement

        if needs_conversion:
            converter = self.statement_config.parameter_converter
            converted_sql, param_info_mapping = converter._transform_sql_for_parsing(statement, param_info)
            self._original_sql = statement
            self._placeholder_mapping = param_info_mapping

            placeholder_map: dict[str, Union[str, int]] = {}
            for param_name, p_info in param_info_mapping.items():
                placeholder_map[param_name] = p_info.ordinal

            self._parameter_conversion_state = ParameterStyleConversionState(
                was_transformed=True,
                original_styles=list({p.style for p in param_info}),
                transformation_style=ParameterStyle.NAMED_COLON,
                placeholder_map=placeholder_map,
                original_param_info=param_info,
            )
        else:
            self._parameter_conversion_state = None

        use_base_cache = not needs_conversion and not param_info and isinstance(statement, str)

        if use_base_cache:
            self._base_statement_key = (statement.strip(), str(self._dialect) if self._dialect else "default")
            try:
                return base_statement_cache.get_or_parse(statement, str(self._dialect) if self._dialect else None)
            except ParseError:
                pass
        else:
            cached_expr = ast_fragment_cache.parse_with_cache(
                converted_sql, fragment_type="QUERY", dialect=self._dialect
            )

            if cached_expr:
                return cached_expr

        try:
            expressions = sqlglot.parse(converted_sql, dialect=self._dialect)  # pyright: ignore
            if not expressions:
                return exp.Anonymous(this=statement)
            first_expr = expressions[0]
            if first_expr is None:
                return exp.Anonymous(this=statement)

            if not use_base_cache:
                ast_fragment_cache.set_fragment(
                    sql=converted_sql,
                    expression=first_expr,
                    fragment_type="QUERY",
                    dialect=self._dialect,
                    parameter_count=len(param_info),
                )

        except ParseError as e:
            logger.warning(
                "Failed to parse SQL, returning Anonymous expression.", extra={"sql": statement, "error": str(e)}
            )
            return exp.Anonymous(this=statement)
        return first_expr

    @staticmethod
    def _extract_filter_parameters(filter_obj: StatementFilter) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters from a filter object."""
        if can_extract_parameters(filter_obj):
            return filter_obj.extract_parameters()
        return [], {}

    def _copy_with(self, **overrides: Any) -> "SQL":
        """Create a new SQL instance with all state copied and specified overrides.

        This is an internal method used to maintain immutability.
        """
        new_sql = SQL.__new__(SQL)

        attrs_to_copy = [
            "_base_statement_key",
            "_builder_result_type",
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
            "statement_config",
        ]

        for attr in attrs_to_copy:
            if attr in overrides:
                value = overrides[attr]
            else:
                value = getattr(self, attr)
                if attr == "_positional_params" and isinstance(value, list):
                    value = list(value)
                elif attr == "_named_params" and isinstance(value, dict):
                    value = dict(value)
                elif attr == "_filters" and isinstance(value, list):
                    value = list(value)
                elif attr == "_placeholder_mapping" and isinstance(value, dict):
                    value = dict(value)

            setattr(new_sql, attr, value)

        return new_sql

    def copy(
        self,
        statement: "Optional[Union[str, exp.Expression]]" = None,
        parameters: "Optional[Any]" = None,
        dialect: "DialectType" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQL":
        """Create a copy with optional modifications.

        This is the primary method for creating modified SQL objects.
        """
        if statement is not None and statement != self._statement:
            return SQL(
                statement,
                parameters if parameters is not None else (self._positional_params, self._named_params),
                _dialect=dialect if dialect is not None else self._dialect,
                statement_config=statement_config if statement_config is not None else self.statement_config,
                _builder_result_type=self._builder_result_type,
                **kwargs,
            )

        overrides: dict[str, Any] = {}

        if dialect is not None:
            overrides["_dialect"] = dialect
        if statement_config is not None:
            overrides["_config"] = statement_config

        if parameters is not None:
            overrides["_positional_params"] = []
            overrides["_named_params"] = {}
            overrides["_original_parameters"] = parameters
            if isinstance(parameters, (list, tuple)):
                overrides["_positional_params"] = list(parameters)
            elif isinstance(parameters, dict):
                overrides["_named_params"] = dict(parameters)
            else:
                overrides["_positional_params"] = [parameters]

        overrides.update({key: value for key, value in kwargs.items() if key.startswith("_") and hasattr(self, key)})

        return self._copy_with(**overrides)

    def add_named_parameter(self, name: "str", value: Any) -> "SQL":
        """Add a named parameter and return a new SQL instance."""
        new_params = dict(self._named_params)
        new_params[name] = value
        return self._copy_with(_named_params=new_params)

    def get_unique_parameter_name(
        self, base_name: "str", namespace: "Optional[str]" = None, preserve_original: bool = True
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
        pos_params, named_params = self._extract_filter_parameters(filter_obj)

        new_filters = list(self._filters)
        new_filters.append(filter_obj)

        new_positional = list(self._positional_params)
        new_positional.extend(pos_params)

        new_named = dict(self._named_params)
        new_named.update(named_params)

        return self._copy_with(_filters=new_filters, _positional_params=new_positional, _named_params=new_named)

    def as_many(self, parameters: "Optional[Union[list[Any], Sequence[StatementParameters]]]" = None) -> "SQL":
        """Mark for executemany with optional parameters.

        If no parameters are provided, uses the existing positional parameters
        as the batch execution sequence.
        """
        overrides: dict[str, Any] = {"_is_many": True}

        if parameters is not None:
            overrides["_positional_params"] = []
            overrides["_named_params"] = {}
            overrides["_original_parameters"] = parameters
        elif self._positional_params:
            overrides["_original_parameters"] = self._positional_params

        return self._copy_with(**overrides)

    def as_script(self) -> "SQL":
        """Mark as script for execution."""
        return self._copy_with(_is_script=True)

    def _build_final_state(self) -> tuple[exp.Expression, Any]:
        """Build final expression and parameters after applying filters."""
        final_expr = self._statement
        accumulated_positional = list(self._positional_params)
        accumulated_named = dict(self._named_params)

        for filter_obj in self._filters:
            if can_append_to_statement(filter_obj):
                temp_sql = SQL(final_expr, config=self.statement_config, dialect=self._dialect)
                temp_sql._positional_params = list(accumulated_positional)
                temp_sql._named_params = dict(accumulated_named)
                result = filter_obj.append_to_statement(temp_sql)

                if isinstance(result, SQL):
                    final_expr = result._statement
                    accumulated_positional = list(result._positional_params)
                    accumulated_named = dict(result._named_params)
                else:
                    final_expr = result

        if accumulated_named and not accumulated_positional:
            return final_expr, dict(accumulated_named)
        if accumulated_positional and not accumulated_named:
            return final_expr, list(accumulated_positional)
        if accumulated_positional and accumulated_named:
            final_params = dict(accumulated_named)
            for i, param in enumerate(accumulated_positional):
                param_name = f"arg_{i}"
                while param_name in final_params:
                    param_name = f"arg_{i}_{id(param)}"
                final_params[param_name] = param
            return final_expr, final_params
        return final_expr, None

    @property
    def sql(self) -> str:
        """Get SQL string with default QMARK placeholder style."""
        if not self._raw_sql or (self._raw_sql and not self._raw_sql.strip()):
            return ""

        if self._is_script and self._raw_sql:
            return self._raw_sql
        if not self.statement_config.enable_parsing and self._raw_sql:
            return self._raw_sql
        self._ensure_processed()
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)

        if self._is_many:
            processed_sql = cast("_ProcessedState", self._processed_state).processed_sql
            processed_params = cast("_ProcessedState", self._processed_state).merged_parameters
            if processed_params:
                sql, _ = self._apply_placeholder_style(processed_sql, processed_params, "qmark")
                return sql
            return processed_sql

        if (
            self._processing_context
            and self._processing_context.metadata
            and self._processing_context.metadata.get("literals_parameterized", False)
            and self._processing_context.metadata.get("parameter_count", 0) > 0
        ):
            sql, _ = self.compile(placeholder_style="qmark")
            return sql

        return cast("_ProcessedState", self._processed_state).processed_sql

    @property
    def config(self) -> "StatementConfig":
        """Get the SQL configuration."""
        return self.statement_config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """Get the final expression."""
        if not self.statement_config.enable_parsing:
            return None
        self._ensure_processed()
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        return cast("_ProcessedState", self._processed_state).processed_expression

    @property
    def filters(self) -> "list[StatementFilter]":
        """Get the list of filters applied to this statement."""
        return list(self._filters)

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

    def returns_rows(self, expression: "Optional[exp.Expression]" = None) -> bool:
        """Check if the SQL expression is expected to return rows.

        Args:
            expression: The SQL expression to check. If None, uses this SQL's expression.

        Returns:
            True if the expression is a SELECT, VALUES, WITH (not CTE definition),
            INSERT/UPDATE/DELETE with RETURNING, or certain command types.
        """
        if expression is None:
            if not self._raw_sql or not self._raw_sql.strip():
                return False
            expression = self.expression

        if expression is None:
            return False

        if isinstance(expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)):
            return True

        if isinstance(expression, exp.With) and expression.expressions:
            return self.returns_rows(expression.expressions[-1])

        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return bool(expression.find(exp.Returning))

        if isinstance(expression, exp.Anonymous):
            sql_text = str(expression.this) if expression.this else ""
            if not sql_text.strip():
                return False
            return self._check_anonymous_returns_rows(sql_text)

        return False

    def _check_anonymous_returns_rows(self, sql_text: str) -> bool:
        """Uncached implementation of anonymous expression checking."""
        import contextlib

        with contextlib.suppress(Exception):
            parsed = sqlglot.parse_one(sql_text, read=None)
            if isinstance(parsed, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)):
                return True
            if isinstance(parsed, exp.With) and parsed.expressions:
                return self.returns_rows(parsed.expressions[-1])
            if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete)):
                return bool(parsed.find(exp.Returning))

        try:
            tokens = list(sqlglot.tokenize(sql_text, read=None))
            for token in tokens:
                if token.token_type in {TokenType.COMMENT, TokenType.SEMICOLON}:
                    continue
                return token.token_type in ROW_RETURNING_TOKENS
        except Exception:
            return False

        return False

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
        self._ensure_processed()

        sql = self._processed_state.processed_sql if self._processed_state is not Empty else self.sql

        param_sets = self._original_parameters or []

        if self._processed_state is not Empty and self._processing_context:
            extracted_literals = self._processing_context.metadata.get("extracted_literals", [])

            if extracted_literals:
                enhanced_params: list[Any] = []
                for param_set in param_sets:
                    if isinstance(param_set, (list, tuple)):
                        literal_values = [get_value_attribute(p) for p in extracted_literals]
                        enhanced_set = list(param_set) + literal_values
                        enhanced_params.append(tuple(enhanced_set))
                    elif isinstance(param_set, dict):
                        enhanced_dict = dict(param_set)
                        for i, literal in enumerate(extracted_literals):
                            param_name = f"_literal_{i}"
                            enhanced_dict[param_name] = get_value_attribute(literal)
                        enhanced_params.append(enhanced_dict)
                    else:
                        literals = [get_value_attribute(p) for p in extracted_literals]
                        enhanced_params.append((param_set, *literals))
                param_sets = enhanced_params

        if self._processing_context and self._processing_context.metadata.get("parameter_conversion"):
            norm_state = self._processing_context.metadata["parameter_conversion"]

            if norm_state.was_transformed and norm_state.original_styles:
                original_style = norm_state.original_styles[0]
                should_denormalize = placeholder_style is None or (
                    placeholder_style and ParameterStyle(placeholder_style) == original_style
                )

                if should_denormalize and original_style in SQLGLOT_INCOMPATIBLE_STYLES:
                    current_param_info = self.statement_config.parameter_validator.extract_parameters(sql)
                    sql = self.statement_config.parameter_converter._convert_sql_placeholders(
                        sql, original_style, current_param_info
                    )

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

    def compile(
        self, placeholder_style: "Optional[str]" = None, flatten_single_params: bool = False
    ) -> "tuple[str, Any]":
        """Compile to SQL and parameters with driver awareness.

        Args:
            placeholder_style: Target parameter placeholder style
            flatten_single_params: If True, flatten single-element lists for scalar parameters
        """
        if self._is_script:
            return self.sql, None

        if self._is_many and self._original_parameters is not None:
            return self._compile_execute_many(placeholder_style)

        if not self.statement_config.enable_parsing and self._raw_sql:
            return self._raw_sql, self._original_parameters

        sql, params = self._get_processed_sql_and_params()

        if params is not None and self._processing_context:
            sql, params = self._apply_parameter_transformations(sql, params, placeholder_style)

        if (
            placeholder_style is None
            and self._original_parameters is not None
            and not (self._processing_context and self._processing_context.metadata.get("parameter_conversion"))
        ):
            if isinstance(self._original_parameters, tuple) and not self._named_params:
                params = self._original_parameters
            elif (
                isinstance(self._original_parameters, (tuple, list))
                and len(self._original_parameters) == 1
                and isinstance(self._original_parameters[0], dict)
            ):
                params = self._original_parameters[0]

        params = self._unwrap_typed_parameters(params)

        if placeholder_style:
            sql, params = self._apply_placeholder_style(sql, params, placeholder_style)

        if flatten_single_params and params is not None:
            params = self._flatten_single_params(sql, params)

        return sql, params

    def _flatten_single_params(self, sql: str, params: Any) -> Any:
        """Flatten single-element lists for scalar parameters.

        This helps with ADBC compatibility where PostgreSQL interprets
        lists as array types instead of scalar parameters.

        Args:
            sql: The compiled SQL string
            params: Parameters to potentially flatten

        Returns:
            Flattened parameters if applicable, otherwise original params
        """
        if not isinstance(params, list) or len(params) != 1:
            return params

        param_count = sql.count("$1") + sql.count("$2") + sql.count("?") + sql.count("%s")

        single_param = params[0]
        if (
            param_count == 1
            and isinstance(single_param, list)
            and len(single_param) == 1
            and not isinstance(single_param[0], (list, tuple))
        ):
            return single_param

        return params

    def _get_processed_sql_and_params(self) -> "tuple[str, Any]":
        """Get processed SQL and parameters from the processed state."""
        self._ensure_processed()

        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)

        return self._processed_state.processed_sql, self._processed_state.merged_parameters

    def _apply_parameter_transformations(
        self, sql: str, params: Any, placeholder_style: "Optional[str]"
    ) -> "tuple[str, Any]":
        """Apply parameter reordering and conversion transformations."""
        if self._processing_context is None:
            return sql, params

        parameter_mapping = self._processing_context.metadata.get("parameter_position_mapping")
        if parameter_mapping:
            params = self._reorder_parameters(params, parameter_mapping)

        conversion_state = self._processing_context.metadata.get("parameter_conversion")
        if conversion_state:
            sql, params = self._apply_parameter_conversion(sql, params, conversion_state, placeholder_style)

        return sql, params

    def _apply_parameter_conversion(
        self, sql: str, params: Any, norm_state: Any, placeholder_style: "Optional[str]"
    ) -> "tuple[str, Any]":
        """Apply parameter conversion based on normalization state."""
        if not (norm_state.was_transformed and norm_state.original_styles):
            return sql, params

        original_style = norm_state.original_styles[0]
        should_denormalize = placeholder_style is None or (
            placeholder_style and ParameterStyle(placeholder_style) == original_style
        )

        if should_denormalize and original_style in SQLGLOT_INCOMPATIBLE_STYLES:
            current_param_info = self.statement_config.parameter_validator.extract_parameters(sql)
            sql = self.statement_config.parameter_converter._convert_sql_placeholders(
                sql, original_style, current_param_info
            )

            if original_style == ParameterStyle.POSITIONAL_COLON and is_dict(params):
                params = self._convert_colon_parameters(params, norm_state)
            elif original_style == ParameterStyle.POSITIONAL_PYFORMAT and is_dict(params):
                params = self._convert_positional_pyformat_parameters(params, norm_state)
            elif original_style == ParameterStyle.NAMED_PYFORMAT and is_dict(params):
                params = self._convert_named_pyformat_parameters(params, norm_state)

        return sql, params

    def _convert_colon_parameters(self, params: dict[str, Any], norm_state: Any) -> dict[str, Any]:
        """Convert parameters for Oracle POSITIONAL_COLON style."""
        if all(p.name in params for p in norm_state.original_param_info if p.name):
            return params

        original_params = {}
        all_numeric = all(p.name and p.name.isdigit() for p in norm_state.original_param_info)

        if all_numeric:
            for original_param in norm_state.original_param_info:
                param_num = int(original_param.name)
                normalized_key = "param_0" if param_num == 0 else f"param_{param_num - 1}"
                if normalized_key in params:
                    original_params[original_param.name] = params[normalized_key]
        else:
            for i, original_param in enumerate(norm_state.original_param_info):
                normalized_key = f"param_{i}"
                if normalized_key in params:
                    original_params[original_param.name] = params[normalized_key]

        return original_params

    def _convert_positional_pyformat_parameters(self, params: dict[str, Any], norm_state: Any) -> "tuple[Any, ...]":
        """Convert parameters for POSITIONAL_PYFORMAT style (%s placeholders).

        For positional pyformat, we need to convert from dict back to tuple
        in the correct order based on parameter appearance.
        """
        if not isinstance(params, dict):
            return params

        ordered_params = []
        for i, _ in enumerate(norm_state.original_param_info):
            normalized_key = f"param_{i}"
            if normalized_key in params:
                ordered_params.append(params[normalized_key])

        return tuple(ordered_params)

    def _convert_named_pyformat_parameters(self, params: dict[str, Any], norm_state: Any) -> dict[str, Any]:
        """Convert parameters for NAMED_PYFORMAT style (%(name)s placeholders).

        Since SQL placeholder conversion from param_0 back to original names isn't working properly,
        we need to convert the parameter dictionary to use param_0, param_1, etc. keys to match
        the SQL placeholders that are currently in the SQL.
        """
        if not isinstance(params, dict):
            return params

        param_keys = {f"param_{i}" for i in range(len(norm_state.original_param_info))}
        current_keys = set(params.keys())

        if param_keys.issubset(current_keys):
            return params

        original_names = {p.name for p in norm_state.original_param_info if p.name}
        if original_names.issubset(current_keys):
            converted_params = {}
            for i, original_param in enumerate(norm_state.original_param_info):
                if original_param.name and original_param.name in params:
                    converted_params[f"param_{i}"] = params[original_param.name]
            return converted_params

        return params

    def generate_cache_key_with_config(self, config: "Optional[StatementConfig]" = None) -> str:
        """Generate cache key that includes StatementConfig context.

        This method creates a deterministic cache key that includes both the SQL content
        and the StatementConfig settings to prevent cross-contamination between different
        configurations.

        Args:
            config: Optional StatementConfig to use for key generation.
                   Uses self.statement_config if not provided.

        Returns:
            String cache key that includes both SQL and configuration context
        """

        effective_config = config or self.statement_config

        config_hash = hash(
            (
                effective_config.enable_parsing,
                effective_config.enable_validation,
                effective_config.enable_transformations,
                effective_config.enable_analysis,
                effective_config.enable_expression_simplification,
                effective_config.enable_parameter_type_wrapping,
                effective_config.enable_caching,
                effective_config.dialect,
                effective_config.parameter_config.hash(),
                tuple(effective_config.pre_process_steps) if effective_config.pre_process_steps else (),
                tuple(effective_config.post_process_steps) if effective_config.post_process_steps else (),
            )
        )

        filter_hash = hash(tuple(str(f) for f in self._filters)) if self._filters else 0

        base_hash = hash_sql_statement(self)
        return f"sql:{base_hash}:{config_hash}:{filter_hash}"

    def _apply_placeholder_style(self, sql: "str", params: Any, placeholder_style: "str") -> "tuple[str, Any]":
        """Apply placeholder style conversion using ParameterConverter."""
        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
        converter = self.statement_config.parameter_converter
        return converter.convert_placeholder_style(sql, params, target_style, is_many=self._is_many)

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
            return params
        return params

    def _convert_placeholder_style(self, sql: str, params: Any, placeholder_style: str) -> tuple[str, Any]:
        """Convert SQL and parameters using ParameterConverter."""
        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
        converter = self.statement_config.parameter_converter
        return converter.convert_placeholder_style(sql, params, target_style, is_many=self._is_many)

    @property
    def validation_errors(self) -> list[Any]:
        """Get validation errors."""
        if not self.statement_config.enable_validation:
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

    @property
    def parameter_info(self) -> list[Any]:
        """Get parameter information from the SQL statement.

        Returns the original parameter info before any conversion.
        """
        validator = self.statement_config.parameter_validator
        if self._raw_sql:
            return validator.extract_parameters(self._raw_sql)

        self._ensure_processed()

        if self._processing_context:
            return self._processing_context.metadata.get("parameter_info", [])  # type: ignore[no-any-return]

        return []

    @property
    def param_list(self) -> "Optional[list[Any]]":
        """Get parameter list for execute_many operations."""
        if self._is_many and self._original_parameters is not None:
            return self._original_parameters  # type: ignore[no-any-return]
        return None

    def detect_parameter_style(self) -> "Optional[ParameterStyle]":
        """Detect the parameter style of the SQL statement.

        Returns:
            The dominant parameter style found in the SQL, or None if no parameters
        """
        if not self._raw_sql:
            return None

        validator = self.statement_config.parameter_validator
        param_info = validator.extract_parameters(self._raw_sql)

        if not param_info:
            return None

        return validator.get_parameter_style(param_info)

    @property
    def statement(self) -> exp.Expression:
        """Get statement for compatibility."""
        return self._statement
