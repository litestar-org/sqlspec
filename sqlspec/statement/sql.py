"""SQL statement handling with centralized parameter management."""

from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

import sqlglot
import sqlglot.expressions as exp
from mypy_extensions import mypyc_attr
from sqlglot.errors import ParseError
from sqlglot.tokens import TokenType
from typing_extensions import TypeAlias

from sqlspec.parameters import ParameterConverter, ParameterStyle, ParameterStyleConfig, ParameterValidator
from sqlspec.statement.cache import ast_fragment_cache, base_statement_cache
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.transformer import SQLTransformer
from sqlspec.typing import Empty
from sqlspec.utils.logging import get_logger
from sqlspec.utils.statement_hashing import hash_sql_statement
from sqlspec.utils.type_guards import (
    can_append_to_statement,
    can_extract_parameters,
    has_parameter_value,
    is_dict,
    is_expression,
    is_statement_filter,
    supports_where,
)

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


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

OPERATION_TYPE_MAP = {"INSERT": "INSERT", "UPDATE": "UPDATE", "DELETE": "DELETE", "SELECT": "SELECT", "COPY": "COPY"}

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
    "execution_mode",
    "execution_args",
    "output_transformer",
    "parameter_config",
    "parameter_converter",
    "parameter_validator",
)


class _ProcessedState:
    """Cached state from SQLTransformer processing."""

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

    Execution Strategy Configuration:
        execution_mode: Optional string indicating special execution handling (default: None)
        execution_args: Optional dict of arguments for the execution mode (default: None)

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
        execution_mode: "Optional[str]" = None,
        execution_args: "Optional[dict[str, Any]]" = None,
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
        self.execution_mode = execution_mode
        self.execution_args = execution_args
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
        "_named_parameters",
        "_original_parameters",
        "_original_param_info",
        "_original_sql",
        "_placeholder_mapping",
        "_positional_parameters",
        "_processed_state",
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
        is_many: "Optional[bool]" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL with centralized parameter management.

        Args:
            statement: SQL string, expression, or existing SQL object
            *parameters: Parameters for the SQL statement
            _dialect: SQL dialect for parsing and generation
            statement_config: Configuration for SQL processing
            _builder_result_type: Builder result type for chaining
            is_many: If True, marks this SQL for execute_many operations
            **kwargs: Additional keyword arguments
        """

        # If no statement_config provided, detect parameter style and preserve it

        self.statement_config = statement_config or self._create_auto_config(statement, parameters, kwargs)

        self._dialect = self._normalize_dialect(_dialect or self.statement_config.dialect)
        self._builder_result_type = _builder_result_type
        self._processed_state: Any = Empty
        self._positional_parameters: list[Any] = []
        self._named_parameters: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression
        self._raw_sql: str = ""
        self._original_parameters: Any = None
        self._original_sql: str = ""
        self._placeholder_mapping: dict[str, Any] = {}
        self._is_many: bool = False
        self._is_script: bool = False
        self._base_statement_key: Optional[tuple[str, str]] = None

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement, _dialect, statement_config or StatementConfig(), _builder_result_type)
            # Override is_many if explicitly provided
            if is_many is not None:
                self._is_many = is_many
        else:
            self._init_from_str_or_expression(statement, parameters)
            # Set is_many based on parameter or auto-detection
            if is_many is not None:
                self._is_many = is_many
            elif self._should_auto_detect_many(parameters):
                self._is_many = True

        if not isinstance(statement, SQL):
            self._set_original_parameters(*parameters)

        self._process_parameters(*parameters, **kwargs)

    def _should_auto_detect_many(self, parameters: tuple) -> bool:
        """Auto-detect if this should be treated as execute_many based on parameter structure.

        Args:
            parameters: The parameters tuple passed to __init__

        Returns:
            True if this looks like execute_many parameters (list of tuples/lists)
        """
        # If we have exactly one parameter and it's a list of tuples/lists, treat as execute_many
        if len(parameters) == 1 and isinstance(parameters[0], list):
            param_list = parameters[0]
            # Check if all items in the list are tuples or lists (parameter sets)
            # and we have more than one item (single item lists could be array parameters)
            if len(param_list) > 1 and all(isinstance(item, (tuple, list)) for item in param_list):
                return True
        return False

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

    def _create_auto_config(
        self, statement: "Union[str, exp.Expression, 'SQL']", parameters: tuple, kwargs: dict[str, Any]
    ) -> "StatementConfig":
        """Create auto-detected StatementConfig when none provided.

        Detects parameter style from the SQL string and creates a config
        that preserves the original parameter format instead of converting
        to QMARK style.

        Args:
            statement: The SQL statement to analyze
            parameters: Positional parameters passed to SQL()
            kwargs: Keyword parameters passed to SQL()

        Returns:
            StatementConfig configured to preserve detected parameter style
        """
        # Only auto-detect for string statements
        if not isinstance(statement, str):
            return StatementConfig()

        # Detect parameter style from the SQL string
        validator = ParameterValidator()
        param_info = validator.extract_parameters(statement)

        if param_info:
            # Get the dominant parameter style
            styles = {p.style for p in param_info}
            # Only auto-configure for POSITIONAL_COLON style to preserve Oracle compatibility
            from sqlspec.parameters import ParameterStyle

            if ParameterStyle.POSITIONAL_COLON in styles:
                # Create a config that preserves the POSITIONAL_COLON style
                return StatementConfig(
                    parameter_config=ParameterStyleConfig(
                        default_parameter_style=ParameterStyle.POSITIONAL_COLON,
                        supported_parameter_styles={ParameterStyle.POSITIONAL_COLON},
                        preserve_parameter_format=True,  # Key: preserve original format
                    )
                )

        # Fallback to default config
        return StatementConfig()

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
        self._base_statement_key = statement._base_statement_key
        self._positional_parameters.extend(statement._positional_parameters)
        self._named_parameters.update(statement._named_parameters)
        self._filters.extend(statement._filters)

    def _init_from_str_or_expression(self, statement: "Union[str, exp.Expression]", parameters: Any = None) -> None:
        """Initialize from a string or expression."""
        if isinstance(statement, str):
            self._raw_sql = statement
            self._statement = self._to_expression(statement, parameters)
        else:
            self._raw_sql = statement.sql(dialect=self._dialect, copy=False, comments=False)
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
            param_list = list(parameters)
            if "parameters" in kwargs:
                param_value = kwargs.pop("parameters")
                if isinstance(param_value, list):
                    param_list.extend(param_value)
                else:
                    param_list.append(param_value)
            self._process_batch_parameters(param_list)
        else:
            for param in parameters:
                self._process_parameter_item(param)

            if "parameters" in kwargs:
                param_value = kwargs.pop("parameters")
                if isinstance(param_value, (list, tuple)):
                    self._positional_parameters.extend(param_value)
                    # Also set _original_parameters if not already set
                    if self._original_parameters is None:
                        self._original_parameters = tuple(param_value)
                elif is_dict(param_value):
                    self._named_parameters.update(param_value)
                else:
                    self._positional_parameters.append(param_value)
                    if self._original_parameters is None:
                        self._original_parameters = (param_value,)

        self._named_parameters.update({k: v for k, v in kwargs.items() if not k.startswith("_")})

        # IMPORTANT: If SQL was converted to :param_N format, remap parameters to match
        if self._original_sql and self._named_parameters:
            validator = self.statement_config.parameter_validator
            param_info = validator.extract_parameters(self._original_sql)
            if param_info:
                # Check if original SQL had incompatible parameter styles that were converted
                converter = self.statement_config.parameter_converter
                dialect_str = str(self.statement_config.dialect) if self.statement_config.dialect else None
                incompatible_styles = converter.validator.get_sqlglot_incompatible_styles(dialect_str)
                {p.style for p in param_info}
                needs_conversion = any(p.style in incompatible_styles for p in param_info)

                if needs_conversion:
                    # Remap parameters from original names to param_N names
                    remapped_params = {}
                    for i, p in enumerate(param_info):
                        if p.name and p.name in self._named_parameters:
                            remapped_params[f"param_{i}"] = self._named_parameters[p.name]
                    # Replace the named parameters with the remapped ones
                    if remapped_params:
                        self._named_parameters = remapped_params

    def _process_parameter_item(self, item: Any) -> None:
        """Process a single item from the parameters list."""
        if is_statement_filter(item):
            self._filters.append(item)
            # Don't extract parameters from filters during initial processing
            # They will be added when the filter is applied in _build_final_state
        elif isinstance(item, list):
            # Check if this is a single array parameter (for PostgreSQL arrays etc.)
            # Don't flatten lists that are meant to be single array parameters
            if len(self._original_parameters) == 1 and self._original_parameters[0] == item:
                # This is the only parameter and it's a list - treat as array parameter
                self._positional_parameters.append(item)
            else:
                # Multiple parameters or nested structure - process recursively
                for sub_item in item:
                    self._process_parameter_item(sub_item)
        elif is_dict(item):
            self._named_parameters.update(item)
        elif isinstance(item, tuple):
            self._positional_parameters.extend(item)
        else:
            self._positional_parameters.append(item)

    def _process_batch_parameters(self, parameters: list[Any]) -> None:
        """Process parameters for batch execution (execute_many).

        Preserves tuple structure for parameter sets while still handling
        statement filters and other special cases.
        """
        for item in parameters:
            if is_statement_filter(item):
                self._filters.append(item)
                # Don't extract parameters from filters during initial processing
                # They will be added when the filter is applied in _build_final_state
            elif isinstance(item, (tuple, list)) or is_dict(item):
                self._positional_parameters.append(item)
            else:
                self._positional_parameters.append(item)

    def _ensure_processed(self) -> None:
        """Ensure the SQL has been processed through SQLTransformer (lazy initialization).

        This method implements the facade pattern with lazy processing.
        It's called by public methods that need processed state.
        """
        if self._processed_state is not Empty:
            return

        final_expression, final_parameters = self._build_final_state()

        # Check if we need to convert list parameters to dict for POSITIONAL_COLON style
        # This ensures numeric parameter names are preserved
        if isinstance(final_parameters, (list, tuple)) and self._original_sql:
            # Use the original SQL to detect the parameter style, not the generated SQL
            validator = self.statement_config.parameter_validator
            original_param_info = validator.extract_parameters(self._original_sql)

            # If all parameters are POSITIONAL_COLON style, convert list to dict with numeric keys
            from sqlspec.parameters import ParameterStyle

            if original_param_info and all(p.style == ParameterStyle.POSITIONAL_COLON for p in original_param_info):
                converted_params = {}
                # Sort parameters by their numeric value to get the mapping
                sorted_params = sorted(
                    original_param_info, key=lambda p: int(p.name) if p.name and p.name.isdigit() else 0
                )
                # Map list items to the actual numeric placeholders in the SQL
                for i, param in enumerate(sorted_params):
                    if i < len(final_parameters) and param.name:
                        converted_params[param.name] = final_parameters[i]
                final_parameters = converted_params

        # Use SQLTransformer directly for parameter processing only
        transformer = SQLTransformer(
            parameters=final_parameters, dialect=self._dialect or "", config=self.statement_config
        )

        # Generate final SQL for compilation, but reuse the already-parsed expression
        # Remove comments from the SQL output
        final_sql = final_expression.sql(dialect=self._dialect or "", comments=False)

        # Only transform parameters - don't re-parse the expression
        processed_sql, processed_parameters = transformer.compile(final_sql)

        # Reuse the successfully parsed expression instead of re-parsing
        # This eliminates the redundant Parse #2 that was causing failures
        processed_expression = final_expression

        # Store the processed state
        self._processed_state = _ProcessedState(
            processed_expression=processed_expression,
            processed_sql=processed_sql,
            merged_parameters=processed_parameters,
            validation_errors=[],
            analysis_results={},
            transformation_results={},
        )

    def _to_expression(self, statement: "Union[str, exp.Expression]", parameters: Any = None) -> exp.Expression:
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

        converter = self.statement_config.parameter_converter
        incompatible_styles = converter.validator.get_sqlglot_incompatible_styles(
            str(self.statement_config.dialect) if self.statement_config.dialect else None
        )
        needs_conversion = any(p.style in incompatible_styles for p in param_info)

        converted_sql = statement

        # Phase 1: Always convert if we have incompatible parameter styles for SQLGlot parsing
        if needs_conversion:
            converter = self.statement_config.parameter_converter
            converted_sql, original_param_info = converter.normalize_sql_for_parsing(
                statement, str(self._dialect) if self._dialect else None
            )
            self._original_sql = statement
            self._original_param_info = original_param_info

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
                # Use converted_sql for Anonymous to preserve parameter conversions
                return exp.Anonymous(this=converted_sql)
            first_expr = expressions[0]
            if first_expr is None:
                # Use converted_sql for Anonymous to preserve parameter conversions
                return exp.Anonymous(this=converted_sql)

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
            # Use converted_sql for Anonymous to preserve parameter conversions
            return exp.Anonymous(this=converted_sql)
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
            "_named_parameters",
            "_original_parameters",
            "_original_sql",
            "_placeholder_mapping",
            "_positional_parameters",
            "_processed_state",
            "_raw_sql",
            "_statement",
            "statement_config",
        ]

        for attr in attrs_to_copy:
            if attr in overrides:
                value = overrides[attr]
            else:
                value = getattr(self, attr)
                if attr == "_positional_parameters" and isinstance(value, list):
                    value = list(value)
                elif attr == "_named_parameters" and isinstance(value, dict):
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
            # Properly handle parameters when copying
            if parameters is None:
                # Pass positional and named parameters properly
                if self._named_parameters:
                    # Merge named parameters with kwargs
                    merged_kwargs = dict(kwargs)
                    merged_kwargs.update(self._named_parameters)
                    return SQL(
                        statement,
                        *self._positional_parameters,
                        _dialect=dialect if dialect is not None else self._dialect,
                        statement_config=statement_config if statement_config is not None else self.statement_config,
                        _builder_result_type=self._builder_result_type,
                        **merged_kwargs,
                    )
                # Only positional parameters
                parameters = self._positional_parameters

            return SQL(
                statement,
                parameters,
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
            overrides["_positional_parameters"] = []
            overrides["_named_parameters"] = {}
            overrides["_original_parameters"] = parameters
            if isinstance(parameters, (list, tuple)):
                overrides["_positional_parameters"] = list(parameters)
            elif isinstance(parameters, dict):
                overrides["_named_parameters"] = dict(parameters)
            else:
                overrides["_positional_parameters"] = [parameters]

        overrides.update({key: value for key, value in kwargs.items() if key.startswith("_") and hasattr(self, key)})

        return self._copy_with(**overrides)

    def add_named_parameter(self, name: "str", value: Any) -> "SQL":
        """Add a named parameter and return a new SQL instance."""
        new_parameters = dict(self._named_parameters)
        new_parameters[name] = value
        return self._copy_with(_named_parameters=new_parameters)

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
        all_param_names = set(self._named_parameters.keys())

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
        condition_expr = self._to_expression(condition, None) if isinstance(condition, str) else condition

        if supports_where(self._statement):
            new_statement = self._statement.where(condition_expr)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).where(condition_expr)  # pyright: ignore

        return self.copy(statement=new_statement)

    def filter(self, filter_obj: StatementFilter) -> "SQL":
        """Apply a filter and return a new SQL instance."""
        pos_parameters, named_parameters = self._extract_filter_parameters(filter_obj)

        new_filters = list(self._filters)
        new_filters.append(filter_obj)

        new_positional = list(self._positional_parameters)
        new_positional.extend(pos_parameters)

        new_named = dict(self._named_parameters)
        new_named.update(named_parameters)

        return self._copy_with(_filters=new_filters, _positional_parameters=new_positional, _named_parameters=new_named)

    def as_script(self) -> "SQL":
        """Mark as script for execution."""
        return self._copy_with(_is_script=True)

    def _build_final_state(self) -> tuple[exp.Expression, Any]:
        """Build final expression and parameters after applying filters."""
        final_expr = self._statement
        accumulated_positional = list(self._positional_parameters)
        accumulated_named = dict(self._named_parameters)

        for filter_obj in self._filters:
            if can_append_to_statement(filter_obj):
                temp_sql = SQL(final_expr, statement_config=self.statement_config, _dialect=self._dialect)
                temp_sql._positional_parameters = list(accumulated_positional)
                temp_sql._named_parameters = dict(accumulated_named)
                result = filter_obj.append_to_statement(temp_sql)

                if isinstance(result, SQL):
                    final_expr = result._statement
                    accumulated_positional = list(result._positional_parameters)
                    accumulated_named = dict(result._named_parameters)
                else:
                    final_expr = result

        if accumulated_named and not accumulated_positional:
            return final_expr, dict(accumulated_named)
        if accumulated_positional and not accumulated_named:
            return final_expr, list(accumulated_positional)
        if accumulated_positional and accumulated_named:
            final_parameters = dict(accumulated_named)
            for i, param in enumerate(accumulated_positional):
                param_name = f"param_{i}"
                while param_name in final_parameters:
                    param_name = f"param_{i}_{id(param)}"
                final_parameters[param_name] = param
            return final_expr, final_parameters
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
            processed_parameters = cast("_ProcessedState", self._processed_state).merged_parameters
            if processed_parameters:
                # Use the execution parameter style from config instead of hardcoded qmark
                execution_style = self.statement_config.parameter_config.default_execution_parameter_style
                if execution_style is not None:
                    placeholder_style = self._parameter_style_to_placeholder_style(execution_style)
                else:
                    placeholder_style = None
                if placeholder_style is not None:
                    sql, _ = self._apply_placeholder_style(processed_sql, processed_parameters, placeholder_style)
                else:
                    sql = processed_sql
                return sql
            return processed_sql

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
        """Get parameters automatically formatted for the execution context.

        This property unifies parameter formatting for execute(), execute_many(),
        and execute_script() based on statement_config and execution flags.
        """
        # Handle execute_many case
        if self._is_many and self._original_parameters is not None:
            return self._format_parameters_for_execution_context(self._original_parameters)

        # When we have both positional and named parameters, return the merged dict
        # This is the expected behavior for mixed parameter styles
        if self._positional_parameters and self._named_parameters:
            # Build the merged dict without going through the transformer
            merged = dict(self._named_parameters)
            for i, param in enumerate(self._positional_parameters):
                param_name = f"param_{i}"
                while param_name in merged:
                    param_name = f"param_{i}_{id(param)}"
                merged[param_name] = param
            return merged

        # When we have only named parameters, return the dict
        if self._named_parameters and not self._positional_parameters:
            return dict(self._named_parameters)

        if (
            self._original_parameters is not None
            and isinstance(self._original_parameters, tuple)
            and not self._named_parameters
        ):
            # Check if we have POSITIONAL_COLON style parameters that need dict conversion
            if self._raw_sql:
                param_info = self.parameter_info
                if param_info:
                    from sqlspec.parameters import ParameterStyle

                    # If all parameters are POSITIONAL_COLON style, convert to dict
                    if all(p.style == ParameterStyle.POSITIONAL_COLON for p in param_info):
                        result = {}
                        # Map positional params to their numeric placeholders
                        sorted_params = sorted(
                            param_info, key=lambda p: int(p.name) if p.name and p.name.isdigit() else 0
                        )
                        for i, param in enumerate(sorted_params):
                            if i < len(self._original_parameters) and param.name:
                                result[param.name] = self._original_parameters[i]
                        return result

            # For other positional-only parameters, return the original tuple
            # This preserves the expected format for simple positional queries
            return self._original_parameters

        self._ensure_processed()
        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)
        parameters = self._processed_state.merged_parameters
        if parameters is None:
            return {}

        should_convert_to_list = False

        if (
            isinstance(parameters, dict)
            and parameters
            and all(key.startswith("param_") for key in parameters)
            and (
                self._original_parameters is not None
                and isinstance(self._original_parameters, tuple)
                and self._raw_sql
                and "?" in self._raw_sql
            )
        ):
            should_convert_to_list = True

        if should_convert_to_list:
            sorted_parameters = []
            for i in range(len(parameters)):
                key = f"param_{i}"
                if key in parameters:
                    sorted_parameters.append(parameters[key])
            return self._format_parameters_for_execution_context(sorted_parameters)

        return self._format_parameters_for_execution_context(parameters)

    def _format_parameters_for_execution_context(self, parameters: Any) -> Any:
        """Format parameters based on execution context and statement config.

        This method ensures parameters match the expected format for the
        driver's execution_parameter_style configuration.
        """
        if parameters is None:
            return parameters

        # Get the execution parameter style from config
        execution_style = self.statement_config.parameter_config.default_execution_parameter_style

        # For execute_many, we need to handle batch parameters specially
        if (
            self._is_many
            and isinstance(parameters, (list, tuple))
            and parameters
            and all(isinstance(item, (list, tuple, dict)) for item in parameters)
            and execution_style is not None
        ):
            return self._format_batch_parameters(parameters, execution_style)

        # Single parameter set formatting
        return (
            self._format_single_parameter_set(parameters, execution_style)
            if execution_style is not None
            else parameters
        )

    def _format_batch_parameters(self, batch_parameters: Any, execution_style: "ParameterStyle") -> Any:
        """Format a batch of parameters for execute_many operations."""
        from sqlspec.parameters import ParameterStyle

        # Check if we should preserve the original parameter format
        if self.statement_config.parameter_config.preserve_parameter_format:
            return batch_parameters

        # Named styles require dict parameters
        named_styles = {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.POSITIONAL_COLON,  # Also uses dict format with numeric keys
        }

        expects_dict = execution_style in named_styles

        # If we have tuple/list parameters but need dict format for named placeholders
        if expects_dict and all(isinstance(item, (list, tuple)) for item in batch_parameters):
            # Get the current SQL to extract parameter names
            current_sql = self.sql if hasattr(self, "_processed_state") else self._raw_sql
            if current_sql and self._needs_tuple_to_dict_conversion(current_sql, batch_parameters[0]):
                return [self._convert_tuple_to_dict(current_sql, params) for params in batch_parameters]

        # For positional styles (pyformat_positional, qmark, etc.), keep batch structure
        # Don't flatten - executemany expects list of parameter sets
        return batch_parameters

    def _format_single_parameter_set(self, parameters: Any, execution_style: "ParameterStyle") -> Any:
        """Format a single parameter set for execution."""
        from sqlspec.parameters import ParameterStyle

        # Check if we should preserve the original parameter format
        if self.statement_config.parameter_config.preserve_parameter_format:
            return parameters

        # Named styles require dict parameters
        named_styles = {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.POSITIONAL_COLON,  # Also uses dict format with numeric keys
        }

        expects_dict = execution_style in named_styles

        # Convert tuple/list to dict if SQL uses named placeholders
        if expects_dict and isinstance(parameters, (list, tuple)):
            current_sql = self.sql if hasattr(self, "_processed_state") else self._raw_sql
            if current_sql and self._needs_tuple_to_dict_conversion(current_sql, parameters):
                return self._convert_tuple_to_dict(current_sql, parameters)

        return parameters

    def _needs_tuple_to_dict_conversion(self, sql: str, parameters: Any) -> bool:
        """Check if tuple parameters need conversion to dict for named placeholders."""
        if not isinstance(parameters, (list, tuple)):
            return False

        # Check for any colon-style placeholders (both named and positional)
        import re

        # Check for colon-style placeholders (:name or :1)
        colon_placeholders = re.findall(r":(\w+)", sql)
        if colon_placeholders:
            return True

        # Check for named pyformat placeholders
        named_placeholders = re.findall(r"%\((\w+)\)s", sql)
        return len(named_placeholders) > 0

    def _convert_tuple_to_dict(self, sql: str, parameters: Any) -> "dict[Any, Any]":
        """Convert tuple/list parameters to dict based on SQL placeholders."""
        if not isinstance(parameters, (list, tuple)):
            return parameters if isinstance(parameters, dict) else {}

        # Extract parameter info from the SQL to get the correct names
        validator = self.statement_config.parameter_validator
        param_info = validator.extract_parameters(sql)

        # If we have parameter info with names, use them
        if param_info:
            # Check if all parameters are numeric (POSITIONAL_COLON style)
            all_numeric_names = all(
                p.name and (p.name.isdigit() or (p.name.startswith("param_") and p.name[6:].isdigit()))
                for p in param_info
            )

            if all_numeric_names:
                # For POSITIONAL_COLON, map list[i] to str(i+1)
                result = {}
                for i, value in enumerate(parameters):
                    result[str(i + 1)] = value
                return result
            # For other styles, map by position in SQL
            result = {}
            for i, value in enumerate(parameters):
                if i < len(param_info) and param_info[i].name:
                    name = param_info[i].name
                    # Handle converted names like "param_1" -> extract the numeric part
                    if name and name.startswith("param_") and len(name) > 6 and name[6:].isdigit():
                        # For POSITIONAL_COLON converted names, use the numeric part
                        result[name[6:]] = value
                    elif name:
                        # Use the actual name from the SQL placeholder
                        result[name] = value
                else:
                    # Fallback for extra parameters or unnamed placeholders
                    result[f"param_{i}"] = value
            return result

        # Fallback to generic param names
        return {f"param_{i}": value for i, value in enumerate(parameters)}

    def _parameter_style_to_placeholder_style(self, parameter_style: "ParameterStyle") -> str:
        """Convert ParameterStyle enum to placeholder style string."""
        # _apply_placeholder_style expects strings that can construct ParameterStyle enum
        # Just return the enum value directly since that's what it expects
        return parameter_style.value

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
            # For script execution, delegate to robust checking without naive string matching
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
        _, parameters = self.compile(placeholder_style=style)
        return parameters

    def _compile_execute_many(self, placeholder_style: "Optional[str]") -> "tuple[str, Any]":
        """Compile for execute_many operations using unified parameter formatting."""
        # Use unified properties that automatically handle execution context
        sql = self.sql
        parameters = self.parameters

        # Apply specific placeholder style if requested
        if placeholder_style:
            sql, parameters = self._convert_placeholder_style(sql, parameters, placeholder_style)

        return sql, parameters

    def _get_extracted_parameters(self) -> "list[Any]":
        """Get extracted parameters from SQLTransformer processing."""
        extracted_parameters = []
        if self._processed_state is not Empty and self._processed_state.merged_parameters:
            merged = self._processed_state.merged_parameters
            if isinstance(merged, list) and merged and not isinstance(merged[0], (tuple, list)):
                extracted_parameters = merged
        return extracted_parameters

    def compile(
        self, placeholder_style: "Optional[str]" = None, flatten_single_parameters: bool = False
    ) -> "tuple[str, Any]":
        """Compile to SQL and parameters with driver awareness.

        Args:
            placeholder_style: Target parameter placeholder style
            flatten_single_parameters: If True, flatten single-element lists for scalar parameters
        """
        if self._is_script:
            return self.sql, None

        if self._is_many and self._original_parameters is not None:
            return self._compile_execute_many(placeholder_style)

        if not self.statement_config.enable_parsing and self._raw_sql:
            return self._raw_sql, self._original_parameters

        sql, parameters = self._get_processed_sql_and_parameters()

        parameters = self._unwrap_typed_parameters(parameters)

        if placeholder_style:
            sql, parameters = self._apply_placeholder_style(sql, parameters, placeholder_style)

        if flatten_single_parameters and parameters is not None:
            parameters = self._flatten_single_parameters(sql, parameters)

        return sql, parameters

    def _flatten_single_parameters(self, sql: str, parameters: Any) -> Any:
        """Flatten single-element lists for scalar parameters.

        This helps with ADBC compatibility where PostgreSQL interprets
        lists as array types instead of scalar parameters.

        Args:
            sql: The compiled SQL string
            parameters: Parameters to potentially flatten

        Returns:
            Flattened parameters if applicable, otherwise original parameters
        """
        if not isinstance(parameters, list) or len(parameters) != 1:
            return parameters

        param_count = sql.count("$1") + sql.count("$2") + sql.count("?") + sql.count("%s")

        single_param = parameters[0]
        if (
            param_count == 1
            and isinstance(single_param, list)
            and len(single_param) == 1
            and not isinstance(single_param[0], (list, tuple))
        ):
            return single_param

        return parameters

    def _get_processed_sql_and_parameters(self) -> "tuple[str, Any]":
        """Get processed SQL and parameters from the processed state."""
        self._ensure_processed()

        if self._processed_state is Empty:
            msg = "Failed to process SQL statement"
            raise RuntimeError(msg)

        return self._processed_state.processed_sql, self._processed_state.merged_parameters

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

    def _apply_placeholder_style(self, sql: "str", parameters: Any, placeholder_style: "str") -> "tuple[str, Any]":
        """Apply placeholder style conversion using ParameterConverter."""
        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
        converter = self.statement_config.parameter_converter
        return converter.convert_placeholder_style(sql, parameters, target_style, is_many=self._is_many)

    @staticmethod
    def _unwrap_typed_parameters(parameters: Any) -> Any:
        """Unwrap TypedParameter objects to their actual values.

        Args:
            parameters: Parameters that may contain TypedParameter objects

        Returns:
            Parameters with TypedParameter objects unwrapped to their values
        """
        if parameters is None:
            return None

        if is_dict(parameters):
            unwrapped_dict = {}
            for key, value in parameters.items():
                if has_parameter_value(value):
                    unwrapped_dict[key] = value.value
                else:
                    unwrapped_dict[key] = value
            return unwrapped_dict

        if isinstance(parameters, (list, tuple)):
            unwrapped_list = []
            for value in parameters:
                if has_parameter_value(value):
                    unwrapped_list.append(value.value)
                else:
                    unwrapped_list.append(value)
            return type(parameters)(unwrapped_list)

        if has_parameter_value(parameters):
            return parameters.value

        return parameters

    @staticmethod
    def _reorder_parameters(parameters: Any, mapping: dict[int, int]) -> Any:
        """Reorder parameters based on the position mapping.

        Args:
            parameters: Original parameters (list, tuple, or dict)
            mapping: Dict mapping new positions to original positions

        Returns:
            Reordered parameters in the same format as input
        """
        if isinstance(parameters, (list, tuple)):
            reordered_list = [None] * len(parameters)  # pyright: ignore
            for new_pos, old_pos in mapping.items():
                if old_pos < len(parameters):
                    reordered_list[new_pos] = parameters[old_pos]  # pyright: ignore

            for i in range(len(reordered_list)):
                if reordered_list[i] is None and i < len(parameters) and i not in mapping:
                    reordered_list[i] = parameters[i]  # pyright: ignore

            return tuple(reordered_list) if isinstance(parameters, tuple) else reordered_list

        if is_dict(parameters):
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                reordered_dict: dict[str, Any] = {}
                for new_pos, old_pos in mapping.items():
                    old_key = f"param_{old_pos}"
                    new_key = f"param_{new_pos}"
                    if old_key in parameters:
                        reordered_dict[new_key] = parameters[old_key]

                for key, value in parameters.items():
                    if key not in reordered_dict and key.startswith("param_"):
                        idx = int(key[6:])
                        if idx not in mapping:
                            reordered_dict[key] = value

                return reordered_dict
            return parameters
        return parameters

    def _convert_placeholder_style(self, sql: str, parameters: Any, placeholder_style: str) -> tuple[str, Any]:
        """Convert SQL and parameters using ParameterConverter."""
        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
        converter = self.statement_config.parameter_converter
        return converter.convert_placeholder_style(sql, parameters, target_style, is_many=self._is_many)

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
    def parameter_info(self) -> "list[Any]":
        """Get detailed parameter information from the SQL statement.

        Returns:
            List of ParameterInfo objects with details about each parameter
        """
        if not self._raw_sql:
            return []

        validator = self.statement_config.parameter_validator
        return validator.extract_parameters(self._raw_sql)

    @property
    def statement(self) -> exp.Expression:
        """Get statement for compatibility."""
        return self._statement

    @property
    def operation_type(self) -> str:
        """Get the operation type of this SQL statement."""
        if self.is_script:
            return "SCRIPT"

        try:
            expression = self.expression
        except Exception:
            return "EXECUTE"

        if not expression:
            return "EXECUTE"

        expr_type = type(expression).__name__.upper()
        for key, value in OPERATION_TYPE_MAP.items():
            if key in expr_type:
                return value
        return "EXECUTE"
