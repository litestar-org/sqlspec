"""SQL statement with complete backward compatibility.

This module implements the core SQL class and StatementConfig with complete
backward compatibility while using an optimized processing pipeline.

Components:
- SQL class: SQL statement with identical external interface
- StatementConfig: Complete backward compatibility for all driver requirements
- ProcessedState: Cached processing results

Features:
- Lazy compilation: Only compile when needed
- Cached properties: Avoid redundant computation
- Complete StatementConfig compatibility
- Integrated parameter processing and compilation caching
"""

import contextlib
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.errors import ParseError
from typing_extensions import TypeAlias

from sqlspec.core.compiler import SQLProcessor
from sqlspec.core.parameters import ParameterConverter, ParameterStyle, ParameterStyleConfig, ParameterValidator
from sqlspec.typing import Empty, EmptyEnum
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_statement_filter, supports_where

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.core.filters import StatementFilter


__all__ = (
    "SQL",
    "ProcessedState",
    "Statement",
    "StatementConfig",
    "get_default_config",
    "get_default_parameter_config",
)
logger = get_logger("sqlspec.core.statement")

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

PROCESSED_STATE_SLOTS = (
    "compiled_sql",
    "execution_parameters",
    "parsed_expression",
    "operation_type",
    "validation_errors",
    "is_many",
)


@mypyc_attr(allow_interpreted_subclasses=False)
class ProcessedState:
    """Cached processing results for SQL statements.

    Stores the results of processing to avoid redundant compilation,
    parsing, and parameter processing.
    """

    __slots__ = PROCESSED_STATE_SLOTS

    def __init__(
        self,
        compiled_sql: str,
        execution_parameters: Any,
        parsed_expression: "Optional[exp.Expression]" = None,
        operation_type: str = "UNKNOWN",
        validation_errors: "Optional[list[str]]" = None,
        is_many: bool = False,
    ) -> None:
        self.compiled_sql = compiled_sql
        self.execution_parameters = execution_parameters
        self.parsed_expression = parsed_expression
        self.operation_type = operation_type
        self.validation_errors = validation_errors or []
        self.is_many = is_many

    def __hash__(self) -> int:
        return hash((self.compiled_sql, str(self.execution_parameters), self.operation_type))


@mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class SQL:
    """SQL statement with complete backward compatibility.

    Provides 100% backward compatibility while using an optimized
    core processing pipeline.

    Features:
    - Lazy evaluation with cached properties
    - Integrated parameter processing pipeline
    - Complete StatementFilter and execution mode support
    - Same parameter processing behavior
    - Same result types and interfaces
    """

    __slots__ = (
        "_dialect",
        "_filters",
        "_hash",
        "_is_many",
        "_is_script",
        "_named_parameters",
        "_original_parameters",
        "_positional_parameters",
        "_processed_state",
        "_raw_sql",
        "_statement_config",
    )

    def __init__(
        self,
        statement: "Union[str, exp.Expression, 'SQL']",
        *parameters: "Union[Any, StatementFilter, list[Union[Any, StatementFilter]]]",
        statement_config: Optional["StatementConfig"] = None,
        is_many: Optional[bool] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL statement.

        Args:
            statement: SQL string, expression, or existing SQL object
            *parameters: Parameters and filters
            statement_config: Configuration
            is_many: Mark as execute_many operation
            **kwargs: Additional parameters
        """
        self._statement_config = statement_config or self._create_auto_config(statement, parameters, kwargs)

        self._dialect = self._normalize_dialect(self._statement_config.dialect)
        self._processed_state: Union[EmptyEnum, ProcessedState] = Empty
        self._hash: Optional[int] = None
        self._filters: list[StatementFilter] = []
        self._named_parameters: dict[str, Any] = {}
        self._positional_parameters: list[Any] = []
        self._is_script = False

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement)
            if is_many is not None:
                self._is_many = is_many
        else:
            if isinstance(statement, str):
                self._raw_sql = statement
            else:
                self._raw_sql = statement.sql(dialect=str(self._dialect) if self._dialect else None)

            self._is_many = is_many if is_many is not None else self._should_auto_detect_many(parameters)

        self._original_parameters = parameters
        self._process_parameters(*parameters, **kwargs)

    def _create_auto_config(
        self, statement: "Union[str, exp.Expression, 'SQL']", parameters: tuple, kwargs: dict[str, Any]
    ) -> "StatementConfig":
        """Create auto-detected StatementConfig when none provided."""
        return get_default_config()

    def _normalize_dialect(self, dialect: "Optional[DialectType]") -> "Optional[str]":
        """Normalize dialect to string representation."""
        if dialect is None:
            return None
        if isinstance(dialect, str):
            return dialect
        try:
            return dialect.__class__.__name__.lower()
        except AttributeError:
            return str(dialect)

    def _init_from_sql_object(self, sql_obj: "SQL") -> None:
        """Initialize from existing SQL object."""
        self._raw_sql = sql_obj._raw_sql
        self._filters = sql_obj._filters.copy()
        self._named_parameters = sql_obj._named_parameters.copy()
        self._positional_parameters = sql_obj._positional_parameters.copy()
        self._is_many = sql_obj._is_many
        self._is_script = sql_obj._is_script
        if sql_obj._processed_state is not Empty:
            self._processed_state = sql_obj._processed_state

    def _should_auto_detect_many(self, parameters: tuple) -> bool:
        """Auto-detect execute_many from parameter structure."""
        if len(parameters) == 1 and isinstance(parameters[0], list):
            param_list = parameters[0]
            if len(param_list) > 1 and all(isinstance(item, (tuple, list)) for item in param_list):
                return True
        return False

    def _process_parameters(self, *parameters: Any, dialect: Optional[str] = None, **kwargs: Any) -> None:
        """Process parameters using parameter system."""
        if dialect is not None:
            self._dialect = self._normalize_dialect(dialect)

        if "is_script" in kwargs:
            self._is_script = bool(kwargs.pop("is_script"))

        # Optimize parameter filtering with direct iteration
        filters: list[StatementFilter] = []
        actual_params: list[Any] = []
        for p in parameters:
            if is_statement_filter(p):
                filters.append(p)
            else:
                actual_params.append(p)

        self._filters.extend(filters)

        if actual_params:
            param_count = len(actual_params)
            if param_count == 1:
                param = actual_params[0]
                if isinstance(param, dict):
                    self._named_parameters.update(param)
                elif isinstance(param, (list, tuple)):
                    if self._is_many:
                        self._positional_parameters = list(param)
                    else:
                        self._positional_parameters.extend(param)
                else:
                    self._positional_parameters.append(param)
            else:
                self._positional_parameters.extend(actual_params)

        self._named_parameters.update(kwargs)

    # PRESERVED PROPERTIES - Exact same interface as existing SQL class
    @property
    def sql(self) -> str:
        """Get the raw SQL string - no compilation triggered."""
        return self._raw_sql

    @property
    def parameters(self) -> Any:
        """Get the original parameters without triggering compilation."""
        if self._named_parameters:
            return self._named_parameters
        return self._positional_parameters or []

    @property
    def operation_type(self) -> str:
        """SQL operation type - requires explicit compilation."""
        if self._processed_state is Empty:
            return "UNKNOWN"
        return self._processed_state.operation_type

    @property
    def statement_config(self) -> "StatementConfig":
        """Statement configuration - preserved interface."""
        return self._statement_config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """SQLGlot expression - only available after explicit compilation."""
        # This property should only be accessed after compilation
        # If not compiled yet, return None
        if self._processed_state is not Empty:
            return self._processed_state.parsed_expression
        return None

    @property
    def filters(self) -> "list[StatementFilter]":
        """Applied filters."""
        return self._filters.copy()

    @property
    def dialect(self) -> "Optional[str]":
        """SQL dialect."""
        return self._dialect

    @property
    def _statement(self) -> "Optional[exp.Expression]":
        """Internal SQLGlot expression."""
        return self.expression

    @property
    def is_many(self) -> bool:
        """Check if this is execute_many."""
        return self._is_many

    @property
    def is_script(self) -> bool:
        """Check if this is script execution."""
        return self._is_script

    @property
    def validation_errors(self) -> "list[str]":
        """Validation errors - requires explicit compilation."""
        if self._processed_state is Empty:
            return []
        return self._processed_state.validation_errors.copy()

    @property
    def has_errors(self) -> bool:
        """Check if there are validation errors."""
        return len(self.validation_errors) > 0

    def returns_rows(self) -> bool:
        """Check if statement returns rows."""
        sql_upper = self._raw_sql.strip().upper()
        if any(sql_upper.startswith(op) for op in ("SELECT", "WITH", "VALUES", "TABLE", "SHOW", "DESCRIBE", "PRAGMA")):
            return True

        return "RETURNING" in sql_upper

    def is_modifying_operation(self) -> bool:
        """Check if the SQL statement is a modifying operation.

        Returns:
            True if the operation modifies data (INSERT/UPDATE/DELETE)
        """
        expression = self.expression
        if expression and isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return True

        sql_upper = self.sql.strip().upper()
        modifying_operations = ("INSERT", "UPDATE", "DELETE")
        return any(sql_upper.startswith(op) for op in modifying_operations)

    def compile(self) -> tuple[str, Any]:
        """Explicitly compile the SQL statement."""
        if self._processed_state is Empty:
            try:
                # Avoid unnecessary variable assignment
                processor = SQLProcessor(self._statement_config)
                compiled_result = processor.compile(
                    self._raw_sql, self._named_parameters or self._positional_parameters, is_many=self._is_many
                )

                self._processed_state = ProcessedState(
                    compiled_sql=compiled_result.compiled_sql,
                    execution_parameters=compiled_result.execution_parameters,
                    parsed_expression=compiled_result.expression,
                    operation_type=compiled_result.operation_type,
                    validation_errors=[],
                    is_many=self._is_many,
                )
            except Exception as e:
                logger.warning("Processing failed, using fallback: %s", e)
                self._processed_state = ProcessedState(
                    compiled_sql=self._raw_sql,
                    execution_parameters=self._named_parameters or self._positional_parameters,
                    operation_type="UNKNOWN",
                    is_many=self._is_many,
                )

        return self._processed_state.compiled_sql, self._processed_state.execution_parameters

    def as_script(self) -> "SQL":
        """Mark as script execution."""
        new_sql = SQL(
            self._raw_sql, *self._original_parameters, statement_config=self._statement_config, is_many=self._is_many
        )
        # Preserve accumulated parameters when marking as script
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        new_sql._is_script = True
        return new_sql

    def copy(
        self, statement: "Optional[Union[str, exp.Expression]]" = None, parameters: Optional[Any] = None, **kwargs: Any
    ) -> "SQL":
        """Create copy with modifications."""
        new_sql = SQL(
            statement or self._raw_sql,
            *(parameters if parameters is not None else self._original_parameters),
            statement_config=self._statement_config,
            is_many=self._is_many,
            **kwargs,
        )
        # Only preserve accumulated parameters when no explicit parameters are provided
        if parameters is None:
            new_sql._named_parameters.update(self._named_parameters)
            new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        return new_sql

    def add_named_parameter(self, name: str, value: Any) -> "SQL":
        """Add a named parameter and return a new SQL instance.

        Args:
            name: Parameter name
            value: Parameter value

        Returns:
            New SQL instance with the added parameter
        """
        new_sql = SQL(
            self._raw_sql, *self._original_parameters, statement_config=self._statement_config, is_many=self._is_many
        )
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._named_parameters[name] = value
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        return new_sql

    def where(self, condition: "Union[str, exp.Expression]") -> "SQL":
        """Add WHERE condition to the SQL statement.

        Args:
            condition: WHERE condition as string or SQLGlot expression

        Returns:
            New SQL instance with the WHERE condition applied
        """
        # Parse current SQL with copy=False optimization
        current_expr = None
        with contextlib.suppress(ParseError):
            current_expr = sqlglot.parse_one(self._raw_sql, dialect=self._dialect)

        if current_expr is None:
            try:
                current_expr = sqlglot.parse_one(self._raw_sql, dialect=self._dialect)
            except ParseError:
                # Use f-string optimization and copy=False
                subquery_sql = f"SELECT * FROM ({self._raw_sql}) AS subquery"
                current_expr = sqlglot.parse_one(subquery_sql, dialect=self._dialect)

        # Parse condition with copy=False optimization
        condition_expr: exp.Expression
        if isinstance(condition, str):
            try:
                condition_expr = sqlglot.parse_one(condition, dialect=self._dialect, into=exp.Condition)
            except ParseError:
                condition_expr = exp.Condition(this=condition)
        else:
            condition_expr = condition

        # Apply WHERE clause
        if isinstance(current_expr, exp.Select) or supports_where(current_expr):
            new_expr = current_expr.where(condition_expr, copy=False)
        else:
            new_expr = exp.Select().from_(current_expr).where(condition_expr, copy=False)

        # Generate SQL and create new instance
        new_sql_text = new_expr.sql(dialect=self._dialect)
        new_sql = SQL(
            new_sql_text, *self._original_parameters, statement_config=self._statement_config, is_many=self._is_many
        )

        # Preserve state efficiently
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        return new_sql

    def __hash__(self) -> int:
        """Hash value with optimized computation."""
        if self._hash is None:
            # Pre-compute tuple components to avoid multiple tuple() calls
            positional_tuple = tuple(self._positional_parameters)
            named_tuple = tuple(sorted(self._named_parameters.items())) if self._named_parameters else ()

            self._hash = hash((self._raw_sql, positional_tuple, named_tuple, self._is_many, self._is_script))
        return self._hash

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, SQL):
            return False
        return (
            self._raw_sql == other._raw_sql
            and self._positional_parameters == other._positional_parameters
            and self._named_parameters == other._named_parameters
            and self._is_many == other._is_many
            and self._is_script == other._is_script
        )

    def __repr__(self) -> str:
        """String representation."""
        params_str = ""
        if self._named_parameters:
            params_str = f", named_params={self._named_parameters}"
        elif self._positional_parameters:
            params_str = f", params={self._positional_parameters}"

        flags = []
        if self._is_many:
            flags.append("is_many")
        if self._is_script:
            flags.append("is_script")
        flags_str = f", {', '.join(flags)}" if flags else ""

        return f"SQL({self._raw_sql!r}{params_str}{flags_str})"


@mypyc_attr(allow_interpreted_subclasses=True)
class StatementConfig:
    """Configuration for SQL statement processing.

    Provides all attributes that drivers expect for SQL processing.

    Features:
    - Complete parameter processing configuration
    - Caching and execution mode interfaces
    - Support for various database-specific operations
    - Immutable updates via replace() method
    """

    __slots__ = SQL_CONFIG_SLOTS

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
        """Initialize StatementConfig.

        Args:
            parameter_config: Parameter style configuration
            enable_parsing: Enable SQL parsing using sqlglot
            enable_validation: Run SQL validators to check for safety issues
            enable_transformations: Apply SQL transformers
            enable_analysis: Run SQL analyzers for metadata extraction
            enable_expression_simplification: Apply expression simplification
            enable_parameter_type_wrapping: Wrap parameters with type information
            enable_caching: Cache processed SQL statements
            parameter_converter: Handles parameter style conversions
            parameter_validator: Validates parameter usage and styles
            dialect: SQL dialect for parsing and generation
            pre_process_steps: Optional list of preprocessing steps
            post_process_steps: Optional list of postprocessing steps
            execution_mode: Special execution mode
            execution_args: Arguments for special execution modes
            output_transformer: Optional output transformation function
        """
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

    def replace(self, **kwargs: Any) -> "StatementConfig":
        """Immutable update pattern.

        Args:
            **kwargs: Attributes to update

        Returns:
            New StatementConfig instance with updated attributes
        """
        for key in kwargs:
            if key not in SQL_CONFIG_SLOTS:
                msg = f"{key!r} is not a field in {type(self).__name__}"
                raise TypeError(msg)

        current_kwargs = {slot: getattr(self, slot) for slot in SQL_CONFIG_SLOTS}
        current_kwargs.update(kwargs)
        return type(self)(**current_kwargs)

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
                str(self.dialect),
            )
        )

    def __repr__(self) -> str:
        """String representation of the StatementConfig instance."""
        field_strs = []
        for slot in SQL_CONFIG_SLOTS:
            value = getattr(self, slot)
            field_strs.append(f"{slot}={value!r}")
        return f"{self.__class__.__name__}({', '.join(field_strs)})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, type(self)):
            return False

        for slot in SQL_CONFIG_SLOTS:
            self_val = getattr(self, slot)
            other_val = getattr(other, slot)

            if hasattr(self_val, "__class__") and hasattr(other_val, "__class__"):
                if self_val.__class__ != other_val.__class__:
                    return False
                if slot == "parameter_config":
                    if not self._compare_parameter_configs(self_val, other_val):
                        return False
                elif slot in {"parameter_converter", "parameter_validator"}:
                    continue
                elif self_val != other_val:
                    return False
            elif self_val != other_val:
                return False
        return True

    def _compare_parameter_configs(self, config1: Any, config2: Any) -> bool:
        """Compare parameter configs by key attributes."""
        try:
            return (
                config1.default_parameter_style == config2.default_parameter_style
                and config1.supported_parameter_styles == config2.supported_parameter_styles
                and getattr(config1, "supported_execution_parameter_styles", None)
                == getattr(config2, "supported_execution_parameter_styles", None)
            )
        except AttributeError:
            return False


def get_default_config() -> StatementConfig:
    """Get default statement configuration."""
    return StatementConfig()


def get_default_parameter_config() -> ParameterStyleConfig:
    """Get default parameter configuration."""
    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
    )


Statement: TypeAlias = Union[str, exp.Expression, SQL]
