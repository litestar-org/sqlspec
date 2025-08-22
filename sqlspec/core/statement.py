"""SQL statement and configuration management."""

from typing import TYPE_CHECKING, Any, Callable, Final, Optional, Union

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.errors import ParseError
from typing_extensions import TypeAlias

from sqlspec.core.compiler import OperationType, SQLProcessor
from sqlspec.core.parameters import ParameterConverter, ParameterStyle, ParameterStyleConfig, ParameterValidator
from sqlspec.exceptions import SQLSpecError
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

RETURNS_ROWS_OPERATIONS: Final = {"SELECT", "WITH", "VALUES", "TABLE", "SHOW", "DESCRIBE", "PRAGMA"}
MODIFYING_OPERATIONS: Final = {"INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT"}

SQL_CONFIG_SLOTS: Final = (
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

PROCESSED_STATE_SLOTS: Final = (
    "compiled_sql",
    "execution_parameters",
    "parsed_expression",
    "operation_type",
    "validation_errors",
    "is_many",
)


@mypyc_attr(allow_interpreted_subclasses=False)
class ProcessedState:
    """Processing results for SQL statements.

    Contains the compiled SQL, execution parameters, parsed expression,
    operation type, and validation errors for a processed SQL statement.
    """

    __slots__ = PROCESSED_STATE_SLOTS
    operation_type: "OperationType"

    def __init__(
        self,
        compiled_sql: str,
        execution_parameters: Any,
        parsed_expression: "Optional[exp.Expression]" = None,
        operation_type: "OperationType" = "UNKNOWN",
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


@mypyc_attr(allow_interpreted_subclasses=False)
class SQL:
    """SQL statement with parameter and filter support.

    Represents a SQL statement that can be compiled with parameters and filters.
    Supports both positional and named parameters, statement filtering,
    and various execution modes including batch operations.
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
        config = statement_config or self._create_auto_config(statement, parameters, kwargs)
        self._statement_config = config
        self._dialect = self._normalize_dialect(config.dialect)
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
                dialect = self._dialect
                self._raw_sql = statement.sql(dialect=str(dialect) if dialect else None)

            self._is_many = is_many if is_many is not None else self._should_auto_detect_many(parameters)

        self._original_parameters = parameters
        self._process_parameters(*parameters, **kwargs)

    def _create_auto_config(
        self, statement: "Union[str, exp.Expression, 'SQL']", parameters: tuple, kwargs: dict[str, Any]
    ) -> "StatementConfig":
        """Create default StatementConfig when none provided.

        Args:
            statement: The SQL statement
            parameters: Statement parameters
            kwargs: Additional keyword arguments

        Returns:
            Default StatementConfig instance
        """
        return get_default_config()

    def _normalize_dialect(self, dialect: "Optional[DialectType]") -> "Optional[str]":
        """Convert dialect to string representation.

        Args:
            dialect: Dialect type or string

        Returns:
            String representation of the dialect or None
        """
        if dialect is None:
            return None
        if isinstance(dialect, str):
            return dialect
        return dialect.__class__.__name__.lower()

    def _init_from_sql_object(self, sql_obj: "SQL") -> None:
        """Initialize instance attributes from existing SQL object.

        Args:
            sql_obj: Existing SQL object to copy from
        """
        self._raw_sql = sql_obj._raw_sql
        self._filters = sql_obj._filters.copy()
        self._named_parameters = sql_obj._named_parameters.copy()
        self._positional_parameters = sql_obj._positional_parameters.copy()
        self._is_many = sql_obj._is_many
        self._is_script = sql_obj._is_script
        if sql_obj._processed_state is not Empty:
            self._processed_state = sql_obj._processed_state

    def _should_auto_detect_many(self, parameters: tuple) -> bool:
        """Detect execute_many mode from parameter structure.

        Args:
            parameters: Parameter tuple to analyze

        Returns:
            True if parameters indicate batch execution
        """
        if len(parameters) == 1 and isinstance(parameters[0], list):
            param_list = parameters[0]
            if param_list and all(isinstance(item, (tuple, list)) for item in param_list):
                return len(param_list) > 1
        return False

    def _process_parameters(self, *parameters: Any, dialect: Optional[str] = None, **kwargs: Any) -> None:
        """Process and organize parameters and filters.

        Args:
            *parameters: Variable parameters and filters
            dialect: SQL dialect override
            **kwargs: Additional named parameters
        """
        if dialect is not None:
            self._dialect = self._normalize_dialect(dialect)

        if "is_script" in kwargs:
            self._is_script = bool(kwargs.pop("is_script"))

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
                        # For drivers with native list expansion support, each item in the tuple/list
                        # should be treated as a separate parameter (but preserve inner lists/arrays)
                        # This allows passing arrays/lists as single JSONB parameters
                        self._positional_parameters.extend(param)
                else:
                    self._positional_parameters.append(param)
            else:
                self._positional_parameters.extend(actual_params)

        self._named_parameters.update(kwargs)

    @property
    def sql(self) -> str:
        """Get the raw SQL string."""
        return self._raw_sql

    @property
    def parameters(self) -> Any:
        """Get the original parameters."""
        if self._named_parameters:
            return self._named_parameters
        return self._positional_parameters or []

    @property
    def operation_type(self) -> "OperationType":
        """SQL operation type."""
        if self._processed_state is Empty:
            return "UNKNOWN"
        return self._processed_state.operation_type

    @property
    def statement_config(self) -> "StatementConfig":
        """Statement configuration."""
        return self._statement_config

    @property
    def expression(self) -> "Optional[exp.Expression]":
        """SQLGlot expression."""
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
        """Validation errors."""
        if self._processed_state is Empty:
            return []
        return self._processed_state.validation_errors.copy()

    @property
    def has_errors(self) -> bool:
        """Check if there are validation errors."""
        return len(self.validation_errors) > 0

    def returns_rows(self) -> bool:
        """Check if statement returns rows.

        Returns:
            True if the SQL statement returns result rows
        """
        if self._processed_state is Empty:
            self.compile()
            if self._processed_state is Empty:
                return False

        op_type = self._processed_state.operation_type
        if op_type in RETURNS_ROWS_OPERATIONS:
            return True

        if self._processed_state.parsed_expression:
            expr = self._processed_state.parsed_expression
            if isinstance(expr, (exp.Insert, exp.Update, exp.Delete)) and expr.args.get("returning"):
                return True

        return False

    def is_modifying_operation(self) -> bool:
        """Check if the SQL statement is a modifying operation.

        Returns:
            True if the operation modifies data (INSERT/UPDATE/DELETE)
        """
        if self._processed_state is Empty:
            return False

        op_type = self._processed_state.operation_type
        if op_type in MODIFYING_OPERATIONS:
            return True

        if self._processed_state.parsed_expression:
            return isinstance(self._processed_state.parsed_expression, (exp.Insert, exp.Update, exp.Delete, exp.Merge))

        return False

    def compile(self) -> tuple[str, Any]:
        """Compile SQL statement with parameters.

        Returns:
            Tuple of compiled SQL string and execution parameters
        """
        if self._processed_state is Empty:
            try:
                config = self._statement_config
                raw_sql = self._raw_sql
                params = self._named_parameters or self._positional_parameters
                is_many = self._is_many
                compiled_result = SQLProcessor(config).compile(raw_sql, params, is_many=is_many)

                self._processed_state = ProcessedState(
                    compiled_sql=compiled_result.compiled_sql,
                    execution_parameters=compiled_result.execution_parameters,
                    parsed_expression=compiled_result.expression,
                    operation_type=compiled_result.operation_type,
                    validation_errors=[],
                    is_many=self._is_many,
                )
            except SQLSpecError:
                raise
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
        """Create copy marked for script execution.

        Returns:
            New SQL instance configured for script execution
        """
        original_params = self._original_parameters
        config = self._statement_config
        is_many = self._is_many
        new_sql = SQL(self._raw_sql, *original_params, statement_config=config, is_many=is_many)
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        new_sql._is_script = True
        return new_sql

    def copy(
        self, statement: "Optional[Union[str, exp.Expression]]" = None, parameters: Optional[Any] = None, **kwargs: Any
    ) -> "SQL":
        """Create copy with modifications.

        Args:
            statement: New SQL statement to use
            parameters: New parameters to use
            **kwargs: Additional modifications

        Returns:
            New SQL instance with modifications applied
        """
        new_sql = SQL(
            statement or self._raw_sql,
            *(parameters if parameters is not None else self._original_parameters),
            statement_config=self._statement_config,
            is_many=self._is_many,
            **kwargs,
        )
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
        original_params = self._original_parameters
        config = self._statement_config
        is_many = self._is_many
        new_sql = SQL(self._raw_sql, *original_params, statement_config=config, is_many=is_many)
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
        try:
            current_expr = sqlglot.parse_one(self._raw_sql, dialect=self._dialect)
        except ParseError:
            subquery_sql = f"SELECT * FROM ({self._raw_sql}) AS subquery"
            current_expr = sqlglot.parse_one(subquery_sql, dialect=self._dialect)

        condition_expr: exp.Expression
        if isinstance(condition, str):
            try:
                condition_expr = sqlglot.parse_one(condition, dialect=self._dialect, into=exp.Condition)
            except ParseError:
                condition_expr = exp.Condition(this=condition)
        else:
            condition_expr = condition

        if isinstance(current_expr, exp.Select) or supports_where(current_expr):
            new_expr = current_expr.where(condition_expr, copy=False)
        else:
            new_expr = exp.Select().from_(current_expr).where(condition_expr, copy=False)

        original_params = self._original_parameters
        config = self._statement_config
        is_many = self._is_many
        new_sql = SQL(new_expr, *original_params, statement_config=config, is_many=is_many)

        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        return new_sql

    def __hash__(self) -> int:
        """Hash value computation."""
        if self._hash is None:
            positional_tuple = tuple(self._positional_parameters)
            named_tuple = tuple(sorted(self._named_parameters.items())) if self._named_parameters else ()
            raw_sql = self._raw_sql
            is_many = self._is_many
            is_script = self._is_script
            self._hash = hash((raw_sql, positional_tuple, named_tuple, is_many, is_script))
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
        params_parts = []
        if self._positional_parameters:
            params_parts.append(f"params={self._positional_parameters}")
        if self._named_parameters:
            params_parts.append(f"named_params={self._named_parameters}")
        params_str = f", {', '.join(params_parts)}" if params_parts else ""

        flags = []
        if self._is_many:
            flags.append("is_many")
        if self._is_script:
            flags.append("is_script")
        flags_str = f", {', '.join(flags)}" if flags else ""

        return f"SQL({self._raw_sql!r}{params_str}{flags_str})"


@mypyc_attr(allow_interpreted_subclasses=False)
class StatementConfig:
    """Configuration for SQL statement processing.

    Controls SQL parsing, validation, transformations, parameter handling,
    and other processing options for SQL statements.
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
            enable_parsing: Enable SQL parsing
            enable_validation: Run SQL validators
            enable_transformations: Apply SQL transformers
            enable_analysis: Run SQL analyzers
            enable_expression_simplification: Apply expression simplification
            enable_parameter_type_wrapping: Wrap parameters with type information
            enable_caching: Cache processed SQL statements
            parameter_converter: Handles parameter style conversions
            parameter_validator: Validates parameter usage and styles
            dialect: SQL dialect
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

        current_kwargs: dict[str, Any] = {
            "parameter_config": self.parameter_config,
            "enable_parsing": self.enable_parsing,
            "enable_validation": self.enable_validation,
            "enable_transformations": self.enable_transformations,
            "enable_analysis": self.enable_analysis,
            "enable_expression_simplification": self.enable_expression_simplification,
            "enable_parameter_type_wrapping": self.enable_parameter_type_wrapping,
            "enable_caching": self.enable_caching,
            "parameter_converter": self.parameter_converter,
            "parameter_validator": self.parameter_validator,
            "dialect": self.dialect,
            "pre_process_steps": self.pre_process_steps,
            "post_process_steps": self.post_process_steps,
            "execution_mode": self.execution_mode,
            "execution_args": self.execution_args,
            "output_transformer": self.output_transformer,
        }
        current_kwargs.update(kwargs)
        return type(self)(**current_kwargs)

    def __hash__(self) -> int:
        """Hash based on configuration settings."""
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
        field_strs = [
            f"parameter_config={self.parameter_config!r}",
            f"enable_parsing={self.enable_parsing!r}",
            f"enable_validation={self.enable_validation!r}",
            f"enable_transformations={self.enable_transformations!r}",
            f"enable_analysis={self.enable_analysis!r}",
            f"enable_expression_simplification={self.enable_expression_simplification!r}",
            f"enable_parameter_type_wrapping={self.enable_parameter_type_wrapping!r}",
            f"enable_caching={self.enable_caching!r}",
            f"parameter_converter={self.parameter_converter!r}",
            f"parameter_validator={self.parameter_validator!r}",
            f"dialect={self.dialect!r}",
            f"pre_process_steps={self.pre_process_steps!r}",
            f"post_process_steps={self.post_process_steps!r}",
            f"execution_mode={self.execution_mode!r}",
            f"execution_args={self.execution_args!r}",
            f"output_transformer={self.output_transformer!r}",
        ]
        return f"{self.__class__.__name__}({', '.join(field_strs)})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, type(self)):
            return False

        if not self._compare_parameter_configs(self.parameter_config, other.parameter_config):
            return False

        return (
            self.enable_parsing == other.enable_parsing
            and self.enable_validation == other.enable_validation
            and self.enable_transformations == other.enable_transformations
            and self.enable_analysis == other.enable_analysis
            and self.enable_expression_simplification == other.enable_expression_simplification
            and self.enable_parameter_type_wrapping == other.enable_parameter_type_wrapping
            and self.enable_caching == other.enable_caching
            and self.dialect == other.dialect
            and self.pre_process_steps == other.pre_process_steps
            and self.post_process_steps == other.post_process_steps
            and self.execution_mode == other.execution_mode
            and self.execution_args == other.execution_args
            and self.output_transformer == other.output_transformer
        )

    def _compare_parameter_configs(self, config1: Any, config2: Any) -> bool:
        """Compare parameter configs."""
        return bool(
            config1.default_parameter_style == config2.default_parameter_style
            and config1.supported_parameter_styles == config2.supported_parameter_styles
            and config1.supported_execution_parameter_styles == config2.supported_execution_parameter_styles
        )


def get_default_config() -> StatementConfig:
    """Get default statement configuration.

    Returns:
        StatementConfig with default settings
    """
    return StatementConfig()


def get_default_parameter_config() -> ParameterStyleConfig:
    """Get default parameter configuration.

    Returns:
        ParameterStyleConfig with QMARK style as default
    """
    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
    )


Statement: TypeAlias = Union[str, exp.Expression, SQL]
