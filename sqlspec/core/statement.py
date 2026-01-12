"""SQL statement and configuration management."""

import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, TypeAlias

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.errors import ParseError

import sqlspec.exceptions
from sqlspec.core import pipeline
from sqlspec.core.cache import FiltersView
from sqlspec.core.compiler import OperationProfile, OperationType
from sqlspec.core.explain import ExplainFormat, ExplainOptions
from sqlspec.core.parameters import (
    ParameterConverter,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    ParameterValidator,
)
from sqlspec.core.query_modifiers import (
    apply_limit,
    apply_offset,
    apply_select_only,
    apply_where,
    create_between_condition,
    create_condition,
    create_in_condition,
    create_not_in_condition,
    expr_eq,
    expr_gt,
    expr_gte,
    expr_ilike,
    expr_is_not_null,
    expr_is_null,
    expr_like,
    expr_lt,
    expr_lte,
    expr_neq,
    extract_column_name,
    safe_modify_with_cte,
)
from sqlspec.typing import Empty, EmptyEnum
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_statement_filter, supports_where

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder import QueryBuilder
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
_ORDER_PARTS_COUNT: Final = 2
_MAX_PARAM_COLLISION_ATTEMPTS: Final = 1000


SQL_CONFIG_SLOTS: Final = (
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
    "statement_transformers",
    "parameter_config",
    "parameter_converter",
    "parameter_validator",
)

PROCESSED_STATE_SLOTS: Final = (
    "compiled_sql",
    "execution_parameters",
    "parsed_expression",
    "operation_type",
    "parameter_casts",
    "parameter_profile",
    "operation_profile",
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
        parsed_expression: "exp.Expression | None" = None,
        operation_type: "OperationType" = "UNKNOWN",
        parameter_casts: "dict[int, str] | None" = None,
        validation_errors: "list[str] | None" = None,
        parameter_profile: "ParameterProfile | None" = None,
        operation_profile: "OperationProfile | None" = None,
        is_many: bool = False,
    ) -> None:
        self.compiled_sql = compiled_sql
        self.execution_parameters = execution_parameters
        self.parsed_expression = parsed_expression
        self.operation_type = operation_type
        self.parameter_casts = parameter_casts or {}
        self.validation_errors = validation_errors or []
        self.parameter_profile = parameter_profile or ParameterProfile.empty()
        self.operation_profile = operation_profile or OperationProfile.empty()
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
        "_raw_expression",
        "_raw_sql",
        "_sql_param_counters",
        "_statement_config",
    )

    # Type annotation for mypyc compatibility
    _sql_param_counters: "dict[str, int]"

    def __init__(
        self,
        statement: "str | exp.Expression | 'SQL'",
        *parameters: "Any | StatementFilter | list[Any | StatementFilter]",
        statement_config: "StatementConfig | None" = None,
        is_many: bool | None = None,
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
        self._processed_state: EmptyEnum | ProcessedState = Empty
        self._hash: int | None = None
        self._filters: list[StatementFilter] = []
        self._named_parameters: dict[str, Any] = {}
        self._positional_parameters: list[Any] = []
        self._sql_param_counters = {}
        self._is_script = False
        self._raw_expression: exp.Expression | None = None

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
                self._raw_expression = statement

            self._is_many = is_many if is_many is not None else self._should_auto_detect_many(parameters)

        self._original_parameters = parameters
        self._process_parameters(*parameters, **kwargs)

    def _create_auto_config(
        self, _statement: "str | exp.Expression | 'SQL'", _parameters: tuple, _kwargs: "dict[str, Any]"
    ) -> "StatementConfig":
        """Create default StatementConfig when none provided.

        Args:
            _statement: The SQL statement (unused)
            _parameters: Statement parameters (unused)
            _kwargs: Additional keyword arguments (unused)

        Returns:
            Default StatementConfig instance
        """
        return get_default_config()

    def _normalize_dialect(self, dialect: "DialectType") -> "str | None":
        """Convert dialect to string representation.

        Args:
            dialect: Dialect type, string, or None

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
        self._raw_sql = sql_obj.raw_sql
        self._raw_expression = sql_obj.raw_expression
        self._filters = sql_obj.filters.copy()
        self._named_parameters = sql_obj.named_parameters.copy()
        self._positional_parameters = sql_obj.positional_parameters.copy()
        self._sql_param_counters = sql_obj._sql_param_counters.copy()
        self._is_many = sql_obj.is_many
        self._is_script = sql_obj.is_script
        if sql_obj.is_processed:
            self._processed_state = sql_obj.get_processed_state()

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

    def _process_parameters(self, *parameters: Any, dialect: str | None = None, **kwargs: Any) -> None:
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

        self._filters.extend(self._extract_filters(parameters))
        self._normalize_parameters(parameters)
        self._named_parameters.update(kwargs)

    def _extract_filters(self, parameters: "tuple[Any, ...]") -> "list[StatementFilter]":
        return [p for p in parameters if is_statement_filter(p)]

    def _normalize_parameters(self, parameters: "tuple[Any, ...]") -> None:
        actual_params = [p for p in parameters if not is_statement_filter(p)]
        if not actual_params:
            return

        if len(actual_params) == 1:
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

    @property
    def sql(self) -> str:
        """Get the raw SQL string."""
        return self._raw_sql

    @property
    def raw_sql(self) -> str:
        """Get raw SQL string (public API).

        Returns:
            The raw SQL string
        """
        return self._raw_sql

    @property
    def parameters(self) -> Any:
        """Get the original parameters."""
        if self._named_parameters:
            return self._named_parameters
        return self._positional_parameters or []

    @property
    def positional_parameters(self) -> "list[Any]":
        """Get positional parameters (public API)."""
        return self._positional_parameters or []

    @property
    def named_parameters(self) -> "dict[str, Any]":
        """Get named parameters (public API)."""
        return self._named_parameters

    @property
    def original_parameters(self) -> Any:
        """Get original parameters (public API)."""
        return self._original_parameters

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
    def expression(self) -> "exp.Expression | None":
        """SQLGlot expression."""
        if self._processed_state is not Empty:
            return self._processed_state.parsed_expression
        return self._raw_expression

    @property
    def raw_expression(self) -> "exp.Expression | None":
        """Original expression supplied at construction, if available."""
        return self._raw_expression

    @property
    def filters(self) -> "list[StatementFilter]":
        """Applied filters."""
        return self._filters.copy()

    def get_filters_view(self) -> "FiltersView":
        """Get zero-copy filters view (public API).

        Returns:
            Read-only view of filters without copying
        """
        return FiltersView(self._filters)

    @property
    def is_processed(self) -> bool:
        """Check if SQL has been processed (public API)."""
        return self._processed_state is not Empty

    def get_processed_state(self) -> Any:
        """Get processed state (public API)."""
        return self._processed_state

    @property
    def dialect(self) -> "str | None":
        """SQL dialect."""
        return self._dialect

    @property
    def statement_expression(self) -> "exp.Expression | None":
        """Get parsed statement expression (public API).

        Returns:
            Parsed SQLGlot expression or None if not parsed
        """
        if self._processed_state is not Empty:
            return self._processed_state.parsed_expression
        return self._raw_expression

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

        profile = self._processed_state.operation_profile
        if profile.returns_rows:
            return True

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

        profile = self._processed_state.operation_profile
        if profile.modifies_rows:
            return True

        op_type = self._processed_state.operation_type
        if op_type in MODIFYING_OPERATIONS:
            return True

        if self._processed_state.parsed_expression:
            return isinstance(self._processed_state.parsed_expression, (exp.Insert, exp.Update, exp.Delete, exp.Merge))

        return False

    def compile(self) -> "tuple[str, Any]":
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
                compiled_result = pipeline.compile_with_pipeline(
                    config, raw_sql, params, is_many=is_many, expression=self._raw_expression
                )

                self._processed_state = ProcessedState(
                    compiled_sql=compiled_result.compiled_sql,
                    execution_parameters=compiled_result.execution_parameters,
                    parsed_expression=compiled_result.expression,
                    operation_type=compiled_result.operation_type,
                    parameter_casts=compiled_result.parameter_casts,
                    parameter_profile=compiled_result.parameter_profile,
                    operation_profile=compiled_result.operation_profile,
                    validation_errors=[],
                    is_many=self._is_many,
                )
            except sqlspec.exceptions.SQLSpecError:
                raise
            except Exception as e:
                self._processed_state = self._handle_compile_failure(e)

        return self._processed_state.compiled_sql, self._processed_state.execution_parameters

    def as_script(self) -> "SQL":
        """Create copy marked for script execution.

        Returns:
            New SQL instance configured for script execution
        """
        original_params = self._original_parameters
        config = self._statement_config
        is_many = self._is_many
        statement_seed = self._raw_expression or self._raw_sql
        new_sql = SQL(statement_seed, *original_params, statement_config=config, is_many=is_many)
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        new_sql._is_script = True
        return new_sql

    def copy(
        self, statement: "str | exp.Expression | None" = None, parameters: Any | None = None, **kwargs: Any
    ) -> "SQL":
        """Create copy with modifications.

        Args:
            statement: New SQL statement to use
            parameters: New parameters to use
            **kwargs: Additional modifications

        Returns:
            New SQL instance with modifications applied
        """
        statement_expression = self._raw_expression if statement is None else statement
        new_sql = SQL(
            statement_expression or self._raw_sql,
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

    def _handle_compile_failure(self, error: Exception) -> ProcessedState:
        logger.debug("Processing failed, using fallback: %s", error)
        return ProcessedState(
            compiled_sql=self._raw_sql,
            execution_parameters=self._named_parameters or self._positional_parameters,
            operation_type="UNKNOWN",
            parameter_casts={},
            parameter_profile=ParameterProfile.empty(),
            operation_profile=OperationProfile.empty(),
            is_many=self._is_many,
        )

    # ==========================================================================
    # Parameter Generation Helpers
    # ==========================================================================

    def _generate_sql_param_name(self, base_name: str) -> str:
        """Generate unique parameter name with _sqlspec_ prefix.

        Uses _sqlspec_ prefix to avoid collision with user-provided parameters.
        Auto-generated parameters are namespaced to prevent conflicts.

        Args:
            base_name: The base name for the parameter (e.g., column name)

        Returns:
            A unique parameter name that doesn't exist in current parameters
        """
        prefixed_base = f"_sqlspec_{base_name}"
        current_index = self._sql_param_counters.get(prefixed_base, 0)

        if prefixed_base not in self._named_parameters:
            self._sql_param_counters[prefixed_base] = current_index
            return prefixed_base

        next_index = current_index + 1
        candidate = f"{prefixed_base}_{next_index}"

        while candidate in self._named_parameters:
            next_index += 1
            if next_index > _MAX_PARAM_COLLISION_ATTEMPTS:
                return f"{prefixed_base}_{uuid.uuid4().hex[:8]}"
            candidate = f"{prefixed_base}_{next_index}"

        self._sql_param_counters[prefixed_base] = next_index
        return candidate

    def _get_or_parse_expression(self) -> exp.Expression:
        """Get the current expression or parse the raw SQL.

        Returns:
            The SQLGlot expression for this statement
        """
        if self.statement_expression is not None:
            return self.statement_expression.copy()
        if not self._statement_config.enable_parsing:
            return exp.Select().from_(f"({self._raw_sql})")
        try:
            return sqlglot.parse_one(self._raw_sql, dialect=self._dialect)
        except ParseError:
            return exp.Select().from_(f"({self._raw_sql})")

    def _create_modified_copy_with_expression(self, new_expr: exp.Expression) -> "SQL":
        """Create a new SQL instance with a modified expression.

        Args:
            new_expr: The new SQLGlot expression

        Returns:
            New SQL instance with the expression and copied state
        """
        new_sql = SQL(
            new_expr, *self._original_parameters, statement_config=self._statement_config, is_many=self._is_many
        )
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        new_sql._sql_param_counters = self._sql_param_counters.copy()
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
        statement_seed = self._raw_expression or self._raw_sql
        new_sql = SQL(statement_seed, *original_params, statement_config=config, is_many=is_many)
        new_sql._named_parameters.update(self._named_parameters)
        new_sql._named_parameters[name] = value
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        return new_sql

    def where(self, condition: "str | exp.Expression") -> "SQL":
        """Add WHERE condition to the SQL statement.

        Args:
            condition: WHERE condition as string or SQLGlot expression

        Returns:
            New SQL instance with the WHERE condition applied
        """
        if self.statement_expression is not None:
            current_expr = self.statement_expression.copy()
        elif not self._statement_config.enable_parsing:
            current_expr = exp.Select().from_(f"({self._raw_sql})")
        else:
            try:
                current_expr = sqlglot.parse_one(self._raw_sql, dialect=self._dialect)
            except ParseError:
                subquery_sql = f"SELECT * FROM ({self._raw_sql}) AS subquery"
                current_expr = sqlglot.parse_one(subquery_sql, dialect=self._dialect)

        condition_expr: exp.Expression
        if isinstance(condition, str):
            if not self._statement_config.enable_parsing:
                condition_expr = exp.Condition(this=condition)
            else:
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

    # ==========================================================================
    # Parameterized WHERE Methods (using shared utilities)
    # ==========================================================================

    def where_eq(self, column: "str | exp.Column", value: Any) -> "SQL":
        """Add WHERE column = value condition.

        Args:
            column: Column name or expression
            value: Value to compare against

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_eq)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = value
        return new_sql

    def where_neq(self, column: "str | exp.Column", value: Any) -> "SQL":
        """Add WHERE column != value condition.

        Args:
            column: Column name or expression
            value: Value to compare against

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_neq)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = value
        return new_sql

    def where_lt(self, column: "str | exp.Column", value: Any) -> "SQL":
        """Add WHERE column < value condition.

        Args:
            column: Column name or expression
            value: Value to compare against

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_lt)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = value
        return new_sql

    def where_lte(self, column: "str | exp.Column", value: Any) -> "SQL":
        """Add WHERE column <= value condition.

        Args:
            column: Column name or expression
            value: Value to compare against

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_lte)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = value
        return new_sql

    def where_gt(self, column: "str | exp.Column", value: Any) -> "SQL":
        """Add WHERE column > value condition.

        Args:
            column: Column name or expression
            value: Value to compare against

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_gt)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = value
        return new_sql

    def where_gte(self, column: "str | exp.Column", value: Any) -> "SQL":
        """Add WHERE column >= value condition.

        Args:
            column: Column name or expression
            value: Value to compare against

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_gte)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = value
        return new_sql

    def where_like(self, column: "str | exp.Column", pattern: str) -> "SQL":
        """Add WHERE column LIKE pattern condition.

        Args:
            column: Column name or expression
            pattern: LIKE pattern (e.g., '%search%')

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_like)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = pattern
        return new_sql

    def where_ilike(self, column: "str | exp.Column", pattern: str) -> "SQL":
        """Add WHERE column ILIKE pattern condition (case-insensitive).

        Args:
            column: Column name or expression
            pattern: ILIKE pattern (e.g., '%search%')

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        param_name = self._generate_sql_param_name(col_name)
        condition = create_condition(column, param_name, expr_ilike)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[param_name] = pattern
        return new_sql

    def where_is_null(self, column: "str | exp.Column") -> "SQL":
        """Add WHERE column IS NULL condition.

        Args:
            column: Column name or expression

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        condition = create_condition(column, "_unused", expr_is_null)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        return self._create_modified_copy_with_expression(new_expr)

    def where_is_not_null(self, column: "str | exp.Column") -> "SQL":
        """Add WHERE column IS NOT NULL condition.

        Args:
            column: Column name or expression

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        condition = create_condition(column, "_unused", expr_is_not_null)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        return self._create_modified_copy_with_expression(new_expr)

    def where_in(self, column: "str | exp.Column", values: "Sequence[Any]") -> "SQL":
        """Add WHERE column IN (values) condition.

        Args:
            column: Column name or expression
            values: Sequence of values for IN clause

        Returns:
            New SQL instance with WHERE condition applied
        """
        if not values:
            expression = self._get_or_parse_expression()
            false_condition = exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(0))
            new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, false_condition))
            return self._create_modified_copy_with_expression(new_expr)

        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)

        param_names: list[str] = []
        param_values: dict[str, Any] = {}
        for i, val in enumerate(values):
            param_name = self._generate_sql_param_name(f"{col_name}_in_{i}")
            param_names.append(param_name)
            param_values[param_name] = val

        condition = create_in_condition(column, param_names)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters.update(param_values)
        return new_sql

    def where_not_in(self, column: "str | exp.Column", values: "Sequence[Any]") -> "SQL":
        """Add WHERE column NOT IN (values) condition.

        Args:
            column: Column name or expression
            values: Sequence of values for NOT IN clause

        Returns:
            New SQL instance with WHERE condition applied
        """
        if not values:
            return self

        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)

        param_names: list[str] = []
        param_values: dict[str, Any] = {}
        for i, val in enumerate(values):
            param_name = self._generate_sql_param_name(f"{col_name}_not_in_{i}")
            param_names.append(param_name)
            param_values[param_name] = val

        condition = create_not_in_condition(column, param_names)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters.update(param_values)
        return new_sql

    def where_between(self, column: "str | exp.Column", low: Any, high: Any) -> "SQL":
        """Add WHERE column BETWEEN low AND high condition.

        Args:
            column: Column name or expression
            low: Lower bound value
            high: Upper bound value

        Returns:
            New SQL instance with WHERE condition applied
        """
        expression = self._get_or_parse_expression()
        col_name = extract_column_name(column)
        low_param = self._generate_sql_param_name(f"{col_name}_low")
        high_param = self._generate_sql_param_name(f"{col_name}_high")
        condition = create_between_condition(column, low_param, high_param)
        new_expr = safe_modify_with_cte(expression, lambda e: apply_where(e, condition))
        new_sql = self._create_modified_copy_with_expression(new_expr)
        new_sql._named_parameters[low_param] = low
        new_sql._named_parameters[high_param] = high
        return new_sql

    def order_by(self, *items: "str | exp.Expression", desc: bool = False) -> "SQL":
        """Add ORDER BY clause to the SQL statement.

        Args:
            *items: ORDER BY expressions as strings or SQLGlot expressions
            desc: Apply descending order to each item

        Returns:
            New SQL instance with ORDER BY applied
        """
        if not items:
            return self

        if self.statement_expression is not None:
            current_expr = self.statement_expression.copy()
        elif not self._statement_config.enable_parsing:
            current_expr = exp.Select().from_(f"({self._raw_sql})")
        else:
            try:
                current_expr = sqlglot.parse_one(self._raw_sql, dialect=self._dialect)
            except ParseError:
                current_expr = exp.Select().from_(f"({self._raw_sql})")

        def parse_order_item(order_item: str) -> exp.Expression:
            normalized = order_item.strip()
            if not normalized:
                return exp.column(order_item)

            if self._statement_config.enable_parsing:
                try:
                    parsed = sqlglot.parse_one(normalized, dialect=self._dialect, into=exp.Ordered)
                except ParseError:
                    parsed = None
                if parsed is not None:
                    return parsed

            parts = normalized.rsplit(None, 1)
            if len(parts) == _ORDER_PARTS_COUNT and parts[1].lower() in {"asc", "desc"}:
                base_expr = exp.column(parts[0]) if parts[0] else exp.column(normalized)
                return base_expr.desc() if parts[1].lower() == "desc" else base_expr.asc()

            return exp.column(normalized)

        new_expr = current_expr
        for item in items:
            if isinstance(item, str):
                order_expr = parse_order_item(item)
                if desc and not isinstance(order_expr, exp.Ordered):
                    order_expr = order_expr.desc()
            else:
                order_expr = item.desc() if desc and not isinstance(item, exp.Ordered) else item
            if isinstance(new_expr, exp.Select):
                new_expr = new_expr.order_by(order_expr, copy=False)
            else:
                new_expr = exp.Select().from_(new_expr).order_by(order_expr)

        original_params = self._original_parameters
        config = self._statement_config
        is_many = self._is_many
        new_sql = SQL(new_expr, *original_params, statement_config=config, is_many=is_many)

        new_sql._named_parameters.update(self._named_parameters)
        new_sql._positional_parameters = self._positional_parameters.copy()
        new_sql._filters = self._filters.copy()
        return new_sql

    # ==========================================================================
    # Pagination Methods
    # ==========================================================================

    def limit(self, value: int) -> "SQL":
        """Add LIMIT clause to the SQL statement.

        Args:
            value: Maximum number of rows to return

        Returns:
            New SQL instance with LIMIT applied

        Raises:
            SQLSpecError: If statement is not a SELECT
        """
        expression = self._get_or_parse_expression()
        new_expr = safe_modify_with_cte(expression, lambda e: apply_limit(e, value))
        return self._create_modified_copy_with_expression(new_expr)

    def offset(self, value: int) -> "SQL":
        """Add OFFSET clause to the SQL statement.

        Args:
            value: Number of rows to skip

        Returns:
            New SQL instance with OFFSET applied

        Raises:
            SQLSpecError: If statement is not a SELECT
        """
        expression = self._get_or_parse_expression()
        new_expr = safe_modify_with_cte(expression, lambda e: apply_offset(e, value))
        return self._create_modified_copy_with_expression(new_expr)

    def paginate(self, page: int, page_size: int) -> "SQL":
        """Add LIMIT and OFFSET for pagination.

        Args:
            page: Page number (1-indexed)
            page_size: Number of items per page

        Returns:
            New SQL instance with LIMIT and OFFSET applied

        Example:
            # Get page 3 with 20 items per page
            stmt = SQL("SELECT * FROM users").paginate(3, 20)
            # Results in: SELECT * FROM users LIMIT 20 OFFSET 40
        """
        if page < 1:
            msg = "paginate page must be >= 1"
            raise sqlspec.exceptions.SQLSpecError(msg)
        if page_size < 1:
            msg = "paginate page_size must be >= 1"
            raise sqlspec.exceptions.SQLSpecError(msg)
        offset_value = (page - 1) * page_size
        return self.limit(page_size).offset(offset_value)

    # ==========================================================================
    # Column Projection Methods
    # ==========================================================================

    def select_only(self, *columns: "str | exp.Expression") -> "SQL":
        """Replace SELECT columns with only the specified columns.

        This is useful for narrowing down the columns returned by a query
        without modifying the FROM clause or WHERE conditions.

        Args:
            *columns: Column names or expressions to select

        Returns:
            New SQL instance with only the specified columns

        Example:
            stmt = SQL("SELECT * FROM users WHERE active = 1")
            narrow = stmt.select_only("id", "name", "email")
            # Results in: SELECT id, name, email FROM users WHERE active = 1
        """
        if not columns:
            return self

        expression = self._get_or_parse_expression()
        new_expr = safe_modify_with_cte(expression, lambda e: apply_select_only(e, columns))
        return self._create_modified_copy_with_expression(new_expr)

    def explain(self, analyze: bool = False, verbose: bool = False, format: "str | None" = None) -> "SQL":
        """Create an EXPLAIN statement for this SQL.

        Wraps the current SQL statement in an EXPLAIN clause with
        dialect-aware syntax generation.

        Args:
            analyze: Execute the statement and show actual runtime statistics
            verbose: Show additional information
            format: Output format (TEXT, JSON, XML, YAML, TREE, TRADITIONAL)

        Returns:
            New SQL instance containing the EXPLAIN statement

        Examples:
            Basic EXPLAIN:
                stmt = SQL("SELECT * FROM users")
                explain_stmt = stmt.explain()

            With options:
                explain_stmt = stmt.explain(analyze=True, format="json")
        """
        from sqlspec.builder import Explain

        fmt = None
        if format is not None:
            fmt = ExplainFormat(format.lower())

        options = ExplainOptions(analyze=analyze, verbose=verbose, format=fmt)

        explain_builder = Explain(self, dialect=self._dialect, options=options)
        return explain_builder.build()

    def builder(self, dialect: "DialectType | None" = None) -> "QueryBuilder":
        """Create a query builder seeded from this SQL statement.

        Args:
            dialect: Optional SQL dialect override for parsing and rendering.

        Returns:
            QueryBuilder instance initialized with the parsed statement.

        Raises:
            SQLBuilderError: If the statement cannot be parsed.

        Notes:
            Statements outside the DML set return an ExpressionBuilder without
            DML-specific helper methods.
        """
        if self._is_many:
            msg = "QueryBuilder does not support execute_many SQL statements."
            raise sqlspec.exceptions.SQLBuilderError(msg)

        from sqlspec.builder import Delete, ExpressionBuilder, Insert, Merge, Select, Update

        builder_dialect = dialect or self._dialect
        converter = self._statement_config.parameter_converter or ParameterConverter(
            self._statement_config.parameter_validator
        )
        raw_params = self.parameters
        converted_sql, converted_params = converter.convert_placeholder_style(
            self._raw_sql, raw_params, ParameterStyle.NAMED_COLON, is_many=False
        )

        if self._raw_expression is not None and converted_sql == self._raw_sql and (builder_dialect == self._dialect):
            expression = self._raw_expression.copy()
        else:
            try:
                expression = sqlglot.parse_one(converted_sql, dialect=builder_dialect)
            except ParseError as exc:
                msg = f"Failed to parse SQL for builder: {exc}"
                raise sqlspec.exceptions.SQLBuilderError(msg) from exc

        base_expression = expression
        ctes: list[exp.CTE] | None = None
        if isinstance(expression, exp.With):
            if expression.this is None:
                msg = "WITH expression does not include a base statement."
                raise sqlspec.exceptions.SQLBuilderError(msg)
            base_expression = expression.this
            ctes = list(expression.expressions)

        builder: QueryBuilder
        if isinstance(base_expression, (exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Values)):
            builder = Select(dialect=builder_dialect)
            builder.set_expression(base_expression.copy())
        elif isinstance(base_expression, exp.Insert):
            builder = Insert(dialect=builder_dialect)
            builder.set_expression(base_expression.copy())
        elif isinstance(base_expression, exp.Update):
            builder = Update(dialect=builder_dialect)
            builder.set_expression(base_expression.copy())
        elif isinstance(base_expression, exp.Delete):
            builder = Delete(dialect=builder_dialect)
            builder.set_expression(base_expression.copy())
        elif isinstance(base_expression, exp.Merge):
            builder = Merge(dialect=builder_dialect)
            builder.set_expression(base_expression.copy())
        else:
            builder = ExpressionBuilder(base_expression.copy(), dialect=builder_dialect)

        if ctes:
            builder.load_ctes(ctes)

        if isinstance(converted_params, Mapping):
            builder.load_parameters(converted_params)
            return builder

        if (
            converted_params
            and isinstance(converted_params, Sequence)
            and not isinstance(converted_params, (str, bytes, bytearray))
        ):
            param_info = converter.validator.extract_parameters(converted_sql)
            param_map: dict[str, Any] = {}
            for index, param in enumerate(param_info):
                if index >= len(converted_params):
                    break
                param_name = param.name or f"param_{param.ordinal}"
                param_map[param_name] = converted_params[index]
            builder.load_parameters(param_map)

        return builder

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
        parameter_config: "ParameterStyleConfig | None" = None,
        enable_parsing: bool = True,
        enable_validation: bool = True,
        enable_transformations: bool = True,
        enable_analysis: bool = False,
        enable_expression_simplification: bool = False,
        enable_parameter_type_wrapping: bool = True,
        enable_caching: bool = True,
        parameter_converter: "ParameterConverter | None" = None,
        parameter_validator: "ParameterValidator | None" = None,
        dialect: "DialectType | None" = None,
        execution_mode: "str | None" = None,
        execution_args: "dict[str, Any] | None" = None,
        output_transformer: "Callable[[str, Any], tuple[str, Any]] | None" = None,
        statement_transformers: "Sequence[Callable[[exp.Expression, Any], tuple[exp.Expression, Any]]] | None" = None,
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
            execution_mode: Special execution mode
            execution_args: Arguments for special execution modes
            output_transformer: Optional output transformation function
            statement_transformers: Optional AST transformers executed during compilation
        """
        self.enable_parsing = enable_parsing
        self.enable_validation = enable_validation
        self.enable_transformations = enable_transformations
        self.enable_analysis = enable_analysis
        self.enable_expression_simplification = enable_expression_simplification
        self.enable_parameter_type_wrapping = enable_parameter_type_wrapping
        self.enable_caching = enable_caching
        if parameter_converter is None:
            if parameter_validator is None:
                parameter_validator = ParameterValidator()
            self.parameter_converter = ParameterConverter(parameter_validator)
        else:
            self.parameter_converter = parameter_converter

        if parameter_validator is None:
            self.parameter_validator = self.parameter_converter.validator
        else:
            self.parameter_validator = parameter_validator
            self.parameter_converter.validator = parameter_validator
        self.parameter_config = parameter_config or ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )

        self.dialect = dialect
        self.execution_mode = execution_mode
        self.execution_args = execution_args
        self.output_transformer = output_transformer
        if statement_transformers:
            self.statement_transformers = tuple(statement_transformers)
        else:
            self.statement_transformers = ()

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
            "execution_mode": self.execution_mode,
            "execution_args": self.execution_args,
            "output_transformer": self.output_transformer,
            "statement_transformers": self.statement_transformers,
        }
        current_kwargs.update(kwargs)
        return type(self)(**current_kwargs)

    def __hash__(self) -> int:
        """Hash based on configuration settings."""
        return hash((
            self.enable_parsing,
            self.enable_validation,
            self.enable_transformations,
            self.enable_analysis,
            self.enable_expression_simplification,
            self.enable_parameter_type_wrapping,
            self.enable_caching,
            str(self.dialect),
            self.parameter_config.hash(),
            self.execution_mode,
            self.output_transformer,
            self.statement_transformers,
        ))

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
            f"execution_mode={self.execution_mode!r}",
            f"execution_args={self.execution_args!r}",
            f"output_transformer={self.output_transformer!r}",
            f"statement_transformers={self.statement_transformers!r}",
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
            and self.execution_mode == other.execution_mode
            and self.execution_args == other.execution_args
            and self.output_transformer == other.output_transformer
            and self.statement_transformers == other.statement_transformers
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


Statement: TypeAlias = str | exp.Expression | SQL
