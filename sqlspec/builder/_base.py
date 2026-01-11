"""Base query builder with validation and parameter binding.

Provides abstract base classes and core functionality for SQL query builders.
"""

import hashlib
import re
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from typing import Any, NoReturn, cast

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError
from sqlglot.optimizer import optimize
from typing_extensions import Self

from sqlspec.builder._vector_expressions import VectorDistance
from sqlspec.core import (
    SQL,
    ParameterStyle,
    ParameterStyleConfig,
    SQLResult,
    StatementConfig,
    get_cache,
    get_cache_config,
    hash_optimized_expression,
)
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_expression_and_parameters, has_name, has_with_method, is_expression

__all__ = ("BuiltQuery", "ExpressionBuilder", "QueryBuilder")

MAX_PARAMETER_COLLISION_ATTEMPTS = 1000
PARAMETER_INDEX_PATTERN = re.compile(r"^param_(?P<index>\d+)$")


class _ExpressionParameterizer:
    __slots__ = ("_builder",)

    def __init__(self, builder: "QueryBuilder") -> None:
        self._builder = builder

    def __call__(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Literal):
            if node.this in {True, False, None}:
                return node

            parent = node.parent
            if isinstance(parent, exp.Array) and node.find_ancestor(VectorDistance) is not None:
                return node

            value = node.this
            if node.is_number and isinstance(node.this, str):
                try:
                    value = float(node.this) if "." in node.this or "e" in node.this.lower() else int(node.this)
                except ValueError:
                    value = node.this

            param_name = self._builder.add_parameter_for_expression(value, context="where")
            return exp.Placeholder(this=param_name)
        return node


class _PlaceholderReplacer:
    __slots__ = ("_param_mapping",)

    def __init__(self, param_mapping: dict[str, str]) -> None:
        self._param_mapping = param_mapping

    def __call__(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Placeholder) and str(node.this) in self._param_mapping:
            return exp.Placeholder(this=self._param_mapping[str(node.this)])
        return node


def _unquote_identifier(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Identifier):
        node.set("quoted", False)
    return node


logger = get_logger(__name__)


class BuiltQuery:
    """SQL query with bound parameters."""

    __slots__ = ("dialect", "parameters", "sql")

    def __init__(self, sql: str, parameters: dict[str, Any] | None = None, dialect: DialectType | None = None) -> None:
        self.sql = sql
        self.parameters = parameters if parameters is not None else {}
        self.dialect = dialect

    def __repr__(self) -> str:
        parameter_keys = sorted(self.parameters.keys())
        return f"BuiltQuery(sql={self.sql!r}, parameters={parameter_keys!r}, dialect={self.dialect!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BuiltQuery):
            return NotImplemented
        return self.sql == other.sql and self.parameters == other.parameters and self.dialect == other.dialect

    def __hash__(self) -> int:
        return hash((self.sql, frozenset(self.parameters.items()), self.dialect))


class QueryBuilder(ABC):
    """Abstract base class for SQL query builders.

    Provides common functionality for dialect handling, parameter management,
    and query construction using SQLGlot.
    """

    __slots__ = (
        "_expression",
        "_lock_targets_quoted",
        "_merge_target_quoted",
        "_parameter_counter",
        "_parameter_name_counters",
        "_parameters",
        "_with_ctes",
        "dialect",
        "enable_optimization",
        "optimize_joins",
        "optimize_predicates",
        "schema",
        "simplify_expressions",
    )

    def __init__(
        self,
        dialect: DialectType | None = None,
        schema: dict[str, dict[str, str]] | None = None,
        enable_optimization: bool = True,
        optimize_joins: bool = True,
        optimize_predicates: bool = True,
        simplify_expressions: bool = True,
    ) -> None:
        self.dialect = dialect
        self.schema = schema
        self.enable_optimization = enable_optimization
        self.optimize_joins = optimize_joins
        self.optimize_predicates = optimize_predicates
        self.simplify_expressions = simplify_expressions

        self._expression: exp.Expression | None = None
        self._parameter_name_counters: dict[str, int] = {}
        self._parameters: dict[str, Any] = {}
        self._parameter_counter: int = 0
        self._with_ctes: dict[str, exp.CTE] = {}
        self._lock_targets_quoted = False
        self._merge_target_quoted = False

    @classmethod
    def _parse_query_builder_kwargs(
        cls, kwargs: "dict[str, Any]"
    ) -> "tuple[DialectType | None, dict[str, dict[str, str]] | None, bool, bool, bool, bool]":
        dialect = kwargs.pop("dialect", None)
        schema = kwargs.pop("schema", None)
        enable_optimization = kwargs.pop("enable_optimization", True)
        optimize_joins = kwargs.pop("optimize_joins", True)
        optimize_predicates = kwargs.pop("optimize_predicates", True)
        simplify_expressions = kwargs.pop("simplify_expressions", True)

        if kwargs:
            unknown = ", ".join(sorted(kwargs.keys()))
            cls._raise_sql_builder_error(f"Unexpected QueryBuilder arguments: {unknown}")

        return (dialect, schema, enable_optimization, optimize_joins, optimize_predicates, simplify_expressions)

    def _initialize_expression(self) -> None:
        """Initialize the base expression. Called after __init__."""
        self._expression = self._create_base_expression()
        if not self._expression:
            self._raise_sql_builder_error(
                "QueryBuilder._create_base_expression must return a valid sqlglot expression."
            )

    def get_expression(self) -> exp.Expression | None:
        """Get expression reference (no copy).

        Returns:
            The current SQLGlot expression or None if not set
        """
        return self._expression

    def set_expression(self, expression: exp.Expression) -> None:
        """Set expression with validation.

        Args:
            expression: SQLGlot expression to set
        """
        if not is_expression(expression):
            self._raise_invalid_expression_type(expression)
        self._expression = expression

    def has_expression(self) -> bool:
        """Check if expression exists.

        Returns:
            True if expression is set, False otherwise
        """
        return self._expression is not None

    @abstractmethod
    def _create_base_expression(self) -> exp.Expression:
        """Create the base sqlglot expression for the specific query type.

        Returns:
            A new sqlglot expression appropriate for the query type.
        """

    @property
    @abstractmethod
    def _expected_result_type(self) -> "type[SQLResult]":
        """The expected result type for the query being built.

        Returns:
            type[ResultT]: The type of the result.
        """

    @staticmethod
    def _raise_sql_builder_error(message: str, cause: BaseException | None = None) -> NoReturn:
        """Helper to raise SQLBuilderError, potentially with a cause.

        Args:
            message: The error message.
            cause: The optional original exception to chain.

        Raises:
            SQLBuilderError: Always raises this exception.
        """
        raise SQLBuilderError(message) from cause

    @staticmethod
    def _raise_invalid_expression_type(expression: Any) -> NoReturn:
        """Raise error for invalid expression type.

        Args:
            expression: The invalid expression object

        Raises:
            TypeError: Always raised for type mismatch
        """
        msg = f"Expected Expression, got {type(expression)}"
        raise TypeError(msg)

    @staticmethod
    def _raise_cte_query_error(alias: str, message: str) -> NoReturn:
        """Raise error for CTE query issues.

        Args:
            alias: CTE alias name
            message: Specific error message

        Raises:
            SQLBuilderError: Always raised for CTE errors
        """
        msg = f"CTE '{alias}': {message}"
        raise SQLBuilderError(msg)

    @staticmethod
    def _raise_cte_parse_error(cause: BaseException) -> NoReturn:
        """Raise error for CTE parsing failures.

        Args:
            cause: The original parsing exception

        Raises:
            SQLBuilderError: Always raised with chained cause
        """
        msg = f"Failed to parse CTE query: {cause!s}"
        raise SQLBuilderError(msg) from cause

    def _build_final_expression(self, *, copy: bool = False) -> exp.Expression:
        """Construct the current expression with attached CTEs.

        Args:
            copy: Whether to copy the underlying expression tree before
                applying transformations.

        Returns:
            Expression representing the current builder state with CTEs applied.
        """
        if self._expression is None:
            self._raise_sql_builder_error("QueryBuilder expression not initialized.")

        base_expression = self._expression.copy() if copy or self._with_ctes else self._expression

        if not self._with_ctes:
            return base_expression

        final_expression: exp.Expression = base_expression
        if has_with_method(final_expression):
            for alias, cte_node in self._with_ctes.items():
                final_expression = cast("Any", final_expression).with_(cte_node.args["this"], as_=alias, copy=False)
            return cast("exp.Expression", final_expression)

        if isinstance(final_expression, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Union)):
            return exp.With(expressions=list(self._with_ctes.values()), this=final_expression)

        return final_expression

    def _spawn_like_self(self: Self) -> Self:
        """Create a new builder instance with matching configuration."""
        return type(self)(
            dialect=self.dialect,
            schema=self.schema,
            enable_optimization=self.enable_optimization,
            optimize_joins=self.optimize_joins,
            optimize_predicates=self.optimize_predicates,
            simplify_expressions=self.simplify_expressions,
        )

    def _resolve_cte_query(self, alias: str, query: "QueryBuilder | exp.Select | str") -> exp.Select:
        """Resolve a CTE query into a Select expression with merged parameters."""
        if isinstance(query, QueryBuilder):
            query_expr = query.get_expression()
            if query_expr is None:
                self._raise_cte_query_error(alias, "query builder has no expression")
            if not isinstance(query_expr, exp.Select):
                self._raise_cte_query_error(alias, f"expression must be a Select, got {type(query_expr).__name__}")
            cte_select_expression = query_expr.copy()
            param_mapping = self._merge_cte_parameters(alias, query.parameters)
            updated_expression = self._update_placeholders_in_expression(cte_select_expression, param_mapping)
            if not isinstance(updated_expression, exp.Select):  # pragma: no cover - defensive
                msg = "CTE placeholder update produced non-select expression"
                raise SQLBuilderError(msg)
            return updated_expression

        if isinstance(query, str):
            try:
                parsed_expression = sqlglot.parse_one(query, read=self.dialect_name)
            except SQLGlotParseError as e:  # pragma: no cover - defensive
                self._raise_cte_parse_error(e)
            if not isinstance(parsed_expression, exp.Select):
                self._raise_cte_query_error(
                    alias, f"query string must parse to SELECT, got {type(parsed_expression).__name__}"
                )
            return parsed_expression

        if isinstance(query, exp.Select):
            return query

        self._raise_cte_query_error(alias, f"invalid query type: {type(query).__name__}")
        msg = "Unreachable"
        raise AssertionError(msg)

    def _add_parameter(self, value: Any, context: str | None = None) -> str:
        """Adds a parameter to the query and returns its placeholder name.

        Args:
            value: The value of the parameter.
            context: Optional context hint for parameter naming (e.g., "where", "join")

        Returns:
            str: The placeholder name for the parameter (e.g., :param_1 or :where_param_1).
        """
        self._parameter_counter += 1

        param_name = f"{context}_param_{self._parameter_counter}" if context else f"param_{self._parameter_counter}"

        self._parameters[param_name] = value
        return param_name

    def add_parameter_for_expression(self, value: Any, context: str | None = None) -> str:
        """Add a parameter for expression parameterization.

        Args:
            value: The value of the parameter.
            context: Optional context hint for parameter naming.

        Returns:
            Parameter placeholder name.
        """
        return self._add_parameter(value, context=context)

    def _parameterize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Replace literal values in an expression with bound parameters.

        This method traverses a SQLGlot expression tree and replaces literal
        values with parameter placeholders, adding the values to the builder's
        parameter collection.

        Args:
            expression: The SQLGlot expression to parameterize

        Returns:
            A new expression with literals replaced by parameter placeholders
        """

        return expression.transform(_ExpressionParameterizer(self), copy=False)

    def add_parameter(self: Self, value: Any, name: str | None = None) -> tuple[Self, str]:
        """Explicitly adds a parameter to the query.

        This is useful for parameters that are not directly tied to a
        builder method like `where` or `values`.

        Args:
            value: The value of the parameter.
            name: Optional explicit name for the parameter. If None, a name
                  will be generated.

        Returns:
            tuple[Self, str]: The builder instance and the parameter name.
        """
        if name:
            if name in self._parameters:
                self._raise_sql_builder_error(f"Parameter name '{name}' already exists.")
            self._parameters[name] = value
            return self, name

        self._parameter_counter += 1
        param_name = f"param_{self._parameter_counter}"
        self._parameters[param_name] = value
        return self, param_name

    def load_parameters(self, parameters: "Mapping[str, Any]") -> None:
        """Load a parameter mapping into the builder.

        Args:
            parameters: Mapping of parameter names to values.

        Raises:
            SQLBuilderError: If a parameter name already exists on the builder.
        """
        if not parameters:
            return

        for name, value in parameters.items():
            if name in self._parameters:
                self._raise_sql_builder_error(f"Parameter name '{name}' already exists.")
            self._parameters[name] = value
            self._update_parameter_counter(name)

    def load_ctes(self, ctes: "Iterable[exp.CTE]") -> None:
        """Load SQLGlot CTE nodes into the builder.

        Args:
            ctes: Iterable of CTE expressions to register.

        Raises:
            SQLBuilderError: If a CTE alias is missing or duplicated.
        """
        for cte in ctes:
            alias = self._resolve_cte_alias(cte)
            if alias in self._with_ctes:
                self._raise_sql_builder_error(f"CTE '{alias}' already exists.")
            self._with_ctes[alias] = cte

    def _resolve_cte_alias(self, cte: exp.CTE) -> str:
        alias_name = cte.alias_or_name
        if not alias_name:
            self._raise_sql_builder_error("CTE alias is required.")
        return str(alias_name)

    def _update_parameter_counter(self, name: str) -> None:
        match = PARAMETER_INDEX_PATTERN.match(name)
        if not match:
            return
        index = int(match.group("index"))
        self._parameter_counter = max(self._parameter_counter, index)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name when collision occurs.

        Args:
            base_name: The desired base name for the parameter

        Returns:
            A unique parameter name that doesn't exist in current parameters
        """
        current_index = self._parameter_name_counters.get(base_name, 0)

        if base_name not in self._parameters:
            # First use keeps the base name, counter stays at 0
            self._parameter_name_counters[base_name] = current_index
            return base_name

        next_index = current_index + 1
        candidate = f"{base_name}_{next_index}"

        while candidate in self._parameters:
            next_index += 1
            if next_index > MAX_PARAMETER_COLLISION_ATTEMPTS:
                return f"{base_name}_{uuid.uuid4().hex[:8]}"
            candidate = f"{base_name}_{next_index}"

        self._parameter_name_counters[base_name] = next_index
        return candidate

    def _create_placeholder(self, value: Any, base_name: str) -> tuple[exp.Placeholder, str]:
        """Backwards-compatible placeholder helper (delegates to create_placeholder)."""
        return self.create_placeholder(value, base_name)

    def create_placeholder(self, value: Any, base_name: str) -> tuple[exp.Placeholder, str]:
        """Create placeholder expression with a unique parameter name.

        Args:
            value: Parameter value to bind.
            base_name: Seed for parameter naming.

        Returns:
            Tuple of placeholder expression and the final parameter name.
        """
        param_name = self._generate_unique_parameter_name(base_name)
        _, param_name = self.add_parameter(value, name=param_name)
        return exp.Placeholder(this=param_name), param_name

    def _merge_cte_parameters(self, cte_name: str, parameters: dict[str, Any]) -> dict[str, str]:
        """Merge CTE parameters with unique naming to prevent collisions.

        Args:
            cte_name: The name of the CTE for parameter prefixing
            parameters: The CTE's parameter dictionary

        Returns:
            Mapping of old parameter names to new unique names
        """
        param_mapping = {}
        for old_name, value in parameters.items():
            new_name = self._generate_unique_parameter_name(f"{cte_name}_{old_name}")
            param_mapping[old_name] = new_name
            self.add_parameter(value, name=new_name)
        return param_mapping

    def _update_placeholders_in_expression(
        self, expression: exp.Expression, param_mapping: dict[str, str]
    ) -> exp.Expression:
        """Update parameter placeholders in expression to use new names.

        Args:
            expression: The SQLGlot expression to update
            param_mapping: Mapping of old parameter names to new names

        Returns:
            Updated expression with new placeholder names
        """

        return expression.transform(_PlaceholderReplacer(param_mapping), copy=False)

    def _generate_builder_cache_key(self, config: "StatementConfig | None" = None) -> str:
        """Generate cache key based on builder state and configuration.

        Args:
            config: Optional SQL configuration that affects the generated SQL

        Returns:
            A unique cache key representing the builder state and configuration
        """
        dialect_name: str = self.dialect_name or "default"

        if self._expression is None:
            self._expression = self._create_base_expression()

        expr_sql: str = self._expression.sql() if self._expression else "None"
        parameters_snapshot = sorted(self._parameters.items())
        parameters_hash = hashlib.sha256(str(parameters_snapshot).encode()).hexdigest()[:8]

        state_parts = [
            f"expression:{expr_sql}",
            f"parameters_hash:{parameters_hash}",
            f"ctes:{sorted(self._with_ctes.keys())}",
            f"dialect:{dialect_name}",
            f"schema_hash:{hashlib.sha256(str(self.schema).encode()).hexdigest()[:8]}",
            f"optimization:{self.enable_optimization}",
            f"optimize_joins:{self.optimize_joins}",
            f"optimize_predicates:{self.optimize_predicates}",
            f"simplify_expressions:{self.simplify_expressions}",
        ]

        if config:
            config_parts = [
                f"config_dialect:{config.dialect or 'default'}",
                f"enable_parsing:{config.enable_parsing}",
                f"enable_validation:{config.enable_validation}",
                f"enable_transformations:{config.enable_transformations}",
                f"enable_analysis:{config.enable_analysis}",
                f"enable_caching:{config.enable_caching}",
                f"param_style:{config.parameter_config.default_parameter_style.value}",
            ]
            state_parts.extend(config_parts)

        state_string = "|".join(state_parts)
        return f"builder:{hashlib.sha256(state_string.encode()).hexdigest()[:16]}"

    def with_cte(self: Self, alias: str, query: "QueryBuilder | exp.Select | str") -> Self:
        """Adds a Common Table Expression (CTE) to the query.

        Args:
            alias: The alias for the CTE.
            query: The CTE query, which can be another QueryBuilder instance,
                   a raw SQL string, or a sqlglot Select expression.

        Returns:
            Self: The current builder instance for method chaining.
        """
        if alias in self._with_ctes:
            self._raise_sql_builder_error(f"CTE with alias '{alias}' already exists.")

        cte_select_expression = self._resolve_cte_query(alias, query)
        self._with_ctes[alias] = exp.CTE(this=cte_select_expression, alias=exp.to_table(alias))
        return self

    def build(self, dialect: DialectType = None) -> "BuiltQuery":
        """Builds the SQL query string and parameters.

        Args:
            dialect: Optional dialect override. If provided, generates SQL for this dialect
                    instead of the builder's default dialect.

        Returns:
            BuiltQuery: A dataclass containing the SQL string and parameters.

        Examples:
            # Use builder's default dialect
            query = sql.select("*").from_("products")
            result = query.build()

            # Override dialect at build time
            postgres_sql = query.build(dialect="postgres")
            mysql_sql = query.build(dialect="mysql")
        """
        final_expression = self._build_final_expression()

        if self.enable_optimization and isinstance(final_expression, exp.Expression):
            final_expression = self._optimize_expression(final_expression)

        target_dialect = str(dialect) if dialect else self.dialect_name

        try:
            if isinstance(final_expression, exp.Expression):
                normalized_expression = (
                    self._unquote_identifiers_for_oracle(final_expression)
                    if self._is_oracle_dialect(target_dialect)
                    else final_expression
                )
                identify = self._should_identify(target_dialect)
                sql_string = normalized_expression.sql(dialect=target_dialect, pretty=True, identify=identify)
                sql_string = self._strip_lock_identifier_quotes(sql_string)
            else:
                sql_string = str(final_expression)
        except Exception as e:
            err_msg = f"Error generating SQL from expression: {e!s}"
            self._raise_sql_builder_error(err_msg, e)

        return BuiltQuery(sql=sql_string, parameters=self._parameters.copy(), dialect=dialect or self.dialect)

    def to_sql(self, show_parameters: bool = False, dialect: DialectType = None) -> str:
        """Return SQL string with optional parameter substitution.

        Args:
            show_parameters: If True, replace parameter placeholders with actual values (for debugging).
                           If False (default), return SQL with parameter placeholders.
            dialect: Optional dialect override. If provided, generates SQL for this dialect
                    instead of the builder's default dialect.

        Returns:
            SQL string with or without parameter values filled in

        Examples:
            Get SQL with placeholders (for execution):
                sql_str = query.to_sql()
                # "SELECT * FROM products WHERE id = :id"

            Get SQL with values (for debugging):
                sql_str = query.to_sql(show_parameters=True)
                # "SELECT * FROM products WHERE id = 123"

            Override dialect at output time:
                postgres_sql = query.to_sql(dialect="postgres")
                mysql_sql = query.to_sql(dialect="mysql")

        Warning:
            SQL with show_parameters=True is for debugging ONLY.
            Never execute SQL with interpolated parameters directly - use parameterized queries.
        """
        safe_query = self.build(dialect=dialect)

        if not show_parameters:
            return safe_query.sql

        sql = safe_query.sql
        parameters = safe_query.parameters

        for param_name, param_value in parameters.items():
            placeholder = f":{param_name}"
            if isinstance(param_value, str):
                replacement = f"'{param_value}'"
            elif param_value is None:
                replacement = "NULL"
            elif isinstance(param_value, bool):
                replacement = "TRUE" if param_value else "FALSE"
            else:
                replacement = str(param_value)

            sql = sql.replace(placeholder, replacement)

        return sql

    def _optimize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Apply SQLGlot optimizations to the expression.

        Args:
            expression: The expression to optimize

        Returns:
            The optimized expression
        """
        if not self.enable_optimization:
            return expression

        if not self.optimize_joins and not self.optimize_predicates and not self.simplify_expressions:
            return expression

        optimizer_settings = {
            "optimize_joins": self.optimize_joins,
            "pushdown_predicates": self.optimize_predicates,
            "simplify_expressions": self.simplify_expressions,
        }

        dialect_name = self.dialect_name or "default"
        cache_key = hash_optimized_expression(
            expression, dialect=dialect_name, schema=self.schema, optimizer_settings=optimizer_settings
        )

        cache = get_cache()
        cached_optimized = cache.get_optimized(cache_key)
        if cached_optimized:
            return cast("exp.Expression", cached_optimized)

        try:
            optimized = optimize(
                expression, schema=self.schema, dialect=self.dialect_name, optimizer_settings=optimizer_settings
            )
            cache.put_optimized(cache_key, optimized)
        except Exception:
            logger.debug("Expression optimization failed, using original expression")
            return expression
        else:
            return optimized

    def to_statement(self, config: "StatementConfig | None" = None) -> "SQL":
        """Converts the built query into a SQL statement object.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        cache_config = get_cache_config()
        if not cache_config.compiled_cache_enabled:
            return self._to_statement(config)

        cache_key_str = self._generate_builder_cache_key(config)

        cache = get_cache()
        cached_sql = cache.get_builder(cache_key_str)
        if cached_sql is not None:
            return cast("SQL", cached_sql)

        sql_statement = self._to_statement(config)
        cache.put_builder(cache_key_str, sql_statement)

        return sql_statement

    def _to_statement(self, config: "StatementConfig | None" = None) -> "SQL":
        """Internal method to create SQL statement.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        dialect_override = config.dialect if config else None
        safe_query = self.build(dialect=dialect_override)

        kwargs, parameters = self._extract_statement_parameters(safe_query.parameters)

        if config is None:
            config = StatementConfig(
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
                dialect=safe_query.dialect,
            )

        sql_string = safe_query.sql
        if (
            config.dialect is not None
            and config.dialect != safe_query.dialect
            and isinstance(self._expression, exp.Expression)
        ):
            try:
                identify = self._should_identify(config.dialect)
                sql_string = self._expression.sql(dialect=config.dialect, pretty=True, identify=identify)
            except Exception:
                sql_string = safe_query.sql

        if kwargs:
            return SQL(sql_string, statement_config=config, **kwargs)
        if parameters:
            return SQL(sql_string, *parameters, statement_config=config)
        return SQL(sql_string, statement_config=config)

    def _extract_statement_parameters(
        self, raw_parameters: Any
    ) -> "tuple[dict[str, Any] | None, tuple[Any, ...] | None]":
        """Extract parameters for SQL statement creation.

        Args:
            raw_parameters: Raw parameter data from BuiltQuery

        Returns:
            Tuple of (kwargs, parameters) for SQL statement construction
        """
        if isinstance(raw_parameters, dict):
            return raw_parameters, None

        if isinstance(raw_parameters, tuple):
            return None, raw_parameters

        if raw_parameters:
            return None, tuple(raw_parameters)

        return None, None

    def __str__(self) -> str:
        """Return the SQL string representation of the query.

        Returns:
            str: The SQL string for this query.
        """
        return self.build().sql

    @property
    def dialect_name(self) -> "str | None":
        """Returns the name of the dialect, if set."""
        if isinstance(self.dialect, str):
            return self.dialect
        if self.dialect is None:
            return None
        if isinstance(self.dialect, type) and issubclass(self.dialect, Dialect):
            return self.dialect.__name__.lower()
        if isinstance(self.dialect, Dialect):
            return type(self.dialect).__name__.lower()
        if has_name(self.dialect):
            return self.dialect.__name__.lower()
        return str(self.dialect).lower()

    def _merge_sql_object_parameters(self, sql_obj: Any) -> None:
        """Merge parameters from a SQL object into the builder.

        Args:
            sql_obj: Object with parameters attribute containing parameter mappings
        """
        if not has_expression_and_parameters(sql_obj):
            return

        sql_parameters = sql_obj.parameters
        for param_name, param_value in sql_parameters.items():
            unique_name = self._generate_unique_parameter_name(param_name)
            self.add_parameter(param_value, name=unique_name)

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        return self._parameters

    def set_parameters(self, parameters: dict[str, Any]) -> None:
        """Set query parameters (public API)."""
        self._parameters = parameters.copy()

    def _is_oracle_dialect(self, dialect: "DialectType | str | None") -> bool:
        """Check if target dialect is Oracle."""
        if dialect is None:
            return False
        return str(dialect).lower() == "oracle"

    def _unquote_identifiers_for_oracle(self, expression: exp.Expression) -> exp.Expression:
        """Remove identifier quoting to avoid Oracle case-sensitive lookup issues."""

        return expression.copy().transform(_unquote_identifier, copy=False)

    def _strip_lock_identifier_quotes(self, sql_string: str) -> str:
        for keyword in ("FOR UPDATE OF ", "FOR SHARE OF "):
            if keyword in sql_string and not self._lock_targets_quoted:
                head, tail = sql_string.split(keyword, 1)
                tail = tail.replace('"', "")
                return f"{head}{keyword}{tail}"
        if sql_string.startswith('MERGE INTO "') and not self._merge_target_quoted:
            # Remove quotes around target table only, leave alias/rest intact
            end_quote = sql_string.find('"', len('MERGE INTO "'))
            if end_quote > 0:
                table_name = sql_string[len('MERGE INTO "') : end_quote]
                remainder = sql_string[end_quote + 1 :]
                return f"MERGE INTO {table_name}{remainder}"
        return sql_string

    def _should_identify(self, dialect: "DialectType | str | None") -> bool:
        """Determine whether to quote identifiers for the given dialect."""
        if dialect is None:
            return True
        dialect_name = str(dialect).lower()
        # Oracle folds unquoted identifiers to uppercase; quoting lower-case breaks table lookup
        return dialect_name != "oracle"

    @property
    def with_ctes(self) -> "dict[str, exp.CTE]":
        """Get WITH clause CTEs (public API)."""
        return dict(self._with_ctes)

    def generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name (public API)."""
        return self._generate_unique_parameter_name(base_name)

    def build_static_expression(
        self,
        expression: exp.Expression | None = None,
        parameters: dict[str, Any] | None = None,
        *,
        cache_key: str | None = None,
        expression_factory: Callable[[], exp.Expression] | None = None,
        copy: bool = True,
        optimize_expression: bool | None = None,
        dialect: DialectType | None = None,
    ) -> "BuiltQuery":
        """Compile a pre-built expression with optional caching and parameters.

        Designed for hot paths that construct an AST once and reuse it with
        different parameters, avoiding repeated parse/optimize cycles.

        Args:
            expression: Pre-built sqlglot expression to render (required when cache_key is not provided).
            parameters: Optional parameter mapping to include in the result.
            cache_key: When provided, the expression will be cached under this key.
            expression_factory: Factory used to build the expression on cache miss.
            copy: Copy the expression before rendering to avoid caller mutation.
            optimize_expression: Override builder optimization toggle for this call.
            dialect: Optional dialect override for SQL generation.

        Returns:
            BuiltQuery containing SQL and parameters.
        """

        expr: exp.Expression | None = None

        if cache_key is not None:
            cache = get_cache()
            cached_expr = cache.get_expression(cache_key)
            if cached_expr is None:
                if expression_factory is None:
                    msg = "expression_factory is required when cache_key is provided"
                    self._raise_sql_builder_error(msg)
                expr_candidate = expression_factory()
                if not is_expression(expr_candidate):
                    self._raise_invalid_expression_type(expr_candidate)
                expr_to_store = expr_candidate.copy() if copy else expr_candidate
                should_optimize = self.enable_optimization if optimize_expression is None else optimize_expression
                if should_optimize:
                    expr_to_store = self._optimize_expression(expr_to_store)
                cache.put_expression(cache_key, expr_to_store)
                cached_expr = expr_to_store
            expr = cached_expr.copy() if copy else cached_expr
        else:
            if expression is None:
                msg = "expression must be provided when cache_key is not set"
                self._raise_sql_builder_error(msg)
            expr = expression.copy() if copy else expression
            should_optimize = self.enable_optimization if optimize_expression is None else optimize_expression
            if should_optimize:
                expr = self._optimize_expression(expr)

        if expr is None:
            self._raise_sql_builder_error("Static expression could not be resolved.")

        target_dialect = str(dialect) if dialect else self.dialect_name
        identify = self._should_identify(target_dialect)
        sql_string = expr.sql(dialect=target_dialect, pretty=True, identify=identify)
        return BuiltQuery(
            sql=sql_string, parameters=parameters.copy() if parameters else {}, dialect=dialect or self.dialect
        )


class ExpressionBuilder(QueryBuilder):
    """Builder wrapper for a pre-parsed SQLGlot expression."""

    __slots__ = ()

    def __init__(self, expression: exp.Expression, **kwargs: Any) -> None:
        (dialect, schema, enable_optimization, optimize_joins, optimize_predicates, simplify_expressions) = (
            self._parse_query_builder_kwargs(kwargs)
        )
        super().__init__(
            dialect=dialect,
            schema=schema,
            enable_optimization=enable_optimization,
            optimize_joins=optimize_joins,
            optimize_predicates=optimize_predicates,
            simplify_expressions=simplify_expressions,
        )
        if not is_expression(expression):
            self._raise_invalid_expression_type(expression)
        self._expression = expression

    def _create_base_expression(self) -> exp.Expression:
        if self._expression is None:
            msg = "ExpressionBuilder requires an expression at construction."
            self._raise_sql_builder_error(msg)
        return self._expression

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        return SQLResult
