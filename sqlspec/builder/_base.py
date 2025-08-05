"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation. Enhanced with SQLGlot's
advanced builder patterns and optimization capabilities.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn, Optional, Union, cast

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError
from sqlglot.optimizer import optimize
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.cache import builder_cache, get_cache_config, optimized_expression_cache
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.statement_hashing import hash_optimized_expression
from sqlspec.utils.type_guards import has_sql_method, has_with_method

if TYPE_CHECKING:
    from sqlspec.statement.result import SQLResult

__all__ = ("QueryBuilder", "SafeQuery")

logger = get_logger(__name__)


@dataclass(frozen=True)
class SafeQuery:
    """A safely constructed SQL query with bound parameters."""

    sql: str
    parameters: dict[str, Any] = field(default_factory=dict)
    dialect: DialectType = field(default=None)


@dataclass
class QueryBuilder(ABC):
    """Abstract base class for SQL query builders with SQLGlot optimization.

    Provides common functionality for dialect handling, parameter management,
    query construction, and automatic query optimization using SQLGlot's
    advanced capabilities.

    New features:
    - Automatic query optimization (join reordering, predicate pushdown)
    - Query complexity analysis
    - Smart parameter naming based on context
    - Expression caching for performance
    """

    dialect: DialectType = field(default=None)
    schema: Optional[dict[str, dict[str, str]]] = field(default=None)
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)
    _parameters: dict[str, Any] = field(default_factory=dict, init=False, repr=False, compare=False, hash=False)
    _parameter_counter: int = field(default=0, init=False, repr=False, compare=False, hash=False)
    _with_ctes: dict[str, exp.CTE] = field(default_factory=dict, init=False, repr=False, compare=False, hash=False)
    enable_optimization: bool = field(default=True, init=True)
    optimize_joins: bool = field(default=True, init=True)
    optimize_predicates: bool = field(default=True, init=True)
    simplify_expressions: bool = field(default=True, init=True)

    def __post_init__(self) -> None:
        self._expression = self._create_base_expression()
        if not self._expression:
            self._raise_sql_builder_error(
                "QueryBuilder._create_base_expression must return a valid sqlglot expression."
            )

    @abstractmethod
    def _create_base_expression(self) -> exp.Expression:
        """Create the base sqlglot expression for the specific query type.

        Examples:
            For a SELECT query, this would return `exp.Select()`.
            For an INSERT query, this would return `exp.Insert()`.

        Returns:
            exp.Expression: A new sqlglot expression.
        """

    @property
    @abstractmethod
    def _expected_result_type(self) -> "type[SQLResult]":
        """The expected result type for the query being built.

        Returns:
            type[ResultT]: The type of the result.
        """

    @staticmethod
    def _raise_sql_builder_error(message: str, cause: Optional[BaseException] = None) -> NoReturn:
        """Helper to raise SQLBuilderError, potentially with a cause.

        Args:
            message: The error message.
            cause: The optional original exception to chain.

        Raises:
            SQLBuilderError: Always raises this exception.
        """
        raise SQLBuilderError(message) from cause

    def _add_parameter(self, value: Any, context: Optional[str] = None) -> str:
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

        def replacer(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Literal):
                if node.this in {True, False, None}:
                    return node
                param_name = self._add_parameter(node.this, context="where")
                return exp.Placeholder(this=param_name)
            return node

        return expression.transform(replacer, copy=True)

    def add_parameter(self: Self, value: Any, name: Optional[str] = None) -> tuple[Self, str]:
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
            param_name_to_use = name
        else:
            self._parameter_counter += 1
            param_name_to_use = f"param_{self._parameter_counter}"

        self._parameters[param_name_to_use] = value
        return self, param_name_to_use

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name when collision occurs.

        Args:
            base_name: The desired base name for the parameter

        Returns:
            A unique parameter name that doesn't exist in current parameters
        """
        if base_name not in self._parameters:
            return base_name

        i = 1
        while True:
            name = f"{base_name}_{i}"
            if name not in self._parameters:
                return name
            i += 1

    def _generate_builder_cache_key(self, config: "Optional[StatementConfig]" = None) -> str:
        """Generate cache key based on builder state and configuration.

        Args:
            config: Optional SQL configuration that affects the generated SQL

        Returns:
            A unique cache key representing the builder state and configuration
        """
        import hashlib

        state_components = [
            f"expression:{self._expression.sql() if self._expression else 'None'}",
            f"parameters:{sorted(self._parameters.items())}",
            f"ctes:{sorted(self._with_ctes.keys())}",
            f"dialect:{self.dialect_name or 'default'}",
            f"schema:{self.schema}",
            f"optimization:{self.enable_optimization}",
            f"optimize_joins:{self.optimize_joins}",
            f"optimize_predicates:{self.optimize_predicates}",
            f"simplify_expressions:{self.simplify_expressions}",
        ]

        if config:
            config_components = [
                f"config_dialect:{config.dialect or 'default'}",
                f"enable_parsing:{config.enable_parsing}",
                f"enable_validation:{config.enable_validation}",
                f"enable_transformations:{config.enable_transformations}",
                f"enable_analysis:{config.enable_analysis}",
                f"enable_caching:{config.enable_caching}",
                f"param_style:{config.parameter_config.default_parameter_style.value}",
            ]
            state_components.extend(config_components)

        state_string = "|".join(state_components)
        cache_key = hashlib.sha256(state_string.encode()).hexdigest()[:16]

        return f"builder:{cache_key}"

    def with_cte(self: Self, alias: str, query: "Union[QueryBuilder, exp.Select, str]") -> Self:
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

        cte_select_expression: exp.Select

        if isinstance(query, QueryBuilder):
            if query._expression is None:
                self._raise_sql_builder_error("CTE query builder has no expression.")
            if not isinstance(query._expression, exp.Select):
                msg = f"CTE query builder expression must be a Select, got {type(query._expression).__name__}."
                self._raise_sql_builder_error(msg)
            cte_select_expression = query._expression.copy()
            for p_name, p_value in query.parameters.items():
                unique_name = self._generate_unique_parameter_name(p_name)
                self.add_parameter(p_value, unique_name)

        elif isinstance(query, str):
            try:
                parsed_expression = sqlglot.parse_one(query, read=self.dialect_name)
                if not isinstance(parsed_expression, exp.Select):
                    msg = f"CTE query string must parse to a SELECT statement, got {type(parsed_expression).__name__}."
                    self._raise_sql_builder_error(msg)
                cte_select_expression = parsed_expression
            except SQLGlotParseError as e:
                self._raise_sql_builder_error(f"Failed to parse CTE query string: {e!s}", e)
            except Exception as e:
                msg = f"An unexpected error occurred while parsing CTE query string: {e!s}"
                self._raise_sql_builder_error(msg, e)
        elif isinstance(query, exp.Select):
            cte_select_expression = query.copy()
        else:
            msg = f"Invalid query type for CTE: {type(query).__name__}"
            self._raise_sql_builder_error(msg)
            return self

        self._with_ctes[alias] = exp.CTE(this=cte_select_expression, alias=exp.to_table(alias))
        return self

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        if self._expression is None:
            self._raise_sql_builder_error("QueryBuilder expression not initialized.")

        if self._with_ctes:
            final_expression = self._expression.copy()
            if has_with_method(final_expression):
                for alias, cte_node in self._with_ctes.items():
                    final_expression = cast("Any", final_expression).with_(cte_node.args["this"], as_=alias, copy=False)
            elif isinstance(final_expression, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Union)):
                final_expression = exp.With(expressions=list(self._with_ctes.values()), this=final_expression)
        else:
            final_expression = self._expression.copy() if self.enable_optimization else self._expression

        if self.enable_optimization and isinstance(final_expression, exp.Expression):
            final_expression = self._optimize_expression(final_expression)

        try:
            if has_sql_method(final_expression):
                sql_string = final_expression.sql(dialect=self.dialect_name, pretty=True)
            else:
                sql_string = str(final_expression)
        except Exception as e:
            err_msg = f"Error generating SQL from expression: {e!s}"
            logger.exception("SQL generation failed")
            self._raise_sql_builder_error(err_msg, e)

        return SafeQuery(sql=sql_string, parameters=self._parameters.copy(), dialect=self.dialect)

    def _optimize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Apply SQLGlot optimizations to the expression with caching.

        Args:
            expression: The expression to optimize

        Returns:
            The optimized expression
        """
        if not self.enable_optimization:
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

        cached_optimized = optimized_expression_cache.get(cache_key)
        if cached_optimized:
            return cast("exp.Expression", cached_optimized.copy())

        try:
            optimized = optimize(
                expression.copy(), schema=self.schema, dialect=self.dialect_name, optimizer_settings=optimizer_settings
            )

            optimized_expression_cache.set(cache_key, optimized.copy())

        except Exception:
            return expression
        else:
            return optimized

    def to_statement(self, config: "Optional[StatementConfig]" = None) -> "SQL":
        """Converts the built query into a SQL statement object with caching.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        cache_config = get_cache_config()
        if not cache_config.builder_cache_enabled:
            return self._to_statement_without_cache(config)

        cache_key = self._generate_builder_cache_key(config)

        cached_sql = builder_cache.get(cache_key)
        if cached_sql is not None:
            return cast("SQL", cached_sql.copy())

        sql_statement = self._to_statement_without_cache(config)
        builder_cache.set(cache_key, sql_statement)

        return sql_statement

    def _to_statement_without_cache(self, config: "Optional[StatementConfig]" = None) -> "SQL":
        """Internal method to create SQL statement without caching.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        safe_query = self.build()

        if isinstance(safe_query.parameters, dict):
            kwargs = safe_query.parameters
            parameters: Optional[tuple] = None
        else:
            kwargs = None
            parameters = (
                safe_query.parameters
                if isinstance(safe_query.parameters, tuple)
                else tuple(safe_query.parameters)
                if safe_query.parameters
                else None
            )

        if config is None:
            from sqlspec.statement.sql import StatementConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            config = StatementConfig(parameter_config=parameter_config, dialect=safe_query.dialect)

        if kwargs:
            return SQL(safe_query.sql, statement_config=config, **kwargs)
        if parameters:
            return SQL(safe_query.sql, *parameters, statement_config=config)
        return SQL(safe_query.sql, statement_config=config)

    def __str__(self) -> str:
        """Return the SQL string representation of the query.

        Returns:
            str: The SQL string for this query.
        """
        try:
            return self.build().sql
        except Exception:
            return super().__str__()

    @property
    def dialect_name(self) -> "Optional[str]":
        """Returns the name of the dialect, if set."""
        if isinstance(self.dialect, str):
            return self.dialect
        if self.dialect is not None:
            if isinstance(self.dialect, type) and issubclass(self.dialect, Dialect):
                return self.dialect.__name__.lower()
            if isinstance(self.dialect, Dialect):
                return type(self.dialect).__name__.lower()
            if hasattr(self.dialect, "__name__"):
                return self.dialect.__name__.lower()
        return None

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        return self._parameters
