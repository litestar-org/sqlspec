"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation. Enhanced with SQLGlot's
advanced builder patterns and optimization capabilities.
"""

import contextlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, NoReturn, Optional, Union

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError
from sqlglot.optimizer import optimize
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.simplify import simplify
from sqlglot.optimizer.unnest_subqueries import unnest_subqueries
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import RowT
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement.result import SQLResult

__all__ = (
    "QueryBuilder",
    "SafeQuery",
)

logger = get_logger("statement.builder")


@dataclass(frozen=True)
class SafeQuery:
    """A safely constructed SQL query with bound parameters."""

    sql: str
    parameters: dict[str, Any] = field(default_factory=dict)
    dialect: Optional[DialectType] = None


@dataclass
class QueryBuilder(ABC, Generic[RowT]):
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
    instrumentation_config: Optional["InstrumentationConfig"] = field(default=None)
    schema: Optional[dict[str, dict[str, str]]] = field(default=None)
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)
    # Internally, builders will use a dictionary for named parameters.
    _parameters: dict[str, Any] = field(default_factory=dict, init=False, repr=False, compare=False, hash=False)
    _parameter_counter: int = field(default=0, init=False, repr=False, compare=False, hash=False)
    _with_ctes: dict[str, exp.CTE] = field(default_factory=dict, init=False, repr=False, compare=False, hash=False)

    # Optimization settings
    enable_optimization: bool = field(default=True, init=True)
    optimize_joins: bool = field(default=True, init=True)
    optimize_predicates: bool = field(default=True, init=True)
    simplify_expressions: bool = field(default=True, init=True)

    # Caching for performance
    _cached_sql: Optional[str] = field(default=None, init=False, repr=False, compare=False, hash=False)
    _cache_valid: bool = field(default=False, init=False, repr=False, compare=False, hash=False)

    def __post_init__(self) -> None:
        self._expression = self._create_base_expression()
        if not self._expression:
            # This path should be unreachable if _raise_sql_builder_error has NoReturn
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
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
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

    def _invalidate_cache(self) -> None:
        """Invalidate the cached SQL when the query changes."""
        self._cached_sql = None
        self._cache_valid = False

    def _add_parameter(self, value: Any, context: Optional[str] = None) -> str:
        """Adds a parameter to the query and returns its placeholder name.

        Args:
            value: The value of the parameter.
            context: Optional context hint for parameter naming (e.g., "where", "join")

        Returns:
            str: The placeholder name for the parameter (e.g., :param_1 or :where_param_1).
        """
        self._invalidate_cache()
        self._parameter_counter += 1

        # Use context-aware naming if provided
        param_name = f"{context}_param_{self._parameter_counter}" if context else f"param_{self._parameter_counter}"

        self._parameters[param_name] = value
        return param_name

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

    def with_cte(self: Self, alias: str, query: "Union[QueryBuilder[Any], exp.Select, str]") -> Self:
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
            for p_name, p_value in query._parameters.items():
                self.add_parameter(p_value, f"cte_{alias}_{p_name}")

        elif isinstance(query, str):
            try:
                parsed_expression = sqlglot.parse_one(query, read=self.dialect_name)
                if not isinstance(parsed_expression, exp.Select):
                    msg = f"CTE query string must parse to a SELECT statement, got {type(parsed_expression).__name__}."
                    self._raise_sql_builder_error(msg)
                # parsed_expression is now known to be exp.Select
                cte_select_expression = parsed_expression
            except SQLGlotParseError as e:
                self._raise_sql_builder_error(f"Failed to parse CTE query string: {e!s}", e)
            except Exception as e:
                msg = f"An unexpected error occurred while parsing CTE query string: {e!s}"
                self._raise_sql_builder_error(msg, e)
        elif isinstance(query, exp.Select):
            cte_select_expression = query.copy()
        else:
            msg = (
                f"Invalid query type for CTE: {type(query).__name__}. Must be QueryBuilder, str, or sqlglot.exp.Select."
            )
            self._raise_sql_builder_error(msg)

        self._with_ctes[alias] = exp.CTE(this=cte_select_expression, alias=exp.to_table(alias))
        return self

    @contextlib.contextmanager
    def _debug_build_phase(self, phase: str) -> "Iterator[None]":
        """Debug logging for build phases.

        Args:
            phase: The name of the build phase

        Yields:
            None: Context manager for the build phase, logging start and end times.
        """
        if self.instrumentation_config and self.instrumentation_config.debug_mode:
            logger.debug("Starting build phase: %s", phase, extra={"phase": phase})
            start = time.perf_counter()
            try:
                yield
            finally:
                duration = time.perf_counter() - start
                logger.debug(
                    "Completed build phase: %s in %.3fms",
                    phase,
                    duration * 1000,
                    extra={"phase": phase, "duration_ms": duration * 1000},
                )
        else:
            yield

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        correlation_id = CorrelationContext.get()
        start_time = time.perf_counter()

        # Log builder start if configured
        if self.instrumentation_config and self.instrumentation_config.log_queries:
            logger.info(
                "Building %s query",
                self.__class__.__name__,
                extra={
                    "builder_type": self.__class__.__name__,
                    "has_ctes": bool(self._with_ctes),
                    "parameter_count": len(self._parameters),
                    "correlation_id": correlation_id,
                },
            )

        if self._expression is None:
            self._raise_sql_builder_error("QueryBuilder expression not initialized.")
        # self._expression is known to be not None here.

        with self._debug_build_phase("copy_expression"):
            final_expression = self._expression.copy()

        if self._with_ctes:
            with self._debug_build_phase("process_ctes"):
                if hasattr(final_expression, "with_") and callable(getattr(final_expression, "with_", None)):
                    for alias, cte_node in self._with_ctes.items():
                        final_expression = final_expression.with_(  # pyright: ignore
                            cte_node.args["this"], as_=alias, copy=False
                        )
                elif (
                    isinstance(final_expression, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Union))
                    and self._with_ctes
                ):
                    final_expression = exp.With(expressions=list(self._with_ctes.values()), this=final_expression)
                else:
                    logger.warning(
                        "Expression type %s may not support CTEs. CTEs will not be added.",
                        type(final_expression).__name__,
                    )

        # Apply SQLGlot optimizations if enabled
        if self.enable_optimization:
            with self._debug_build_phase("optimize_expression"):
                final_expression = self._optimize_expression(final_expression)

        with self._debug_build_phase("generate_sql"):
            try:
                sql_string = final_expression.sql(dialect=self.dialect_name, pretty=True)
            except Exception as e:
                problematic_sql_for_log = "Could not serialize problematic expression."
                with contextlib.suppress(Exception):
                    problematic_sql_for_log = final_expression.sql(dialect=self.dialect_name)

                logger.exception("Error generating SQL. Problematic SQL (approx): %s", problematic_sql_for_log)
                err_msg = f"Error generating SQL from expression: {e!s}"
                # self._raise_sql_builder_error will make sql_string known as potentially unbound if not for NoReturn
                self._raise_sql_builder_error(err_msg, e)

        if self.instrumentation_config and self.instrumentation_config.log_queries:
            duration = time.perf_counter() - start_time
            logger.info(
                "Built %s query in %.3fms",
                self.__class__.__name__,
                duration * 1000,
                extra={
                    "builder_type": self.__class__.__name__,
                    "sql_length": len(sql_string),
                    "parameter_count": len(self._parameters),
                    "duration_ms": duration * 1000,
                    "correlation_id": correlation_id,
                },
            )

        # sql_string is now guaranteed to be assigned if no error was raised.
        return SafeQuery(sql=sql_string, parameters=self._parameters.copy(), dialect=self.dialect)

    def _optimize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Apply SQLGlot optimizations to the expression.

        Args:
            expression: The expression to optimize

        Returns:
            The optimized expression
        """
        if not self.enable_optimization:
            return expression

        optimized = expression

        try:
            # Use SQLGlot's comprehensive optimizer if schema is available
            if hasattr(self, "schema") and self.schema:
                with self._debug_build_phase("comprehensive_optimization"):
                    optimized = optimize(optimized.copy(), schema=self.schema, dialect=self.dialect_name)
            else:
                # Apply individual optimizations in order
                if self.simplify_expressions:
                    with self._debug_build_phase("simplify_expressions"):
                        optimized = simplify(optimized.copy())

                if self.optimize_predicates and isinstance(optimized, (exp.Select, exp.Update, exp.Delete)):
                    with self._debug_build_phase("pushdown_predicates"):
                        optimized = pushdown_predicates(optimized.copy(), dialect=self.dialect_name)

                if self.optimize_joins and isinstance(optimized, exp.Select):
                    with self._debug_build_phase("optimize_joins"):
                        optimized = optimize_joins(optimized.copy(), dialect=self.dialect_name)

                # Apply additional SQLGlot optimizations
                if isinstance(optimized, exp.Select):
                    with self._debug_build_phase("eliminate_subqueries"):
                        optimized = eliminate_subqueries(optimized.copy())

                    with self._debug_build_phase("unnest_subqueries"):
                        optimized = unnest_subqueries(optimized.copy())

        except Exception as e:
            # Log optimization failure but continue with unoptimized query
            logger.warning(
                "Query optimization failed: %s. Using unoptimized query.",
                str(e),
                extra={"error": str(e), "expression_type": type(expression).__name__},
            )
            return expression

        return optimized

    def to_statement(self, config: "Optional[SQLConfig]" = None) -> "SQL":
        """Converts the built query into a SQL statement object.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        safe_query = self.build()

        return SQL(
            statement=safe_query.sql,
            parameters=safe_query.parameters,
            dialect=safe_query.dialect,
            config=config,
            _builder_result_type=self._expected_result_type,
        )

    def __str__(self) -> str:
        """Return the SQL string representation of the query.

        Returns:
            str: The SQL string for this query.
        """
        try:
            return self.build().sql
        except Exception:
            # Fallback to default representation if build fails
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
            # Handle case where dialect might have a __name__ attribute
            if hasattr(self.dialect, "__name__"):
                return self.dialect.__name__.lower()
        return None
