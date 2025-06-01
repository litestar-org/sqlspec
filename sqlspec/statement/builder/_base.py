# ruff: noqa: SLF001
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import contextlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, NoReturn, Optional, TypeVar, Union

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.sql import SQL, SQLConfig

if TYPE_CHECKING:
    from sqlspec.statement.result import StatementResult

__all__ = (
    "QueryBuilder",
    "SafeQuery",
    "WhereClauseMixin",
)

logger = logging.getLogger("sqlspec")

# Result type variable
ResultT = TypeVar("ResultT", bound="StatementResult[Any]")


@dataclass(frozen=True)
class SafeQuery:
    """A safely constructed SQL query with bound parameters."""

    sql: str
    # Storing parameters as a dictionary for named parameters.
    parameters: dict[str, Any] = field(default_factory=dict)
    dialect: Optional[DialectType] = None


@dataclass
class QueryBuilder(ABC, Generic[ResultT]):
    """Abstract base class for SQL query builders.

    Provides common functionality for dialect handling, parameter management,
    and query construction.
    """

    dialect: DialectType = field(default=None)
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)
    # Internally, builders will use a dictionary for named parameters.
    _parameters: dict[str, Any] = field(default_factory=dict, init=False, repr=False, compare=False, hash=False)
    _parameter_counter: int = field(default=0, init=False, repr=False, compare=False, hash=False)
    _with_ctes: dict[str, exp.CTE] = field(default_factory=dict, init=False, repr=False, compare=False, hash=False)

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
    def _expected_result_type(self) -> "type[ResultT]":
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

    def _add_parameter(self, value: Any) -> str:
        """Adds a parameter to the query and returns its placeholder name.

        Args:
            value: The value of the parameter.

        Returns:
            str: The placeholder name for the parameter (e.g., :param_1).
        """
        self._parameter_counter += 1
        param_name = f"param_{self._parameter_counter}"
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
        param_name_to_use: str
        if name:
            if name in self._parameters:
                self._raise_sql_builder_error(f"Parameter name '{name}' already exists.")
            param_name_to_use = name
        else:
            self._parameter_counter += 1
            param_name_to_use = f"param_{self._parameter_counter}"

        self._parameters[param_name_to_use] = value
        return self, param_name_to_use

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
            except Exception as e:  # noqa: BLE001
                msg = f"An unexpected error occurred while parsing CTE query string: {e!s}"
                self._raise_sql_builder_error(msg, e)
        elif isinstance(query, exp.Select):
            cte_select_expression = query.copy()
        else:
            msg = (  # type: ignore[unreachable]
                f"Invalid query type for CTE: {type(query).__name__}. Must be QueryBuilder, str, or sqlglot.exp.Select."
            )
            self._raise_sql_builder_error(msg)

        self._with_ctes[alias] = exp.CTE(this=cte_select_expression, alias=exp.to_table(alias))
        return self

    def build(self) -> SafeQuery:
        """Builds the SQL query string and parameters.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        if self._expression is None:
            self._raise_sql_builder_error("QueryBuilder expression not initialized.")
        # self._expression is known to be not None here.
        final_expression = self._expression.copy()

        if self._with_ctes:
            if hasattr(final_expression, "with_") and callable(getattr(final_expression, "with_", None)):
                processed_expression = final_expression
                for alias, cte_node in self._with_ctes.items():
                    processed_expression = processed_expression.with_(  # pyright: ignore
                        cte_node.args["this"],  # The SELECT expression
                        as_=alias,  # The alias
                        copy=False,
                    )
                final_expression = processed_expression
            elif isinstance(final_expression, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Union)):
                ctes_for_with_expression = list(self._with_ctes.values())
                if ctes_for_with_expression:
                    final_expression = exp.With(expressions=ctes_for_with_expression, this=final_expression)
            else:
                logger.warning(
                    "Expression type %s may not support CTEs. CTEs will not be added.",
                    type(final_expression).__name__,
                )
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
        # sql_string is now guaranteed to be assigned if no error was raised.
        return SafeQuery(sql=sql_string, parameters=self._parameters.copy(), dialect=self.dialect)

    def to_statement(self, config: Optional[SQLConfig] = None) -> SQL:
        """Converts the built query into a SQL statement object.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        safe_query = self.build()

        return SQL(
            statement=safe_query.sql,
            parameters=safe_query.parameters,  # Pass parameters dict directly
            dialect=safe_query.dialect,  # Pass the dialect object/name
            config=config,
            _builder_result_type=self._expected_result_type,  # Property already returns type
        )

    def __str__(self) -> str:
        """Return the SQL string representation of the query.

        Returns:
            str: The SQL string for this query.
        """
        try:
            return self.build().sql
        except Exception:  # noqa: BLE001
            # Fallback to default representation if build fails
            return super().__str__()

    @property
    def dialect_name(self) -> Optional[str]:
        """Returns the name of the dialect, if set."""
        if isinstance(self.dialect, str):
            return self.dialect
        if self.dialect is not None:
            if isinstance(self.dialect, type) and issubclass(self.dialect, Dialect):
                return self.dialect.__name__.lower()
            if isinstance(self.dialect, Dialect):
                return type(self.dialect).__name__.lower()
            # Handle case where dialect might have a __name__ attribute
            if hasattr(self.dialect, "__name__"):  # type: ignore[unreachable]
                return self.dialect.__name__.lower()  # type: ignore[unreachable]
        return None


class WhereClauseMixin:
    """Mixin providing common WHERE clause convenience methods.

    This mixin can be used by DeleteBuilder, UpdateBuilder, and SelectBuilder
    to provide consistent WHERE filtering capabilities.

    Note: This mixin expects the including class to have:
    - add_parameter method
    - where method
    - dialect_name property
    - _raise_sql_builder_error method
    """

    def where_eq(self, column: Union[str, exp.Column], value: Any) -> "Self":
        """Add an equality condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            value: The value to compare against. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.eq(exp.var(param_name))
        return self.where(condition)  # type: ignore[attr-defined, no-any-return]

    def where_between(
        self,
        column: Union[str, exp.Column],
        low: Any,
        high: Any,
    ) -> "Self":
        """Add a BETWEEN condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            low: The lower bound value. Will be automatically parameterized.
            high: The upper bound value. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, low_param = self.add_parameter(low)  # type: ignore[attr-defined]
        _, high_param = self.add_parameter(high)  # type: ignore[attr-defined]

        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.var(low_param), exp.var(high_param))
        return self.where(condition)  # type: ignore[attr-defined, no-any-return]

    def where_like(self, column: Union[str, exp.Column], pattern: str) -> "Self":
        """Add a LIKE condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            pattern: The LIKE pattern. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name))
        return self.where(condition)  # type: ignore[attr-defined, no-any-return]

    def where_not_like(self, column: Union[str, exp.Column], pattern: str) -> "Self":
        """Add a NOT LIKE condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            pattern: The LIKE pattern. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name)).not_()
        return self.where(condition)  # type: ignore[attr-defined, no-any-return]

    def where_is_null(self, column: Union[str, exp.Column]) -> "Self":
        """Add an IS NULL condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)  # type: ignore[attr-defined, no-any-return]

    def where_is_not_null(self, column: Union[str, exp.Column]) -> "Self":
        """Add an IS NOT NULL condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.where(condition)  # type: ignore[attr-defined, no-any-return]

    def where_exists(self, subquery: Union[str, Any]) -> "Self":
        """Add a WHERE EXISTS clause.

        Args:
            subquery: The subquery for the EXISTS clause. Can be a SelectBuilder instance or raw SQL string.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the subquery cannot be parsed.
        """
        if hasattr(subquery, "_parameters") and hasattr(subquery, "build"):
            # This is a QueryBuilder (like SelectBuilder)
            subquery_builder_params: dict[str, Any] = subquery._parameters  # type: ignore[attr-defined]
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]

            # Get the subquery SQL
            sub_sql_obj = subquery.build()  # type: ignore[attr-defined]
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect_name)  # type: ignore[attr-defined,var-annotated]
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=self.dialect_name)  # type: ignore[attr-defined]

        if not sub_expr:
            msg = f"Could not parse subquery for EXISTS: {subquery}"
            raise SQLBuilderError(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)  # type: ignore[attr-defined, no-any-return]

    def where_not_exists(self, subquery: Union[str, Any]) -> "Self":
        """Add a WHERE NOT EXISTS clause.

        Args:
            subquery: The subquery for the NOT EXISTS clause. Can be a SelectBuilder instance or raw SQL string.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the subquery cannot be parsed.
        """
        if hasattr(subquery, "_parameters") and hasattr(subquery, "build"):
            # This is a QueryBuilder (like SelectBuilder)
            subquery_builder_params: dict[str, Any] = subquery._parameters  # type: ignore[attr-defined]
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]

            # Get the subquery SQL
            sub_sql_obj = subquery.build()  # type: ignore[attr-defined]
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect_name)  # type: ignore[attr-defined,var-annotated]
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=self.dialect_name)  # type: ignore[attr-defined]

        if not sub_expr:
            msg = f"Could not parse subquery for NOT EXISTS: {subquery}"
            raise SQLBuilderError(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)  # type: ignore[attr-defined, no-any-return]
