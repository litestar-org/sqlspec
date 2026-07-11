"""UPDATE statement builder.

Provides a fluent interface for building SQL UPDATE queries with
parameter binding and validation.
"""

from typing import TYPE_CHECKING, Any, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import BuiltQuery, QueryBuilder
from sqlspec.builder._dml import UpdateFromClauseMixin, UpdateSetClauseMixin, UpdateTableClauseMixin
from sqlspec.builder._explain import ExplainMixin
from sqlspec.builder._join import build_join_clause
from sqlspec.builder._select import ReturningClauseMixin, WhereClauseMixin
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder._select import Select
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("Update",)


class Update(
    QueryBuilder,
    WhereClauseMixin,
    ReturningClauseMixin,
    UpdateSetClauseMixin,
    UpdateFromClauseMixin,
    UpdateTableClauseMixin,
    ExplainMixin,
):
    """Builder for UPDATE statements.

    Constructs SQL UPDATE statements with parameter binding and validation.
    """

    __slots__ = ()
    _expression: exp.Expr | None

    def __init__(self, table: str | None = None, **kwargs: Any) -> None:
        """Initialize UPDATE with optional table.

        Args:
            table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        self._init_query_builder(kwargs)
        self._initialize_expression()

        if table:
            self.table(table)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Return the expected result type for this builder."""
        return SQLResult

    def _create_base_expression(self) -> exp.Update:
        """Create a base UPDATE expression.

        Returns:
            A new sqlglot Update expression with empty clauses.
        """
        return exp.Update(this=None, expressions=[])

    def join(
        self,
        table: "str | exp.Expr | Select",
        on: "str | exp.Expr",
        alias: "str | None" = None,
        join_type: str = "INNER",
    ) -> "Self":
        """Add JOIN clause to the UPDATE statement.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.
            join_type: Type of join (INNER, LEFT, RIGHT, FULL).

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add JOIN clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        target_table = self._expression.this
        if not isinstance(target_table, exp.Table):
            msg = "Cannot add JOIN clause before the UPDATE target table is set."
            raise SQLBuilderError(msg)

        join_expr = build_join_clause(cast("SQLBuilderProtocol", self), table, on, alias, join_type)
        target_table.append("joins", join_expr)

        return self

    def build(self, dialect: "DialectType" = None) -> "BuiltQuery":
        """Build the UPDATE query with validation.

        Args:
            dialect: Optional dialect override for SQL generation.

        Returns:
            BuiltQuery: The built query with SQL and parameters.

        Raises:
            SQLBuilderError: If no table is set or expression is not an UPDATE.
        """
        if self._expression is None:
            msg = "UPDATE expression not initialized."
            raise SQLBuilderError(msg)

        if not isinstance(self._expression, exp.Update):
            msg = "No UPDATE expression to build or expression is of the wrong type."
            raise SQLBuilderError(msg)

        if self._expression.this is None:
            msg = "No table specified for UPDATE statement."
            raise SQLBuilderError(msg)

        if not self._expression.args.get("expressions"):
            msg = "At least one SET clause must be specified for UPDATE statement."
            raise SQLBuilderError(msg)

        return super().build(dialect=dialect)
