"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder, SafeQuery
from sqlspec.builder.mixins import (
    ReturningClauseMixin,
    UpdateFromClauseMixin,
    UpdateSetClauseMixin,
    UpdateTableClauseMixin,
    WhereClauseMixin,
)
from sqlspec.core.result import SQLResult
from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.builder._select import Select

__all__ = ("Update",)


class Update(
    QueryBuilder,
    WhereClauseMixin,
    ReturningClauseMixin,
    UpdateSetClauseMixin,
    UpdateFromClauseMixin,
    UpdateTableClauseMixin,
):
    """Builder for UPDATE statements.

    This builder provides a fluent interface for constructing SQL UPDATE statements
    with automatic parameter binding and validation.

    Example:
        ```python
        update_query = (
            Update()
            .table("users")
            .set(name="John Doe")
            .set(email="john@example.com")
            .where("id = 1")
        )

        update_query = (
            Update("users").set(name="John Doe").where("id = 1")
        )

        update_query = (
            Update()
            .table("users")
            .set(status="active")
            .where_eq("id", 123)
        )

        update_query = (
            Update()
            .table("users", "u")
            .set(name="Updated Name")
            .from_("profiles", "p")
            .where("u.id = p.user_id AND p.is_verified = true")
        )
        ```
    """

    __slots__ = ("_table",)
    _expression: Optional[exp.Expression]

    def __init__(self, table: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize UPDATE with optional table.

        Args:
            table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        super().__init__(**kwargs)
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
        return exp.Update(this=None, expressions=[], joins=[])

    def join(
        self,
        table: "Union[str, exp.Expression, Select]",
        on: "Union[str, exp.Expression]",
        alias: "Optional[str]" = None,
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

        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif isinstance(table, QueryBuilder):
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp

            subquery_parameters = table._parameters
            if subquery_parameters:
                for p_name, p_value in subquery_parameters.items():
                    self.add_parameter(p_value, name=p_name)
        else:
            table_expr = table

        on_expr: exp.Expression = exp.condition(on) if isinstance(on, str) else on

        join_type_upper = join_type.upper()
        if join_type_upper == "INNER":
            join_expr = exp.Join(this=table_expr, on=on_expr)
        elif join_type_upper == "LEFT":
            join_expr = exp.Join(this=table_expr, on=on_expr, side="LEFT")
        elif join_type_upper == "RIGHT":
            join_expr = exp.Join(this=table_expr, on=on_expr, side="RIGHT")
        elif join_type_upper == "FULL":
            join_expr = exp.Join(this=table_expr, on=on_expr, side="FULL", kind="OUTER")
        else:
            msg = f"Unsupported join type: {join_type}"
            raise SQLBuilderError(msg)

        if not self._expression.args.get("joins"):
            self._expression.set("joins", [])
        self._expression.args["joins"].append(join_expr)

        return self

    def build(self) -> "SafeQuery":
        """Build the UPDATE query with validation.

        Returns:
            SafeQuery: The built query with SQL and parameters.

        Raises:
            SQLBuilderError: If no table is set or expression is not an UPDATE.
        """
        if self._expression is None:
            msg = "UPDATE expression not initialized."
            raise SQLBuilderError(msg)

        if not isinstance(self._expression, exp.Update):
            msg = "No UPDATE expression to build or expression is of the wrong type."
            raise SQLBuilderError(msg)

        if getattr(self._expression, "this", None) is None:
            msg = "No table specified for UPDATE statement."
            raise SQLBuilderError(msg)

        if not self._expression.args.get("expressions"):
            msg = "At least one SET clause must be specified for UPDATE statement."
            raise SQLBuilderError(msg)

        return super().build()
