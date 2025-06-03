from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("UpdateFromClauseMixin",)


class UpdateFromClauseMixin:
    """Mixin providing FROM clause for UPDATE builders (e.g., PostgreSQL style)."""

    def from_(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
        """Add a FROM clause to the UPDATE statement.

        Args:
            table: The table name, expression, or subquery to add to the FROM clause.
            alias: Optional alias for the table in the FROM clause.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):  # type: ignore[attr-defined]
            msg = "Cannot add FROM clause to non-UPDATE expression. Set the main table first."
            raise SQLBuilderError(msg)
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.to_table(table, alias=alias)
        elif hasattr(table, "build"):
            subquery_builder_params = getattr(table, "_parameters", None)
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            subquery_exp = exp.paren(getattr(table, "_expression", exp.select()))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        elif isinstance(table, exp.Expression):
            table_expr = exp.alias_(table, alias) if alias else table
        else:
            msg = f"Unsupported table type for FROM clause: {type(table)}"
            raise SQLBuilderError(msg)
        if self._expression.args.get("from") is None:  # type: ignore[attr-defined]
            self._expression.set("from", exp.From(expressions=[]))  # type: ignore[attr-defined]
        from_clause = self._expression.args["from"]  # type: ignore[attr-defined]
        if hasattr(from_clause, "append"):
            from_clause.append("expressions", table_expr)
        else:
            if not from_clause.expressions:
                from_clause.expressions = []
            from_clause.expressions.append(table_expr)
        return self
