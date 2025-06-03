from typing import TYPE_CHECKING, Any, Union, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("WhereClauseMixin", )


class WhereClauseMixin:
    """Mixin providing WHERE clause methods for SELECT, UPDATE, and DELETE builders."""

    def where(self, condition: Union[str, exp.Expression, exp.Condition, tuple[str, Any]]) -> Any:
        """Add a WHERE clause to the statement.

        Args:
            condition: The condition for the WHERE clause. Can be a string, sqlglot Expression, or (column, value) tuple.

        Raises:
            SQLBuilderError: If the current expression is not a supported statement type.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            msg = "Cannot add WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)
        valid_types = (exp.Select, exp.Update, exp.Delete)
        if not isinstance(builder._expression, valid_types):
            msg = f"Cannot add WHERE clause to unsupported expression type: {type(builder._expression).__name__}."
            raise SQLBuilderError(msg)

        # Normalize the condition
        if isinstance(condition, exp.Condition):
            condition_expr = condition
        elif isinstance(condition, str):
            condition_expr = exp.condition(condition)
        elif isinstance(condition, tuple):
            param_name = builder.add_parameter(condition[1])[1]
            condition_expr = exp.EQ(
                this=exp.column(condition[0]),
                expression=exp.Placeholder(this=param_name),
            )
        else:
            condition_expr = condition

        # Use dialect if available for Delete
        if isinstance(builder._expression, exp.Delete):
            builder._expression = builder._expression.where(
                condition_expr, dialect=getattr(builder, "dialect_name", None)
            )
        else:
            builder._expression = builder._expression.where(condition_expr, copy=False)
        return builder

    # The following methods are moved from the old WhereClauseMixin in _base.py
    def where_eq(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.eq(exp.var(param_name))
        return self.where(condition)

    def where_between(self, column: "Union[str, exp.Column]", low: Any, high: Any) -> "Self":
        _, low_param = self.add_parameter(low)  # type: ignore[attr-defined]
        _, high_param = self.add_parameter(high)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.var(low_param), exp.var(high_param))
        return self.where(condition)

    def where_like(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name))
        return self.where(condition)

    def where_not_like(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name)).not_()
        return self.where(condition)

    def where_is_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)

    def where_is_not_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.where(condition)

    def where_exists(self, subquery: "Union[str, Any]") -> "Self":
        sub_expr: exp.Expression
        if hasattr(subquery, "_parameters") and hasattr(subquery, "build"):
            subquery_builder_params: dict[str, Any] = subquery._parameters  # pyright: ignore
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            sub_sql_obj = subquery.build()  # pyright: ignore
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=getattr(self, "dialect_name", None))
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=getattr(self, "dialect_name", None))
        if not sub_expr:
            msg = f"Could not parse subquery for EXISTS: {subquery}"
            raise SQLBuilderError(msg)
        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)

    def where_not_exists(self, subquery: "Union[str, Any]") -> "Self":
        sub_expr: exp.Expression
        if hasattr(subquery, "_parameters") and hasattr(subquery, "build"):
            subquery_builder_params: dict[str, Any] = subquery._parameters  # pyright: ignore
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            sub_sql_obj = subquery.build()  # pyright: ignore
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=getattr(self, "dialect_name", None))
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=getattr(self, "dialect_name", None))
        if not sub_expr:
            msg = f"Could not parse subquery for NOT EXISTS: {subquery}"
            raise SQLBuilderError(msg)
        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)
