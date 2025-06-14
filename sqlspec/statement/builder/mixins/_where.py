from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlglot import exp, parse_one
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._parsing_utils import parse_column_expression, parse_condition_expression

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("WhereClauseMixin",)


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
        # Special case: if this is an UpdateBuilder and _expression is not exp.Update, raise the expected error for test coverage
        from sqlspec.statement.builder.update import UpdateBuilder

        if isinstance(self, UpdateBuilder) and not (
            hasattr(self, "_expression") and isinstance(self._expression, exp.Update)
        ):
            msg = "Cannot add WHERE clause to non-UPDATE expression"
            raise SQLBuilderError(msg)
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            msg = "Cannot add WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)
        valid_types = (exp.Select, exp.Update, exp.Delete)
        if not isinstance(builder._expression, valid_types):
            msg = f"Cannot add WHERE clause to unsupported expression type: {type(builder._expression).__name__}."
            raise SQLBuilderError(msg)

        # Normalize the condition using enhanced parsing
        if isinstance(condition, tuple):
            # Handle tuple format with proper parameter binding
            param_name = builder.add_parameter(condition[1])[1]
            condition_expr = exp.EQ(
                this=parse_column_expression(condition[0]), expression=exp.Placeholder(this=param_name)
            )
        else:
            condition_expr = parse_condition_expression(condition)  # type: ignore[assignment]

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
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.eq(exp.var(param_name))
        return cast("Self", self.where(condition))

    def where_between(self, column: "Union[str, exp.Column]", low: Any, high: Any) -> "Self":
        _, low_param = self.add_parameter(low)  # type: ignore[attr-defined]
        _, high_param = self.add_parameter(high)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.var(low_param), exp.var(high_param))
        return cast("Self", self.where(condition))

    def where_like(self, column: "Union[str, exp.Column]", pattern: str, escape: Optional[str] = None) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if escape is not None:
            cond = exp.Like(this=col_expr, expression=exp.var(param_name), escape=exp.Literal.string(str(escape)))
        else:
            cond = col_expr.like(exp.var(param_name))
        condition: exp.Expression = cond
        return cast("Self", self.where(condition))

    def where_not_like(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name)).not_()
        return cast("Self", self.where(condition))

    def where_is_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return cast("Self", self.where(condition))

    def where_is_not_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return cast("Self", self.where(condition))

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
        exists_expr = exp.Exists(this=sub_expr)
        return cast("Self", self.where(exists_expr))

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
        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return cast("Self", self.where(not_exists_expr))

    def where_not_null(self, column: "Union[str, exp.Column]") -> "Self":
        """Alias for where_is_not_null for compatibility with test expectations."""
        return self.where_is_not_null(column)

    def where_in(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        """Add a WHERE ... IN (...) clause. Supports subqueries and iterables."""
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        # Subquery support
        if hasattr(values, "build") or isinstance(values, exp.Expression):
            if hasattr(values, "build"):
                subquery = values.build()  # pyright: ignore
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values
            condition = col_expr.isin(subquery_exp)
            return cast("Self", self.where(condition))
        # Iterable of values
        if not hasattr(values, "__iter__") or isinstance(values, (str, bytes)):
            msg = "Unsupported type for 'values' in WHERE IN"
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        condition = col_expr.isin(*params)
        return cast("Self", self.where(condition))

    def where_not_in(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        """Add a WHERE ... NOT IN (...) clause. Supports subqueries and iterables."""
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if hasattr(values, "build") or isinstance(values, exp.Expression):
            if hasattr(values, "build"):
                subquery = values.build()  # pyright: ignore
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values
            condition = exp.Not(this=col_expr.isin(subquery_exp))
            return cast("Self", self.where(condition))
        if not hasattr(values, "__iter__") or isinstance(values, (str, bytes)):
            msg = "Values for where_not_in must be a non-string iterable or subquery."
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        condition = exp.Not(this=col_expr.isin(*params))
        return cast("Self", self.where(condition))

    def where_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return cast("Self", self.where(condition))

    def where_any(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        """Add a WHERE ... = ANY (...) clause. Supports subqueries and iterables.

        Args:
            column: The column to compare.
            values: A subquery or iterable of values.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if hasattr(values, "build") or isinstance(values, exp.Expression):
            if hasattr(values, "build"):
                subquery = values.build()  # pyright: ignore
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values
            condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return cast("Self", self.where(condition))
        if isinstance(values, str):
            # Try to parse as subquery expression with enhanced parsing
            try:
                # Parse as a subquery expression
                parsed_expr = parse_one(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return cast("Self", self.where(condition))
            except Exception:  # noqa: S110
                # Subquery parsing failed for WHERE ANY
                pass
            # If parsing fails, fall through to error
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        if not hasattr(values, "__iter__") or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        tuple_expr = exp.Tuple(expressions=params)
        condition = exp.EQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return cast("Self", self.where(condition))

    def where_not_any(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        """Add a WHERE ... <> ANY (...) (or NOT = ANY) clause. Supports subqueries and iterables.

        Args:
            column: The column to compare.
            values: A subquery or iterable of values.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if hasattr(values, "build") or isinstance(values, exp.Expression):
            if hasattr(values, "build"):
                subquery = values.build()  # pyright: ignore
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values
            condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return cast("Self", self.where(condition))
        if isinstance(values, str):
            # Try to parse as subquery expression with enhanced parsing
            try:
                # Parse as a subquery expression
                parsed_expr = parse_one(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return cast("Self", self.where(condition))
            except Exception:  # noqa: S110
                # Subquery parsing failed for WHERE NOT ANY
                pass
            # If parsing fails, fall through to error
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        if not hasattr(values, "__iter__") or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        tuple_expr = exp.Tuple(expressions=params)
        condition = exp.NEQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return cast("Self", self.where(condition))
