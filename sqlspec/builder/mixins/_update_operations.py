# pyright: reportPrivateUsage=false
"""UPDATE operation mixins.

Provides mixins for UPDATE statement functionality including
table specification, SET clauses, and FROM clauses.
"""

from collections.abc import Mapping
from typing import Any

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import extract_sql_object_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

__all__ = ("UpdateFromClauseMixin", "UpdateSetClauseMixin", "UpdateTableClauseMixin")

MIN_SET_ARGS = 2


@trait
class UpdateTableClauseMixin:
    """Mixin providing TABLE clause for UPDATE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def table(self, table_name: str, alias: str | None = None) -> Self:
        """Set the table to update.

        Args:
            table_name: The name of the table.
            alias: Optional alias for the table.

        Returns:
            The current builder instance for method chaining.
        """
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Update):
            self.set_expression(exp.Update(this=None, expressions=[], joins=[]))
            current_expr = self.get_expression()

        assert current_expr is not None
        table_expr: exp.Expression = exp.to_table(table_name, alias=alias)
        current_expr.set("this", table_expr)
        setattr(self, "_table", table_name)
        return self


@trait
class UpdateSetClauseMixin:
    """Mixin providing SET clause for UPDATE builders."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: str | None = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _process_update_value(self, val: Any, col: Any) -> exp.Expression:
        """Process a value for UPDATE assignment, handling SQL objects and parameters.

        Args:
            val: The value to process
            col: The column name for parameter naming

        Returns:
            The processed expression for the value
        """
        if isinstance(val, exp.Expression):
            return val
        if has_query_builder_parameters(val):
            subquery = val.build()
            sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
            value_expr = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect", None)))
            if has_query_builder_parameters(val):
                for p_name, p_value in val.parameters.items():
                    self.add_parameter(p_value, name=p_name)
            return value_expr
        if hasattr(val, "expression") and hasattr(val, "sql"):
            return extract_sql_object_expression(val, builder=self)
        column_name = col if isinstance(col, str) else str(col)
        if "." in column_name:
            column_name = column_name.split(".")[-1]
        param_name = self._generate_unique_parameter_name(column_name)
        param_name = self.add_parameter(val, name=param_name)[1]
        return exp.Placeholder(this=param_name)

    def set(self, *args: Any, **kwargs: Any) -> Self:
        """Set columns and values for the UPDATE statement.

        Supports:
        - set_(column, value)
        - set_(mapping)
        - set_(**kwargs)
        - set_(mapping, **kwargs)

        Args:
            *args: Either (column, value) or a mapping.
            **kwargs: Column-value pairs to set.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement or usage is invalid.

        Returns:
            The current builder instance for method chaining.
        """
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Update())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Update):
            msg = "Cannot add SET clause to non-UPDATE expression."
            raise SQLBuilderError(msg)
        assignments = []
        if len(args) == MIN_SET_ARGS and not kwargs:
            col, val = args
            col_expr = col if isinstance(col, exp.Column) else exp.column(col)
            value_expr = self._process_update_value(val, col)
            assignments.append(exp.EQ(this=col_expr, expression=value_expr))
        elif (len(args) == 1 and isinstance(args[0], Mapping)) or kwargs:
            all_values = dict(args[0] if args else {}, **kwargs)
            for col, val in all_values.items():
                value_expr = self._process_update_value(val, col)
                assignments.append(exp.EQ(this=exp.column(col), expression=value_expr))
        else:
            msg = "Invalid arguments for set(): use (column, value), mapping, or kwargs."
            raise SQLBuilderError(msg)
        existing = current_expr.args.get("expressions", [])
        current_expr.set("expressions", existing + assignments)
        return self


@trait
class UpdateFromClauseMixin:
    """Mixin providing FROM clause for UPDATE builders (e.g., PostgreSQL style)."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def from_(self, table: str | exp.Expression | Any, alias: str | None = None) -> Self:
        """Add a FROM clause to the UPDATE statement.

        Args:
            table: The table name, expression, or subquery to add to the FROM clause.
            alias: Optional alias for the table in the FROM clause.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Update):
            msg = "Cannot add FROM clause to non-UPDATE expression. Set the main table first."
            raise SQLBuilderError(msg)
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.to_table(table, alias=alias)
        elif has_query_builder_parameters(table):
            subquery_builder_parameters = getattr(table, "_parameters", None)
            if subquery_builder_parameters:
                for p_name, p_value in subquery_builder_parameters.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            subquery_exp = exp.paren(getattr(table, "_expression", exp.select()))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        elif isinstance(table, exp.Expression):
            table_expr = exp.alias_(table, alias) if alias else table
        else:
            msg = f"Unsupported table type for FROM clause: {type(table)}"
            raise SQLBuilderError(msg)
        if current_expr.args.get("from") is None:
            current_expr.set("from", exp.From(expressions=[]))
        from_clause = current_expr.args["from"]
        if hasattr(from_clause, "append"):
            from_clause.append("expressions", table_expr)
        else:
            if not from_clause.expressions:
                from_clause.expressions = []
            from_clause.expressions.append(table_expr)
        return self
