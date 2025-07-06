"""Update operation mixins for SQL builders."""

from collections.abc import Mapping
from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

__all__ = ("UpdateFromClauseMixin", "UpdateSetClauseMixin", "UpdateTableClauseMixin")

MIN_SET_ARGS = 2


class UpdateTableClauseMixin:
    """Mixin providing TABLE clause for UPDATE builders."""

    _expression: Optional[exp.Expression] = None

    def table(self, table_name: str, alias: Optional[str] = None) -> Self:
        """Set the table to update.

        Args:
            table_name: The name of the table.
            alias: Optional alias for the table.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            self._expression = exp.Update(this=None, expressions=[], joins=[])
        table_expr: exp.Expression = exp.to_table(table_name, alias=alias)
        self._expression.set("this", table_expr)
        setattr(self, "_table", table_name)
        return self


class UpdateSetClauseMixin:
    """Mixin providing SET clause for UPDATE builders."""

    _expression: Optional[exp.Expression] = None

    def set(self, *args: Any, **kwargs: Any) -> Self:
        """Set columns and values for the UPDATE statement.

        Supports:
        - set(column, value)
        - set(mapping)
        - set(**kwargs)
        - set(mapping, **kwargs)

        Args:
            *args: Either (column, value) or a mapping.
            **kwargs: Column-value pairs to set.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement or usage is invalid.

        Returns:
            The current builder instance for method chaining.
        """

        if self._expression is None:
            self._expression = exp.Update()
        if not isinstance(self._expression, exp.Update):
            msg = "Cannot add SET clause to non-UPDATE expression."
            raise SQLBuilderError(msg)
        assignments = []
        # (column, value) signature
        if len(args) == MIN_SET_ARGS and not kwargs:
            col, val = args
            col_expr = col if isinstance(col, exp.Column) else exp.column(col)
            # If value is an expression, use it directly
            if isinstance(val, exp.Expression):
                value_expr = val
            elif has_query_builder_parameters(val):
                # It's a builder (like SelectBuilder), convert to subquery
                subquery = val.build()
                # Parse the SQL and use as expression
                sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
                value_expr = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect", None)))
                # Merge parameters from subquery
                if has_query_builder_parameters(val):
                    for p_name, p_value in val.parameters.items():
                        self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            else:
                param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
                value_expr = exp.Placeholder(this=param_name)
            assignments.append(exp.EQ(this=col_expr, expression=value_expr))
        # mapping and/or kwargs
        elif (len(args) == 1 and isinstance(args[0], Mapping)) or kwargs:
            all_values = dict(args[0] if args else {}, **kwargs)
            for col, val in all_values.items():
                # If value is an expression, use it directly
                if isinstance(val, exp.Expression):
                    value_expr = val
                elif has_query_builder_parameters(val):
                    # It's a builder (like SelectBuilder), convert to subquery
                    subquery = val.build()
                    # Parse the SQL and use as expression
                    sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
                    value_expr = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect", None)))
                    # Merge parameters from subquery
                    if has_query_builder_parameters(val):
                        for p_name, p_value in val.parameters.items():
                            self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
                else:
                    param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
                    value_expr = exp.Placeholder(this=param_name)
                assignments.append(exp.EQ(this=exp.column(col), expression=value_expr))
        else:
            msg = "Invalid arguments for set(): use (column, value), mapping, or kwargs."
            raise SQLBuilderError(msg)
        # Append to existing expressions instead of replacing
        existing = self._expression.args.get("expressions", [])
        self._expression.set("expressions", existing + assignments)
        return self


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
        elif has_query_builder_parameters(table):
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
