"""Update operation mixins for SQL builders."""

from collections.abc import Mapping
from typing import Any, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

__all__ = ("UpdateFromClauseMixin", "UpdateSetClauseMixin", "UpdateTableClauseMixin")

MIN_SET_ARGS = 2


@trait
class UpdateTableClauseMixin:
    """Mixin providing TABLE clause for UPDATE builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

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


@trait
class UpdateSetClauseMixin:
    """Mixin providing SET clause for UPDATE builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
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
            # Handle SQL objects (from sql.raw with parameters)
            expression = getattr(val, "expression", None)
            if expression is not None and isinstance(expression, exp.Expression):
                # Merge parameters from SQL object into builder
                if hasattr(val, "parameters"):
                    sql_parameters = getattr(val, "parameters", {})
                    for param_name, param_value in sql_parameters.items():
                        self.add_parameter(param_value, name=param_name)
                return cast("exp.Expression", expression)
            # If expression is None, fall back to parsing the raw SQL
            sql_text = getattr(val, "sql", "")
            # Merge parameters even when parsing raw SQL
            if hasattr(val, "parameters"):
                sql_parameters = getattr(val, "parameters", {})
                for param_name, param_value in sql_parameters.items():
                    self.add_parameter(param_value, name=param_name)
            parsed_expr = exp.maybe_parse(sql_text)
            return parsed_expr if parsed_expr is not None else exp.convert(str(sql_text))
        column_name = col if isinstance(col, str) else str(col)
        if "." in column_name:
            column_name = column_name.split(".")[-1]
        param_name = self._generate_unique_parameter_name(column_name)
        param_name = self.add_parameter(val, name=param_name)[1]
        return exp.Placeholder(this=param_name)

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
        existing = self._expression.args.get("expressions", [])
        self._expression.set("expressions", existing + assignments)
        return self


@trait
class UpdateFromClauseMixin:
    """Mixin providing FROM clause for UPDATE builders (e.g., PostgreSQL style)."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

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
        if self._expression is None or not isinstance(self._expression, exp.Update):
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
        if self._expression.args.get("from") is None:
            self._expression.set("from", exp.From(expressions=[]))
        from_clause = self._expression.args["from"]
        if hasattr(from_clause, "append"):
            from_clause.append("expressions", table_expr)
        else:
            if not from_clause.expressions:
                from_clause.expressions = []
            from_clause.expressions.append(table_expr)
        return self
