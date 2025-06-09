"""Centralized parsing utilities for SQLSpec builders.

This module provides common parsing functions to handle complex SQL expressions
that users might pass as strings to various builder methods.
"""

from typing import Optional, Union

from sqlglot import exp


def parse_column_expression(column_input: Union[str, exp.Expression]) -> exp.Expression:
    """Parse a column input that might be a complex expression.

    Handles cases like:
    - Simple column names: "name" -> Column(this=name)
    - Qualified names: "users.name" -> Column(table=users, this=name)
    - Aliased columns: "name AS user_name" -> Alias(this=Column(name), alias=user_name)
    - Function calls: "MAX(price)" -> Max(this=Column(price))
    - Complex expressions: "CASE WHEN ... END" -> Case(...)

    Args:
        column_input: String or SQLGlot expression representing a column/expression

    Returns:
        exp.Expression: Parsed SQLGlot expression
    """
    if isinstance(column_input, exp.Expression):
        return column_input

    if not isinstance(column_input, str):
        # Convert to string and try to parse
        column_input = str(column_input)

    try:
        # Try parsing as a full expression first (handles functions, CASE, etc.)
        parsed = exp.maybe_parse(column_input.strip())
        if parsed:
            return parsed
    except Exception:
        # Continue to fallback if parsing fails
        pass

    # Fallback: treat as simple column name
    return exp.column(column_input)


def parse_table_expression(table_input: Union[str, exp.Expression], explicit_alias: Optional[str] = None) -> exp.Expression:
    """Parse a table input that may contain an alias.

    Handles cases like:
    - Simple table names: "users" -> Table(this=users)
    - Table with alias: "users u" -> Table(this=users, alias=u)
    - Explicit AS syntax: "users AS u" -> Table(this=users, alias=u)
    - Subqueries: "(SELECT ...)" -> Subquery(...)

    Args:
        table_input: String or SQLGlot expression representing a table/subquery
        explicit_alias: Explicit alias to use (overrides any alias in table_input)

    Returns:
        exp.Expression: Parsed SQLGlot expression (usually Table or Subquery)
    """
    if isinstance(table_input, exp.Expression):
        if explicit_alias:
            return exp.alias_(table_input, explicit_alias)
        return table_input

    if not isinstance(table_input, str):
        table_input = str(table_input)

    if explicit_alias:
        # If explicit alias provided, use table_input as table name only
        return exp.table_(table_input, alias=explicit_alias)

    # Use SQLGlot's parser to handle table expressions with aliases
    try:
        import sqlglot
        # Parse as FROM clause and extract the table
        parsed = sqlglot.parse_one(f"FROM {table_input}")
        table_expr = parsed.find(exp.Table)
        if table_expr:
            return table_expr
    except Exception:
        # Fallback to basic table creation if parsing fails
        pass

    # Fallback: just table name
    return exp.table_(table_input)


def parse_order_expression(order_input: Union[str, exp.Expression]) -> exp.Expression:
    """Parse an ORDER BY expression that might include direction.

    Handles cases like:
    - Simple column: "name" -> Column(this=name)
    - With direction: "name DESC" -> Ordered(this=Column(name), desc=True)
    - Qualified: "users.name ASC" -> Ordered(this=Column(table=users, this=name), desc=False)
    - Function: "COUNT(*) DESC" -> Ordered(this=Count(this=Star), desc=True)

    Args:
        order_input: String or SQLGlot expression for ORDER BY

    Returns:
        exp.Expression: Parsed SQLGlot expression (usually Ordered or Column)
    """
    if isinstance(order_input, exp.Expression):
        return order_input

    if not isinstance(order_input, str):
        order_input = str(order_input)

    try:
        # Parse as ORDER BY clause and extract the expression
        import sqlglot
        parsed = sqlglot.parse_one(f"SELECT * FROM t ORDER BY {order_input}")
        select_expr = parsed.find(exp.Select)
        if select_expr and select_expr.args.get("order"):
            order_expr = select_expr.args["order"]
            if order_expr.expressions:
                return order_expr.expressions[0]
    except Exception:
        # Fallback to column parsing
        pass

    # Fallback: parse as column expression
    return parse_column_expression(order_input)


def parse_condition_expression(condition_input: Union[str, exp.Expression, tuple], builder=None) -> exp.Expression:
    """Parse a condition that might be complex SQL.

    Handles cases like:
    - Simple conditions: "name = 'John'" -> EQ(Column(name), Literal('John'))
    - Tuple format: ("name", "John") -> EQ(Column(name), Literal('John'))
    - Complex conditions: "age > 18 AND status = 'active'" -> And(GT(...), EQ(...))
    - Function conditions: "LENGTH(name) > 5" -> GT(Length(Column(name)), Literal(5))

    Args:
        condition_input: String, tuple, or SQLGlot expression for condition

    Returns:
        exp.Expression: Parsed SQLGlot expression (usually a comparison or logical op)
    """
    if isinstance(condition_input, exp.Expression):
        return condition_input

    if isinstance(condition_input, tuple) and len(condition_input) == 2:
        # Handle (column, value) tuple format with proper parameter binding
        column, value = condition_input
        column_expr = parse_column_expression(column)
        if value is None:
            return exp.Is(this=column_expr, expression=exp.null())
        # Use builder's parameter system if available
        if builder and hasattr(builder, "add_parameter"):
            _, param_name = builder.add_parameter(value)
            return exp.EQ(this=column_expr, expression=exp.Placeholder(this=param_name))
        # Fallback to literal value
        if isinstance(value, str):
            return exp.EQ(this=column_expr, expression=exp.Literal.string(value))
        if isinstance(value, (int, float)):
            return exp.EQ(this=column_expr, expression=exp.Literal.number(str(value)))
        return exp.EQ(this=column_expr, expression=exp.Literal.string(str(value)))

    if not isinstance(condition_input, str):
        condition_input = str(condition_input)

    try:
        # Parse as condition using SQLGlot's condition parser
        return exp.condition(condition_input)
    except Exception:
        # If that fails, try parsing as a general expression
        try:
            parsed = exp.maybe_parse(condition_input)
            if parsed:
                return parsed
        except Exception:
            pass

    # Ultimate fallback: treat as raw condition string
    return exp.condition(condition_input)


__all__ = (
    "parse_column_expression",
    "parse_condition_expression",
    "parse_order_expression",
    "parse_table_expression",
)
