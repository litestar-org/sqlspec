"""Centralized parsing utilities for SQLSpec builders.

This module provides common parsing functions to handle complex SQL expressions
that users might pass as strings to various builder methods.
"""

import contextlib
from typing import Any, Optional, Union, cast

from sqlglot import exp, maybe_parse, parse_one


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
    return exp.maybe_parse(column_input) or exp.column(str(column_input))


def parse_table_expression(table_input: str, explicit_alias: Optional[str] = None) -> exp.Expression:
    """Parses a table string that can be a name, a name with an alias, or a subquery string."""
    with contextlib.suppress(Exception):
        # Wrapping in a SELECT statement is a robust way to parse various table-like syntaxes
        parsed = parse_one(f"SELECT * FROM {table_input}")
        if isinstance(parsed, exp.Select) and parsed.args.get("from"):
            from_clause = cast("exp.From", parsed.args.get("from"))
            table_expr = from_clause.this

            if explicit_alias:
                return exp.alias_(table_expr, explicit_alias)  # type:ignore[no-any-return]
            return table_expr  # type:ignore[no-any-return]

    return exp.to_table(table_input, alias=explicit_alias)


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

    with contextlib.suppress(Exception):
        parsed = maybe_parse(str(order_input), into=exp.Ordered)
        if parsed:
            return parsed

    return parse_column_expression(order_input)


def parse_condition_expression(
    condition_input: Union[str, exp.Expression, tuple[str, Any]], builder: "Any" = None
) -> exp.Expression:
    """Parse a condition that might be complex SQL.

    Handles cases like:
    - Simple conditions: "name = 'John'" -> EQ(Column(name), Literal('John'))
    - Tuple format: ("name", "John") -> EQ(Column(name), Literal('John'))
    - Complex conditions: "age > 18 AND status = 'active'" -> And(GT(...), EQ(...))
    - Function conditions: "LENGTH(name) > 5" -> GT(Length(Column(name)), Literal(5))

    Args:
        condition_input: String, tuple, or SQLGlot expression for condition
        builder: Optional builder instance for parameter binding

    Returns:
        exp.Expression: Parsed SQLGlot expression (usually a comparison or logical op)
    """
    if isinstance(condition_input, exp.Expression):
        return condition_input

    tuple_condition_parts = 2
    if isinstance(condition_input, tuple) and len(condition_input) == tuple_condition_parts:
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
            parsed = exp.maybe_parse(condition_input)  # type: ignore[var-annotated]
            if parsed:
                return parsed  # type:ignore[no-any-return]
        except Exception:  # noqa: S110
            # SQLGlot condition parsing failed, will use raw condition
            pass

    # Ultimate fallback: treat as raw condition string
    return exp.condition(condition_input)


__all__ = ("parse_column_expression", "parse_condition_expression", "parse_order_expression", "parse_table_expression")
