"""Centralized parsing utilities for SQLSpec builders.

This module provides common parsing functions to handle complex SQL expressions
that users might pass as strings to various builder methods.
"""

import contextlib
from typing import Any, Final, Optional, Union, cast

from sqlglot import exp, maybe_parse, parse_one

from sqlspec.utils.type_guards import has_expression_attr, has_parameter_builder


def parse_column_expression(
    column_input: Union[str, exp.Expression, Any], builder: Optional[Any] = None
) -> exp.Expression:
    """Parse a column input that might be a complex expression.

    Handles cases like:
    - Simple column names: "name" -> Column(this=name)
    - Qualified names: "users.name" -> Column(table=users, this=name)
    - Aliased columns: "name AS user_name" -> Alias(this=Column(name), alias=user_name)
    - Function calls: "MAX(price)" -> Max(this=Column(price))
    - Complex expressions: "CASE WHEN ... END" -> Case(...)
    - Custom Column objects from our builder
    - SQL objects with raw SQL expressions

    Args:
        column_input: String, SQLGlot expression, SQL object, or Column object
        builder: Optional builder instance for parameter merging

    Returns:
        exp.Expression: Parsed SQLGlot expression
    """
    if isinstance(column_input, exp.Expression):
        return column_input

    # Handle SQL objects (from sql.raw with parameters)
    if hasattr(column_input, "expression") and hasattr(column_input, "sql"):
        # This is likely a SQL object
        expression = getattr(column_input, "expression", None)
        if expression is not None and isinstance(expression, exp.Expression):
            # Merge parameters from SQL object into builder if available
            if builder and hasattr(column_input, "parameters") and hasattr(builder, "add_parameter"):
                sql_parameters = getattr(column_input, "parameters", {})
                for param_name, param_value in sql_parameters.items():
                    builder.add_parameter(param_value, name=param_name)
            return cast("exp.Expression", expression)
        # If expression is None, fall back to parsing the raw SQL
        sql_text = getattr(column_input, "sql", "")
        # Merge parameters even when parsing raw SQL
        if builder and hasattr(column_input, "parameters") and hasattr(builder, "add_parameter"):
            sql_parameters = getattr(column_input, "parameters", {})
            for param_name, param_value in sql_parameters.items():
                builder.add_parameter(param_value, name=param_name)
        return exp.maybe_parse(sql_text) or exp.column(str(sql_text))

    if has_expression_attr(column_input):
        try:
            attr_value = column_input._expression
            if isinstance(attr_value, exp.Expression):
                return attr_value
        except AttributeError:
            pass

    return exp.maybe_parse(column_input) or exp.column(str(column_input))


def parse_table_expression(table_input: str, explicit_alias: Optional[str] = None) -> exp.Expression:
    """Parses a table string that can be a name, a name with an alias, or a subquery string."""
    with contextlib.suppress(Exception):
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

    tuple_condition_parts: Final[int] = 2
    if isinstance(condition_input, tuple) and len(condition_input) == tuple_condition_parts:
        column, value = condition_input
        column_expr = parse_column_expression(column)
        if value is None:
            return exp.Is(this=column_expr, expression=exp.null())
        if builder and has_parameter_builder(builder):
            from sqlspec.builder.mixins._where_clause import _extract_column_name

            column_name = _extract_column_name(column)
            param_name = builder._generate_unique_parameter_name(column_name)
            _, param_name = builder.add_parameter(value, name=param_name)
            return exp.EQ(this=column_expr, expression=exp.Placeholder(this=param_name))
        if isinstance(value, str):
            return exp.EQ(this=column_expr, expression=exp.convert(value))
        if isinstance(value, (int, float)):
            return exp.EQ(this=column_expr, expression=exp.convert(str(value)))
        return exp.EQ(this=column_expr, expression=exp.convert(str(value)))

    if not isinstance(condition_input, str):
        condition_input = str(condition_input)

    try:
        return exp.condition(condition_input)
    except Exception:
        try:
            parsed = exp.maybe_parse(condition_input)  # type: ignore[var-annotated]
            return parsed or exp.condition(condition_input)
        except Exception:
            return exp.condition(condition_input)


__all__ = ("parse_column_expression", "parse_condition_expression", "parse_order_expression", "parse_table_expression")
