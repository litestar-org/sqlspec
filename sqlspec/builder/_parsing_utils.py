"""Parsing utilities for SQL builders.

Provides common parsing functions to handle SQL expressions
passed as strings to builder methods.
"""

import contextlib
from typing import Any, Final, cast

from sqlglot import exp, maybe_parse

from sqlspec.builder._column import Column
from sqlspec.builder._expression_wrappers import ExpressionWrapper
from sqlspec.core import ParameterStyle, ParameterValidator
from sqlspec.utils.type_guards import (
    has_expression_and_parameters,
    has_expression_and_sql,
    has_expression_attr,
    has_parameter_builder,
)

ALIAS_PARTS_EXPECTED_COUNT = 2


def extract_column_name(column: str | exp.Column) -> str:
    """Extract column name from column expression for parameter naming.

    Args:
        column: Column expression (string or SQLGlot Column)

    Returns:
        Column name as string for use as parameter name
    """
    if isinstance(column, str):
        col_expr: exp.Expression | None = exp.maybe_parse(column)
        if isinstance(col_expr, exp.Column):
            return col_expr.name
        return column.split(".")[-1] if "." in column else column
    if isinstance(column, exp.Column):
        return column.name
    return "column"


def _merge_sql_parameters(sql_obj: Any, builder: Any) -> None:
    """Merge parameters from SQL object into builder.

    Args:
        sql_obj: SQL object with parameters attribute
        builder: Builder instance with add_parameter method
    """
    if not (builder and has_expression_and_parameters(sql_obj) and has_parameter_builder(builder)):
        return

    for param_name, param_value in sql_obj.parameters.items():
        builder.add_parameter(param_value, name=param_name)


def parse_column_expression(column_input: str | exp.Expression | Any, builder: Any | None = None) -> exp.Expression:
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

    if isinstance(column_input, str):
        return exp.maybe_parse(column_input) or exp.column(column_input)

    if has_expression_and_sql(column_input):
        if column_input.expression is not None and isinstance(column_input.expression, exp.Expression):
            _merge_sql_parameters(column_input, builder)
            return cast("exp.Expression", column_input.expression)

        _merge_sql_parameters(column_input, builder)
        sql_str = column_input.sql
        return exp.maybe_parse(sql_str) or exp.column(sql_str)

    if has_expression_attr(column_input) and isinstance(column_input._expression, exp.Expression):  # pyright: ignore[reportPrivateUsage]
        return column_input._expression  # pyright: ignore[reportPrivateUsage]

    return exp.maybe_parse(column_input) or exp.column(str(column_input))  # pyright: ignore[reportArgumentType]


def parse_table_expression(table_input: str, explicit_alias: str | None = None) -> exp.Expression:
    """Parses a table string that can be a name, a name with an alias, or a subquery string."""
    if explicit_alias is None and " " in table_input.strip():
        parts = table_input.strip().split(None, 1)
        if len(parts) == ALIAS_PARTS_EXPECTED_COUNT:
            base_table, alias = parts
            return exp.to_table(base_table, alias=alias)

    with contextlib.suppress(Exception):
        parsed: exp.Expression | None = exp.maybe_parse(f"SELECT * FROM {table_input}")
        if isinstance(parsed, exp.Select) and parsed.args.get("from"):
            from_clause = cast("exp.From", parsed.args.get("from"))
            table_expr = from_clause.this

            if explicit_alias:
                return exp.alias_(table_expr, explicit_alias)  # type:ignore[no-any-return]
            return table_expr  # type:ignore[no-any-return]

    return exp.to_table(table_input, alias=explicit_alias)


def parse_order_expression(order_input: str | exp.Expression) -> exp.Expression:
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

    parsed = maybe_parse(str(order_input), into=exp.Ordered)
    if parsed:
        return parsed

    return parse_column_expression(order_input)


def parse_condition_expression(
    condition_input: str | exp.Expression | tuple[str, Any], builder: "Any" = None
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

    Notes:
        Database-specific parameter placeholders such as $1, %s, and :1 are rewritten to :param_N format
        so SQLGlot can parse them consistently.
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
            column_name = extract_column_name(column)
            param_name = builder.generate_unique_parameter_name(column_name)  # pyright: ignore[reportAttributeAccessIssue]
            _, param_name = builder.add_parameter(value, name=param_name)
            return exp.EQ(this=column_expr, expression=exp.Placeholder(this=param_name))
        if isinstance(value, str):
            return exp.EQ(this=column_expr, expression=exp.convert(value))
        if isinstance(value, (int, float)):
            return exp.EQ(this=column_expr, expression=exp.convert(str(value)))
        return exp.EQ(this=column_expr, expression=exp.convert(str(value)))

    if not isinstance(condition_input, str):
        condition_input = str(condition_input)

    validator = ParameterValidator()
    param_info = validator.extract_parameters(condition_input)

    if param_info:
        converted_condition = condition_input
        for param in reversed(param_info):  # Reverse to preserve positions
            if param.style in {
                ParameterStyle.NUMERIC,
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
            }:
                placeholder = f":param_{param.ordinal}"
                converted_condition = (
                    converted_condition[: param.position]
                    + placeholder
                    + converted_condition[param.position + len(param.placeholder_text) :]
                )
        condition_input = converted_condition

    parsed: exp.Expression | None = exp.maybe_parse(condition_input)
    if parsed:
        return parsed
    return exp.condition(condition_input)


def extract_sql_object_expression(value: Any, builder: Any | None = None) -> exp.Expression:
    """Extract SQLGlot expression from SQL object value with parameter merging.

    Handles the common pattern of:
    1. Check if value has expression and SQL attributes
    2. Try to get expression first, merge parameters if available
    3. Fall back to parsing raw SQL text if expression is None
    4. Merge parameters in both cases
    5. Handle callable SQL text

    This consolidates duplicated logic across builder files that process
    SQL objects (like those from sql.raw() calls).

    Args:
        value: The SQL object value to process
        builder: Optional builder instance for parameter merging (must have add_parameter method)

    Returns:
        SQLGlot Expression extracted from the SQL object

    Raises:
        ValueError: If the value doesn't appear to be a SQL object
    """
    if not has_expression_and_sql(value):
        msg = f"Value does not have both expression and sql attributes: {type(value)}"
        raise ValueError(msg)

    if value.expression is not None and isinstance(value.expression, exp.Expression):
        _merge_sql_parameters(value, builder)
        return cast("exp.Expression", value.expression)

    _merge_sql_parameters(value, builder)
    sql_text = value.sql if not callable(value.sql) else str(value)

    return exp.maybe_parse(sql_text) or exp.convert(str(sql_text))


def extract_expression(value: Any) -> exp.Expression:
    """Extract SQLGlot expression from value, handling wrapper types.

    Args:
        value: String, SQLGlot expression, or wrapper type.

    Returns:
        Raw SQLGlot expression.
    """
    from sqlspec.builder._select import Case

    if isinstance(value, str):
        return exp.column(value)
    if isinstance(value, Column):
        return value.sqlglot_expression
    if isinstance(value, ExpressionWrapper):
        return value.expression
    if isinstance(value, Case):
        return exp.Case(ifs=value.conditions, default=value.default)
    if isinstance(value, exp.Expression):
        return value
    return exp.convert(value)


def to_expression(value: Any) -> exp.Expression:
    """Convert a Python value to a raw SQLGlot expression.

    Args:
        value: Python value or SQLGlot expression to convert.

    Returns:
        Raw SQLGlot expression.
    """
    if isinstance(value, exp.Expression):
        return value
    return exp.convert(value)


__all__ = (
    "extract_expression",
    "extract_sql_object_expression",
    "parse_column_expression",
    "parse_condition_expression",
    "parse_order_expression",
    "parse_table_expression",
    "to_expression",
)
