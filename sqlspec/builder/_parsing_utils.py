"""Parsing utilities for SQL builders.

Provides common parsing functions to handle SQL expressions
passed as strings to builder methods.
"""

import contextlib
import re
from typing import TYPE_CHECKING, Any, Final

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

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = (
    "extract_expression",
    "extract_sql_object_expression",
    "parse_column_expression",
    "parse_condition_expression",
    "parse_order_expression",
    "parse_table_expression",
    "to_expression",
)

ALIAS_PARTS_EXPECTED_COUNT = 2
QUALIFIED_IDENTIFIER_PARTS = 2
_SIMPLE_IDENTIFIER_RE: Final["re.Pattern[str]"] = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$"
)
_BARE_KEYWORDS: Final[frozenset[str]] = frozenset({
    "all",
    "and",
    "any",
    "asc",
    "between",
    "case",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "delete",
    "desc",
    "distinct",
    "end",
    "exists",
    "false",
    "from",
    "in",
    "insert",
    "interval",
    "is",
    "like",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "or",
    "select",
    "session_user",
    "some",
    "true",
    "update",
    "user",
    "where",
})
_PARAMETER_VALIDATOR = ParameterValidator()


def extract_column_name(column: str | exp.Column) -> str:
    """Extract column name from column expression for parameter naming.

    Args:
        column: Column expression (string or SQLGlot Column)

    Returns:
        Column name as string for use as parameter name
    """
    if isinstance(column, str):
        col_expr: exp.Expr | None = exp.maybe_parse(column)
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


def _is_simple_identifier(value: str) -> bool:
    stripped = value.strip()
    if not _SIMPLE_IDENTIFIER_RE.fullmatch(stripped):
        return False
    return "." in stripped or stripped.lower() not in _BARE_KEYWORDS


def _simple_column_expression(value: str) -> exp.Column:
    parts = value.strip().split(".")
    identifiers = [exp.Identifier(this=part, quoted=False) for part in parts]
    if len(parts) == 1:
        return exp.Column(this=identifiers[0])
    if len(parts) == QUALIFIED_IDENTIFIER_PARTS:
        return exp.Column(this=identifiers[1], table=identifiers[0])
    return exp.Column(this=identifiers[2], table=identifiers[1], db=identifiers[0])


def parse_column_expression(column_input: str | exp.Expr | Any, builder: Any | None = None) -> exp.Expr:
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
        exp.Expr: Parsed SQLGlot expression
    """
    if isinstance(column_input, exp.Expr):
        return column_input

    if isinstance(column_input, str):
        if _is_simple_identifier(column_input):
            return _simple_column_expression(column_input)
        return exp.maybe_parse(column_input) or exp.column(column_input)

    if has_expression_and_sql(column_input):
        if column_input.expression is not None and isinstance(column_input.expression, exp.Expr):
            _merge_sql_parameters(column_input, builder)
            return column_input.expression

        _merge_sql_parameters(column_input, builder)
        sql_str = getattr(column_input, "raw_sql", None)
        if sql_str is None:
            sql_str = column_input.sql
        return exp.maybe_parse(sql_str) or exp.column(sql_str)

    if has_expression_attr(column_input) and isinstance(column_input._expression, exp.Expr):  # pyright: ignore[reportPrivateUsage]
        return column_input._expression  # pyright: ignore[reportPrivateUsage]

    return exp.maybe_parse(column_input) or exp.column(str(column_input))  # pyright: ignore[reportArgumentType]


def parse_table_expression(
    table_input: str, explicit_alias: "str | None" = None, dialect: "DialectType | None" = None
) -> exp.Expr:
    r"""Parses a table string that can be a name, a name with an alias, or a subquery string.

    The ``dialect`` selects the identifier-quoting rules so dialect-quoted identifiers such as
    BigQuery's ``\`project.dataset.table\``` are parsed into their qualified parts instead of a
    single literal name.
    """
    if explicit_alias is None and " " in table_input.strip():
        parts = table_input.strip().split(None, 1)
        if len(parts) == ALIAS_PARTS_EXPECTED_COUNT:
            base_table, alias = parts
            return exp.to_table(base_table, alias=alias, dialect=dialect)

    if _is_simple_identifier(table_input):
        return exp.to_table(table_input, alias=explicit_alias, dialect=dialect)

    with contextlib.suppress(Exception):
        parsed: exp.Expr | None = exp.maybe_parse(f"SELECT * FROM {table_input}", dialect=dialect)
        if isinstance(parsed, exp.Select):
            from_clause = parsed.find(exp.From)
            if from_clause is not None:
                table_expr = from_clause.this
                if explicit_alias:
                    return exp.alias_(table_expr, explicit_alias)
                return table_expr  # type: ignore[no-any-return]

    return exp.to_table(table_input, alias=explicit_alias, dialect=dialect)


def parse_order_expression(order_input: str | exp.Expr) -> exp.Expr:
    """Parse an ORDER BY expression that might include direction.

    Handles cases like:
        - Simple column: "name" -> Column(this=name)
        - With direction: "name DESC" -> Ordered(this=Column(name), desc=True)
        - Qualified: "users.name ASC" -> Ordered(this=Column(table=users, this=name), desc=False)
        - Function: "COUNT(*) DESC" -> Ordered(this=Count(this=Star), desc=True)

    Args:
        order_input: String or SQLGlot expression for ORDER BY

    Returns:
        exp.Expr: Parsed SQLGlot expression (usually Ordered or Column)
    """
    if isinstance(order_input, exp.Expr):
        return order_input

    order_value = str(order_input)
    parts = order_value.rsplit(None, 1)
    if len(parts) == ALIAS_PARTS_EXPECTED_COUNT and parts[1].lower() in {"asc", "desc"}:
        base, direction = parts
        if _is_simple_identifier(base):
            column_expr = _simple_column_expression(base)
            if direction.lower() == "desc":
                return exp.Ordered(this=column_expr, desc=True, nulls_first=False)
            return exp.Ordered(this=column_expr, desc=False, nulls_first=True)

    parsed = maybe_parse(order_value, into=exp.Ordered)
    if parsed:
        return parsed

    return parse_column_expression(order_input)


def parse_condition_expression(condition_input: str | exp.Expr | tuple[str, Any], builder: "Any" = None) -> exp.Expr:
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
        exp.Expr: Parsed SQLGlot expression (usually a comparison or logical op)
    """
    if isinstance(condition_input, exp.Expr):
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

    param_info = _PARAMETER_VALIDATOR.extract_parameters(condition_input)

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

    parsed: exp.Expr | None = exp.maybe_parse(condition_input)
    if parsed:
        return parsed
    return exp.condition(condition_input)


def extract_sql_object_expression(value: Any, builder: Any | None = None) -> exp.Expr:
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

    if value.expression is not None and isinstance(value.expression, exp.Expr):
        _merge_sql_parameters(value, builder)
        return value.expression

    _merge_sql_parameters(value, builder)
    sql_text = getattr(value, "raw_sql", None)
    if sql_text is None:
        sql_text = value.sql if not callable(value.sql) else str(value)

    return exp.maybe_parse(sql_text) or exp.convert(str(sql_text))


def extract_expression(value: Any) -> exp.Expr:
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
    if isinstance(value, exp.Expr):
        return value
    return exp.convert(value)


def to_expression(value: Any) -> exp.Expr:
    """Convert a Python value to a raw SQLGlot expression.

    Args:
        value: Python value or SQLGlot expression to convert.

    Returns:
        Raw SQLGlot expression.
    """
    if isinstance(value, exp.Expr):
        return value
    return exp.convert(value)


def _normalize_partition_by(
    partition_by: str | list[str] | exp.Expr | None,
) -> list[exp.Expr] | None:
    if isinstance(partition_by, str):
        return [exp.column(partition_by)]
    if isinstance(partition_by, list):
        return [exp.column(column) for column in partition_by]
    if isinstance(partition_by, exp.Expr):
        return [partition_by]
    return None


def _normalize_order_by(order_by: str | list[str] | exp.Expr | None) -> exp.Order | None:
    if isinstance(order_by, str):
        return exp.Order(expressions=[exp.column(order_by).asc()])
    if isinstance(order_by, list):
        return exp.Order(expressions=[exp.column(column).asc() for column in order_by])
    if isinstance(order_by, exp.Expr):
        return exp.Order(expressions=[order_by])
    return None


def _coerce_column(value: str | exp.Expr) -> exp.Expr:
    return exp.column(value) if isinstance(value, str) else value


def _resolve_dialect(dialect: "DialectType | None", default: "DialectType | None") -> "DialectType | None":
    return dialect or default
