"""Parsing utilities for SQL builders.

Provides common parsing functions to handle SQL expressions
passed as strings to builder methods.
"""

import importlib
import re
import unicodedata
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Final

import sqlglot
from sqlglot import exp, maybe_parse
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokenizer_core import TokenType

from sqlspec.builder._column import Column, ColumnExpression, FunctionColumn
from sqlspec.builder._expression_wrappers import ExpressionWrapper
from sqlspec.core import SQL, ParameterStyle, ParameterValidator, default_nulls, ordered
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_expression_attr, has_parameter_builder

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.tokenizer_core import Token

__all__ = (
    "extract_expression",
    "extract_sql_object_expression",
    "parse_column_expression",
    "parse_condition_expression",
    "parse_order_expression",
    "parse_table_expression",
    "parse_table_function",
    "resolve_value_expression",
    "sql_named_parameters",
    "to_expression",
    "unwrap_expression_source",
)

ALIAS_PARTS_EXPECTED_COUNT = 2
QUALIFIED_IDENTIFIER_PARTS = 2
_SIMPLE_IDENTIFIER_RE: Final["re.Pattern[str]"] = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$"
)
_QUOTED_NAME_PATTERN: Final[str] = r'"(?:[^"]|"")+"|`(?:[^`]|``)+`|\[(?:[^\]]|\]\])+\]'
_TABLE_NAME_PART_PATTERN: Final[str] = rf"(?:{_QUOTED_NAME_PATTERN}|[\w$#]+(?:-[\w$#]+)*)"
_ALIAS_PATTERN: Final[str] = rf"(?:{_QUOTED_NAME_PATTERN}|[\w$]+)"
_TABLE_NAME_PART_RE: Final["re.Pattern[str]"] = re.compile(_TABLE_NAME_PART_PATTERN)
_TABLE_REFERENCE_RE: Final["re.Pattern[str]"] = re.compile(
    rf"(?P<table>{_TABLE_NAME_PART_PATTERN}(?:\.{_TABLE_NAME_PART_PATTERN})*)"
    rf"(?:\s+(?:as\s+)?(?P<alias>{_ALIAS_PATTERN}))?",
    re.IGNORECASE,
)
_ALIAS_RE: Final["re.Pattern[str]"] = re.compile(_ALIAS_PATTERN)
_TSQL_VARIABLE_RE: Final["re.Pattern[str]"] = re.compile(
    rf"@(?P<name>[A-Za-z_][\w$#@]*)(?:\s+(?:as\s+)?(?P<alias>{_ALIAS_PATTERN}))?", re.IGNORECASE
)
_ALIAS_KEYWORD: Final[str] = "as"
_TABLE_TEXT_SEPARATOR_CHARACTERS: Final[str] = " \t\r\n"
_INVISIBLE_CATEGORIES: Final[frozenset[str]] = frozenset({"Cc", "Cf", "Zl", "Zp", "Zs"})
_QUOTED_TOKEN_TYPES: Final[frozenset[TokenType]] = frozenset({TokenType.IDENTIFIER, TokenType.STRING})
_EXPLICIT_QUOTE_META_KEY: Final[str] = "sqlspec_explicit_table_quote"
_MAX_DIRECT_TABLE_PARTS: Final[int] = 3
_QUOTE_CLOSERS: Final[dict[str, str]] = {'"': '"', "`": "`", "[": "]"}
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
_NON_IDENTIFIER_PATTERN: Final["re.Pattern[str]"] = re.compile(r"\W+")
_DIALECT_ALIASES: Final[dict[str, str]] = {
    "ase": "sybase",
    "cockroach": "postgres",
    "cockroachdb": "postgres",
    "mariadb": "mysql",
    "mssql": "tsql",
    "postgresql": "postgres",
    "sa": "sqlanywhere",
}


def extract_column_name(column: str | exp.Column) -> str:
    """Extract column name from column expression for parameter naming.

    Non-column expressions such as ``COUNT(*)`` or ``SUM(o.amount)`` become
    identifier-safe names (``count``, ``sum_o_amount``) so placeholders stay valid.

    Args:
        column: Column expression (string or SQLGlot Column)

    Returns:
        Column name as string for use as parameter name
    """
    if isinstance(column, str):
        col_expr: exp.Expr | None = exp.maybe_parse(column)
        if isinstance(col_expr, exp.Column):
            return col_expr.name
        return _NON_IDENTIFIER_PATTERN.sub("_", column).strip("_").lower() or "column"
    if isinstance(column, exp.Column):
        return column.name
    return "column"


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
        - Query builders and query nodes, as scalar subqueries when a builder is given

    Args:
        column_input: String, SQLGlot expression, SQL object, Column object, or query builder
        builder: Optional builder instance for parameter merging

    Returns:
        exp.Expr: Parsed SQLGlot expression
    """
    if isinstance(column_input, exp.Expr):
        if builder is not None and isinstance(column_input, (exp.Select, exp.SetOperation)):
            return _scalar_subquery(column_input.copy())
        return column_input

    if isinstance(column_input, str):
        if _is_simple_identifier(column_input):
            return _simple_column_expression(column_input)
        return exp.maybe_parse(column_input) or exp.column(column_input)

    from sqlspec.builder._base import QueryBuilder

    if builder is not None and isinstance(column_input, QueryBuilder):
        return _scalar_subquery(builder._embed_subquery(column_input, alias_hint="subquery"))

    if isinstance(column_input, SQL):
        extracted = extract_sql_object_expression(column_input, builder)
        if builder is not None and isinstance(extracted, (exp.Select, exp.SetOperation)):
            return _scalar_subquery(extracted)
        return extracted

    if has_expression_attr(column_input) and isinstance(column_input._expression, exp.Expr):  # pyright: ignore[reportPrivateUsage]
        return column_input._expression  # pyright: ignore[reportPrivateUsage]

    return exp.maybe_parse(column_input) or exp.column(str(column_input))  # pyright: ignore[reportArgumentType]


def parse_table_expression(
    table_input: str, explicit_alias: "str | None" = None, dialect: "DialectType | None" = None
) -> exp.Expr:
    r"""Parse table text into a table expression.

    The accepted form is a dotted table name followed by an optional alias written ``name alias``
    or ``name AS alias``. The text is read once with the tokenizer of ``dialect``, so BigQuery's
    ``\`project.dataset.table\``` becomes its qualified parts.

    Args:
        table_input: The table text.
        explicit_alias: Optional alias supplied separately from the table text.
        dialect: Dialect used to read the text.

    Returns:
        The parsed table expression.

    Raises:
        SQLBuilderError: If the text is not a table name with an optional alias.
    """
    return _parse_table_reference(table_input, explicit_alias, dialect)


def parse_table_function(
    function_input: str, explicit_alias: "str | None" = None, dialect: "DialectType | None" = None
) -> exp.Expr:
    """Parse a possibly qualified function call used as a lateral join source.

    Arguments may hold literals, placeholders, and column references. A named function runs with the
    arguments it is given, and some engines provide functions that read files or run SQL text, so the
    text must be trusted.

    Args:
        function_input: A single function call with an optional alias.
        explicit_alias: Optional alias supplied separately from the call text.
        dialect: Dialect used to read the call.

    Returns:
        The parsed function source, aliased when an alias is present.

    Raises:
        SQLBuilderError: If the text is anything other than one function call without subqueries,
            statements, or comments, or if the alias is not a single identifier.
    """
    msg = f"Invalid table function {function_input!r}: expected a single function call with an optional alias."
    tokens = _read_table_tokens(function_input, dialect, msg)
    if any(token.token_type in {TokenType.UNKNOWN, TokenType.SEMICOLON} for token in tokens):
        raise SQLBuilderError(msg)
    try:
        statements = sqlglot.parse(f"SELECT * FROM {function_input}", dialect=_table_reader(dialect))
    except SqlglotError as error:
        raise SQLBuilderError(msg) from error

    statement = statements[0] if len(statements) == 1 else None
    if not isinstance(statement, exp.Select) or _set_arguments(statement) != {"expressions", "from_"}:
        raise SQLBuilderError(msg)

    source = statement.args["from_"].this
    call = source.this if isinstance(source, exp.Table) else source
    if (
        not isinstance(call, exp.Func)
        or (isinstance(source, exp.Table) and not _set_arguments(source) <= {"this", "db", "catalog", "alias"})
        or source.find(exp.Query, exp.DDL, exp.DML, exp.Command) is not None
        or any(node.comments for node in statement.walk())
    ):
        raise SQLBuilderError(msg)

    if explicit_alias:
        if source.args.get("alias"):
            msg = f"Invalid table alias {explicit_alias!r}: table function {function_input!r} already has an alias."
            raise SQLBuilderError(msg)
        return exp.alias_(source, _parse_alias_identifier(explicit_alias))
    return source  # type: ignore[no-any-return]


def _parse_table_reference(
    table_input: str, explicit_alias: "str | None" = None, dialect: "DialectType | None" = None
) -> exp.Table:
    """Parse table reference text into an ``exp.Table``.

    The result is built from parsed identifiers and is never read again as SQL. Names the dialect
    cannot parse, such as quoting styles it does not define or ``#`` inside a bare name, become
    identifiers directly.

    Args:
        table_input: The table reference text.
        explicit_alias: Optional alias supplied separately from the table text.
        dialect: Dialect used to read the text.

    Returns:
        The table expression carrying its alias as a single identifier.

    Raises:
        SQLBuilderError: If the text is anything other than a table name and optional alias, or
            if an alias appears both in the text and as ``explicit_alias``.
    """
    if "(" in table_input and any(
        token.token_type == TokenType.L_PAREN
        for token in _read_table_tokens(table_input, dialect, _table_reference_error(table_input))
    ):
        msg = (
            f"Invalid table reference {table_input!r}: table text cannot hold function calls or subqueries. "
            "Pass an expression or builder instead, such as sql.func('fn', 1) or sql.raw('fn(1)'), with alias=."
        )
        raise SQLBuilderError(msg)

    stripped = table_input.strip(_TABLE_TEXT_SEPARATOR_CHARACTERS)
    inline_alias: str | None = None
    table: exp.Table | None
    variable = _TSQL_VARIABLE_RE.fullmatch(stripped) if _reads_as_tsql(dialect) else None
    is_simple = _SIMPLE_IDENTIFIER_RE.fullmatch(stripped) is not None and _is_simple_identifier(stripped)
    if is_simple and stripped.lower() != _ALIAS_KEYWORD:
        table = _table_from_identifiers([exp.Identifier(this=part, quoted=False) for part in stripped.split(".")])
    elif variable is not None:
        table = exp.Table(this=exp.Parameter(this=exp.Var(this=variable.group("name"))))
        inline_alias = variable.group("alias")
    else:
        error = _table_reference_error(table_input)
        tokens = _read_table_tokens(stripped, dialect, error)
        if TokenType.DOT in {tokens[0].token_type, tokens[-1].token_type}:
            raise SQLBuilderError(error)
        table_name, inline_alias = _split_inline_alias(stripped, tokens, error)
        has_separator = any(token.token_type == TokenType.SEMICOLON for token in tokens)
        table = None if has_separator else _read_table_name(table_name, dialect)
        if table is None:
            table, inline_alias = _table_from_text(stripped, error)

    if inline_alias is not None and explicit_alias:
        msg = f"Invalid table alias {explicit_alias!r}: table reference {table_input!r} already has an alias."
        raise SQLBuilderError(msg)
    alias_text = inline_alias if inline_alias is not None else explicit_alias
    if alias_text:
        table.set("alias", exp.TableAlias(this=_parse_alias_identifier(alias_text)))
    return table


def _reads_as_tsql(dialect: "DialectType | None") -> bool:
    return type(_table_reader(dialect)).__name__ == "TSQL"


def _table_reference_error(table_input: str) -> str:
    return f"Invalid table reference {table_input!r}: expected a table name with an optional alias."


def _table_reader(dialect: "DialectType | None") -> Dialect:
    """Resolve the dialect that reads table text, using the base dialect for unknown names."""
    resolved: DialectType | None = _normalize_dialect(dialect) if isinstance(dialect, str) else dialect
    try:
        return Dialect.get_or_raise(resolved)
    except ValueError:
        return Dialect.get_or_raise(None)


def _read_table_tokens(text: str, dialect: "DialectType | None", error: str) -> "list[Token]":
    """Tokenize table text, rejecting comments and invisible characters outside quotes.

    Args:
        text: The table text.
        dialect: Dialect whose tokenizer reads the text.
        error: Message raised for rejected text.

    Returns:
        The tokens of the text.

    Raises:
        SQLBuilderError: If the text cannot be tokenized, is empty, holds a comment, or holds
            whitespace, format, control, or separator characters outside quotes other than
            the ASCII space, tab, and line breaks.
    """
    try:
        tokens = list(_table_reader(dialect).tokenize(text))
    except SqlglotError as exc:
        raise SQLBuilderError(error) from exc
    if not tokens or any(token.comments for token in tokens) or _has_invisible_unquoted_text(text, tokens):
        raise SQLBuilderError(error)
    return tokens


def _has_invisible_unquoted_text(text: str, tokens: "list[Token]") -> bool:
    """Report whether text outside quoted tokens holds characters an engine may read as separators."""
    position = 0
    for token in tokens:
        if token.token_type in _QUOTED_TOKEN_TYPES:
            if _holds_invisible_character(text[position : token.start]):
                return True
            position = token.end + 1
    return _holds_invisible_character(text[position:])


def _holds_invisible_character(text: str) -> bool:
    return any(
        character not in _TABLE_TEXT_SEPARATOR_CHARACTERS and unicodedata.category(character) in _INVISIBLE_CATEGORIES
        for character in text
    )


def _split_inline_alias(text: str, tokens: "list[Token]", error: str) -> "tuple[str, str | None]":
    """Split a trailing alias token from table text.

    Args:
        text: The table text the tokens were read from.
        tokens: The tokens of the text.
        error: Message raised when the text ends in a bare ``AS``.

    Returns:
        The table name text and the alias text, if any.

    Raises:
        SQLBuilderError: If ``AS`` has no table name or no alias beside it.
    """
    last = tokens[-1]
    if last.token_type == TokenType.ALIAS:
        raise SQLBuilderError(error)
    if len(tokens) == 1:
        return text, None

    previous = tokens[-2]
    alias_text = text[last.start : last.end + 1]
    if _ALIAS_RE.fullmatch(alias_text) is None:
        return text, None
    if previous.token_type == TokenType.ALIAS:
        if len(tokens) == ALIAS_PARTS_EXPECTED_COUNT:
            raise SQLBuilderError(error)
        return text[: tokens[-3].end + 1], alias_text
    if previous.token_type != TokenType.DOT and last.start > previous.end + 1:
        return text[: previous.end + 1], alias_text
    return text, None


def _read_table_name(name: str, dialect: "DialectType | None") -> "exp.Table | None":
    """Parse a table name, returning ``None`` unless the result is only identifiers."""
    try:
        table = sqlglot.parse_one(name, into=exp.Table, dialect=_table_reader(dialect))
    except SqlglotError:
        return None
    if (
        not isinstance(table, exp.Table)
        or not _set_arguments(table) <= {"this", "db", "catalog"}
        or not all(isinstance(part, exp.Identifier) for part in table.parts)
        or any(node.comments for node in table.walk())
    ):
        return None
    for part in table.parts:
        if part.args.get("quoted"):
            part.meta[_EXPLICIT_QUOTE_META_KEY] = True
    return table


def _table_from_text(text: str, error: str) -> "tuple[exp.Table, str | None]":
    """Build a table directly from text matching the table name grammar.

    Args:
        text: The table text.
        error: Message raised when the text does not match.

    Returns:
        The table expression and the alias text, if any.

    Raises:
        SQLBuilderError: If the text does not match or has too many name parts.
    """
    match = _TABLE_REFERENCE_RE.fullmatch(text)
    if match is None:
        raise SQLBuilderError(error)
    parts = _TABLE_NAME_PART_RE.findall(match.group("table"))
    if len(parts) > _MAX_DIRECT_TABLE_PARTS:
        raise SQLBuilderError(error)
    return _table_from_identifiers([_identifier_from_text(part) for part in parts]), match.group("alias")


def _table_from_identifiers(identifiers: "list[exp.Identifier]") -> exp.Table:
    ordered_parts = identifiers[::-1]
    table = exp.Table(this=ordered_parts[0])
    for key, part in zip(("db", "catalog"), ordered_parts[1:], strict=False):
        table.set(key, part)
    return table


def _set_arguments(expression: exp.Expr) -> "set[str]":
    return {key for key, value in expression.args.items() if value}


def _parse_alias_identifier(alias: str) -> exp.Identifier:
    """Parse alias text into a single identifier.

    Args:
        alias: A bare word or a quoted identifier.

    Returns:
        The alias identifier.

    Raises:
        SQLBuilderError: If the text is not a single identifier.
    """
    stripped = alias.strip(_TABLE_TEXT_SEPARATOR_CHARACTERS)
    if _ALIAS_RE.fullmatch(stripped) is None or stripped.lower() == _ALIAS_KEYWORD:
        msg = f"Invalid alias {alias!r}: expected a single identifier."
        raise SQLBuilderError(msg)
    return _identifier_from_text(stripped)


def _identifier_from_text(text: str) -> exp.Identifier:
    """Build an identifier from a bare word or a quoted span."""
    closing = _QUOTE_CLOSERS.get(text[0])
    if closing is None:
        return exp.to_identifier(text)
    identifier = exp.to_identifier(text[1:-1].replace(closing * 2, closing), quoted=True)
    identifier.meta[_EXPLICIT_QUOTE_META_KEY] = True
    return identifier


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
            return ordered(column_expr, desc=direction.lower() == "desc")

    parsed = maybe_parse(order_value, into=exp.Ordered)
    if parsed:
        return default_nulls(parsed, order_value)

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
        return exp.EQ(this=column_expr, expression=exp.convert(value))

    if not isinstance(condition_input, str):
        condition_input = str(condition_input)

    param_info = _PARAMETER_VALIDATOR.extract_parameters(condition_input)

    if param_info:
        converted_condition = condition_input
        for param in param_info[::-1]:
            if param.style in {
                ParameterStyle.NUMERIC,
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
                ParameterStyle.QMARK,
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


def extract_sql_object_expression(
    value: Any, builder: Any | None = None, parse_sql: Callable[[str], exp.Expr] | None = None
) -> exp.Expr:
    """Extract the SQLGlot expression from a SQL object and merge its parameters.

    Only ``SQL`` instances are accepted, so an object that merely exposes the same attributes
    never renders as SQL.

    Args:
        value: The SQL object to process
        builder: Optional builder instance for parameter merging (must have add_parameter method)
        parse_sql: Optional parser for raw SQL fallback text.

    Returns:
        SQLGlot Expression extracted from the SQL object

    Raises:
        TypeError: If the value is not a SQL object
    """
    if not isinstance(value, SQL):
        msg = f"Value is not a SQL object: {type(value)}"
        raise TypeError(msg)

    if value.expression is not None and isinstance(value.expression, exp.Expr):
        expr = value.expression.copy()
        param_mapping = _merge_sql_parameters(value, builder)
        if param_mapping and builder is not None and hasattr(builder, "_update_placeholders"):
            expr = builder._update_placeholders(expr, param_mapping)
        return expr

    param_mapping = _merge_sql_parameters(value, builder)
    sql_text = value.raw_sql
    expr = parse_sql(sql_text) if parse_sql is not None else (exp.maybe_parse(sql_text) or exp.convert(str(sql_text)))
    if param_mapping and builder is not None and hasattr(builder, "_update_placeholders"):
        expr = builder._update_placeholders(expr, param_mapping)
    return expr


def resolve_value_expression(value: Any, builder: Any, alias_hint: str) -> exp.Expr | None:
    """Resolve a builder value to the expression it renders as.

    Only SQLGlot nodes, SQLSpec expression wrappers, ``SQL`` objects, and query builders render as SQL.
    Every other value binds as a parameter, whatever attributes it exposes, so application data cannot
    become SQL by having fields named like a SQL carrier's.

    Args:
        value: Value passed to a builder method.
        builder: Query builder receiving merged parameters.
        alias_hint: Name seed for parameters merged from a subquery builder.

    Returns:
        An expression independent of the caller's objects, or None when the value binds as a parameter.
    """
    if value is None or isinstance(value, (str, int, float, bytes, bytearray)):
        return None
    from sqlspec.builder._base import QueryBuilder

    expression: exp.Expr
    if isinstance(value, exp.Expr):
        expression = value.copy()
    elif isinstance(value, ExpressionWrapper):
        expression = value.expression.copy()
    elif isinstance(value, (Column, FunctionColumn, ColumnExpression)):
        expression = value.sqlglot_expression.copy()
    elif isinstance(value, QueryBuilder):
        return _scalar_subquery(builder._embed_subquery(value, alias_hint=alias_hint))
    elif isinstance(value, SQL):
        expression = extract_sql_object_expression(value, builder=builder)
    else:
        return None
    merged: exp.Expr = builder._merge_subquery_fragments(expression)
    if isinstance(merged, (exp.Select, exp.SetOperation)):
        return _scalar_subquery(merged)
    return merged


def _scalar_subquery(query: exp.Expr) -> exp.Expr:
    """Wrap a query as a scalar operand.

    Set operations use ``exp.Subquery`` because the optimizer unwraps their parentheses.
    Other queries use ``exp.Paren`` because the optimizer unnests ``exp.Subquery`` into joins.

    Args:
        query: Query expression to wrap.

    Returns:
        The wrapped query.
    """
    if isinstance(query, exp.SetOperation):
        return exp.Subquery(this=query)
    return exp.paren(query)


def extract_expression(value: Any) -> exp.Expr:
    """Extract SQLGlot expression from value, handling wrapper types.

    Args:
        value: String, SQLGlot expression, or wrapper type.

    Returns:
        Raw SQLGlot expression.
    """
    if isinstance(value, str):
        return exp.column(value)
    if isinstance(value, (Column, FunctionColumn, ColumnExpression)):
        return value.sqlglot_expression
    if isinstance(value, ExpressionWrapper):
        return value.expression
    if isinstance(value, exp.Expr):
        return value
    return exp.convert(value)


def unwrap_expression_source(value: Any, builder: Any = None) -> exp.Expr | None:
    """Extract a SQLGlot expression from an expression, builder wrapper, or SQL object.

    Args:
        value: Candidate expression, wrapper, or SQL object.
        builder: Optional builder for merging SQL object parameters.

    Returns:
        Underlying ``exp.Expr``, or ``None`` when ``value`` is not an expression or wrapper.
    """
    if isinstance(value, exp.Expr):
        return value
    if isinstance(value, (Column, FunctionColumn, ColumnExpression)):
        return value.sqlglot_expression.copy()
    if isinstance(value, ExpressionWrapper):
        return value.expression.copy()
    if builder is not None and isinstance(value, SQL):
        return extract_sql_object_expression(value, builder=builder)
    return None


def to_expression(value: Any) -> exp.Expr:
    """Convert a Python value to a raw SQLGlot expression.

    Args:
        value: Python value or SQLGlot expression to convert.

    Returns:
        Raw SQLGlot expression.
    """
    if isinstance(value, (Column, FunctionColumn, ColumnExpression)):
        return value.sqlglot_expression
    if isinstance(value, ExpressionWrapper):
        return value.expression
    if isinstance(value, exp.Expr):
        return value
    return exp.convert(value)


def _mark_explicit_table_quotes(table: exp.Table, source: str) -> exp.Table:
    """Distinguish supplied table quotes from quoting added by optimization."""
    if not any(quote in source for quote in ('"', "`", "[")):
        return table
    remaining = source.strip()
    for identifier in table.parts:
        if not isinstance(identifier, exp.Identifier) or not remaining:
            break
        name = identifier.name
        opening = remaining[0]
        if opening in ('"', "`", "["):
            closing = "]" if opening == "[" else opening
            spelling = opening + name.replace(closing, closing * 2) + closing
            if not remaining.startswith(spelling):
                if not (name.startswith(opening) and name.endswith(closing) and remaining.startswith(name)):
                    break
                spelling = name
                identifier.set("this", name[1:-1].replace(closing * 2, closing))
            identifier.set("quoted", True)
            identifier.meta["sqlspec_explicit_table_quote"] = True
        else:
            spelling = name
            if not remaining.startswith(spelling):
                break
        remaining = remaining[len(spelling) :].lstrip()
        if not remaining.startswith("."):
            break
        remaining = remaining[1:].lstrip()
    return table


def _normalize_partition_by(
    partition_by: str | list[Any] | exp.Expr | Column | FunctionColumn | ColumnExpression | ExpressionWrapper | None,
) -> list[exp.Expr] | None:
    if isinstance(partition_by, str):
        return [exp.column(partition_by)]
    if isinstance(partition_by, list):
        return [extract_expression(column) for column in partition_by]
    if isinstance(partition_by, (Column, FunctionColumn, ColumnExpression, ExpressionWrapper)):
        return [extract_expression(partition_by)]
    if isinstance(partition_by, exp.Expr):
        return [partition_by]
    return None


def _normalize_order_by(
    order_by: str | list[Any] | exp.Expr | Column | FunctionColumn | ColumnExpression | ExpressionWrapper | None,
) -> exp.Order | None:
    if isinstance(order_by, str):
        return exp.Order(expressions=[ordered(exp.column(order_by))])
    if isinstance(order_by, list):
        return exp.Order(
            expressions=[
                column if isinstance(column, exp.Ordered) else ordered(extract_expression(column))
                for column in order_by
            ]
        )
    if isinstance(order_by, (Column, FunctionColumn, ColumnExpression, ExpressionWrapper)):
        return exp.Order(expressions=[ordered(extract_expression(order_by))])
    if isinstance(order_by, exp.Expr):
        return exp.Order(expressions=[order_by])
    return None


def _coerce_column(value: str | exp.Expr | Column | FunctionColumn | ColumnExpression | ExpressionWrapper) -> exp.Expr:
    if isinstance(value, str):
        return exp.column(value)
    if isinstance(value, (Column, FunctionColumn, ColumnExpression)):
        return value.sqlglot_expression
    if isinstance(value, ExpressionWrapper):
        return value.expression
    return value


def _rollup_expression(
    *columns: str | exp.Expr | Column | FunctionColumn | ColumnExpression | ExpressionWrapper,
) -> exp.Rollup:
    return exp.Rollup(expressions=[_coerce_column(column) for column in columns])


def _cube_expression(
    *columns: str | exp.Expr | Column | FunctionColumn | ColumnExpression | ExpressionWrapper,
) -> exp.Cube:
    return exp.Cube(expressions=[_coerce_column(column) for column in columns])


def _grouping_sets_expression(*column_sets: tuple[Any, ...] | list[Any]) -> exp.GroupingSets:
    sets: list[exp.Tuple] = []
    for column_set in column_sets:
        if not isinstance(column_set, (tuple, list)):
            msg = "Each grouping set must be a tuple or list of columns."
            raise SQLBuilderError(msg)
        sets.append(exp.Tuple(expressions=[_coerce_column(column) for column in column_set]))
    return exp.GroupingSets(expressions=sets)


def _resolve_dialect(dialect: "DialectType | None", default: "DialectType | None") -> "DialectType | None":
    return dialect or default


def _normalize_dialect(dialect: "DialectType | None") -> str | None:
    """Resolve public dialect aliases to SQLGlot dialect names."""
    if dialect is None:
        return None
    if isinstance(dialect, str):
        name = dialect.lower()
    elif isinstance(dialect, type):
        name = dialect.__name__.lower()
    else:
        name = type(dialect).__name__.lower()
    resolved = _DIALECT_ALIASES.get(name, name)
    if resolved in {"sybase", "sqlanywhere"} and resolved not in Dialect.classes:
        importlib.import_module("sqlspec.dialects.sybase")
    return resolved


def _truthy_metadata_flag(value: Any) -> bool:
    """Interpret a catalog flag value (bool, number, or ``yes``/``true``/``1`` text) as a boolean.

    Args:
        value: Raw catalog flag value.

    Returns:
        Whether the flag is set.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def sql_named_parameters(sql_obj: Any) -> "Mapping[str, Any]":
    """Return a SQL object's named parameters.

    Args:
        sql_obj: SQL object with parameters attribute

    Returns:
        The named parameters, empty when the object has none.

    Raises:
        SQLBuilderError: If the SQL object carries positional parameters.
    """
    parameters = sql_obj.parameters
    if sql_obj.positional_parameters or (parameters and not isinstance(parameters, Mapping)):
        msg = "Positional parameters on a SQL object cannot merge into a builder; use named parameters."
        raise SQLBuilderError(msg)
    return parameters or {}


def _merge_sql_parameters(sql_obj: Any, builder: Any) -> dict[str, str]:
    """Merge parameters from SQL object into builder and return renamed placeholder mapping.

    Args:
        sql_obj: SQL object with parameters attribute
        builder: Builder instance with add_parameter method

    Returns:
        Mapping of original parameter names to renamed parameter names.

    Raises:
        SQLBuilderError: If the SQL object carries positional parameters.
    """
    param_mapping: dict[str, str] = {}
    if not (builder and has_parameter_builder(builder)):
        return param_mapping

    parameters = sql_named_parameters(sql_obj)
    if not parameters:
        return param_mapping

    existing_params = getattr(builder, "parameters", None)
    if existing_params is None:
        existing_params = getattr(builder, "_parameters", {})
    for param_name, param_value in parameters.items():
        target_name = param_name
        if param_name in existing_params:
            if hasattr(builder, "generate_unique_parameter_name"):
                target_name = builder.generate_unique_parameter_name(param_name)  # pyright: ignore[reportAttributeAccessIssue]
            elif hasattr(builder, "_next_parameter_name"):
                target_name = builder._next_parameter_name(param_name)  # pyright: ignore[reportAttributeAccessIssue,reportPrivateUsage]
        _, actual_name = builder.add_parameter(param_value, name=target_name)
        if actual_name != param_name:
            param_mapping[param_name] = actual_name
    return param_mapping


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
