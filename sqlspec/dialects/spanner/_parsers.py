"""Shared property parsing for the Spanner GoogleSQL and PostgreSQL dialects.

Spanner DDL extensions are wired into sqlglot through ``PROPERTY_PARSERS``
dict entries: sqlglot invokes those with the parser as an explicit argument
(``PROPERTY_PARSERS[key](self)``), so the callables work identically under
pure-Python sqlglot and sqlglot[c]. Monkeypatching parser methods is not an
option: compiled parser internals dispatch through the native vtable and
never see a Python-level method override.

All clauses normalize to the canonical property nodes defined alongside the
generators, so either dialect can re-render them.
"""

import re
from typing import Any, Final, cast

from sqlglot import exp
from sqlglot.parsers.bigquery import BigQueryParser
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokenizer_core import TokenType

from sqlspec.dialects.spanner._expressions import (
    ApproxCosineDistance,
    CosineDistance,
    DotProduct,
    EuclideanDistance,
    GetNextSequenceValue,
    Score,
    Search,
    SearchSubstring,
    TokenizeFulltext,
    TokenizeNgrams,
    TokenizeSubstring,
)
from sqlspec.dialects.spanner._generators import (
    _INTERLEAVE_IN_NAME,
    _INTERLEAVE_NAME,
    _ROW_DELETION_NAME,
    _normalize_interval_expression,
)

__all__ = (
    "SpangresParser",
    "SpannerParser",
    "attach_create_property",
    "build_interleave_property",
    "extract_interleave_property",
    "register_spanner_property_parsers",
)

_PROPERTY_PARSERS_REGISTERED_ATTR: Final[str] = "_sqlspec_spanner_property_parsers"
_SPANNER_DIALECT_NAMES: Final[frozenset[str]] = frozenset({"Spangres", "Spanner"})

_INTERLEAVE_PATTERN: Final["re.Pattern[str]"] = re.compile(
    r"""
    ,?\s*\bINTERLEAVE\s+IN\s+
    (?P<parent_keyword>PARENT\s+)?
    (?P<parent>.+?)
    (?:\s+ON\s+DELETE\s+(?P<on_delete>CASCADE|NO\s+ACTION))?
    (?=\s*,?\s*(?:ROW\s+DELETION\s+POLICY|TTL)\b|\s*$)
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)


def build_interleave_property(parent: exp.Expr, on_delete: "str | None" = None, in_parent: bool = True) -> exp.Property:
    """Build the canonical interleave property node."""
    if not in_parent:
        return exp.Property(this=exp.Literal.string(_INTERLEAVE_IN_NAME), value=exp.Tuple(expressions=[parent]))
    values: list[exp.Expr] = [parent]
    if on_delete is not None:
        values.append(exp.Literal.string(_normalize_on_delete_value(on_delete)))
    return exp.Property(this=exp.Literal.string(_INTERLEAVE_NAME), value=exp.Tuple(expressions=values))


def register_spanner_property_parsers() -> None:
    """Retained as an idempotent no-op for backward compatibility."""
    return


def extract_interleave_property(sql: str) -> "tuple[str, exp.Property | None]":
    """Strip an INTERLEAVE clause out of raw DDL, returning the repaired SQL and property."""
    match = _INTERLEAVE_PATTERN.search(sql)
    if match is None:
        return sql, None

    parent = exp.to_table(match.group("parent").strip())
    on_delete = match.group("on_delete")
    in_parent = match.group("parent_keyword") is not None
    interleave_property = build_interleave_property(parent, on_delete, in_parent=in_parent)
    repaired_sql = f"{sql[: match.start()]} {sql[match.end() :]}".strip()
    return repaired_sql, interleave_property


def attach_create_property(create: exp.Create, property_expression: exp.Property) -> exp.Create:
    """Insert a property at the front of a CREATE statement's property list."""
    properties = create.args.get("properties")
    if isinstance(properties, exp.Properties):
        expressions = list(properties.expressions)
        expressions.insert(0, property_expression)
        properties.set("expressions", expressions)
    else:
        create.set("properties", exp.Properties(expressions=[property_expression]))
    return create


def _normalize_on_delete_value(on_delete: str) -> str:
    return " ".join(on_delete.upper().split())


def _build_row_deletion_property(column: exp.Expr, interval: exp.Expr) -> exp.Property:
    return exp.Property(this=exp.Literal.string(_ROW_DELETION_NAME), value=exp.Tuple(expressions=[column, interval]))


def _is_spanner_parser(parser: Any) -> bool:
    dialect = getattr(parser, "dialect", None)
    return dialect is not None and type(dialect).__name__ in _SPANNER_DIALECT_NAMES


def _parse_interleave(parser: Any) -> "exp.Property | None":
    """Parse ``INTERLEAVE IN [PARENT] table [ON DELETE {CASCADE | NO ACTION}]``.

    The INTERLEAVE token is already consumed by sqlglot's property dispatch.
    """
    if not parser._match_text_seq("IN"):
        parser._retreat(parser._index - 1)
        return None

    in_parent = bool(parser._match_text_seq("PARENT"))
    parent = cast("exp.Expr", parser._parse_table(schema=True, is_db_reference=True))
    on_delete: str | None = None

    if in_parent and parser._match_text_seq("ON", "DELETE"):
        if parser._match_text_seq("CASCADE"):
            on_delete = "CASCADE"
        elif parser._match_text_seq("NO", "ACTION"):
            on_delete = "NO ACTION"

    return build_interleave_property(parent, on_delete, in_parent=in_parent)


def _parse_row_deletion_policy(parser: Any) -> "exp.Property | None":
    """Parse ``ROW DELETION POLICY (OLDER_THAN(column, INTERVAL n DAY))``.

    The ROW token is already consumed by sqlglot's property dispatch.
    """
    if not parser._match_text_seq("DELETION", "POLICY"):
        parser._retreat(parser._index - 1)
        return None

    parser._match(TokenType.L_PAREN)
    parser._match_text_seq("OLDER_THAN")
    parser._match(TokenType.L_PAREN)
    column = cast("exp.Expr", parser._parse_id_var())
    parser._match(TokenType.COMMA)
    parser._match_text_seq("INTERVAL")
    interval = _normalize_interval_expression(cast("exp.Expr", parser._parse_expression()))
    parser._match(TokenType.R_PAREN)
    parser._match(TokenType.R_PAREN)

    return _build_row_deletion_property(column, interval)


def _parse_ttl(parser: Any) -> "exp.Property | None":
    """Parse ``TTL INTERVAL interval_spec ON column`` into the canonical policy node.

    The TTL token is already consumed by sqlglot's property dispatch.
    """
    parser._match_text_seq("INTERVAL")
    interval = _normalize_interval_expression(cast("exp.Expr", parser._parse_expression()))
    parser._match_text_seq("ON")
    column = cast("exp.Expr", parser._parse_id_var())

    return _build_row_deletion_property(column, interval)


def _build_property_entry(handler: Any, original: Any) -> Any:
    def _entry(parser: Any, **kwargs: Any) -> Any:
        if _is_spanner_parser(parser):
            return handler(parser)
        if original is not None:
            return original(parser, **kwargs)
        parser._retreat(parser._index - 1)
        return None

    return _entry


def _parse_get_next_sequence_value(parser: Any) -> exp.Func:
    """Parse GET_NEXT_SEQUENCE_VALUE(SEQUENCE sequence_name)."""
    parser._match_text_seq("SEQUENCE")
    seq_name = parser._parse_id_var()
    parser._match(TokenType.R_PAREN)
    return GetNextSequenceValue(this=seq_name)


def _parse_options_properties(parser: Any) -> "exp.Properties | None":
    """Parse an OPTIONS (key = value, ...) property block."""
    if not parser._match_text_seq("OPTIONS"):
        return None
    parser._match(TokenType.L_PAREN)
    props = parser._parse_csv(parser._parse_property)
    parser._match(TokenType.R_PAREN)
    return exp.Properties(expressions=props)


def _parse_create_vector_index(parser: Any) -> exp.Index:
    """Parse CREATE VECTOR INDEX name ON table (cols) [WHERE ...] [OPTIONS (...)]."""
    name = parser._parse_id_var()
    parser._match_text_seq("ON")
    table = exp.Table(this=parser._parse_id_var())
    parser._match(TokenType.L_PAREN)
    cols = parser._parse_csv(parser._parse_column)
    parser._match(TokenType.R_PAREN)
    where = parser._parse_where()
    options = _parse_options_properties(parser)
    params = exp.IndexParameters(columns=cols)
    idx = exp.Index(this=name, table=table, params=params, where=where, kind="VECTOR")
    if options:
        idx.set("options", options)
    return idx


def _parse_create_search_index(parser: Any) -> exp.Index:
    """Parse CREATE SEARCH INDEX name ON table (cols) [STORING (cols)] [PARTITION BY cols] [ORDER BY order] [OPTIONS (...)]."""
    name = parser._parse_id_var()
    parser._match_text_seq("ON")
    table = exp.Table(this=parser._parse_id_var())
    parser._match(TokenType.L_PAREN)
    cols = parser._parse_csv(parser._parse_column)
    parser._match(TokenType.R_PAREN)
    storing: list[exp.Expr] | None = None
    if parser._match_text_seq("STORING"):
        parser._match(TokenType.L_PAREN)
        storing = parser._parse_csv(parser._parse_column)
        parser._match(TokenType.R_PAREN)
    partition_by: list[exp.Expr] | None = None
    if parser._match(TokenType.PARTITION_BY) or parser._match_text_seq("PARTITION", "BY"):
        partition_by = parser._parse_csv(parser._parse_column)
    order = parser._parse_order()
    options = _parse_options_properties(parser)
    params = exp.IndexParameters(columns=cols)
    idx = exp.Index(this=name, table=table, params=params, kind="SEARCH")
    if storing:
        idx.set("storing", storing)
    if partition_by:
        idx.set("partition_by", partition_by)
    if order:
        idx.set("order", order)
    if options:
        idx.set("options", options)
    return idx


def _parse_create_sequence(parser: Any) -> exp.Create:
    """Parse CREATE SEQUENCE name [OPTIONS (...)]."""
    name = parser._parse_id_var()
    options = _parse_options_properties(parser)
    return exp.Create(this=name, kind="SEQUENCE", properties=options)


def _parse_alter_sequence(parser: Any) -> exp.Alter:
    """Parse ALTER SEQUENCE name SET OPTIONS (...)."""
    name = parser._parse_id_var()
    options: exp.Properties | None = None
    if parser._match_text_seq("SET", "OPTIONS") or parser._match_text_seq("OPTIONS"):
        parser._retreat(parser._index - 1)
        options = _parse_options_properties(parser)
    return exp.Alter(this=name, kind="SEQUENCE", options=options)


def _parse_create_change_stream(parser: Any) -> exp.Create:
    """Parse CREATE CHANGE STREAM name [FOR ...] [OPTIONS (...)]."""
    name = parser._parse_id_var()
    for_expressions: list[exp.Expr] = []
    if parser._match_text_seq("FOR"):
        if parser._match_text_seq("ALL"):
            for_expressions.append(exp.var("ALL"))
        else:

            def _parse_stream_target() -> exp.Expr:
                target_table = parser._parse_id_var()
                if parser._match(TokenType.L_PAREN):
                    cols = parser._parse_csv(parser._parse_column)
                    parser._match(TokenType.R_PAREN)
                    return exp.Anonymous(this=target_table.name, expressions=cols)
                return target_table

            targets = parser._parse_csv(_parse_stream_target)
            for_expressions.extend(targets)
    options = _parse_options_properties(parser)
    return exp.Create(this=name, kind="CHANGE STREAM", expressions=for_expressions, properties=options)


def _parse_alter_change_stream(parser: Any) -> exp.Alter:
    """Parse ALTER CHANGE STREAM name [SET ...] [OPTIONS (...)]."""
    name = parser._parse_id_var()
    options: exp.Properties | None = None
    if parser._match_text_seq("SET", "OPTIONS") or parser._match_text_seq("OPTIONS"):
        parser._retreat(parser._index - 1)
        options = _parse_options_properties(parser)
    return exp.Alter(this=name, kind="CHANGE STREAM", options=options)


def _parse_drop_change_stream(parser: Any) -> exp.Drop:
    """Parse DROP CHANGE STREAM name."""
    name = parser._parse_id_var()
    return exp.Drop(this=name, kind="CHANGE STREAM")


def _parse_spanner_hint(parser: Any) -> exp.Hint:
    """Parse @{key=value, ...} into canonical exp.Hint."""
    parser._advance(2)
    exprs: list[exp.Expr] = []
    while parser._curr and parser._curr.token_type != TokenType.R_BRACE:
        key = parser._parse_id_var()
        if parser._match(TokenType.EQ):
            val = parser._parse_number() or parser._parse_var() or parser._parse_string() or parser._parse_id_var()
            exprs.append(exp.EQ(this=key, expression=val))
        else:
            exprs.append(key)
        parser._match(TokenType.COMMA)
    parser._match(TokenType.R_BRACE)
    return exp.Hint(expressions=exprs)


def _parse_spangres_comment_hint(comments: "list[str] | None") -> "exp.Hint | None":
    """Extract and parse /*@ ... */ comment hint into canonical exp.Hint."""
    if not comments:
        return None
    for comment in list(comments):
        stripped = comment.strip()
        if stripped.startswith("@"):
            comments.remove(comment)
            raw = stripped[1:].strip()
            pairs = [p.strip() for p in raw.split(",") if p.strip()]
            exprs: list[exp.Expr] = []
            for pair in pairs:
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    v_str = v.strip()
                    if v_str.isdigit():
                        val_expr: exp.Expr = exp.Literal.number(int(v_str))
                    else:
                        val_expr = exp.var(v_str)
                    exprs.append(exp.EQ(this=exp.to_identifier(k.strip()), expression=val_expr))
                else:
                    exprs.append(exp.to_identifier(pair.strip()))
            return exp.Hint(expressions=exprs)
    return None


class SpannerParser(BigQueryParser):
    """Parser for Cloud Spanner GoogleSQL dialect."""

    PROPERTY_PARSERS = {
        **BigQueryParser.PROPERTY_PARSERS,
        "INTERLEAVE": _parse_interleave,
        "ROW": _parse_row_deletion_policy,
        "TTL": _parse_ttl,
    }

    FUNCTIONS = {
        **BigQueryParser.FUNCTIONS,
        "COSINE_DISTANCE": CosineDistance.from_arg_list,
        "EUCLIDEAN_DISTANCE": EuclideanDistance.from_arg_list,
        "DOT_PRODUCT": DotProduct.from_arg_list,
        "APPROX_COSINE_DISTANCE": ApproxCosineDistance.from_arg_list,
        "SEARCH": Search.from_arg_list,
        "SEARCH_SUBSTRING": SearchSubstring.from_arg_list,
        "SCORE": Score.from_arg_list,
        "TOKENIZE_FULLTEXT": TokenizeFulltext.from_arg_list,
        "TOKENIZE_SUBSTRING": TokenizeSubstring.from_arg_list,
        "TOKENIZE_NGRAMS": TokenizeNgrams.from_arg_list,
    }

    FUNCTION_PARSERS = {**BigQueryParser.FUNCTION_PARSERS, "GET_NEXT_SEQUENCE_VALUE": _parse_get_next_sequence_value}

    def _parse_create(self) -> "exp.Create | exp.Index | exp.Command":
        """Parse Spanner CREATE statements including VECTOR INDEX, SEARCH INDEX, SEQUENCE, and CHANGE STREAM."""
        if self._match_text_seq("VECTOR", "INDEX"):
            return _parse_create_vector_index(self)
        if self._match_text_seq("SEARCH", "INDEX"):
            return _parse_create_search_index(self)
        if self._match_text_seq("SEQUENCE"):
            return _parse_create_sequence(self)
        if self._match_text_seq("CHANGE", "STREAM"):
            return _parse_create_change_stream(self)
        return cast("exp.Create | exp.Index | exp.Command", super()._parse_create())

    def _parse_alter(self) -> "exp.Alter | exp.Command":
        """Parse Spanner ALTER statements including SEQUENCE and CHANGE STREAM."""
        if self._match_text_seq("SEQUENCE"):
            return _parse_alter_sequence(self)
        if self._match_text_seq("CHANGE", "STREAM"):
            return _parse_alter_change_stream(self)
        return cast("exp.Alter | exp.Command", super()._parse_alter())

    def _parse_drop(self, *args: Any, **kwargs: Any) -> "exp.Drop | exp.Command":
        """Parse Spanner DROP statements including CHANGE STREAM."""
        if self._match_text_seq("CHANGE", "STREAM"):
            return _parse_drop_change_stream(self)
        return cast("exp.Drop | exp.Command", super()._parse_drop(*args, **kwargs))

    def _parse_statement(self) -> "exp.Expr | None":
        """Parse statement with optional preceding statement-level hint @{...}."""
        hint: exp.Hint | None = None
        if (
            self._curr
            and self._curr.token_type == TokenType.PARAMETER
            and self._next
            and self._next.token_type == TokenType.L_BRACE
        ):
            hint = _parse_spanner_hint(self)
        statement = super()._parse_statement()
        if hint is not None and statement is not None:
            statement.set("hint", hint)
        return statement

    def _parse_table_alias(self, alias_tokens: Any = None) -> "exp.TableAlias | None":
        """Avoid treating @{ as an implicit table alias."""
        if (
            self._curr
            and self._curr.token_type == TokenType.PARAMETER
            and self._next
            and self._next.token_type == TokenType.L_BRACE
        ):
            return None
        return super()._parse_table_alias(alias_tokens=alias_tokens)

    def _parse_table(self, *args: Any, **kwargs: Any) -> "exp.Expr | None":
        """Parse table with optional table-level hint @{...}."""
        table = super()._parse_table(*args, **kwargs)
        if (
            isinstance(table, exp.Table)
            and self._curr
            and self._curr.token_type == TokenType.PARAMETER
            and self._next
            and self._next.token_type == TokenType.L_BRACE
        ):
            hint = _parse_spanner_hint(self)
            table.set("hints", [hint])
        return table


class SpangresParser(PostgresParser):
    """Parser for Cloud Spanner PostgreSQL-compatible dialect."""

    PROPERTY_PARSERS = {
        **PostgresParser.PROPERTY_PARSERS,
        "INTERLEAVE": _parse_interleave,
        "ROW": _parse_row_deletion_policy,
        "TTL": _parse_ttl,
    }

    FUNCTIONS = {
        **PostgresParser.FUNCTIONS,
        "COSINE_DISTANCE": CosineDistance.from_arg_list,
        "EUCLIDEAN_DISTANCE": EuclideanDistance.from_arg_list,
        "DOT_PRODUCT": DotProduct.from_arg_list,
        "APPROX_COSINE_DISTANCE": ApproxCosineDistance.from_arg_list,
    }

    FUNCTION_PARSERS = {**PostgresParser.FUNCTION_PARSERS, "GET_NEXT_SEQUENCE_VALUE": _parse_get_next_sequence_value}

    def _parse_statement(self) -> "exp.Expr | None":
        """Parse statement with optional preceding comment hint /*@ ... */."""
        hint: exp.Hint | None = None
        if self._curr and self._curr.comments:
            hint = _parse_spangres_comment_hint(self._curr.comments)
        statement = super()._parse_statement()
        if hint is not None and statement is not None:
            statement.set("hint", hint)
        return statement

    def _parse_table(self, *args: Any, **kwargs: Any) -> "exp.Expr | None":
        """Parse table with optional trailing comment hint /*@ ... */."""
        table = super()._parse_table(*args, **kwargs)
        if isinstance(table, exp.Table):
            comments = getattr(table.this, "comments", None)
            hint = _parse_spangres_comment_hint(comments) if comments else None
            if hint is None and self._prev and self._prev.comments:
                hint = _parse_spangres_comment_hint(self._prev.comments)
            if hint is not None:
                table.set("hints", [hint])
        return table
