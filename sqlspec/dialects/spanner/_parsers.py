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

from sqlspec.dialects.spanner._generators import (
    _INTERLEAVE_IN_NAME,
    _INTERLEAVE_NAME,
    _ROW_DELETION_NAME,
    _normalize_interval_expression,
)

__all__ = (
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


def _normalize_on_delete_value(on_delete: str) -> str:
    return " ".join(on_delete.upper().split())


def build_interleave_property(parent: exp.Expr, on_delete: "str | None" = None, in_parent: bool = True) -> exp.Property:
    """Build the canonical interleave property node."""
    if not in_parent:
        return exp.Property(this=exp.Literal.string(_INTERLEAVE_IN_NAME), value=exp.Tuple(expressions=[parent]))
    values: list[exp.Expr] = [parent]
    if on_delete is not None:
        values.append(exp.Literal.string(_normalize_on_delete_value(on_delete)))
    return exp.Property(this=exp.Literal.string(_INTERLEAVE_NAME), value=exp.Tuple(expressions=values))


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


def register_spanner_property_parsers() -> None:
    """Install Spanner property parsers on the BigQuery and Postgres parser classes."""
    for parser_class in (BigQueryParser, PostgresParser):
        if getattr(parser_class, _PROPERTY_PARSERS_REGISTERED_ATTR, False):
            continue
        property_parsers: dict[str, Any] = dict(parser_class.PROPERTY_PARSERS)
        for key, handler in (
            ("INTERLEAVE", _parse_interleave),
            ("ROW", _parse_row_deletion_policy),
            ("TTL", _parse_ttl),
        ):
            property_parsers[key] = _build_property_entry(handler, property_parsers.get(key))
        setattr(parser_class, "PROPERTY_PARSERS", property_parsers)
        setattr(parser_class, _PROPERTY_PARSERS_REGISTERED_ATTR, True)


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
