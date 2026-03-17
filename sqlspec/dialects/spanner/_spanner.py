"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

Extends the BigQuery dialect with Spanner-only DDL features:
`INTERLEAVE IN PARENT` for interleaved tables and `ROW DELETION POLICY`
for row-level time-to-live policies (GoogleSQL).
"""

import re
from typing import Any, Final, cast

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.parsers.bigquery import BigQueryParser
from sqlglot.tokenizer_core import TokenType

__all__ = ("Spanner",)

_SPANNER_KEYWORDS: "dict[str, TokenType]" = {}
interleave_token = cast("TokenType | None", TokenType.__dict__.get("INTERLEAVE"))
if interleave_token is not None:
    _SPANNER_KEYWORDS["INTERLEAVE"] = interleave_token
ttl_token = cast("TokenType | None", TokenType.__dict__.get("TTL"))
if ttl_token is not None:
    _SPANNER_KEYWORDS["TTL"] = ttl_token

_TTL_MIN_COMPONENTS = 2
_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_INTERLEAVE_NAME = "INTERLEAVE_IN_PARENT"
_ORIGINAL_PARSE_PROPERTY_ATTR: Final[str] = "_sqlspec_original_parse_property"
_HOOKS_REGISTERED_ATTR: Final[str] = "_sqlspec_spanner_hooks_registered"
_INTERLEAVE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"""
    \bINTERLEAVE\s+IN\s+PARENT\s+
    (?P<parent>.+?)
    (?:\s+ON\s+DELETE\s+(?P<on_delete>CASCADE|NO\s+ACTION))?
    (?=\s+(?:ROW\s+DELETION\s+POLICY|TTL)\b|\s*$)
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)


def _is_spanner_dialect(parser: Any) -> bool:
    dialect = getattr(parser, "dialect", None)
    return dialect is not None and dialect.__class__.__name__ == "Spanner"


def _original_bigquery_parse_property() -> Any:
    original = getattr(BigQueryParser, _ORIGINAL_PARSE_PROPERTY_ATTR, None)
    if callable(original):
        return original
    original = BigQueryParser._parse_property
    setattr(BigQueryParser, _ORIGINAL_PARSE_PROPERTY_ATTR, original)
    return original


def _normalize_on_delete_value(on_delete: str) -> str:
    return " ".join(on_delete.upper().split())


def _build_interleave_property(parent: exp.Expr, on_delete: str | None = None) -> exp.Property:
    values: list[exp.Expr] = [parent]
    if on_delete is not None:
        values.append(exp.Literal.string(_normalize_on_delete_value(on_delete)))
    return exp.Property(this=exp.Literal.string(_INTERLEAVE_NAME), value=exp.Tuple(expressions=values))


def _extract_interleave_property(sql: str) -> tuple[str, exp.Property | None]:
    match = _INTERLEAVE_PATTERN.search(sql)
    if match is None:
        return sql, None

    parent = exp.to_table(match.group("parent").strip())
    on_delete = match.group("on_delete")
    interleave_property = _build_interleave_property(parent, on_delete)
    repaired_sql = f"{sql[: match.start()]} {sql[match.end() :]}".strip()
    return repaired_sql, interleave_property


def _attach_create_property(create: exp.Create, property_expression: exp.Property) -> exp.Create:
    properties = create.args.get("properties")
    if isinstance(properties, exp.Properties):
        expressions = list(properties.expressions)
        expressions.insert(0, property_expression)
        properties.set("expressions", expressions)
    else:
        create.set("properties", exp.Properties(expressions=[property_expression]))
    return create


def _is_post_schema_spanner_property(expression: exp.Expr) -> bool:
    if not isinstance(expression, exp.Property) or not isinstance(expression.this, exp.Literal):
        return False
    return expression.this.name.upper() in {_INTERLEAVE_NAME, _ROW_DELETION_NAME, "TTL"}


def _spanner_parse_property(self: Any) -> exp.Expr:
    if _is_spanner_dialect(self):
        if self._match_text_seq("INTERLEAVE", "IN", "PARENT"):
            parent = cast("exp.Expr", self._parse_table(schema=True, is_db_reference=True))
            on_delete: str | None = None

            if self._match_text_seq("ON", "DELETE"):
                if self._match_text_seq("CASCADE"):
                    on_delete = "CASCADE"
                elif self._match_text_seq("NO", "ACTION"):
                    on_delete = "NO ACTION"

            return _build_interleave_property(parent, on_delete)

        if self._match_text_seq("ROW", "DELETION", "POLICY"):
            self._match(TokenType.L_PAREN)
            self._match_text_seq("OLDER_THAN")
            self._match(TokenType.L_PAREN)
            column = cast("exp.Expr", self._parse_id_var())
            self._match(TokenType.COMMA)
            self._match_text_seq("INTERVAL")
            interval = cast("exp.Expr", self._parse_expression())
            self._match(TokenType.R_PAREN)
            self._match(TokenType.R_PAREN)

            return exp.Property(
                this=exp.Literal.string(_ROW_DELETION_NAME), value=exp.Tuple(expressions=[column, interval])
            )

        if self._match_text_seq("TTL"):
            self._match_text_seq("INTERVAL")
            interval = cast("exp.Expr", self._parse_expression())
            self._match_text_seq("ON")
            column = cast("exp.Expr", self._parse_id_var())

            return exp.Property(this=exp.Literal.string("TTL"), value=exp.Tuple(expressions=[interval, column]))

    return cast("exp.Expr", _original_bigquery_parse_property()(self))


def _register_bigquery_spanner_parser_hooks() -> None:
    if getattr(BigQueryParser, _HOOKS_REGISTERED_ATTR, False):
        return

    _original_bigquery_parse_property()
    setattr(BigQueryParser, "_parse_property", _spanner_parse_property)
    setattr(BigQueryParser, _HOOKS_REGISTERED_ATTR, True)


class SpannerTokenizer(BigQuery.Tokenizer):
    """Tokenizer adds Spanner-only keywords when supported by sqlglot."""

    KEYWORDS = {**BigQuery.Tokenizer.KEYWORDS, **_SPANNER_KEYWORDS}


class SpannerGenerator(BigQuery.Generator):
    """Generate Spanner-specific DDL syntax."""

    def locate_properties(self, properties: exp.Properties) -> Any:
        """Keep custom Spanner CREATE TABLE properties at the schema boundary."""
        properties_locs = super().locate_properties(properties)
        with_properties = list(properties_locs[exp.Properties.Location.POST_WITH])
        if not with_properties:
            return properties_locs

        retained_with_properties: list[exp.Expr] = []
        for property_expression in with_properties:
            if _is_post_schema_spanner_property(property_expression):
                properties_locs[exp.Properties.Location.POST_SCHEMA].append(property_expression)
            else:
                retained_with_properties.append(property_expression)

        properties_locs[exp.Properties.Location.POST_WITH] = retained_with_properties
        return properties_locs

    def properties_sql(self, expression: exp.Properties) -> str:
        """Render custom Spanner properties without BigQuery's OPTIONS wrapper."""
        root_properties: list[exp.Expr] = []
        with_properties: list[exp.Expr] = []

        for property_expression in expression.expressions:
            if _is_post_schema_spanner_property(property_expression):
                root_properties.append(property_expression)
                continue

            property_location = self.PROPERTIES_LOCATION[property_expression.__class__]
            if property_location == exp.Properties.Location.POST_WITH:
                with_properties.append(property_expression)
            elif property_location == exp.Properties.Location.POST_SCHEMA:
                root_properties.append(property_expression)

        root_props_ast = exp.Properties(expressions=root_properties)
        root_props_ast.parent = expression.parent
        with_props_ast = exp.Properties(expressions=with_properties)
        with_props_ast.parent = expression.parent

        root_props = self.root_properties(root_props_ast)
        with_props = self.with_properties(with_props_ast)

        if root_props and with_props and not self.pretty:
            with_props = f" {with_props}"

        return root_props + with_props

    def property_sql(self, expression: exp.Property) -> str:
        """Render Spanner-specific properties."""
        if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _INTERLEAVE_NAME:
            values = expression.args.get("value")
            if isinstance(values, exp.Tuple) and values.expressions:
                parent = self.sql(values.expressions[0])
                sql = f"INTERLEAVE IN PARENT {parent}"
                if len(values.expressions) >= _TTL_MIN_COMPONENTS:
                    on_delete_expr = values.expressions[1]
                    if isinstance(on_delete_expr, exp.Literal):
                        sql = f"{sql} ON DELETE {on_delete_expr.this}"
                return sql

        if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _ROW_DELETION_NAME:
            values = expression.args.get("value")
            if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
                column = self.sql(values.expressions[0])
                interval_sql = self.sql(values.expressions[1])
                if not interval_sql.upper().startswith("INTERVAL"):
                    interval_sql = f"INTERVAL {interval_sql}"
                return f"ROW DELETION POLICY (OLDER_THAN({column}, {interval_sql}))"

        if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == "TTL":
            values = expression.args.get("value")
            if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
                interval = self.sql(values.expressions[0])
                column = self.sql(values.expressions[1])
                return f"TTL INTERVAL {interval} ON {column}"

        return super().property_sql(expression)


_register_bigquery_spanner_parser_hooks()


class Spanner(BigQuery):
    """Google Cloud Spanner SQL dialect."""

    Tokenizer = SpannerTokenizer
    Parser = BigQuery.Parser
    Generator = SpannerGenerator

    def parse(self, sql: str, **opts: Any) -> list[exp.Expr | None]:
        """Repair CREATE TABLE statements that sqlglot still falls back to Command for."""
        expressions = super().parse(sql, **opts)
        if len(expressions) != 1 or not isinstance(expressions[0], exp.Command):
            return expressions

        repaired_sql, interleave_property = _extract_interleave_property(sql)
        if interleave_property is None:
            return expressions

        reparsed = BigQuery.parse(self, repaired_sql, **opts)
        if len(reparsed) != 1 or not isinstance(reparsed[0], exp.Create):
            return expressions

        return [_attach_create_property(reparsed[0], interleave_property)]
