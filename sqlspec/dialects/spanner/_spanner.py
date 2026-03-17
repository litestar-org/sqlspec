"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

Extends the BigQuery dialect with Spanner-only DDL features:
`INTERLEAVE IN PARENT` for interleaved tables and `ROW DELETION POLICY`
for row-level time-to-live policies (GoogleSQL).
"""

from typing import cast

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
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


class SpannerTokenizer(BigQuery.Tokenizer):
    """Tokenizer adds Spanner-only keywords when supported by sqlglot."""

    KEYWORDS = {**BigQuery.Tokenizer.KEYWORDS, **_SPANNER_KEYWORDS}


class SpannerParser(BigQuery.Parser):  # type: ignore[valid-type, misc]
    """Parse Spanner extensions such as INTERLEAVE and row deletion policies."""

    def _parse_table_parts(
        self, schema: "bool" = False, is_db_reference: "bool" = False, wildcard: "bool" = False
    ) -> exp.Table:
        """Parse Spanner table options including interleaving metadata."""
        table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference, wildcard=wildcard)

        if self._match_text_seq("INTERLEAVE", "IN", "PARENT"):
            parent = cast("exp.Expr", self._parse_table(schema=True, is_db_reference=True))
            on_delete: str | None = None

            if self._match_text_seq("ON", "DELETE"):
                if self._match_text_seq("CASCADE"):
                    on_delete = "CASCADE"
                elif self._match_text_seq("NO", "ACTION"):
                    on_delete = "NO ACTION"

            table.set("interleave_parent", parent)
            if on_delete:
                table.set("interleave_on_delete", on_delete)

        return table  # type: ignore[no-any-return]

    def _parse_property(self) -> exp.Expr:
        """Parse Spanner row deletion policy or PostgreSQL-style TTL."""
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

        return cast("exp.Expr", super()._parse_property())


class SpannerGenerator(BigQuery.Generator):
    """Generate Spanner-specific DDL syntax."""

    def table_sql(self, expression: exp.Table, sep: str = " ") -> str:
        """Render INTERLEAVE clause when present on a table expression."""
        sql = super().table_sql(expression, sep=sep)

        parent = expression.args.get("interleave_parent")
        if parent:
            sql = f"{sql}\nINTERLEAVE IN PARENT {self.sql(parent)}"
            on_delete = expression.args.get("interleave_on_delete")
            if on_delete:
                sql = f"{sql} ON DELETE {on_delete}"

        return sql

    def property_sql(self, expression: exp.Property) -> str:
        """Render row deletion policy or TTL."""
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


class Spanner(BigQuery):
    """Google Cloud Spanner SQL dialect."""

    Tokenizer = SpannerTokenizer
    Parser = SpannerParser
    Generator = SpannerGenerator
