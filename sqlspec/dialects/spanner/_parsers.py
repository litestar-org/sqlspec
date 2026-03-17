"""Compilable Parser classes for Spanner dialects.

Parser subclasses can be compiled by mypyc (sqlglot's Parser has no __slots__).
Tokenizer subclasses cannot be compiled (Tokenizer has __slots__) and remain
in the dialect modules.
"""

from typing import cast

from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.parsers.bigquery import BigQueryParser
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokenizer_core import TokenType

__all__ = ("SpangresParser", "SpannerParser")

_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_TTL_MIN_COMPONENTS = 2


@mypyc_attr(allow_interpreted_subclasses=True)
class SpannerParser(BigQueryParser):
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

        return table

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


@mypyc_attr(allow_interpreted_subclasses=True)
class SpangresParser(PostgresParser):
    """Parse Spanner row deletion policies."""

    def _parse_property(self) -> exp.Expr:
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

        return cast("exp.Expr", super()._parse_property())
