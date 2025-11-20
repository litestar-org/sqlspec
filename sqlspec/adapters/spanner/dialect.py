"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

Extends the BigQuery dialect with Spanner-only DDL features:
`INTERLEAVE IN PARENT` for interleaved tables and `TTL INTERVAL ... ON ...`
for row-level time-to-live policies.
"""

from typing import Any, cast

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.tokens import TokenType

__all__ = ("Spanner",)


_SPANNER_KEYWORDS: dict[str, TokenType] = {}
interleave_token = getattr(TokenType, "INTERLEAVE", None)
if interleave_token is not None:
    _SPANNER_KEYWORDS["INTERLEAVE"] = interleave_token
ttl_token = getattr(TokenType, "TTL", None)
if ttl_token is not None:
    _SPANNER_KEYWORDS["TTL"] = ttl_token

_TTL_MIN_COMPONENTS = 2


class Spanner(BigQuery):
    """Google Cloud Spanner SQL dialect."""

    class Tokenizer(BigQuery.Tokenizer):
        """Tokenizer adds Spanner-only keywords when supported by sqlglot."""

        KEYWORDS = {**BigQuery.Tokenizer.KEYWORDS, **_SPANNER_KEYWORDS}

    class Parser(BigQuery.Parser):
        """Parse Spanner extensions such as INTERLEAVE and TTL clauses."""

        def _parse_table_parts(
            self, schema: "bool" = False, is_db_reference: "bool" = False, wildcard: "bool" = False
        ) -> exp.Table:
            """Parse Spanner table options including interleaving metadata."""
            table = super()._parse_table_parts(schema=schema, is_db_reference=is_db_reference, wildcard=wildcard)

            if self._match_text_seq("INTERLEAVE", "IN", "PARENT"):  # type: ignore[no-untyped-call]
                parent = cast("exp.Expression", self._parse_table(schema=True, is_db_reference=True))
                on_delete: str | None = None

                if self._match_text_seq("ON", "DELETE"):  # type: ignore[no-untyped-call]
                    if self._match_text_seq("CASCADE"):  # type: ignore[no-untyped-call]
                        on_delete = "CASCADE"
                    elif self._match_text_seq("NO", "ACTION"):  # type: ignore[no-untyped-call]
                        on_delete = "NO ACTION"

                table.set("interleave_parent", parent)
                if on_delete:
                    table.set("interleave_on_delete", on_delete)

            return table

        def _parse_property(self) -> exp.Expression:
            """Parse Spanner TTL property as a Property expression."""
            if self._match_text_seq("TTL"):  # type: ignore[no-untyped-call]
                self._match_text_seq("INTERVAL")  # type: ignore[no-untyped-call]
                interval = cast("exp.Expression", self._parse_string())
                self._match_text_seq("ON")  # type: ignore[no-untyped-call]
                column = cast("exp.Expression", self._parse_id_var())

                return exp.Property(this=exp.Literal.string("TTL"), value=exp.Tuple(expressions=[interval, column]))

            return cast("exp.Expression", super()._parse_property())

    class Generator(BigQuery.Generator):
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
            """Render TTL property in Spanner syntax."""
            if getattr(expression.this, "name", "").upper() == "TTL":
                values = cast("Any", expression.args.get("value"))
                if values and getattr(values, "expressions", None) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
                    interval = self.sql(values.expressions[0])
                    column = self.sql(values.expressions[1])
                    return f"TTL INTERVAL {interval} ON {column}"

            return super().property_sql(expression)
