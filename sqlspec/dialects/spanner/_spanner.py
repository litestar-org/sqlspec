"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

Extends the BigQuery dialect with Spanner-only DDL features:
`INTERLEAVE IN PARENT` for interleaved tables and `ROW DELETION POLICY`
for row-level time-to-live policies (GoogleSQL).
"""

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery

from sqlspec.dialects.spanner._parsers import SpannerParser, SpannerTokenizer

__all__ = ("Spanner",)

_TTL_MIN_COMPONENTS = 2
_ROW_DELETION_NAME = "ROW_DELETION_POLICY"


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
