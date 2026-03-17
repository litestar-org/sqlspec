r"""Google Cloud Spanner PostgreSQL-interface dialect ("Spangres")."""

from typing import cast

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokenizer_core import TokenType

__all__ = ("Spangres",)


_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_TTL_MIN_COMPONENTS = 2


class SpangresParser(Postgres.Parser):
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


class SpangresGenerator(Postgres.Generator):
    """Generate Spanner row deletion policies."""

    def property_sql(self, expression: exp.Property) -> str:
        if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _ROW_DELETION_NAME:
            values = expression.args.get("value")
            if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
                column = self.sql(values.expressions[0])
                interval_sql = self.sql(values.expressions[1])
                if not interval_sql.upper().startswith("INTERVAL"):
                    interval_sql = f"INTERVAL {interval_sql}"
                return f"ROW DELETION POLICY (OLDER_THAN({column}, {interval_sql}))"

        return super().property_sql(expression)


class Spangres(Postgres):
    """Spanner PostgreSQL-compatible dialect."""

    Parser = SpangresParser
    Generator = SpangresGenerator
