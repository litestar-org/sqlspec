r"""Google Cloud Spanner PostgreSQL-interface dialect ("Spangres")."""

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.spanner._parsers import SpangresParser

__all__ = ("Spangres",)

_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_TTL_MIN_COMPONENTS = 2


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
