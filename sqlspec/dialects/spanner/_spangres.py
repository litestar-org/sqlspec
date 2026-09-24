r"""Google Cloud Spanner PostgreSQL-interface dialect ("Spangres").

Extends the Postgres dialect with the Spanner PostgreSQL-interface DDL
extensions: ``INTERLEAVE IN [PARENT]`` for interleaved tables and ``TTL
INTERVAL ... ON`` for row-level time-to-live. The GoogleSQL ``ROW DELETION
POLICY`` form is accepted on parse and normalized to the canonical policy
node so generation always emits the valid PostgreSQL-dialect ``TTL`` form.
"""

from typing import Any, cast

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.spanner._generators import SpangresGenerator
from sqlspec.dialects.spanner._parsers import (
    SpangresParser,
    attach_create_property,
    attach_hints,
    extract_interleave_property,
)

__all__ = ("Spangres",)


class Spangres(Postgres):
    """Spanner PostgreSQL-compatible dialect."""

    Parser = SpangresParser
    Generator = SpangresGenerator

    def parse(self, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse Spangres SQL statements and attach hints."""
        expressions = super().parse(sql, **opts)
        if len(expressions) == 1 and isinstance(expressions[0], exp.Command):
            repaired_sql, interleave_property = extract_interleave_property(sql)
            if interleave_property is not None:
                reparsed = Postgres.parse(self, repaired_sql, **opts)
                if len(reparsed) == 1 and isinstance(reparsed[0], exp.Create):
                    expressions = cast(
                        "list[exp.Expr | None]", [attach_create_property(reparsed[0], interleave_property)]
                    )

        for expression in expressions:
            if expression is not None:
                attach_hints(expression)
        return expressions

    def parse_into(self, expression_type: Any, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse into specific expression type with attached hints."""
        expressions = super().parse_into(expression_type, sql, **opts)
        for expression in expressions:
            if expression is not None:
                attach_hints(expression)
        return expressions
