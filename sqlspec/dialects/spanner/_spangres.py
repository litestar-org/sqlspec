r"""Google Cloud Spanner PostgreSQL-interface dialect ("Spangres").

Extends the Postgres dialect with the Spanner PostgreSQL-interface DDL
extensions: ``INTERLEAVE IN [PARENT]`` for interleaved tables and ``TTL
INTERVAL ... ON`` for row-level time-to-live. The GoogleSQL ``ROW DELETION
POLICY`` form is accepted on parse and normalized to the canonical policy
node so generation always emits the valid PostgreSQL-dialect ``TTL`` form.
"""

from typing import Any

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.spanner._generators import SpangresGenerator
from sqlspec.dialects.spanner._parsers import (
    attach_create_property,
    extract_interleave_property,
    register_spanner_property_parsers,
)

__all__ = ("Spangres",)

register_spanner_property_parsers()


class Spangres(Postgres):
    """Spanner PostgreSQL-compatible dialect."""

    Generator = SpangresGenerator

    def parse(self, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Repair CREATE TABLE statements that sqlglot still falls back to Command for."""
        expressions = super().parse(sql, **opts)
        if len(expressions) != 1 or not isinstance(expressions[0], exp.Command):
            return expressions

        repaired_sql, interleave_property = extract_interleave_property(sql)
        if interleave_property is None:
            return expressions

        reparsed = Postgres.parse(self, repaired_sql, **opts)
        if len(reparsed) != 1 or not isinstance(reparsed[0], exp.Create):
            return expressions

        return [attach_create_property(reparsed[0], interleave_property)]
