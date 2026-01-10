"""Temporal query expressions.

Provides custom expressions for time-travel queries (Flashback, AS OF SYSTEM TIME).
"""

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.dialects.oracle import Oracle
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.generator import Generator

__all__ = ("FlashbackTable",)


class FlashbackTable(exp.Expression):
    """Expression for table with AS OF clause (Time Travel/Flashback).

    Generates dialect-specific SQL for time travel queries:
    - CockroachDB (Postgres): table AS OF SYSTEM TIME timestamp
    - Oracle: table AS OF TIMESTAMP timestamp
    - BigQuery: table FOR SYSTEM_TIME AS OF timestamp
    - Snowflake: table AT (TIMESTAMP => timestamp)
    - DuckDB: table AT (TIMESTAMP => timestamp)
    """

    arg_types = {"this": True, "as_of": True, "kind": False}


def _flashback_table_sql(generator: Generator, expression: FlashbackTable) -> str:
    table_sql = generator.sql(expression, "this")
    as_of_expr = expression.args.get("as_of")
    as_of_sql = generator.sql(as_of_expr) if as_of_expr else ""

    # Default kind if not specified
    kind_expr = expression.args.get("kind")
    kind = kind_expr.this if kind_expr else None

    # Resolve dialect name
    dialect = getattr(generator, "dialect", None)
    dialect_name = ""

    # Try to resolve dialect from generator's configured dialect
    if dialect:
        if isinstance(dialect, str):
            dialect_name = dialect
        elif hasattr(dialect, "name") and dialect.name:
            dialect_name = dialect.name
        else:
            dialect_name = type(dialect).__name__

    # Fallback: check generator module/class if dialect not explicitly set
    if not dialect_name:
        module_name = generator.__class__.__module__.lower()
        if "oracle" in module_name:
            dialect_name = "oracle"
        elif "bigquery" in module_name:
            dialect_name = "bigquery"
        elif "snowflake" in module_name:
            dialect_name = "snowflake"
        elif "duckdb" in module_name:
            dialect_name = "duckdb"
        elif "postgres" in module_name:
            dialect_name = "postgres"

    dialect_name = dialect_name.lower()

    if dialect_name == "oracle":
        kind = kind or "TIMESTAMP"
        return f"{table_sql} AS OF {kind} {as_of_sql}"

    if dialect_name == "bigquery":
        return f"{table_sql} FOR SYSTEM_TIME AS OF {as_of_sql}"

    if dialect_name in {"snowflake", "duckdb"}:
        return f"{table_sql} AT (TIMESTAMP => {as_of_sql})"

    # Default / CockroachDB / Postgres
    # Note: "Postgres" dialect here generates CockroachDB compatible syntax
    # as standard Postgres does not support AS OF SYSTEM TIME.
    kind = kind or "SYSTEM TIME"
    return f"{table_sql} AS OF {kind} {as_of_sql}"


def _register_with_sqlglot() -> None:
    # Register for base Generator and specific dialects that might shadow it
    Generator.TRANSFORMS[FlashbackTable] = _flashback_table_sql
    BigQuery.Generator.TRANSFORMS[FlashbackTable] = _flashback_table_sql
    DuckDB.Generator.TRANSFORMS[FlashbackTable] = _flashback_table_sql
    Oracle.Generator.TRANSFORMS[FlashbackTable] = _flashback_table_sql
    Postgres.Generator.TRANSFORMS[FlashbackTable] = _flashback_table_sql
    Snowflake.Generator.TRANSFORMS[FlashbackTable] = _flashback_table_sql


_register_with_sqlglot()
