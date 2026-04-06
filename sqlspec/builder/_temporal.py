"""Temporal query expressions.

Overrides sqlglot's built-in version_sql generators to produce dialect-specific
time-travel query syntax. Uses sqlglot's exp.Version and Table.version slot.

Supported syntax by dialect:
- BigQuery: table FOR SYSTEM_TIME AS OF timestamp (built-in)
- Oracle: table AS OF TIMESTAMP timestamp / AS OF SCN scn
- Snowflake: table AT (TIMESTAMP => timestamp) / BEFORE (...)
- DuckDB: table AT (TIMESTAMP => timestamp)
- CockroachDB (Postgres): table AS OF SYSTEM TIME timestamp
"""

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.dialects.oracle import Oracle
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.generator import (
    _DISPATCH_CACHE,  # pyright: ignore[reportPrivateUsage]
    Generator,
)
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.generators.duckdb import DuckDBGenerator
from sqlglot.generators.oracle import OracleGenerator
from sqlglot.generators.postgres import PostgresGenerator
from sqlglot.generators.snowflake import SnowflakeGenerator

__all__ = ("create_temporal_table", "register_version_generators")


def _oracle_version_sql(self: OracleGenerator, expression: exp.Version) -> str:
    """Oracle: AS OF TIMESTAMP timestamp or AS OF SCN scn."""
    expr = self.sql(expression, "expression")
    this = expression.name or "TIMESTAMP"
    return f"AS OF {this} {expr}"


def _bigquery_version_sql(self: BigQueryGenerator, expression: exp.Version) -> str:
    """BigQuery: FOR SYSTEM_TIME AS OF timestamp."""
    expr = self.sql(expression, "expression")
    return f"FOR SYSTEM_TIME AS OF {expr}"


def _snowflake_version_sql(self: SnowflakeGenerator, expression: exp.Version) -> str:
    """Snowflake: AT (TIMESTAMP => timestamp) or BEFORE (TIMESTAMP => ...).

    AS OF is mapped to AT, and BEFORE is supported for point-before queries.
    """
    kind = expression.text("kind")
    expr = self.sql(expression, "expression")
    this = expression.name or "TIMESTAMP"
    if kind and "BEFORE" in kind.upper():
        return f"BEFORE ({this} => {expr})"
    return f"AT ({this} => {expr})"


def _duckdb_version_sql(self: DuckDBGenerator, expression: exp.Version) -> str:
    """DuckDB: AT (TIMESTAMP => timestamp)."""
    expr = self.sql(expression, "expression")
    return f"AT (TIMESTAMP => {expr})"


def _cockroachdb_version_sql(self: PostgresGenerator, expression: exp.Version) -> str:
    """CockroachDB (via Postgres dialect): AS OF SYSTEM TIME timestamp."""
    expr = self.sql(expression, "expression")
    return f"AS OF SYSTEM TIME {expr}"


def _default_version_sql(self: Generator, expression: exp.Version) -> str:
    """Default: AS OF SYSTEM TIME timestamp (CockroachDB style).

    When no dialect is specified, we default to CockroachDB/Postgres style
    which is commonly expected for time-travel queries.
    """
    expr = self.sql(expression, "expression")
    return f"AS OF SYSTEM TIME {expr}"


def create_temporal_table(
    table: "str | exp.Table | exp.Expr", as_of: "exp.Expr | str", kind: "str | None" = None
) -> exp.Table:
    """Create a table expression with temporal (time-travel) version clause.

    Args:
        table: Table name or expression.
        as_of: Timestamp or SCN expression for the point-in-time.
        kind: Optional version kind (e.g., "TIMESTAMP", "SCN", "SYSTEM TIME").
              Defaults to dialect-appropriate value.

    Returns:
        Table expression with version clause that generates dialect-specific SQL.

    Notes:
        Inputs are normalized before building the ``exp.Version`` clause so both string table names and literal timestamps
        work consistently.

    Example:
        >>> from sqlspec.builder import create_temporal_table
        >>> from sqlglot import exp
        >>> t = create_temporal_table(
        ...     "orders", exp.Literal.string("2024-01-01")
        ... )
        >>> t.sql(dialect="oracle")
        "orders AS OF TIMESTAMP '2024-01-01'"
        >>> t.sql(dialect="bigquery")
        "orders FOR SYSTEM_TIME AS OF '2024-01-01'"
    """
    if isinstance(table, str):
        table_expr = exp.to_table(table)
    elif isinstance(table, exp.Table):
        table_expr = table.copy()
    else:
        table_expr = exp.to_table(str(table))

    as_of_expr = exp.Literal.string(as_of) if isinstance(as_of, str) else as_of

    version = exp.Version(this=kind or "TIMESTAMP", kind="AS OF", expression=as_of_expr)

    table_expr.set("version", version)
    return table_expr


_VERSION_GENERATORS_REGISTERED = False


def register_version_generators() -> None:
    """Register dialect-specific version_sql generators.

    This function is idempotent - calling it multiple times has no effect
    after the first call. It is called automatically when the builder
    module is imported.

    Registers custom SQL generators for temporal (time-travel) queries:
    - Default (no dialect): AS OF SYSTEM TIME (CockroachDB style)
    - BigQuery: FOR SYSTEM_TIME AS OF
    - Oracle: AS OF TIMESTAMP / AS OF SCN
    - Snowflake: AT (TIMESTAMP => ...) / BEFORE (...)
    - DuckDB: AT (TIMESTAMP => ...)
    - Postgres/CockroachDB: AS OF SYSTEM TIME
    """
    global _VERSION_GENERATORS_REGISTERED
    if _VERSION_GENERATORS_REGISTERED:
        return

    Generator.TRANSFORMS[exp.Version] = _default_version_sql

    BigQuery.Generator.TRANSFORMS[exp.Version] = _bigquery_version_sql
    Oracle.Generator.TRANSFORMS[exp.Version] = _oracle_version_sql
    Snowflake.Generator.TRANSFORMS[exp.Version] = _snowflake_version_sql
    DuckDB.Generator.TRANSFORMS[exp.Version] = _duckdb_version_sql
    Postgres.Generator.TRANSFORMS[exp.Version] = _cockroachdb_version_sql

    # Invalidate sqlglot's per-class dispatch cache so new TRANSFORMS entries
    # are picked up by the next Generator instantiation.
    for gen_cls in (
        Generator,
        BigQuery.Generator,
        Oracle.Generator,
        Snowflake.Generator,
        DuckDB.Generator,
        Postgres.Generator,
    ):
        _DISPATCH_CACHE.pop(gen_cls, None)

    _VERSION_GENERATORS_REGISTERED = True
