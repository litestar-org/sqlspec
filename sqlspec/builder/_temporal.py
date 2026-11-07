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

from typing import TYPE_CHECKING, Final

from sqlglot import exp
from sqlglot.generator import Generator

from sqlspec.builder._generation import invalidate_generator_dispatch
from sqlspec.builder._parsing_utils import _parse_table_reference

if TYPE_CHECKING:
    from sqlglot.generators.bigquery import BigQueryGenerator
    from sqlglot.generators.duckdb import DuckDBGenerator
    from sqlglot.generators.oracle import OracleGenerator
    from sqlglot.generators.postgres import PostgresGenerator
    from sqlglot.generators.snowflake import SnowflakeGenerator

__all__ = (
    "create_temporal_table",
    "create_temporal_version",
    "is_temporal_version_expression",
    "register_version_generators",
)

_TEMPORAL_VERSION_KEY: Final[str] = "sqlspec_temporal_version"


def is_temporal_version_expression(expression: object) -> bool:
    """Return True when an exp.Version node is a SQLSpec temporal version expression."""
    return isinstance(expression, exp.Version) and bool(expression.args.get(_TEMPORAL_VERSION_KEY))


def create_temporal_version(this: "str | None" = None, expression: "exp.Expr | str | None" = None) -> exp.Version:
    """Construct an exp.Version node tagged as a SQLSpec temporal version."""
    as_of_expr = exp.Literal.string(expression) if isinstance(expression, str) else expression
    version = exp.Version(this=this or "TIMESTAMP", kind="AS OF", expression=as_of_expr)
    version.set(_TEMPORAL_VERSION_KEY, True)
    return version


def create_temporal_table(
    table: "str | exp.Table | exp.Expr", as_of: "exp.Expr | str", kind: "str | None" = None
) -> exp.Table:
    """Create a table expression with temporal (time-travel) version clause.

    Args:
        table: Table name or expression.
        as_of: Timestamp or SCN expression for the point-in-time.
        kind: Optional version kind.
            Defaults to dialect-appropriate value.

    Returns:
        Table expression with version clause that generates dialect-specific SQL.
    """
    register_version_generators()

    if isinstance(table, str):
        table_expr = _parse_table_reference(table)
    elif isinstance(table, exp.Table):
        table_expr = table.copy()
    else:
        table_expr = _parse_table_reference(str(table))

    version = create_temporal_version(this=kind, expression=as_of)

    table_expr.set("version", version)
    return table_expr


_VERSION_GENERATORS_REGISTERED = False


def register_version_generators() -> None:
    """Register dialect-specific version_sql generators.

    This function is idempotent - calling it multiple times has no effect
    after the first call. It is called lazily when temporal tables or joins
    are constructed.

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

    from sqlglot.dialects.bigquery import BigQuery
    from sqlglot.dialects.duckdb import DuckDB
    from sqlglot.dialects.oracle import Oracle
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.dialects.snowflake import Snowflake

    Generator.TRANSFORMS[exp.Version] = _default_version_sql

    BigQuery.Generator.TRANSFORMS[exp.Version] = _bigquery_version_sql
    Oracle.Generator.TRANSFORMS[exp.Version] = _oracle_version_sql
    Snowflake.Generator.TRANSFORMS[exp.Version] = _snowflake_version_sql
    DuckDB.Generator.TRANSFORMS[exp.Version] = _duckdb_version_sql
    Postgres.Generator.TRANSFORMS[exp.Version] = _cockroachdb_version_sql

    invalidate_generator_dispatch(
        Generator, BigQuery.Generator, Oracle.Generator, Snowflake.Generator, DuckDB.Generator, Postgres.Generator
    )

    _VERSION_GENERATORS_REGISTERED = True


def _oracle_version_sql(self: "OracleGenerator", expression: exp.Version) -> str:
    """Oracle: AS OF TIMESTAMP timestamp or AS OF SCN scn."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        this = expression.name or "TIMESTAMP"
        return f"AS OF {this} {expr}"
    return self.version_sql(expression)


def _bigquery_version_sql(self: "BigQueryGenerator", expression: exp.Version) -> str:
    """BigQuery: FOR SYSTEM_TIME AS OF timestamp."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        return f"FOR SYSTEM_TIME AS OF {expr}"
    return self.version_sql(expression)


def _snowflake_version_sql(self: "SnowflakeGenerator", expression: exp.Version) -> str:
    """Snowflake: AT (TIMESTAMP => timestamp) or BEFORE (TIMESTAMP => ...).

    AS OF is mapped to AT, and BEFORE is supported for point-before queries.
    """
    if is_temporal_version_expression(expression):
        kind = expression.text("kind")
        expr = self.sql(expression, "expression")
        this = expression.name or "TIMESTAMP"
        if kind and "BEFORE" in kind.upper():
            return f"BEFORE ({this} => {expr})"
        return f"AT ({this} => {expr})"
    return self.version_sql(expression)


def _duckdb_version_sql(self: "DuckDBGenerator", expression: exp.Version) -> str:
    """DuckDB: AT (TIMESTAMP => timestamp)."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        this = expression.name or "TIMESTAMP"
        return f"AT ({this} => {expr})"
    return self.version_sql(expression)


def _cockroachdb_version_sql(self: "PostgresGenerator", expression: exp.Version) -> str:
    """CockroachDB (via Postgres dialect): AS OF SYSTEM TIME timestamp."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        return f"AS OF SYSTEM TIME {expr}"
    return self.version_sql(expression)


def _default_version_sql(self: "Generator", expression: exp.Version) -> str:
    """Default: AS OF SYSTEM TIME timestamp (CockroachDB style).

    When no dialect is specified, we default to CockroachDB/Postgres style
    which is commonly expected for time-travel queries. Dialect subclasses
    that inherit this transform fall back to their own ``version_sql``.
    """
    if type(self) is Generator and is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        return f"AS OF SYSTEM TIME {expr}"
    return self.version_sql(expression)
