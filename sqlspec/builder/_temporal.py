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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final

from sqlglot import exp

from sqlspec.builder._generation import invalidate_generator_dispatch

if TYPE_CHECKING:
    from sqlglot.generator import Generator
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

_TEMPORAL_VERSION_META_KEY: Final[str] = "sqlspec_temporal_version"

_VersionTransform = Callable[[Any, exp.Version], str]
_BASE_VERSION_TRANSFORM: _VersionTransform | None = None
_BIGQUERY_VERSION_TRANSFORM: _VersionTransform | None = None
_ORACLE_VERSION_TRANSFORM: _VersionTransform | None = None
_SNOWFLAKE_VERSION_TRANSFORM: _VersionTransform | None = None
_DUCKDB_VERSION_TRANSFORM: _VersionTransform | None = None
_POSTGRES_VERSION_TRANSFORM: _VersionTransform | None = None


def is_temporal_version_expression(expression: object) -> bool:
    """Return True when an exp.Version node is a SQLSpec temporal version expression."""
    return isinstance(expression, exp.Version) and _TEMPORAL_VERSION_META_KEY in expression.meta


def create_temporal_version(this: "str | None" = None, expression: "exp.Expr | str | None" = None) -> exp.Version:
    """Construct an exp.Version node tagged as a SQLSpec temporal version."""
    as_of_expr = exp.Literal.string(expression) if isinstance(expression, str) else expression
    version = exp.Version(this=this or "TIMESTAMP", kind="AS OF", expression=as_of_expr)
    version.meta[_TEMPORAL_VERSION_META_KEY] = True
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
        table_expr = exp.to_table(table)
    elif isinstance(table, exp.Table):
        table_expr = table.copy()
    else:
        table_expr = exp.to_table(str(table))

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
    global _BASE_VERSION_TRANSFORM, _BIGQUERY_VERSION_TRANSFORM, _ORACLE_VERSION_TRANSFORM
    global _SNOWFLAKE_VERSION_TRANSFORM, _DUCKDB_VERSION_TRANSFORM, _POSTGRES_VERSION_TRANSFORM
    global _VERSION_GENERATORS_REGISTERED
    if _VERSION_GENERATORS_REGISTERED:
        return

    from sqlglot.dialects.bigquery import BigQuery
    from sqlglot.dialects.duckdb import DuckDB
    from sqlglot.dialects.oracle import Oracle
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.dialects.snowflake import Snowflake
    from sqlglot.generator import Generator

    _BASE_VERSION_TRANSFORM = Generator.TRANSFORMS.get(exp.Version)
    _BIGQUERY_VERSION_TRANSFORM = BigQuery.Generator.TRANSFORMS.get(exp.Version)
    _ORACLE_VERSION_TRANSFORM = Oracle.Generator.TRANSFORMS.get(exp.Version)
    _SNOWFLAKE_VERSION_TRANSFORM = Snowflake.Generator.TRANSFORMS.get(exp.Version)
    _DUCKDB_VERSION_TRANSFORM = DuckDB.Generator.TRANSFORMS.get(exp.Version)
    _POSTGRES_VERSION_TRANSFORM = Postgres.Generator.TRANSFORMS.get(exp.Version)

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
    if _ORACLE_VERSION_TRANSFORM is not None:
        return _ORACLE_VERSION_TRANSFORM(self, expression)
    return self.version_sql(expression)


def _bigquery_version_sql(self: "BigQueryGenerator", expression: exp.Version) -> str:
    """BigQuery: FOR SYSTEM_TIME AS OF timestamp."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        return f"FOR SYSTEM_TIME AS OF {expr}"
    if _BIGQUERY_VERSION_TRANSFORM is not None:
        return _BIGQUERY_VERSION_TRANSFORM(self, expression)
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
    if _SNOWFLAKE_VERSION_TRANSFORM is not None:
        return _SNOWFLAKE_VERSION_TRANSFORM(self, expression)
    return self.version_sql(expression)


def _duckdb_version_sql(self: "DuckDBGenerator", expression: exp.Version) -> str:
    """DuckDB: AT (TIMESTAMP => timestamp)."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        this = expression.name or "TIMESTAMP"
        return f"AT ({this} => {expr})"
    if _DUCKDB_VERSION_TRANSFORM is not None:
        return _DUCKDB_VERSION_TRANSFORM(self, expression)
    return self.version_sql(expression)


def _cockroachdb_version_sql(self: "PostgresGenerator", expression: exp.Version) -> str:
    """CockroachDB (via Postgres dialect): AS OF SYSTEM TIME timestamp."""
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        return f"AS OF SYSTEM TIME {expr}"
    if _POSTGRES_VERSION_TRANSFORM is not None:
        return _POSTGRES_VERSION_TRANSFORM(self, expression)
    return self.version_sql(expression)


def _default_version_sql(self: "Generator", expression: exp.Version) -> str:
    """Default: AS OF SYSTEM TIME timestamp (CockroachDB style).

    When no dialect is specified, we default to CockroachDB/Postgres style
    which is commonly expected for time-travel queries.
    """
    if is_temporal_version_expression(expression):
        expr = self.sql(expression, "expression")
        return f"AS OF SYSTEM TIME {expr}"
    if _BASE_VERSION_TRANSFORM is not None:
        return _BASE_VERSION_TRANSFORM(self, expression)
    return self.version_sql(expression)
