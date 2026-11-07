import re
import subprocess
import sys

from sqlglot import exp

from sqlspec.builder import create_temporal_table, sql


def normalize_sql(sql_str: str) -> str:
    # Collapse whitespace and strip quotes/backticks
    return " ".join(sql_str.replace('"', "").replace("`", "").split())


def strip_trailing_alias(sql_str: str, table_name: str) -> str:
    """Remove trailing 'AS tablename' or just 'tablename' alias if redundant.

    The optimizer may add redundant aliases like 'users AS users' which are
    valid SQL but not meaningful. This helper strips them for cleaner assertions.
    """
    # Pattern: temporal_clause (AS tablename | tablename) at end or before ON/WHERE
    patterns = [
        rf"(\)) AS {table_name}(\s|$)",  # ) AS users
        rf"(\)) {table_name}(\s|$)",  # ) users
        rf"(') AS {table_name}(\s|$)",  # ' AS users (for string literals)
        rf"(') {table_name}(\s|$)",  # ' users
    ]
    result = sql_str
    for pattern in patterns:
        result = re.sub(pattern, r"\1\2", result)
    return result


def test_as_of_system_time_default() -> None:
    query = sql.select("*").from_("users", as_of="-10s")
    sql_str = query.build().sql
    normalized = normalize_sql(sql_str)
    # Default behavior (CockroachDB style)
    # Optimizer may add redundant alias, so check for temporal clause
    assert "FROM users AS OF SYSTEM TIME '-10s'" in normalized


def test_as_of_timestamp_oracle() -> None:
    query = sql.select("*").from_("users", as_of=sql.raw("TIMESTAMP '2023-01-01 00:00:00'"))
    sql_str = query.build(dialect="oracle").sql
    normalized = normalize_sql(sql_str)
    # Oracle uses CAST for typed literals if they are parsed as such
    # Check for correct temporal syntax
    assert "FROM users AS OF TIMESTAMP CAST('2023-01-01 00:00:00' AS TIMESTAMP)" in normalized


def test_as_of_bigquery() -> None:
    query = sql.select("*").from_("users", as_of=sql.raw("TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"))
    sql_str = query.build(dialect="bigquery").sql
    # Expected: FOR SYSTEM_TIME AS OF ...
    # Use normalized checking to avoid sqlglot formatting fragility
    normalized = normalize_sql(sql_str)
    assert "FOR SYSTEM_TIME AS OF" in normalized
    assert "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL" in normalized


def test_as_of_snowflake() -> None:
    query = sql.select("*").from_("users", as_of=sql.raw("'2023-01-01'::TIMESTAMP"))
    sql_str = query.build(dialect="snowflake").sql
    normalized = normalize_sql(sql_str)
    # Check for Snowflake AT syntax
    assert "FROM users AT (TIMESTAMP => CAST('2023-01-01' AS TIMESTAMP))" in normalized


def test_as_of_duckdb() -> None:
    query = sql.select("*").from_("users", as_of=sql.raw("'2023-01-01'"))
    sql_str = query.build(dialect="duckdb").sql
    normalized = normalize_sql(sql_str)
    # Check for DuckDB AT syntax
    assert "FROM users AT (TIMESTAMP => '2023-01-01')" in normalized


def test_join_as_of() -> None:
    query = (
        sql
        .select("*")
        .from_("orders")
        .join(sql.left_join_("audit_log", alias="log").as_of("-1h").on("orders.id = log.order_id"))
    )
    sql_str = query.build().sql
    normalized = normalize_sql(sql_str)
    # Check for temporal clause in join - AS keyword may or may not be present before alias
    assert "LEFT JOIN audit_log AS OF SYSTEM TIME '-1h'" in normalized
    assert "log ON orders.id = log.order_id" in normalized


def test_join_as_of_dialect() -> None:
    """Test building a generic join but outputting for a specific dialect (BigQuery)."""
    query = (
        sql
        .select("*")
        .from_("orders")
        .join(sql.left_join_("audit_log", alias="log").as_of("-1h").on("orders.id = log.order_id"))
    )
    sql_str = query.build(dialect="bigquery").sql
    normalized = normalize_sql(sql_str)
    # Should use FOR SYSTEM_TIME AS OF because dialect is passed at build time
    assert "LEFT JOIN audit_log" in normalized
    assert "FOR SYSTEM_TIME AS OF '-1h'" in normalized
    assert "ON orders.id = log.order_id" in normalized


def test_join_as_of_dialect_override() -> None:
    """Test building a generic join but outputting for a specific dialect (Oracle)."""
    query = (
        sql
        .select("*")
        .from_("orders")
        .join(
            sql
            .left_join_("audit_log", alias="log")
            .as_of(sql.raw("TIMESTAMP '2023-01-01'"))
            .on("orders.id = log.order_id")
        )
    )
    # Build for Oracle
    sql_str = query.build(dialect="oracle").sql
    normalized = normalize_sql(sql_str)
    # Should use AS OF TIMESTAMP because dialect is passed at build time
    # AS keyword may or may not be present before alias
    assert "LEFT JOIN audit_log AS OF TIMESTAMP CAST('2023-01-01' AS TIMESTAMP)" in normalized
    assert "log ON orders.id = log.order_id" in normalized


def test_builder_import_does_not_eagerly_import_temporal_dialects() -> None:
    """Importing sqlspec.builder should not load temporal dialect modules."""
    code = (
        "import sys; import sqlspec.builder; "
        "loaded = [name for name in ('sqlglot.dialects.snowflake', 'sqlglot.dialects.oracle') if name in sys.modules]; "
        "assert not loaded, loaded"
    )
    result = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr


def test_create_temporal_table_registers_version_generators_lazily() -> None:
    """Temporal API calls should register exp.Version rendering before SQL generation."""
    table_expr = create_temporal_table("users", exp.Literal.string("2024-01-01"))

    assert "AS OF TIMESTAMP '2024-01-01'" in normalize_sql(table_expr.sql(dialect="oracle"))


def test_as_of_with_string_alias() -> None:
    query = sql.select("*").from_("users", alias="u", as_of="2024-01-01")
    built = normalize_sql(query.build(dialect="oracle").sql)
    assert built == "SELECT * FROM users AS OF TIMESTAMP '2024-01-01' u"


def test_temporal_registration_does_not_mutate_non_builder_version_rendering() -> None:
    """Non-builder exp.Version expressions should retain default sqlglot rendering before and after temporal joins."""
    code = (
        "from sqlglot import exp; "
        "from sqlspec import sql; "
        "table = exp.Table(this=exp.to_identifier('t'), version=exp.Version(this='SCN', kind='AS OF', expression=exp.Literal.number(42))); "
        "query = exp.select('*').from_(table); "
        "dialects = (None, 'duckdb', 'bigquery', 'oracle', 'snowflake', 'postgres'); "
        "before = {d: query.sql(d) for d in dialects}; "
        "sql.select('*').from_('a').join('b', on='a.id = b.id', as_of='2024-01-01'); "
        "after = {d: query.sql(d) for d in dialects}; "
        "assert before == after, f'{before} != {after}'"
    )
    result = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_non_builder_version_rendering_unaffected() -> None:
    table = exp.Table(
        this=exp.to_identifier("t"), version=exp.Version(this="SCN", kind="AS OF", expression=exp.Literal.number(42))
    )
    query = exp.select("*").from_(table)
    assert query.sql(dialect="duckdb") == "SELECT * FROM t FOR SCN AS OF 42"
    assert query.sql(dialect="bigquery") == "SELECT * FROM t FOR SCN AS OF 42"
    assert query.sql() == "SELECT * FROM t FOR SCN AS OF 42"


def test_temporal_version_cache_key_isolation() -> None:
    """Builder-tagged and untagged exp.Version nodes must not collide in the builder/optimizer cache."""
    raw_table = exp.Table(
        this=exp.to_identifier("users"),
        version=exp.Version(this="TIMESTAMP", kind="AS OF", expression=exp.Literal.string("2024-01-01")),
    )
    raw_built = normalize_sql(sql.select("*").from_(raw_table).build(dialect="duckdb").sql)
    builder_built = normalize_sql(sql.select("*").from_("users", as_of="2024-01-01").build(dialect="duckdb").sql)
    raw_built_again = normalize_sql(sql.select("*").from_(raw_table).build(dialect="duckdb").sql)

    assert raw_built == "SELECT * FROM users FOR TIMESTAMP AS OF '2024-01-01' AS users"
    assert builder_built == "SELECT * FROM users AT (TIMESTAMP => '2024-01-01') AS users"
    assert raw_built_again == raw_built


def test_temporal_registration_preserves_late_imported_dialect_version_sql() -> None:
    """Dialects imported after register_version_generators() must keep their own version_sql."""
    code = (
        "import sys; "
        "from sqlspec import sql; "
        "from sqlspec.builder._temporal import register_version_generators; "
        "assert 'sqlglot.dialects.tsql' not in sys.modules; "
        "register_version_generators(); "
        "built = sql.select('*').from_('t', as_of='2024-01-01').build(dialect='tsql').sql; "
        "assert 'FOR SYSTEM_TIME AS OF' in built, built"
    )
    result = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
