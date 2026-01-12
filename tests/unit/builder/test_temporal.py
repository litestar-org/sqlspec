import re

from sqlspec.builder import sql


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
    assert "LEFT JOIN audit_log FOR SYSTEM_TIME AS OF '-1h'" in normalized
    assert "log ON orders.id = log.order_id" in normalized


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
