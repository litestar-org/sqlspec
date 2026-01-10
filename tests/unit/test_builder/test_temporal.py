from sqlspec.builder import sql


def normalize_sql(sql_str: str) -> str:
    # Collapse whitespace and strip quotes/backticks
    return " ".join(sql_str.replace('"', "").replace("`", "").split())


def test_as_of_system_time_default() -> None:
    query = sql.select("*").from_("users", as_of="-10s")
    sql_str = query.build().sql
    # Default behavior (CockroachDB style)
    assert normalize_sql(sql_str) == "SELECT * FROM users AS OF SYSTEM TIME '-10s'"


def test_as_of_timestamp_oracle() -> None:
    query = sql.select("*").from_("users", as_of=sql.raw("TIMESTAMP '2023-01-01 00:00:00'"))
    sql_str = query.build(dialect="oracle").sql
    # Oracle uses CAST for typed literals if they are parsed as such
    assert normalize_sql(sql_str) == "SELECT * FROM users AS OF TIMESTAMP CAST('2023-01-01 00:00:00' AS TIMESTAMP)"


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
    assert normalize_sql(sql_str) == "SELECT * FROM users AT (TIMESTAMP => CAST('2023-01-01' AS TIMESTAMP))"


def test_as_of_duckdb() -> None:
    query = sql.select("*").from_("users", as_of=sql.raw("'2023-01-01'"))
    sql_str = query.build(dialect="duckdb").sql
    assert normalize_sql(sql_str) == "SELECT * FROM users AT (TIMESTAMP => '2023-01-01')"


def test_join_as_of() -> None:
    query = (
        sql
        .select("*")
        .from_("orders")
        .join(sql.left_join_("audit_log", alias="log").as_of("-1h").on("orders.id = log.order_id"))
    )
    sql_str = query.build().sql
    normalized = normalize_sql(sql_str)
    assert "LEFT JOIN audit_log AS OF SYSTEM TIME '-1h' AS log" in normalized


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
    assert "LEFT JOIN audit_log FOR SYSTEM_TIME AS OF '-1h' AS log" in normalized


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
    assert "LEFT JOIN audit_log AS OF TIMESTAMP CAST('2023-01-01' AS TIMESTAMP) AS log" in normalized
