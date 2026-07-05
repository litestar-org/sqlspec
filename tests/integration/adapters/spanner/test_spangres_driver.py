"""Spanner PostgreSQL interface (Spangres) residual coverage.

Runtime Spangres coverage needs a PostgreSQL-dialect Spanner database and
fixtures that this suite does not currently provide. The shared contract matrix
therefore cannot activate Spangres yet; this file keeps only dialect rendering
coverage that can run without those fixtures.
"""

from sqlglot import parse_one


def test_spangres_dialect_sql_generation() -> None:
    """Spangres dialect normalizes row deletion policies to TTL syntax."""
    sql = "CREATE TABLE test (id VARCHAR PRIMARY KEY, ts TIMESTAMP) ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL '30 days'))"
    parsed = parse_one(sql, dialect="spangres")
    rendered = parsed.sql(dialect="spangres")

    assert "ROW DELETION POLICY" not in rendered
    assert "TTL INTERVAL '30 days' ON ts" in rendered
