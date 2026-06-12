"""Dialect unit tests for the Spangres (Spanner PostgreSQL-interface) dialect.

DDL fixtures follow the official Spanner PostgreSQL-dialect grammar: PRIMARY
KEY inside the column list, INTERLEAVE and TTL clauses after the closing paren
without commas, and ``TTL INTERVAL 'n days' ON column`` for row-level TTL.
"""

from sqlglot import exp, parse_one

OFFICIAL_INTERLEAVE_DDL = """
CREATE TABLE albums (
  singer_id     BIGINT,
  album_id      BIGINT,
  album_title   VARCHAR,
  PRIMARY KEY (singer_id, album_id)
)
INTERLEAVE IN PARENT singers ON DELETE CASCADE
"""

OFFICIAL_TTL_DDL = """
CREATE TABLE mytable (
  key BIGINT NOT NULL,
  created_at TIMESTAMPTZ,
  PRIMARY KEY (key)
) TTL INTERVAL '3 days' ON created_at
"""


def _render(sql: str) -> str:
    return parse_one(sql, dialect="spangres").sql(dialect="spangres")


def test_official_interleave_example_parses_and_renders() -> None:
    expression = parse_one(OFFICIAL_INTERLEAVE_DDL, dialect="spangres")
    assert isinstance(expression, exp.Create)
    rendered = expression.sql(dialect="spangres")
    assert "INTERLEAVE IN PARENT singers ON DELETE CASCADE" in rendered
    assert ", INTERLEAVE" not in rendered


def test_interleave_without_parent_keyword() -> None:
    sql = """
    CREATE TABLE resources (
      project_id BIGINT,
      resource_id BIGINT,
      PRIMARY KEY (project_id, resource_id)
    ) INTERLEAVE IN projects
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN projects" in rendered
    assert "PARENT" not in rendered


def test_official_ttl_example_roundtrip() -> None:
    rendered = _render(OFFICIAL_TTL_DDL)
    assert "TTL INTERVAL '3 days' ON created_at" in rendered


def test_googlesql_row_deletion_policy_normalizes_to_ttl() -> None:
    sql = """
    CREATE TABLE events (
        id VARCHAR PRIMARY KEY,
        created_at TIMESTAMP
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))
    """
    rendered = _render(sql)
    assert "ROW DELETION POLICY" not in rendered
    assert "TTL INTERVAL '30 days' ON created_at" in rendered


def test_googlesql_day_interval_normalizes_to_ttl() -> None:
    sql = """
    CREATE TABLE events (
        id VARCHAR PRIMARY KEY,
        created_at TIMESTAMP
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
    """
    rendered = _render(sql)
    assert "TTL INTERVAL '30 days' ON created_at" in rendered


def test_roundtrip_is_stable() -> None:
    for sql in (OFFICIAL_INTERLEAVE_DDL, OFFICIAL_TTL_DDL):
        first = _render(sql)
        assert _render(first) == first


def test_transpile_spangres_ttl_to_googlesql() -> None:
    rendered = parse_one(OFFICIAL_TTL_DDL, dialect="spangres").sql(dialect="spanner")
    assert "ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 3 DAY))" in rendered
    assert "TTL INTERVAL" not in rendered
