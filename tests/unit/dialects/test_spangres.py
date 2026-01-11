"""Dialect unit tests for the Spangres (Spanner PostgreSQL) dialect."""

from sqlglot import parse_one

import sqlspec.adapters.spanner  # noqa: F401


def _render(sql: str) -> str:
    return parse_one(sql, dialect="spangres").sql(dialect="spangres")


def test_row_deletion_policy_roundtrip() -> None:
    sql = """
    CREATE TABLE events (
        id VARCHAR PRIMARY KEY,
        created_at TIMESTAMP
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))
    """
    rendered = _render(sql)
    assert "ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))" in rendered


def test_row_deletion_policy_with_day_literal() -> None:
    sql = """
    CREATE TABLE events (
        id VARCHAR PRIMARY KEY,
        created_at TIMESTAMP
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
    """
    rendered = _render(sql)
    assert "ROW DELETION POLICY" in rendered
    assert "INTERVAL 30" in rendered
