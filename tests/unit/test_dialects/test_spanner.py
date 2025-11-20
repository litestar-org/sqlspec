"""Dialect unit tests for the custom Spanner dialect."""

from sqlglot import parse_one

# Ensure dialect registration side effects run
import sqlspec.adapters.spanner  # noqa: F401


def _render(sql: str) -> str:
    return parse_one(sql, dialect="spanner").sql(dialect="spanner")


def test_parse_and_generate_interleave_clause() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table" in rendered


def test_parse_interleave_with_on_delete_cascade() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_parse_ttl_clause_roundtrip() -> None:
    sql = """
    CREATE TABLE orders (
        order_id INT64,
        created_at TIMESTAMP,
        PRIMARY KEY (order_id)
    ) TTL INTERVAL '30 days' ON created_at
    """
    rendered = _render(sql)
    assert "TTL INTERVAL '30 days' ON created_at" in rendered


def test_roundtrip_interleave_and_ttl_together() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        expires_at TIMESTAMP,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
      TTL INTERVAL '7 days' ON expires_at
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION" in rendered
    assert "TTL INTERVAL '7 days' ON expires_at" in rendered
